# Election Fairness — Virginia Gerrymandering MVP

## Context

The goal is a system for investigating possible gerrymandering outcomes, starting with Virginia only, but built so a second state is a config change, not a rewrite. The repo currently has only a Maven parent scaffold (`com.diggs:election-fairness-parent`, packaging `pom`, Java 25, Spring Boot 4.1.0, Lombok, TestNG, google-java-format) with no child modules yet — this plan adds those modules.

Three scope decisions were confirmed with the user before this plan was written:
1. **Persistence**: file-based/in-memory, no database. A state's block-level dataset (~285k blocks for Virginia) is small enough (~200–500MB in memory, ~100MB on disk) to load into a JVM directly — no Postgres/PostGIS needed for MVP.
2. **Web output**: a minimal server-rendered HTML page with an inline SVG map, no CSS framework/JS/map library.
3. **Scope framing** (user's own words): *"Given a file that maps our smallest division to district, show the map."* This means the web controller only renders a precomputed block→district assignment file — it never runs optimization itself. Optimization is a separate offline step that produces plan files.

**Why "smaller than precinct" requires a derived dataset, not a direct one**: Census geography is Block < Block Group < Tract < County < State — the Block is the smallest unit that exists, period. But actual votes are only ever tallied at the precinct (Census calls this a "Voting District"/VTD) level — no election authority records votes at the block level, because that isn't how ballots are administered. So block-level election data is inherently a **derived/estimated** dataset: precinct results disaggregated down to blocks, weighted by population. This is standard, well-established redistricting-industry practice, not something we need to invent: the **Redistricting Data Hub** (a national, nonpartisan clearinghouse — not VA-specific, which fits "prefer national datasets") already publishes exactly this for every state, including **"Virginia 2024 General Election Results Disaggregated to the 2020 Census Block"**. We consume that rather than re-deriving disaggregation ourselves for MVP.

## Data sources (national, parameterized by state FIPS code)

| Source | What | Access |
|---|---|---|
| Census TIGER/Line | Block boundary geometry (`tl_2020_{fips}_tabblock20.zip`) | Free direct download, no auth, e.g. `www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/` |
| Census PL 94-171 API | Block-level population / voting-age population | Free, scriptable via `api.census.gov/data/2020/dec/pl` (block-level queries likely need per-county looping, not a statewide wildcard — confirm at implementation time) |
| Redistricting Data Hub | Block-disaggregated 2024 election results (votes by party) | **Manual step**: free account registration required, no stable scripted API — download once per state into a staging folder |

All three are keyed by state FIPS code (Virginia = `51`), not Virginia-specific formats — this is what makes the pipeline reusable.

## Architecture: 4 Maven modules

```
        pipeline        algorithm        web
            \               |             /
             \              |            /
              +---------- model --------+
```

`web` has no dependency edge to `algorithm` or `pipeline` — it structurally cannot run optimization, which directly enforces the "dumb renderer" framing.

| Module | Artifact ID | Responsibility |
|---|---|---|
| `model` | `election-fairness-model` | Domain records (`Block`, `StateConfig`, `PlanMetrics`, etc.) plus shared readers/writers (`model.io`) for every file format that crosses a module boundary, so producer and consumer of a file always agree on its shape. Depends only on JTS, Jackson, commons-csv — no Spring. |
| `pipeline` | `election-fairness-pipeline` | ETL: fetch/parse raw Census + RDH data, reproject, join by GEOID, build block adjacency graph, simplify geometry, write the per-state base dataset. Plain `main()`. Needs GeoTools (heavy: shapefile I/O + CRS reprojection) — kept isolated here so nothing else pays for it. Runs rarely (once per state onboarding). |
| `algorithm` | `election-fairness-algorithm` | Loads the base dataset, partitions it into districts under a swappable objective (compact baseline / maximize-party-R / maximize-party-D), computes fairness metrics, dissolves block geometry into district shapes, writes plan files. Plain `main()`. Needs JTS + JGraphT (light). Runs often (every new objective/seed/iteration count). |
| `web` | `election-fairness-web` | The one Spring Boot app. Single controller reads a plan's small output files and renders server-side HTML + inline SVG. Never touches block-level data. |

Reasoning for keeping `pipeline` and `algorithm` as two modules rather than one: different dependency weight (GeoTools vs. not) and different re-run cadence (rare vs. frequent) — merging them would make every algorithm-tuning iteration pay GeoTools's build/startup cost for no reason.

## StateConfig — the reusability mechanism

A hand-authored, git-committed YAML file (not compiled in) holds per-state config; adding a state later is an edit to this file, not a code change:

**`data/config/state-configs.yaml`** (committed):
```yaml
states:
  - stateAbbreviation: VA
    stateFips: "51"
    rdhStateSlug: virginia          # confirm exact slug at first real RDH download
    censusVintage: 2020
    electionYear: 2024
    defaultChamber: CONGRESS
    chambers:
      - chamberId: CONGRESS
        districtCount: 11
        populationDeviationTolerance: 0.01   # congressional: case law wants ~exact equality
      - chamberId: SENATE
        districtCount: 40
        populationDeviationTolerance: 0.05   # state legislative: looser statutory tolerance
      - chamberId: HOUSE_OF_DELEGATES
        districtCount: 100
        populationDeviationTolerance: 0.05
    rawDataPaths:
      tigerBlockShapefileZip: data/raw/va/tiger/tl_2020_51_tabblock20.zip
      rdhBlockElectionCsv: data/raw/va/rdh/va_2024_gen_block.csv
    processedDataRoot: data/processed/va
```
MVP default chamber is `CONGRESS` (11 districts — smallest count, most legible map/story). `model.io.StateConfigRegistry` loads this into `Map<String, StateConfig>`.

## `data/` layout

```
data/
  config/state-configs.yaml                          # COMMITTED
  raw/{state}/tiger/tl_2020_{fips}_tabblock20.zip     # gitignored
  raw/{state}/census-pl94171/...                      # gitignored
  raw/{state}/rdh/{state}_2024_gen_block.csv          # gitignored — manual download, RDH terms of use
  processed/{state}/manifest.json                     # gitignored, regeneratable
  processed/{state}/blocks.geojson                    # gitignored
  processed/{state}/adjacency.csv                      # gitignored
  processed/{state}/plans/{planName}/assignments.csv  # gitignored
  processed/{state}/plans/{planName}/districts.geojson # gitignored
  processed/{state}/plans/{planName}/metrics.json     # gitignored
```
Add `data/raw/` and `data/processed/` to `.gitignore`; `data/config/` stays committed.

## `pipeline` module — stages in order

1. **Fetch/stage raw inputs** (`pipeline.fetch`): `TigerShapefileFetcher` downloads the block shapefile zip (idempotent). `CensusPl94171Client` calls the Census API per county via `java.net.http.HttpClient` (no extra dependency), caches raw JSON. `RdhStagingValidator` doesn't fetch anything — it checks the manually-downloaded RDH CSV exists and fails fast with the dataset name + URL if not.
2. **Parse + reproject + join by GEOID** (`pipeline.parse/reproject/join`): `TigerShapefileReader` (GeoTools) reads geometry + `GEOID20`. `CensusPl94171Parser` and `RdhBlockElectionCsvParser` parse population and vote counts. `CrsReprojector` (GeoTools) reprojects into a single planar CRS for Virginia (confirm exact EPSG at implementation time — a VA State Plane zone or state-centered Lambert Conformal Conic). `BlockDatasetJoiner` joins by GEOID; a block present in TIGER/Census but absent from the RDH file (unpopulated blocks) defaults votes to zero rather than erroring.
3. **Build block adjacency graph** (`pipeline.adjacency`): `BlockAdjacencyBuilder` uses a JTS `STRtree` spatial index (avoids O(n²) over ~285k blocks), then applies **rook contiguity** (shared boundary segment, not just a touching corner). Known edge case: a few Virginia blocks are genuinely disjoint islands (e.g. Tangier Island) — these need an explicit nearest-centroid fallback attachment rather than being left with zero adjacency, which would make them un-assignable.
4. **Simplify geometry** (`pipeline.simplify`): `BlockGeometrySimplifier` (JTS `TopologyPreservingSimplifier`) trims vertex count for storage/render size. Note: simplifying each block independently can desync a shared edge between neighbors, leaving slivers when later dissolved — handled downstream in `algorithm` via a buffer(ε)/buffer(-ε) pass after dissolve, not here.
5. **Serialize** (`pipeline.write`): `BaseDatasetWriter` writes `manifest.json`, `blocks.geojson`, `adjacency.csv` via `model.io`.

## `algorithm` module

`AlgorithmMain` args: state, chamber, plan name, objective, `--seed`, `--iterations`. One invocation produces one named plan; get `baseline`/`gerrymander-r`/`gerrymander-d` by invoking it three times (a small shell script, not a new abstraction).

- **Graph loading**: `AdjacencyGraphLoader` builds a JGraphT graph over a dense `int` GEOID index (not string-keyed) — local search does millions of neighbor lookups, so this matters for performance.
- **Initial plan**: `SeedSelector` picks well-spread seed blocks (farthest-point sampling); `RegionGrowingSeeder` does population-weighted BFS flood-fill from all seeds simultaneously toward `idealPopulation = totalPopulation / districtCount`. Contiguous by construction.
- **Local search** (`LocalSearchOptimizer`): repeatedly picks a boundary block, proposes moving it to a neighboring district, rejects the move if it would break contiguity (`ContiguityValidator`: scoped BFS restricted to the affected district only, with an O(1) fast path when the block is a leaf) or break population tolerance, otherwise scores it via the pluggable objective and accepts if it improves (hill-climbing first; layer simulated annealing's probabilistic acceptance on top once hill-climbing works).
- **Swappable objective** (`algorithm.objective.PlanObjective`): `CompactnessObjective` (minimize cut-edge count — a standard cheap compactness proxy) for `baseline`; `PartisanAdvantageObjective(targetParty)` (maximize districts where target party's running vote total exceeds 50%) for `gerrymander-r`/`gerrymander-d`. Deliberately optimizes for seats, not directly for efficiency gap — packing/cracking emerges naturally, and fairness metrics are computed afterward as diagnostics on the result.
- **Fairness metrics** (`PlanMetricsCalculator`, pure functions, no geometry): efficiency gap, mean-median difference, partisan bias (uniform-swing method) — see Verification section for worked examples used as test fixtures.
- **Dissolve + render-prep** (`algorithm.render`): `DistrictDissolver` uses JTS `CascadedPolygonUnion` per district over member blocks, followed by a buffer(ε)/buffer(-ε) pass to close simplification-induced slivers, then a second (coarser) simplification pass tuned for map-scale rendering. This happens **here, offline, once per plan** — not in `web` — because dissolving up to 285k block polygons into ~11 district polygons is a real cost, and paying it once at plan-write time (batch process, already dominated by local-search runtime) is strictly better than paying it at every web request or app startup.
- **Output**: `PlanWriter` writes `assignments.csv`, `districts.geojson`, `metrics.json`.

## `web` module

Single controller, `GET /states/{state}/plans/{planName}` → renders HTML + inline SVG (`/` redirects to a configured default plan). Loads nothing at startup; reads the small `districts.geojson` + `metrics.json` for the requested plan fresh per request (kilobytes, not worth caching for MVP — a `PlanRenderDataLoader` interface keeps that swappable later without touching the controller).

`districts.geojson` stays real GeoJSON in the pipeline's projected CRS (so it's still openable/debuggable in QGIS) — the only work left for `web` is trivial linear scaling of the bounding box into an SVG viewBox plus a Y-axis flip (`ViewBoxProjector`, `SvgPathFormatter`), not real geometry processing. `web` therefore never needs GeoTools, and doesn't call JTS directly either.

```
com.diggs.electionfairness.web.WebApplication
com.diggs.electionfairness.web.controller.MapController      // GET /states/{state}/plans/{planName}
com.diggs.electionfairness.web.service.PlanRenderDataLoader   // reads districts.geojson + metrics.json
com.diggs.electionfairness.web.svg.ViewBoxProjector           // bbox -> px, with Y-flip
com.diggs.electionfairness.web.svg.SvgPathFormatter
src/main/resources/templates/map.html                          // Thymeleaf: th:each over districts + inline <svg>, inline <style>
```
Thymeleaf (`spring-boot-starter-thymeleaf`) over hand-built HTML strings — one extra starter, standard Spring choice, cleanly separates template from path-building logic, and leaves a natural extension point if real UI happens later. No CSS framework, no JS.

## File formats (concrete examples)

**Block→district assignment — plain CSV, not JSON.** It's a flat 1:1 mapping (JSON nesting buys nothing and repeats keys ~285,000 times); it's diffable/inspectable in git or Excel, matching the user's own phrasing ("a file that maps... to district"); and it mirrors the Census Bureau's own official "Block Assignment File" format used in real redistricting submissions.
```csv
geoid,district_id
510199501001000,7
510199501001001,7
```

`metrics.json`:
```json
{
  "state": "VA", "plan": "gerrymander-r", "chamber": "CONGRESS",
  "statewide": { "seatsDem": 4, "seatsRep": 7, "efficiencyGap": 0.146, "meanMedianDem": -0.038 },
  "districts": [ { "districtId": 1, "population": 789310, "votesDem": 210433, "votesRep": 165221, "winner": "DEM" } ]
}
```
`blocks.geojson` / `districts.geojson` are standard GeoJSON FeatureCollections (Polygon geometry + a properties bag of population/votes/districtId). `adjacency.csv` is `geoid_a,geoid_b`. `manifest.json` records source URLs, block count, build timestamp — supports "as repeatable as possible" by making provenance explicit.

## Dependencies to add (confirm exact current patch versions at implementation time)

| Library | Used by | Notes |
|---|---|---|
| `org.locationtech.jts:jts-core` | model, pipeline, algorithm | geometry |
| GeoTools (`gt-shapefile`, `gt-referencing`, `gt-epsg-hsql`, `gt-main`) | pipeline only | needs the OSGeo Maven repo (`repo.osgeo.org/repository/release/`) added — not fully on Central |
| `org.jgrapht:jgrapht-core` | algorithm only | adjacency graph |
| `org.apache.commons:commons-csv` | model | CSV read/write |
| Jackson databind + YAML | model | GeoJSON/JSON + `state-configs.yaml` |
| `spring-boot-starter-web`, `-thymeleaf` | web only | via BOM import of `spring-boot-dependencies:${spring-boot.version}` |
| `org.codehaus.mojo:exec-maven-plugin` | pipeline, algorithm | dev-loop execution (`mvn -pl pipeline exec:java -Dexec.args=VA`) — sufficient for MVP; skip maven-shade-plugin/fat-jars until a portable artifact is actually needed |

Parent POM needs a `<modules>` section listing all four, plus the version properties/dependencyManagement entries above. Lombok/TestNG/google-java-format are already inherited from the parent — no per-module setup needed.

## Explicitly out of scope for MVP

No database/docker-compose. No JS/CSS frontend framework (leave `frontend-maven-plugin`/Node dormant). No multi-state data beyond Virginia in `state-configs.yaml` (the abstraction supports it; the data doesn't exist yet). No ReCom/Markov-chain ensemble algorithm (documented future enhancement — no off-the-shelf Java equivalent exists; region-growing + local search is the buildable MVP substitute). No auth. No comparison-against-real-current-district-lines feature. No automated RDH scraping (manual download + fail-fast validation instead). No web-layer caching.

## Verification

1. **Unit tests** (TestNG, matching repo convention — not JUnit), runnable with no real data: `mvn test` across all modules.
   - `pipeline`: adjacency rook-vs-queen on a hand-built 3×3 grid of unit squares (edge-sharing = adjacent, corner-only = not).
   - `algorithm`: `ContiguityValidator` on a small "bridge" graph (removing a bridge node rejected, removing a leaf accepted); `PlanMetricsCalculator` against hand-computed examples, e.g. 4 districts of 1000 votes with Dem/Rep splits (600,400)/(550,450)/(300,700)/(350,650) → efficiency gap = 0.10 (verified by hand).
   - `web`: `SvgPathFormatter`/`ViewBoxProjector` as plain unit tests; one `@SpringBootTest` smoke test against a fixture plan directory asserting `GET /states/va/plans/baseline` returns 200 with `<svg` in the body.
2. **Real end-to-end run** (requires the one manual RDH download step first):
   - `mvn -pl pipeline exec:java -Dexec.args=VA` → confirm `data/processed/va/blocks.geojson` + `adjacency.csv` + `manifest.json` exist, and `manifest.json`'s block count is in the expected ~285k range.
   - Run `algorithm` three times (`baseline`, `gerrymander-r`, `gerrymander-d`) → confirm each `metrics.json` shows exactly 11 districts, statewide population matching `manifest.json`, and every district within its configured population tolerance.
   - Start `web`, open `/states/va/plans/baseline`, `/states/va/plans/gerrymander-r`, `/states/va/plans/gerrymander-d` in a browser → confirm each renders an 11-district colored SVG map and a metrics table, and that the seat split visibly differs between the three plans (that difference, from identical population/vote data, is the actual point of the project).

## Representative new files (per module — pattern repeats, not exhaustive)

- `/workspaces/ElectionFairness/pom.xml` (add `<modules>`, dependency versions)
- `/workspaces/ElectionFairness/model/src/main/java/com/diggs/electionfairness/model/StateConfig.java`
- `/workspaces/ElectionFairness/model/src/main/java/com/diggs/electionfairness/model/io/` (shared readers/writers)
- `/workspaces/ElectionFairness/pipeline/src/main/java/com/diggs/electionfairness/pipeline/join/BlockDatasetJoiner.java`
- `/workspaces/ElectionFairness/pipeline/src/main/java/com/diggs/electionfairness/pipeline/adjacency/BlockAdjacencyBuilder.java`
- `/workspaces/ElectionFairness/algorithm/src/main/java/com/diggs/electionfairness/algorithm/partition/LocalSearchOptimizer.java`
- `/workspaces/ElectionFairness/algorithm/src/main/java/com/diggs/electionfairness/algorithm/metrics/PlanMetricsCalculator.java`
- `/workspaces/ElectionFairness/web/src/main/java/com/diggs/electionfairness/web/controller/MapController.java`
- `/workspaces/ElectionFairness/web/src/main/resources/templates/map.html`
- `/workspaces/ElectionFairness/data/config/state-configs.yaml`
- `/workspaces/ElectionFairness/.gitignore` (add `data/raw/`, `data/processed/`)
