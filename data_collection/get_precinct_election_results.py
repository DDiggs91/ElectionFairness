import os
import re
import time
from urllib.parse import urljoin
import requests

BASE = "https://historical.elections.virginia.gov"
START_ELECTION_ID = 167946  # your starting page
OUTDIR = "data_collection/precinct_election_results"
NEEDED = 4  # VA only has registration data by locality back to 2012 so we only get 4 elections. We could do it by precinct, but then we only get 2 more elections. The data also looks terrible to format
# Registration data was manually taken from https://www.elections.virginia.gov/resultsreports/registration-statistics/ because of the variety of formats
DL_TPL = "/elections/download/{eid}/precincts_include:1/"
JUMP_TPL = "/elections/jump_list/{eid}/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (+https://github.com/psf/requests) VA precinct fetcher"
}


def get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    for attempt in range(5):
        r = session.get(url, headers=HEADERS, timeout=30, **kwargs)
        # retry only for 5xx
        if r.status_code >= 500:
            time.sleep(1.2 * (attempt + 1))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r  # never reached


def parse_jump_list(session: requests.Session, current_id: int):
    """
    Returns (ordered_ids, ordered_labels, selected_id) from the Similar results <select>.
    """
    jump_url = urljoin(BASE, JUMP_TPL.format(eid=current_id))
    r = get(session, jump_url)
    html = r.text

    # Pull all <option value="...">Label</option> in order
    option_pattern = re.compile(
        r'<option[^>]*value="(\d+)"([^>]*)>(.*?)</option>', flags=re.I | re.S
    )
    ids, labels, selected_id = [], [], None
    for val, attrs, label in option_pattern.findall(html):
        eid = int(val)
        text = re.sub(r"\s+", " ", label).strip()
        ids.append(eid)
        labels.append(text)
        if re.search(r"\bselected\b", attrs, flags=re.I):
            selected_id = eid

    if not ids:
        raise RuntimeError("No <option> entries found in Similar results.")
    if selected_id is None:
        # fall back to current_id if not marked selected
        selected_id = current_id if current_id in ids else ids[0]

    return ids, labels, selected_id


def next_after_selected(ids, selected_id):
    """Return the next id after selected_id (no wrap)."""
    try:
        idx = ids.index(selected_id)
    except ValueError:
        idx = -1
    nxt = idx + 1
    if nxt >= len(ids):
        raise StopIteration("Reached the end of the Similar results list.")
    return ids[nxt]


def download_precinct_csv(
    session: requests.Session, election_id: int, label: str, outdir: str
) -> str:
    """
    Downloads the precinct CSV and saves it. Filename is normalized to include President_General if present in the label.
    Returns the saved path.
    """
    os.makedirs(outdir, exist_ok=True)
    dl_url = urljoin(BASE, DL_TPL.format(eid=election_id))
    r = get(session, dl_url, stream=True)

    # Heuristic: ensure we're not saving HTML as CSV (e.g., if auth/redirect)
    ctype = r.headers.get("Content-Type", "")
    if "text/html" in ctype.lower():
        raise RuntimeError(f"Download for {election_id} returned HTML, not CSV.")

    # Build a clean filename
    # Example label: "President/General/2024" → "President_General_2024"
    normalized_label = label.replace("/", "_").replace(" ", "_")
    base_name = f"VA_{normalized_label}_precincts.csv"

    # If server provides a filename, we’ll prefer its extension but keep our prefix
    cd = r.headers.get("Content-Disposition", "")
    ext = ".csv"
    m = re.search(r'filename="?(?P<fn>[^";]+)"?', cd)
    if m:
        _, srv_ext = os.path.splitext(m.group("fn"))
        if srv_ext:
            ext = srv_ext
    fname = os.path.splitext(base_name)[0] + ext

    path = os.path.join(outdir, fname)
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 15):
            if chunk:
                f.write(chunk)
    return path


def main():
    session = requests.Session()

    # Prime the jump list to know the ordered elections and where we are
    ids, labels, selected = parse_jump_list(session, START_ELECTION_ID)

    kept = 0
    saved = []

    current = selected
    # We'll keep stepping "next" until we accumulate NEEDED kept datasets
    while kept < NEEDED:
        # Find the next election (no wrap). For the very first iteration, we want the current (selected)
        # as our first candidate before moving next—because your starting link is a valid item.
        candidate_id = current
        try:
            # Determine the label corresponding to this candidate
            try:
                idx = ids.index(candidate_id)
                label = labels[idx]
            except ValueError:
                # If somehow not present, refresh jump list from this page
                ids, labels, selected = parse_jump_list(session, candidate_id)
                idx = ids.index(selected)
                label = labels[idx]

            print(f"Considering {candidate_id}: {label}")

            # Keep ONLY if label includes 'President/General'
            if "President/General" in label:
                try:
                    path = download_precinct_csv(session, candidate_id, label, OUTDIR)
                    kept += 1
                    saved.append((candidate_id, label, path))
                    print(f"  ✓ KEPT [{kept}/{NEEDED}]: {path}")
                except Exception as e:
                    print(f"  ✗ Skip {candidate_id} (download issue): {e}")
            else:
                print(f"  ✗ Skip {candidate_id} (label not President/General)")

            # Move to the next item in the **same** list (no wrap)
            current = next_after_selected(ids, candidate_id)

        except StopIteration:
            print("No more similar results to traverse.")
            break
        except Exception as e:
            print(f"Error while processing {candidate_id}: {e}")
            # Try to advance anyway; if that also fails we'll bail
            try:
                current = next_after_selected(ids, candidate_id)
            except StopIteration:
                print("No more similar results to traverse.")
                break

        # be polite to the server
        time.sleep(0.6)

    print("\nFinished.")
    for eid, label, p in saved:
        print(f"  {eid} | {label} → {p}")

    if kept < NEEDED:
        print(
            f"\nNote: only {kept} President/General datasets were available in this list."
        )


if __name__ == "__main__":
    main()
