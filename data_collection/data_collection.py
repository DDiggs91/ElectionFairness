import os
import re
import time
from urllib.parse import urljoin

import requests

BASE = "https://historical.elections.virginia.gov"
START_ELECTION_ID = 167946  # 2024 President General (your starting link)
OUTDIR = "data_collection/va_precincts"
N_FILES = 10

# Endpoints:
#  - Precinct CSV download: /elections/download/{id}/precincts_include:1/
#  - "Similar results" list (HTML): /elections/jump_list/{id}/
DL_TPL = "/elections/download/{eid}/precincts_include:1/"
JUMP_TPL = "/elections/jump_list/{eid}/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (+https://github.com/requests/requests) Python script for research"
}


def get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """GET with basic retry for transient 5xx, polite delay."""
    for attempt in range(5):
        r = session.get(url, headers=HEADERS, timeout=30, **kwargs)
        if r.status_code >= 500:
            time.sleep(1.5 * (attempt + 1))
            continue
        return r
    r.raise_for_status()
    return r


def download_precinct_csv(
    session: requests.Session, election_id: int, outdir: str
) -> str:
    os.makedirs(outdir, exist_ok=True)
    dl_url = urljoin(BASE, DL_TPL.format(eid=election_id))
    r = get(session, dl_url, stream=True)
    if r.status_code != 200:
        raise RuntimeError(f"Download failed for {election_id}: HTTP {r.status_code}")

    # Try to get a decent filename; fall back to <id>_precincts.csv
    fname = f"{election_id}_precincts.csv"
    cd = r.headers.get("Content-Disposition")
    if cd:
        m = re.search(r'filename="?([^";]+)"?', cd)
        if m:
            fname = m.group(1)
            # Ensure unique per election id if server reuses names
            root, ext = os.path.splitext(fname)
            fname = f"{root}_{election_id}{ext or '.csv'}"

    path = os.path.join(outdir, fname)
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 15):
            if chunk:
                f.write(chunk)
    return path


def next_similar_election_id(session: requests.Session, current_id: int) -> int:
    """
    Fetch the 'Similar results' jump list for current_id, find all /elections/view/{id}/
    entries, and return the one immediately after current_id in that list.
    If current_id isn't present, return the first in the list.
    """
    jump_url = urljoin(BASE, JUMP_TPL.format(eid=current_id))
    r = get(session, jump_url)
    if r.status_code != 200:
        raise RuntimeError(
            f"Jump list fetch failed for {current_id}: HTTP {r.status_code}"
        )

    # Extract every elections/view/{id}/ in order
    ids = [int(x) for x in re.findall(r"/elections/view/(\d+)/", r.text)]
    # De-duplicate while preserving order
    seen = set()
    ordered = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            ordered.append(x)

    if not ordered:
        raise RuntimeError(f"No similar results found for {current_id}")

    # Find current index and choose the next one (wrap if needed)
    try:
        idx = ordered.index(current_id)
        nxt = ordered[(idx + 1) % len(ordered)]
    except ValueError:
        # If current not listed, just take the first item
        nxt = ordered[0]
    return nxt


def main():
    session = requests.Session()
    current = START_ELECTION_ID
    saved = []

    for i in range(N_FILES):
        print(f"[{i+1}/{N_FILES}] Downloading precinct CSV for election {current} …")
        path = download_precinct_csv(session, current, OUTDIR)
        print(f"  → saved: {path}")
        saved.append((current, path))

        # Move to the next election via "Similar results"
        try:
            nxt = next_similar_election_id(session, current)
        except Exception as e:
            print(f"Could not locate next 'Similar results' from {current}: {e}")
            break

        # Be polite to the server
        time.sleep(0.8)
        current = nxt

    print("\nDone. Files saved:")
    for eid, p in saved:
        print(f"  {eid}: {p}")


if __name__ == "__main__":
    main()
