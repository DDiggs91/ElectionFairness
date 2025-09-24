import os
import re
import pandas as pd

INPUT_XL = "data_collection/registrant_counts/2012.xls"
OUTPUT_CSV = "data_collection/registrant_counts/2012.csv"

# Match the header cell for the precinct number column
PRECINCT_NO_HEADER = re.compile(r"\bprecinct\s*(?:no\.?|number|#)\b", re.I)


def _norm_cell(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return re.sub(r"\s+", " ", s)


def _find_precinct_no_header(df_nohdr: pd.DataFrame, max_scan_rows: int = 60):
    """
    Find (header_row_index, precinct_no_col_index) by scanning top rows for a header
    that matches PRECINCT_NO_HEADER. Returns (row_idx, col_idx).
    """
    norm = df_nohdr.map(_norm_cell)
    scan = min(max_scan_rows, len(norm))
    for r in range(scan):
        for c in range(norm.shape[1]):
            if PRECINCT_NO_HEADER.search(norm.iat[r, c]):
                return r, c
    raise ValueError("Could not find a 'Precinct No' header within the first rows.")


def _clean_int_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace(r"[^\d\-]", "", regex=True)  # remove commas, spaces, etc.
        .replace({"": pd.NA, "-": pd.NA})
        .astype("Int64")
    )


def extract_sheet(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Given a sheet (read with header=None), locate the Precinct No column header,
    then use column deltas +1 (Name), +2 (Active), +3 (Inactive).
    """
    hdr_row, col_pno = _find_precinct_no_header(df_raw)

    # Data starts after header row
    data = df_raw.iloc[hdr_row + 1 :].copy()

    # Column indices by delta
    col_name = col_pno + 1
    col_active = col_pno + 5
    col_inactive = col_pno + 6

    # Safely select columns (in case a sheet is short)
    def safe_col(idx):
        return (
            data.iloc[:, idx] if idx < data.shape[1] else pd.Series([pd.NA] * len(data))
        )

    precinct_no = safe_col(col_pno).map(_norm_cell)
    precinct_name = safe_col(col_name).map(
        lambda x: str(x).strip() if not pd.isna(x) else x
    )
    active = safe_col(col_active)
    inactive = safe_col(col_inactive)

    out = pd.DataFrame(
        {
            "Precinct No.": precinct_no,
            "Precinct Name": precinct_name,
            "Active": active,
            "Inactive": inactive,
        }
    )

    # Keep rows with at least a precinct_no or name and some numeric content
    mask_keep = (out["Precinct No."].astype(str).str.len() > 0) | (
        out["Precinct Name"].notna()
    )
    out = out[mask_keep].copy()

    # Clean numerics
    out["Active"] = _clean_int_series(out["Active"])
    out["Inactive"] = _clean_int_series(out["Inactive"])

    # Drop rows that are totally empty after cleanup
    out = out[
        ~(
            out["Precinct No."].eq("")
            & out["Precinct Name"].isna()
            & out["Active"].isna()
            & out["Inactive"].isna()
        )
    ]

    return out.reset_index(drop=True)


def extract_all_sheets(xl_path: str) -> pd.DataFrame:
    engine = "xlrd" if xl_path.lower().endswith(".xls") else None
    xls = pd.ExcelFile(xl_path, engine=engine)

    dfs = []
    for sheet in xls.sheet_names:
        try:
            df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
            df_part = extract_sheet(df_raw)
            if not df_part.empty:
                dfs.append(df_part)
                print(f"✓ {sheet}: {len(df_part)} rows")
            else:
                print(f"… {sheet}: no rows")
        except Exception as e:
            print(f"! {sheet}: {e}")

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame(columns=["Precinct No.", "Precinct Name", "Active", "Inactive"])


if __name__ == "__main__":
    df = extract_all_sheets(INPUT_XL)
    print(df.head(10))

    os.makedirs(os.path.dirname(OUTPUT_CSV) or ".", exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {OUTPUT_CSV}")
