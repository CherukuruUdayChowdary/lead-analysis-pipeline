#!/usr/bin/env python3
"""
Full pipeline, in order:

STEP 1 — DROP ROWS WITH NEITHER WEBSITE NOR PHONE
    Any record missing BOTH Website and Mobile Number is removed first —
    there's nothing to contact them with, so they're dropped outright
    (not deduped, not kept in a separate file).

STEP 2 — CASCADING DEDUPE on what's left, in this exact order:
    1) Mobile Number pass — drop rows that share a phone with a row
       already kept. Rows with no phone are left alone (can't compare).
    2) Website pass         — on what's left after step 1, drop rows that
       share a Website with a row already kept. Rows with no Website are
       left alone.
    3) Mobile + Website pass — on what's left after step 2, drop rows that
       share BOTH a phone AND a website with a row already kept (final
       safety-net check).

    Phone is normalized to digits only (so "+31 76099 75410" and
    "076099-75410" match). Website is normalized (trimmed, lowercased,
    https://, www., and trailing slashes stripped).

Prints counts at every stage in the terminal, then writes the final
result to a new file: final_output.xlsx (in OUTPUT_DIR).
"""

from pathlib import Path
import pandas as pd

# ──────────────────────────────────────────────────────────────
# CONFIG — edit these
# ──────────────────────────────────────────────────────────────
INPUT_FILE = r"C:\Users\Administrator\Documents\LEAD_ANALYSI\cleaning_raw_data\carpetcleaning_output.csv"
OUTPUT_DIR      = r"C:\Users\Administrator\Documents\LEAD_ANALYSI\cleaning_raw_data\AUS_CLEANED_DATA"

WEBSITE_COLUMN  = "Website"
PHONE_COLUMN    = "Mobile Number"

NOT_AVAILABLE_TEXT = "not available"   # matched case-insensitively
# ──────────────────────────────────────────────────────────────


def read_table(path):
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def write_table(df, path):
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        df.to_csv(path, index=False)
    else:
        with pd.ExcelWriter(path, engine="xlsxwriter",
                             engine_kwargs={"options": {"strings_to_urls": False}}) as writer:
            df.to_excel(writer, index=False)


def has_value(series):
    text = series.astype(str).str.strip().str.lower()
    return text.notna() & (text != "") & (text != "nan") & (text != NOT_AVAILABLE_TEXT)


def _normalized_website(df):
    return (
        df[WEBSITE_COLUMN].astype(str).str.strip().str.lower()
        .str.replace(r"^https?://", "", regex=True)
        .str.replace(r"^www\.", "", regex=True)
        .str.rstrip("/")
    )


def _normalized_phone(df):
    return df[PHONE_COLUMN].astype(str).str.replace(r"\D", "", regex=True)  # digits only


def _make_key(df, base_key, missing_mask):
    # Rows missing the field(s) being matched on can't be safely compared,
    # so give each of them its own unique key instead of merging them.
    row_num = pd.Series(range(len(df)), index=df.index).astype(str)
    return base_key.where(~missing_mask, base_key + "||row" + row_num)


def phone_only_key(df):
    missing = ~has_value(df[PHONE_COLUMN])
    return _make_key(df, _normalized_phone(df), missing)


def website_only_key(df):
    missing = ~has_value(df[WEBSITE_COLUMN])
    return _make_key(df, _normalized_website(df), missing)


def website_and_phone_key(df):
    base = _normalized_website(df) + "||" + _normalized_phone(df)
    missing = ~has_value(df[WEBSITE_COLUMN]) | ~has_value(df[PHONE_COLUMN])
    return _make_key(df, base, missing)


def main():
    df = read_table(Path(INPUT_FILE))

    for col in (WEBSITE_COLUMN, PHONE_COLUMN):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found. Available columns: {list(df.columns)}")

    total = len(df)
    print(f"Total records read : {total}")
    print()

    # ---- Step 1: drop rows with neither website nor phone ----
    has_site = has_value(df[WEBSITE_COLUMN])
    has_phone = has_value(df[PHONE_COLUMN])
    neither_count = (~has_site & ~has_phone).sum()
    remaining = df[has_site | has_phone]

    print(f"[Step 1] Dropped records with neither website nor phone : {neither_count}")
    print(f"          Remaining                                     : {len(remaining)}")
    print()

    # ---- Step 2a: Mobile Number pass ----
    key = phone_only_key(remaining)
    removed = len(remaining) - key.nunique()
    remaining = remaining.loc[~key.duplicated(keep="first")]
    print(f"[Step 2a] Mobile Number pass")
    print(f"          Duplicates removed : {removed}")
    print(f"          Remaining          : {len(remaining)}")
    print()

    # ---- Step 2b: Website pass (on what's left after 2a) ----
    key = website_only_key(remaining)
    removed = len(remaining) - key.nunique()
    remaining = remaining.loc[~key.duplicated(keep="first")]
    print(f"[Step 2b] Website pass (on remainder after step 2a)")
    print(f"          Duplicates removed : {removed}")
    print(f"          Remaining          : {len(remaining)}")
    print()

    # ---- Step 2c: Mobile + Website pass (on what's left after 2b) ----
    key = website_and_phone_key(remaining)
    removed = len(remaining) - key.nunique()
    remaining = remaining.loc[~key.duplicated(keep="first")]
    print(f"[Step 2c] Mobile Number + Website pass (on remainder after step 2b)")
    print(f"          Duplicates removed : {removed}")
    print(f"          Remaining          : {len(remaining)}")
    print()

    print(f"FINAL: {len(remaining)} records out of {total} "
          f"({total - len(remaining)} removed in total: "
          f"{neither_count} with no contact info + "
          f"{total - neither_count - len(remaining)} duplicates)")
    print()

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = Path(INPUT_FILE).suffix.lower() if Path(INPUT_FILE).suffix.lower() == ".csv" else ".xlsx"
    out_path = out_dir / f"final_output{ext}"
    write_table(remaining, out_path)
    print(f"Wrote {len(remaining)} records -> {out_path}")


if __name__ == "__main__":
    main()