"""
Universal Website Status Checker
================================
Accepts common input formats automatically:
    .xlsx, .xls, .csv, .tsv, .txt, .json

The output is written in the SAME FORMAT as the input.

Examples:
    input.xlsx  -> checked_input.xlsx
    input.csv   -> checked_input.csv
    input.tsv   -> checked_input.tsv
    input.txt   -> checked_input.txt
    input.json  -> checked_input.json

Features
--------
- Automatically detects the website/URL column.
- Keeps ALL original input columns.
- Adds: Status Code, Status Note, Available.
- Automatically adds https:// when a URL has no scheme.
- Uses HEAD first and GET as fallback for sites that reject HEAD.
- Retries failed requests.
- Saves progress periodically.
- Resume support after Ctrl+C/crash.
- Does not require you to change the code when the input format changes.
- Output format follows the input format.

IMPORTANT
---------
For .xls input, install xlrd:
    pip install xlrd

For Excel output, pandas/openpyxl are required:
    pip install pandas openpyxl requests

For .xls output, the script uses xlwt:
    pip install xlwt
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import random
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests


# ============================================================
# SETTINGS
# ============================================================

# Put the input file here.
# The extension determines the format automatically.
INPUT_FILE = r"C:\Users\Administrator\Documents\LEAD_ANALYSI\cleaning_raw_data\AUS_CLEANED_DATA\final_output.csv"

# Leave as None to automatically create the output beside the input.
# Example:
# final_output.csv -> final_output_WEBSITE_CHECKED.csv
#
# You can also give a full output path, but the extension MUST match
# the input format if you want the "same format" behavior.
OUTPUT_FILE = None

# Leave as None to automatically detect the website column.
# You can set a specific column name if automatic detection is not suitable.
COLUMN_NAME = None

# Excel sheet to read. 0 = first sheet.
SHEET = 0

TIMEOUT_SECONDS = 10
MAX_WORKERS = 30
RETRY_COUNT = 1
SAVE_EVERY = 10

MIN_DELAY_SECONDS = 0.1
MAX_DELAY_SECONDS = 0.6

# ============================================================
# USER AGENTS
# ============================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",

    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) "
    "Gecko/20100101 Firefox/126.0",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",

    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",

    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",

    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",

    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

save_lock = threading.Lock()


# ============================================================
# FORMAT HELPERS
# ============================================================

SUPPORTED_FORMATS = {
    ".xlsx",
    ".xls",
    ".csv",
    ".tsv",
    ".txt",
    ".json",
}


def get_extension(path: Path) -> str:
    return path.suffix.lower()


def validate_input_format(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist:\n{path}")

    ext = get_extension(path)

    if ext not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported input format: {ext or '(no extension)'}\n"
            f"Supported formats: {', '.join(sorted(SUPPORTED_FORMATS))}"
        )


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(
        f"{input_path.stem}_WEBSITE_CHECKED{input_path.suffix}"
    )


def read_table(path: Path, sheet=SHEET) -> pd.DataFrame:
    """
    Read the input automatically according to its extension.
    """
    validate_input_format(path)

    ext = get_extension(path)

    print(f"Reading {path} ...")

    if ext == ".xlsx":
        return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")

    if ext == ".xls":
        return pd.read_excel(path, sheet_name=sheet, engine="xlrd")

    if ext == ".csv":
        return pd.read_csv(path, dtype=object)

    if ext == ".tsv":
        return pd.read_csv(path, sep="\t", dtype=object)

    if ext == ".txt":
        # Automatically try tab first, then semicolon, then comma.
        for sep in ("\t", ";", ","):
            try:
                df = pd.read_csv(path, sep=sep, dtype=object)
                if len(df.columns) > 1:
                    return df
            except Exception:
                pass

        # Last fallback: one-column text file.
        with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
            lines = [line.rstrip("\r\n") for line in f]

        return pd.DataFrame({"Website": lines})

    if ext == ".json":
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)

        if isinstance(data, list):
            return pd.DataFrame(data)

        if isinstance(data, dict):
            # Normal table JSON:
            # {"records": [...]} or {"data": [...]}
            for key in ("records", "data", "results", "items"):
                if key in data and isinstance(data[key], list):
                    return pd.DataFrame(data[key])

            # Single record JSON.
            return pd.DataFrame([data])

    raise ValueError(f"Could not read input format: {ext}")


def write_table(df: pd.DataFrame, path: Path) -> None:
    """
    Write the dataframe automatically in the SAME FORMAT as the input.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    ext = get_extension(path)

    if ext == ".xlsx":
        df.to_excel(path, index=False, engine="openpyxl")
        return

    if ext == ".xls":
        try:
            df.to_excel(path, index=False, engine="xlwt")
        except ImportError:
            raise RuntimeError(
                "Writing .xls requires xlwt.\n"
                "Install it with: pip install xlwt"
            )
        return

    if ext == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return

    if ext == ".tsv":
        df.to_csv(path, index=False, sep="\t", encoding="utf-8-sig")
        return

    if ext == ".txt":
        # Semicolon-separated output for TXT.
        df.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
        return

    if ext == ".json":
        df.to_json(
            path,
            orient="records",
            force_ascii=False,
            indent=2,
        )
        return

    raise ValueError(f"Unsupported output format: {ext}")


def atomic_write_table(df: pd.DataFrame, final_path: Path, max_retries: int = 5):
    """
    Write to a temporary file first, then replace the destination.
    This reduces the chance of leaving a half-written output file.
    """
    final_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = final_path.with_name(
        f"{final_path.stem}.tmp{final_path.suffix}"
    )

    # Remove stale temp file if one exists.
    try:
        if tmp_path.exists():
            tmp_path.unlink()
    except OSError:
        pass

    write_table(df, tmp_path)

    delay = 0.3

    for attempt in range(max_retries):
        try:
            os.replace(tmp_path, final_path)
            return
        except PermissionError:
            if attempt == max_retries - 1:
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise

            time.sleep(delay)
            delay = min(delay * 2, 3.0)


# ============================================================
# COLUMN DETECTION
# ============================================================

def auto_detect_column(columns):
    """
    Find a likely website/URL column automatically.
    """
    candidates = [
        "website",
        "web site",
        "website url",
        "website_url",
        "url",
        "urls",
        "link",
        "links",
        "site",
        "web",
        "domain",
        "domain name",
        "company website",
        "company url",
    ]

    # Exact match first.
    for col in columns:
        normalized = str(col).strip().lower()
        if normalized in candidates:
            return col

    # Partial match second.
    for col in columns:
        normalized = str(col).strip().lower()
        for candidate in candidates:
            if candidate in normalized:
                return col

    # Last attempt: inspect values and choose the column with the
    # largest percentage of values that look like URLs/domains.
    best_col = None
    best_score = 0.0

    return None


def detect_url_column_from_dataframe(df: pd.DataFrame):
    """
    More reliable automatic URL-column detection based on both
    column names and actual cell values.
    """
    name_match = auto_detect_column(df.columns)
    if name_match is not None:
        return name_match

    best_col = None
    best_score = 0.0

    sample_size = min(len(df), 200)

    if sample_size == 0:
        return None

    for col in df.columns:
        sample = df[col].dropna().astype(str).head(sample_size)

        if sample.empty:
            continue

        valid = 0

        for value in sample:
            value = value.strip()

            if not value:
                continue

            test_value = value
            if not test_value.lower().startswith(
                ("http://", "https://", "www.")
            ):
                test_value = "https://" + test_value

            try:
                parsed = urlparse(test_value)

                if parsed.netloc and "." in parsed.netloc:
                    valid += 1
            except Exception:
                pass

        score = valid / len(sample)

        if score > best_score:
            best_score = score
            best_col = col

    if best_score >= 0.30:
        return best_col

    return None


# ============================================================
# URL CHECKING
# ============================================================

def normalize_url(url) -> str:
    """
    Clean URL and add https:// if necessary.
    """
    if pd.isna(url):
        return ""

    url = str(url).strip()

    if not url or url.lower() in {"nan", "none", "null"}:
        return ""

    # Remove surrounding quotes accidentally present in CSV/TXT files.
    url = url.strip("\"'")

    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url

    return url


def status_note(status_code) -> str:
    if status_code is None:
        return "No Response"

    try:
        code = int(status_code)
    except (TypeError, ValueError):
        return "Error"

    notes = {
        200: "OK",
        201: "Created",
        202: "Accepted",
        204: "No Content",
        301: "Moved Permanently",
        302: "Found",
        303: "See Other",
        304: "Not Modified",
        307: "Temporary Redirect",
        308: "Permanent Redirect",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        405: "Method Not Allowed",
        408: "Request Timeout",
        409: "Conflict",
        410: "Gone",
        429: "Too Many Requests",
        500: "Internal Server Error",
        501: "Not Implemented",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout",
    }

    return notes.get(code, f"HTTP {code}")


def check_one_url(url):
    """
    Return:
        status_code, status_note, available
    """
    clean_url = normalize_url(url)

    if not clean_url:
        return None, "No URL", "No"

    attempts = RETRY_COUNT + 1
    last_error_note = "Error"

    for attempt in range(attempts):
        try:
            time.sleep(
                random.uniform(
                    MIN_DELAY_SECONDS,
                    MAX_DELAY_SECONDS,
                )
            )

            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,"
                          "application/xml;q=0.9,*/*;q=0.8",
                "Connection": "close",
            }

            # HEAD is lightweight.
            response = requests.head(
                clean_url,
                headers=headers,
                timeout=TIMEOUT_SECONDS,
                allow_redirects=True,
            )

            # Some websites reject HEAD. Fall back to GET.
            if response.status_code in {403, 405, 501}:
                response = requests.get(
                    clean_url,
                    headers=headers,
                    timeout=TIMEOUT_SECONDS,
                    allow_redirects=True,
                    stream=True,
                )

            code = response.status_code
            note = status_note(code)
            available = "Yes" if code < 400 else "No"

            return code, note, available

        except requests.exceptions.Timeout:
            last_error_note = "Timeout"

        except requests.exceptions.SSLError:
            last_error_note = "SSL Error"

        except requests.exceptions.ConnectionError:
            last_error_note = "Connection Error"

        except requests.exceptions.TooManyRedirects:
            last_error_note = "Too Many Redirects"

        except requests.exceptions.InvalidURL:
            last_error_note = "Invalid URL"

        except requests.exceptions.RequestException as exc:
            last_error_note = f"Error: {type(exc).__name__}"

        except Exception as exc:
            last_error_note = f"Unexpected Error: {type(exc).__name__}"

        if attempt < attempts - 1:
            time.sleep(1)

    return "Error", last_error_note, "No"


# ============================================================
# RESUME / SAVE
# ============================================================

def has_result(value) -> bool:
    """
    Determine whether a row already has a website-check result.
    """
    if pd.isna(value):
        return False

    text = str(value).strip()

    return text != "" and text.lower() not in {
        "nan",
        "none",
        "nat",
    }


def save_progress(df: pd.DataFrame, output_path: Path):
    """
    Save the current progress in the same format as the input.
    """
    with save_lock:
        try:
            atomic_write_table(df, output_path)
            print(f"  Progress saved -> {output_path}")
        except Exception as exc:
            print(
                f"  WARNING: Could not save progress to:\n"
                f"  {output_path}\n"
                f"  Error: {exc}\n"
                f"  Keep Excel/CSV files closed and retry."
            )


# ============================================================
# SUMMARY
# ============================================================

def format_duration(seconds: float) -> str:
    seconds = int(round(seconds))

    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    parts = []

    if hours:
        parts.append(f"{hours}h")

    if minutes or hours:
        parts.append(f"{minutes}m")

    parts.append(f"{secs}s")

    return " ".join(parts)


def write_summary(
    output_path: Path,
    checked_count: int,
    elapsed_seconds: float,
    total: int,
):
    if elapsed_seconds > 0 and checked_count > 0:
        per_minute = checked_count / (elapsed_seconds / 60)
        per_hour = checked_count / (elapsed_seconds / 3600)
    else:
        per_minute = 0
        per_hour = 0

    lines = [
        "========== WEBSITE CHECK SUMMARY ==========",
        f"Run finished           : {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total input records    : {total}",
        f"Checked this run       : {checked_count}",
        f"Time taken             : {format_duration(elapsed_seconds)}",
        f"Average speed          : {per_minute:.1f} sites/minute",
        f"Approx. hourly speed   : {per_hour:.0f} sites/hour",
        f"Output file            : {output_path}",
        "============================================",
    ]

    text = "\n".join(lines)

    print("\n" + text)

    summary_path = output_path.with_name(
        f"{output_path.stem}_summary.txt"
    )

    try:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(text + "\n\n")

        print(f"Summary saved -> {summary_path}")

    except Exception as exc:
        print(f"WARNING: Could not save summary: {exc}")


# ============================================================
# MAIN
# ============================================================

def main():
    input_path = Path(INPUT_FILE).expanduser()

    validate_input_format(input_path)

    # Automatically preserve input format.
    if OUTPUT_FILE is None:
        output_path = default_output_path(input_path)
    else:
        output_path = Path(OUTPUT_FILE).expanduser()

        if output_path.suffix.lower() != input_path.suffix.lower():
            raise ValueError(
                "\nOUTPUT_FILE format does not match INPUT_FILE format.\n"
                f"Input : {input_path.suffix}\n"
                f"Output: {output_path.suffix}\n\n"
                "Leave OUTPUT_FILE = None and the script will automatically "
                "create the correct same-format output."
            )

    # --------------------------------------------------------
    # RESUME
    # --------------------------------------------------------
    #
    # If the output already exists, load it and continue from
    # rows that do not yet have a Status Code.
    #
    if output_path.exists():
        print(f"Found existing output — resuming:")
        print(f"  {output_path}")

        df = read_table(output_path, sheet=SHEET)

        # If the output exists but has no status column, treat it
        # as a fresh input rather than assuming it is valid progress.
        if "Status Code" not in df.columns:
            print("Existing output has no status column. Starting fresh.")
            df = read_table(input_path, sheet=SHEET)
        else:
            print("Existing status results found. Already-checked rows will be skipped.")

    else:
        df = read_table(input_path, sheet=SHEET)

    if df.empty:
        print("Input contains 0 records. Nothing to check.")
        return

    print(f"Total records read : {len(df)}")

    # --------------------------------------------------------
    # DETECT WEBSITE COLUMN
    # --------------------------------------------------------
    column = COLUMN_NAME

    if column is None:
        column = detect_url_column_from_dataframe(df)

        if column is None:
            print("\nERROR: Could not automatically find a website/URL column.")
            print("Available columns:")
            for col in df.columns:
                print(f"  - {col}")

            print(
                "\nIf your URL column has an unusual name, set "
                "COLUMN_NAME near the top of this script."
            )
            sys.exit(1)

        print(f"Auto-detected website column : {column}")

    if column not in df.columns:
        print(f"\nERROR: Column '{column}' was not found.")
        print("Available columns:")
        for col in df.columns:
            print(f"  - {col}")
        sys.exit(1)

    # --------------------------------------------------------
    # ADD RESULT COLUMNS
    # --------------------------------------------------------
    for col in ("Status Code", "Status Note", "Available"):
        if col not in df.columns:
            df[col] = pd.NA

        df[col] = df[col].astype(object)

    urls = df[column].tolist()
    total = len(urls)

    pending_indices = [
        i for i in range(total)
        if not has_result(df.at[i, "Status Code"])
    ]

    already_done = total - len(pending_indices)

    print(f"Already checked : {already_done}/{total}")
    print(f"Remaining       : {len(pending_indices)}")
    print(f"Workers         : {MAX_WORKERS}")
    print(f"Timeout         : {TIMEOUT_SECONDS}s")
    print(f"Output format    : {output_path.suffix}")
    print("\nPress Ctrl+C at any time to stop safely.")

    if not pending_indices:
        print("\nAll URLs are already checked.")
        save_progress(df, output_path)
        write_summary(output_path, 0, 0, total)
        return

    completed = 0
    completed_since_save = 0
    start_time = time.time()

    # --------------------------------------------------------
    # PARALLEL WEBSITE CHECKING
    # --------------------------------------------------------
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            future_to_index = {
                executor.submit(check_one_url, urls[i]): i
                for i in pending_indices
            }

            for future in concurrent.futures.as_completed(
                future_to_index
            ):
                index = future_to_index[future]

                try:
                    status_code, note, available = future.result()

                except Exception as exc:
                    status_code = "Error"
                    note = f"Unhandled: {type(exc).__name__}"
                    available = "No"

                df.at[index, "Status Code"] = status_code
                df.at[index, "Status Note"] = note
                df.at[index, "Available"] = available

                completed += 1
                completed_since_save += 1

                if completed % 10 == 0 or completed == len(pending_indices):
                    elapsed = time.time() - start_time

                    if elapsed > 0:
                        speed = completed / (elapsed / 60)
                    else:
                        speed = 0

                    print(
                        f"  {completed}/{len(pending_indices)} checked "
                        f"| {speed:.1f}/min"
                    )

                if completed_since_save >= SAVE_EVERY:
                    save_progress(df, output_path)
                    completed_since_save = 0

    except KeyboardInterrupt:
        print("\n\nCtrl+C detected.")
        print("Saving current progress before stopping...")

        save_progress(df, output_path)

        elapsed = time.time() - start_time

        print(
            "\nYou can run the SAME command again. "
            "The script will skip completed rows and continue."
        )

        write_summary(
            output_path,
            completed,
            elapsed,
            total,
        )

        return

    # --------------------------------------------------------
    # FINAL SAVE
    # --------------------------------------------------------
    save_progress(df, output_path)

    elapsed = time.time() - start_time

    print("\n============================================")
    print("DONE")
    print("============================================")
    print(f"Input file  : {input_path}")
    print(f"Output file : {output_path}")

    write_summary(
        output_path,
        completed,
        elapsed,
        total,
    )


if __name__ == "__main__":
    main()
