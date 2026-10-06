"""
scraper_all_in_one.py — Bulk Email & Contact Scraper (single-file edition)
============================================================================

Everything from the original multi-file project (config, database, extractors,
scraper, exporter, run.py) merged into ONE file. Behavior is unchanged from the
original — this is purely a reorganization for anyone who prefers a single
script over a package of files.

    INPUT   : input/urls.csv          (paste one website per line, then run)
              -- OR pass --input pointing at an external master CSV, e.g.
              GMB_NL/NL_OUTPUT/NL_clean_website/clean_urls.csv
    OUTPUT  : follows the input automatically:
              - internal input/urls.csv  -> output stays in input/
              - external file            -> output goes into a sibling
                folder named NL_EMAIL_DATA, next to the input file's own
                folder. E.g. an input of
                    GMB_NL/NL_OUTPUT/NL_clean_website/clean_urls.csv
                writes scraper.db / emails.csv / emails.xlsx /
                emails_flat.csv into
                    GMB_NL/NL_OUTPUT/NL_EMAIL_DATA/
                Point --input at a *new* clean_urls.csv under any
                NL_OUTPUT folder next time and the output folder updates
                to match automatically -- no path editing needed.

    For the project's own internal drop zone, everything — the urls.csv
    you paste into, AND every file the scraper produces (database, CSV,
    XLSX, JSON) — lives in the SAME input/ folder.

    Every run also AUTOMATICALLY retries any website still marked 'failed'
    or 'no_data' from a previous run -- no flags needed. Pass --no-retry to
    skip that and only process brand-new URLs.

Usage:
    python scraper_all_in_one.py                  scrape everything queued (new URLs + auto-retried failed/no_data)
    python scraper_all_in_one.py --no-retry       skip the automatic retry, only scrape brand-new URLs
    python scraper_all_in_one.py --export         scrape, then also write the CSV files
    python scraper_all_in_one.py --export-only    just re-export the database to CSV
    python scraper_all_in_one.py --threads 20 --batch-size 30
    python scraper_all_in_one.py --no-whois

Setup (once):
    pip install -r requirements.txt
    (requests, beautifulsoup4, lxml, tqdm, python-whois, markdownify)

    Optional, for the bot-challenge headless-browser fallback (Cloudflare
    "Just a moment...", "sgcaptcha" Robot Challenge Screen, etc.):
        pip install selenium
        (also requires Chrome/Chromium to be installed on this machine --
        Selenium 4.6+ downloads the matching driver itself, no separate
        webdriver-manager/chromedriver setup needed)
    Pass --no-selenium to skip this fallback if Selenium/Chrome isn't set up.
"""

import os
import re
import csv
import sys
import json
import time
import random
import socket
import sqlite3
import logging
import datetime
import threading
import argparse
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor

import requests
import whois
import markdownify
import openpyxl
from openpyxl.utils import get_column_letter
from bs4 import BeautifulSoup
from tqdm import tqdm


# ============================================================================
# SECTION 1: CONFIG  (was config.py)
# ============================================================================

# ── Paths & Environment ────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(PROJECT_ROOT, "input")     # ← the project's own disposable drop zone

# INPUT: your real master URL list normally lives outside this project, in
# the CANGMB pipeline's cleaned-URL export -- e.g.
#   C:\Users\UDAY\OneDrive\Desktop\GMB_NL\NL_OUTPUT\NL_clean_website\clean_urls.csv
# It's an EXTERNAL file — the "own path" written directly here (not joined
# onto INPUT_DIR; os.path.join with an absolute second argument silently
# drops the first component on Windows, which made the old version of this
# line correct but misleading to read). Because this path isn't inside
# INPUT_DIR, read_and_consume_urls_from_file's is_internal_drop_zone check
# below correctly treats it as external and leaves it untouched after
# reading (no auto-clearing) -- same as before.
DEFAULT_INPUT_CSV = r"C:\Users\Administrator\Documents\LEAD_ANALYSI\cleaning_raw_data\AUS_CLEANED_DATA\AUS_CLEAN_WEBSITE_URLS\clean_urls.csv"

# OUTPUT LOCATION — now follows whatever --input you point the tool at,
# instead of always writing to this project's own input/ folder.
#
# Rule:
#   - If --input is this project's own internal drop zone (input/urls.csv),
#     output stays right there in input/ -- unchanged from before.
#   - If --input is an EXTERNAL file (e.g. any NL_OUTPUT\<subfolder>\...\
#     clean_urls.csv from the CANGMB pipeline), the output folder becomes a
#     SIBLING of that file's parent folder, named "NL_EMAIL_DATA":
#         ...\GMB_NL\NL_OUTPUT\NL_clean_website\clean_urls.csv   (input)
#      → ...\GMB_NL\NL_OUTPUT\NL_EMAIL_DATA\                     (output)
#     So the very next time you point --input at a *new* clean_urls.csv
#     living anywhere under a NL_OUTPUT folder, the scraper.db/emails.csv/
#     emails.xlsx/emails_flat.csv all automatically land in that same
#     NL_OUTPUT\NL_EMAIL_DATA folder -- no manual path editing needed.
OUTPUT_FOLDER_NAME = "AUS_EMAIL_DATA"


def compute_output_dir(input_csv_path: str) -> str:
    """Decide where this run's output (scraper.db, emails.csv, emails.xlsx,
    emails_flat.csv, results.json) should live, based on the --input path."""
    input_csv_path = os.path.abspath(input_csv_path)
    input_dir = os.path.dirname(input_csv_path)

    is_internal_drop_zone = os.path.normcase(input_dir) == os.path.normcase(os.path.abspath(INPUT_DIR))
    if is_internal_drop_zone:
        return INPUT_DIR

    # External file: output goes into "<parent-of-the-csv's-folder>/NL_EMAIL_DATA"
    # e.g. input_dir = .../NL_OUTPUT/NL_clean_website  ->  parent = .../NL_OUTPUT
    parent_of_csv_folder = os.path.dirname(input_dir)
    return os.path.join(parent_of_csv_folder, OUTPUT_FOLDER_NAME)


# Module-level defaults (used when the script is imported/tested directly,
# and as the argparse defaults before --input is known). main() below
# recomputes all of these from the ACTUAL --input the user passes, so
# pointing --input at a different NL_OUTPUT\...\clean_urls.csv automatically
# redirects the output too.
OUTPUT_DIR = compute_output_dir(DEFAULT_INPUT_CSV)

# MAIN OUTPUT: the SQLite database. Every scrape result is written here first.
# The CSV files below are just optional exports generated from this database.
DB_PATH = os.path.join(OUTPUT_DIR, "scraper.db")
DEFAULT_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "emails.csv")
DEFAULT_OUTPUT_XLSX = os.path.join(OUTPUT_DIR, "emails.xlsx")
DEFAULT_FLAT_CSV = os.path.join(OUTPUT_DIR, "emails_flat.csv")
DEFAULT_OUTPUT_JSON = os.path.join(OUTPUT_DIR, "results.json")

# Create required directories if missing
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Auto-create a header-only file ONLY for the project's own disposable
# input/urls.csv drop zone -- NEVER for DEFAULT_INPUT_CSV now that it can
# point at an external master file. Auto-creating an empty file there on
# a transient missing-path (e.g. OneDrive still syncing) would silently
# replace your real 3,900+ URL list with a blank header row.
_internal_urls_csv = os.path.join(INPUT_DIR, "urls.csv")
if not os.path.exists(_internal_urls_csv):
    with open(_internal_urls_csv, "w", encoding="utf-8") as _f:
        _f.write("website\n")

# ── Scraper Settings ────────────────────────────────────────────────────────
DEFAULT_THREADS = 12
DEFAULT_DEPTH = 2
REQUEST_TIMEOUT = 7  # Fast 7s timeout to prevent hanging on dead sites
DELAY_RANGE = (0.2, 0.8)  # Fast delay per request

# ── Auto-export CSV/XLSX while scraping ─────────────────────────────────────
# The scraper writes fresh emails.csv / emails_flat.csv / emails.xlsx
# snapshots periodically as it runs (not just at the very end), and always
# one more time right before exiting -- whether that's a normal finish or a
# Ctrl+C. That way stopping the script at any point still leaves usable,
# up-to-date CSV/XLSX files behind, and re-running afterward picks up right
# where you left off (the export always reflects the FULL database, old
# results + new).
EXPORT_EVERY_N = 200            # write a snapshot every N newly completed sites...
EXPORT_MIN_INTERVAL_SEC = 60    # ...but no more often than this many seconds apart

# ── WHOIS (domain age in days / creation date) ──────────────────────────────
# Turn off with:  python scraper_all_in_one.py --no-whois
WHOIS_ENABLED = True
WHOIS_TIMEOUT = 5        # seconds; give up on a registry that won't answer
WHOIS_MAX_WORKERS = 8    # max WHOIS lookups in flight AT ONCE, regardless of --threads.
                         # Keep this small. Registries (especially .ca) block heavy
                         # users by never replying, which is what stalled earlier runs.

# Rotated Desktop User-Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# Standard WAF Bypass Headers
WAF_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Chromium";v="123", "Not:A-Brand";v="8", "Google Chrome";v="123"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# Setup clean console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ScraperRunner")


# ============================================================================
# SECTION 2: EMAIL EXTRACTOR  (was extractors/email_extractor.py)
# ============================================================================

EMAIL_REGEX = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)

OBFUSCATED_EMAIL_REGEX = re.compile(
    r"\b[A-Za-z0-9._%+\-]+\s*(?:\[at\]|\(at\)|@)\s*[A-Za-z0-9.\-]+\s*(?:\[dot\]|\(dot\)|\.)\s*[A-Za-z]{2,}\b",
    re.IGNORECASE
)

HEX_KEY_REGEX = re.compile(r"^[0-9a-f]{20,64}$", re.I)

INVALID_PREFIXES = ("u002f", "\\u002f", "0x", "%2f", "http", "www", "image", "asset", "icon", "logo", "npm")

INVALID_DOMAIN_SUFFIXES = (
    "sentry.io", "ingest.sentry.io", "bugsnag.com", "rollbar.com", "logrocket.com",
    "segment.io", "segment.com", "mixpanel.com", "amplitude.com", "newrelic.com",
    "datadoghq.com", "wixpress.com", "squarespace.com", "example.com", "domain.com",
    "yourdomain.com", "test.com", "mattboldt.com", "jquery.com", "bootstrap.com",
    "fontawesome.com", "videojs.com", "rive.app", "teamwork.com", "schema.org",
    "w3.org", "github.com", "unpkg.com"
)

IMAGE_EXTENSIONS = ("png", "jpg", "jpeg", "webp", "gif", "svg", "webm", "mp4", "avi", "mov", "css", "js", "ts")


def decode_cloudflare_email(cf_hex: str) -> str:
    """Decode Cloudflare hex-encoded email protection string."""
    try:
        k = int(cf_hex[:2], 16)
        return "".join(chr(int(cf_hex[i:i+2], 16) ^ k) for i in range(2, len(cf_hex), 2))
    except Exception:
        return ""


def is_valid_email(email: str) -> bool:
    """Strict validation to filter out Sentry keys, JS hex hashes, retina images (@2x.jpg), and analytics domains."""
    if not email or "@" not in email:
        return False

    email = email.lower().strip(".")
    username, domain = email.rsplit("@", 1)

    # Reject retina image filenames like `@2x.jpg`, `@3x.png`, `@2x.webm`
    if re.match(r"^[1-4]x\.(png|jpg|jpeg|webp|gif|svg|webm)$", domain, re.I):
        return False

    # Reject domain ending with image/media extension
    ext = domain.split(".")[-1]
    if ext in IMAGE_EXTENSIONS:
        return False

    # Strip unicode slash escapes
    if username.startswith("u002f") or username.startswith("\\u002f"):
        username = username.replace("u002f", "").replace("\\u002f", "")

    # Reject hex hash usernames (e.g. Sentry DSN key 3e6b0deb414549c8901b5382885e478b)
    if HEX_KEY_REGEX.match(username):
        return False

    # Reject invalid prefixes
    if any(username.startswith(prefix) for prefix in INVALID_PREFIXES):
        return False

    # Reject Sentry, analytics, and open-source library domains
    if any(domain == d or domain.endswith("." + d) for d in INVALID_DOMAIN_SUFFIXES):
        return False

    # Valid domain structure check
    if "." not in domain or len(domain.split(".")[-1]) < 2:
        return False

    if len(username) < 2 or len(username) > 64:
        return False

    return True


def extract_emails_from_text(text: str) -> set:
    """Extract standard and de-obfuscated emails from raw text string."""
    found = EMAIL_REGEX.findall(text)
    clean = set()
    for e in found:
        e = e.lower().strip(".")
        if is_valid_email(e):
            clean.add(e)

    # De-obfuscate [at] / (at) / [dot]
    obfuscated = OBFUSCATED_EMAIL_REGEX.findall(text)
    for e in obfuscated:
        de_obs = e.lower().replace(" ", "").replace("[at]", "@").replace("(at)", "@").replace("[dot]", ".").replace("(dot)", ".")
        if is_valid_email(de_obs):
            clean.add(de_obs)

    return clean


def extract_emails_from_html(html: str) -> set:
    """Extract emails from HTML text, mailto: links, and Cloudflare protection attributes."""
    emails = extract_emails_from_text(html)
    soup = BeautifulSoup(html, "lxml")

    # 1. Parse mailto: links
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.startswith("mailto:"):
            addr = href.replace("mailto:", "").split("?")[0].strip().lower()
            if is_valid_email(addr):
                emails.add(addr)

    # 2. Parse Cloudflare email protection data attributes
    for tag in soup.find_all(attrs={"data-cfemail": True}):
        decoded = decode_cloudflare_email(tag["data-cfemail"])
        if is_valid_email(decoded):
            emails.add(decoded.lower())

    # 3. Parse Cloudflare email protection href links
    for tag in soup.find_all("a", href=True):
        if "/cdn-cgi/l/email-protection#" in tag["href"]:
            cf_hex = tag["href"].split("#")[-1]
            decoded = decode_cloudflare_email(cf_hex)
            if is_valid_email(decoded):
                emails.add(decoded.lower())

    return emails


# ============================================================================
# SECTION 3: PHONE EXTRACTOR  (was extractors/phone_extractor.py)
# ============================================================================

DUMMY_PHONE_NUMBERS = {
    "2597462153", "26921562148", "310259121563", "1234567890",
    "0000000000", "9999999999", "1111111111", "1234567"
}

# ── Country detection for phone numbers ─────────────────────────────────────
# The old logic guessed the country purely from the first digit of a bare
# 10-digit number (6-9 => India, else => NANP/+1). That's wrong for a huge
# share of real North American numbers: almost every Canadian/US area code
# also starts with 6, 7, 8, or 9 (780 Edmonton, 587 Calgary, 604 Vancouver,
# 650/707/800/866/877/888 in the US, etc.), so those were all mislabeled
# +91. We now look at the page/address context first (explicit country
# names, province/state abbreviations, postal-code formats, domain TLD),
# then fall back to a table of known Canadian area codes, and only use the
# old "starts with 6-9" guess as a last resort when nothing else is known.

CANADIAN_AREA_CODES = {
    "204", "226", "236", "249", "250", "289", "306", "343", "354", "365",
    "367", "382", "403", "416", "418", "431", "437", "438", "450", "506",
    "514", "519", "548", "579", "581", "584", "587", "604", "613", "639",
    "647", "672", "705", "709", "742", "753", "778", "780", "782", "807",
    "819", "825", "867", "873", "879", "902", "905",
}

CA_PROVINCE_REGEX = re.compile(
    r",\s*(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\b", re.IGNORECASE
)
CA_POSTAL_REGEX = re.compile(r"\b[A-Za-z]\d[A-Za-z]\s?\d[A-Za-z]\d\b")
CA_KEYWORD_REGEX = re.compile(r"\bcanada\b", re.IGNORECASE)

US_STATE_REGEX = re.compile(
    r",\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|"
    r"MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|"
    r"VA|WA|WV|WI|WY)\s*\d{5}", re.IGNORECASE
)
US_KEYWORD_REGEX = re.compile(r"\b(usa|united states)\b", re.IGNORECASE)

IN_KEYWORD_REGEX = re.compile(r"\bindia\b", re.IGNORECASE)
IN_PINCODE_REGEX = re.compile(r"\b\d{6}\b")

# When NOTHING -- not the domain, not the address/page text, not a known
# Canadian area code -- points to a country (e.g. an 855/800/866 toll-free
# number on a plain .com domain with no address text), clean_phone_number
# has to guess. That guess used to always be India. For a dataset that is
# entirely Canadian/US business leads (like this project's own CANGMB
# pipeline), that's backwards -- an unidentifiable NANP-shaped number in a
# Canadian lead-gen run is far more likely to be a Canadian/US toll-free
# line than an Indian one. FALLBACK_COUNTRY controls that last-resort
# guess; default "in" keeps the original behavior unless you opt in.
FALLBACK_COUNTRY = "ca"          # "in" | "ca" | "us" -- set via --assume-country
                                  # Default is "ca": this project's entire pipeline
                                  # (CANGMB) is Canadian business leads, so an
                                  # unidentifiable NANP-shaped number should default
                                  # to Canada, not India. Pass --assume-country in
                                  # to restore the original behavior if ever needed.
FALLBACK_COUNTRY_CODES = {"in": "91", "ca": "1", "us": "1"}


def detect_country_hint(context: str, domain: str = "") -> Optional[str]:
    """Look at page text / address / domain for an explicit country signal.
    Returns 'IN', 'CA', 'US', or None if nothing conclusive was found.
    Checked in this order so an explicit country always wins over a guess
    made from the digits alone."""
    context = context or ""
    domain = (domain or "").lower()

    if domain.endswith(".in") or IN_KEYWORD_REGEX.search(context):
        return "IN"

    if (domain.endswith(".ca")
            or CA_KEYWORD_REGEX.search(context)
            or CA_PROVINCE_REGEX.search(context)
            or CA_POSTAL_REGEX.search(context)):
        return "CA"

    if (domain.endswith(".us")
            or US_KEYWORD_REGEX.search(context)
            or US_STATE_REGEX.search(context)):
        return "US"

    return None


def clean_phone_number(p: str, context: str = "", domain: str = "") -> Optional[str]:
    """Validate and format a phone number with country code.
    `context` (page text, extracted address, etc.) and `domain` are used to
    figure out the right country instead of guessing from the digits alone."""
    if not p:
        return None
    p = p.strip()
    # Strip leading label prefixes
    p = re.sub(r"^(tel:|phone:|call:|call us:)", "", p, flags=re.I).strip()
    digits = re.sub(r"[^\d+]", "", p)

    if digits.startswith("00"):
        digits = "+" + digits[2:]

    # Handle numbers with a leading 0 in front of a 10-digit local number
    # (e.g. 04035610885 -> +914035610885 for an Indian landline). This used
    # to ALWAYS assign +91 here, which skipped the country-detection logic
    # below entirely -- so a Canadian number that happened to pick up a
    # stray leading 0 during scraping (e.g. "0" + a 780/Edmonton number)
    # was forced to India even though the domain/address clearly said
    # Canada. Now it runs through the same detection as every other
    # 10-digit number, and only falls back to the India assumption when
    # nothing else points to a country.
    if digits.startswith("0") and not digits.startswith("+"):
        pure_zero = digits[1:]
        if len(pure_zero) == 10:
            country = detect_country_hint(context, domain)
            if country == "IN":
                digits = "+91" + pure_zero
            elif country in ("CA", "US"):
                digits = "+1" + pure_zero
            elif pure_zero[:3] in CANADIAN_AREA_CODES:
                digits = "+1" + pure_zero
            else:
                # No North American signal found -- fall back to the
                # dataset-level default (see FALLBACK_COUNTRY above)
                # instead of always assuming India.
                fallback_code = FALLBACK_COUNTRY_CODES.get(FALLBACK_COUNTRY, "91")
                digits = "+" + fallback_code + pure_zero

    if not digits.startswith("+"):
        if len(digits) == 10:
            country = detect_country_hint(context, domain)
            if country == "IN":
                digits = "+91" + digits
            elif country in ("CA", "US"):
                digits = "+1" + digits
            elif digits[:3] in CANADIAN_AREA_CODES:
                # No explicit country text found, but the area code itself
                # is unambiguously Canadian (e.g. 780 = Edmonton).
                digits = "+1" + digits
            elif digits[0] in "23456789":
                # No signal at all (e.g. a toll-free 800/855/866/877/888
                # number, which is shared across the US and Canada and
                # can't be tied to a specific area). Fall back to the
                # dataset-level default instead of always assuming India.
                fallback_code = FALLBACK_COUNTRY_CODES.get(FALLBACK_COUNTRY, "91")
                digits = "+" + fallback_code + digits
            else:
                # Leading 0 or 1 isn't a valid area-code/STD-code digit
                # in either numbering plan -- not a recoverable number.
                return None
        elif len(digits) == 11 and digits.startswith("1"):
            digits = "+" + digits
        elif len(digits) == 12 and digits.startswith("91"):
            digits = "+" + digits
        elif len(digits) == 11 and digits.startswith("44"):
            digits = "+" + digits
        else:
            return None

    pure = digits[1:]

    # Reject known dummy / placeholder template numbers
    if any(d in pure for d in DUMMY_PHONE_NUMBERS):
        return None

    # Length requirement: phone digits must be between 7 and 15 digits
    if 7 <= len(pure) <= 15:
        # Exclude date patterns like 20260807
        if not re.match(r"^(19|20)\d{2}(0[1-9]|1[0-2])", pure):
            if len(pure) == 11 and pure.startswith("1"):
                return f"+1 ({pure[1:4]}) {pure[4:7]}-{pure[7:]}"
            elif len(pure) == 12 and pure.startswith("91"):
                return f"+91 {pure[2:7]} {pure[7:]}"
            return digits

    return None


def extract_phones_from_html(html: str, domain: str = "") -> set:
    """Extract strictly verified phone numbers from HTML pages.
    `domain` and the page's own text are used as context for country
    detection so numbers are tagged +1 / +91 / etc. correctly."""
    phones = set()
    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(" ", strip=True)

    # 1. Parse tel: links (most reliable source!)
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("tel:"):
            c = clean_phone_number(href.replace("tel:", "").split("?")[0], context=page_text, domain=domain)
            if c:
                phones.add(c)

    # 2. Parse Schema.org JSON-LD telephone attributes
    for tag in soup.find_all("script", type="application/ld+json"):
        if not tag.string:
            continue
        try:
            data = json.loads(tag.string)
            def find_phones(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in ("telephone", "phone", "fax") and isinstance(v, str):
                            c = clean_phone_number(v, context=page_text, domain=domain)
                            if c:
                                phones.add(c)
                        else:
                            find_phones(v)
                elif isinstance(obj, list):
                    for item in obj:
                        find_phones(item)
            find_phones(data)
        except Exception:
            pass

    # 3. HTML containers matching explicit phone/contact labels
    for tag in soup.find_all(["div", "p", "span", "address", "li", "td", "h4", "h5"]):
        cls_id = (tag.get("class", []) if isinstance(tag.get("class"), list) else []) + [tag.get("id", "")]
        cls_str = " ".join(str(x) for x in cls_id).lower()
        txt = tag.get_text(" ", strip=True)

        if any(k in cls_str for k in ["info", "call", "header", "footer", "contact", "phone", "media", "top"]):
            # Only match if phone label or pattern is present
            if any(l in txt.lower() for l in ["call", "ph:", "phone", "tel:", "contact"]):
                for match in re.findall(r"(?:\+?\d{1,4}[-.\s]?)?\(?\d{2,5}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b", txt):
                    c = clean_phone_number(match, context=txt + " " + page_text, domain=domain)
                    if c:
                        phones.add(c)

    return phones


def extract_phones_from_js(js_text: str, domain: str = "") -> set:
    """Extract phones ONLY from explicit JSON keys in JS code (preventing random math/array numbers)."""
    phones = set()
    phone_matches = re.findall(r'(?:phone|telephone|mobile)\s*:\s*"([^"]{5,30})"', js_text, re.I)
    for ph in phone_matches:
        c = clean_phone_number(ph, context=js_text, domain=domain)
        if c:
            phones.add(c)
    return phones


# ============================================================================
# SECTION 4: ADDRESS EXTRACTOR  (was extractors/address_extractor.py)
# ============================================================================

ADDRESS_KEYWORDS = [
    "complex", "building", "street", "st", "avenue", "ave", "road", "rd",
    "nagar", "dilsukhnagar", "hyderabad", "telangana", "500060", "suite", "ste",
    "floor", "po box", "zip", "postal", "vancouver", "toronto", "london",
    "new york", "bengaluru", "mumbai", "delhi", "chennai", "kolkata", "pune",
    "ahmedabad", "gurgaon", "noida", "sector", "colony", "marg", "chowk",
    "kamala", "croma", "gajam", "india", "usa", "uk", "canada", "san francisco"
]

IGNORE_ADDRESS_PATTERNS = [
    "street-address", "addresslocality", "addressregion", "postalcode", "addresscountry",
    "hours", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
    "dancing street", "huyi", "lorem", "ipsum", "select subject", "submit message",
    "hiring", "support engineer", "leave a message", "ready to help", "explore what we offer",
    "what industries", "can shellx", "helps the clients", "recaptiualize", "metrics",
    "content", "communities", "web-enabled", "strategy", "experience", "blog list", "home blog",
    "categories", "navigation", "menu", "search here", "read more", "solutions engineering",
    "see all solutions", "customer service", "slack connect", "hosting resources", "newsroom",
    "careers investors", "works where your team"
]

# Structural signals used by the class-agnostic fallback pass (step 3 below).
# A real street address, regardless of which container it's sitting in,
# almost always contains EITHER a postal/zip code OR a house-number+street
# pattern. Requiring one of these keeps the fallback precise even though it
# no longer relies on the tag having an "address"/"location"/etc. class name.
POSTAL_OR_ZIP_REGEX = re.compile(
    r"\b(?:[A-Za-z]\d[A-Za-z]\s?\d[A-Za-z]\d|\d{5}(?:-\d{4})?)\b"  # CA postal code (A1A 1A1) or US zip
)
STREET_NUMBER_REGEX = re.compile(
    r"\b\d{1,6}\s+(?:[A-Z][\w.'\-]*\s+){0,3}"
    r"(?i:street|st|avenue|ave|road|rd|blvd|boulevard|drive|dr|way|trail|lane|ln|"
    r"court|ct|place|pl|highway|hwy|parkway|pkwy|circle|cir)\b"
)
# Known limitation: requiring the gap words to be capitalized (proper-noun
# style, like real street names) filters out most ordinary prose, but an
# occasional sentence like "served 100 Main street clients" can still slip
# through since it happens to match number + capitalized word + "street".
# Rare in practice on real business sites; not worth heavier NLP-style
# filtering for a lightweight, dependency-free heuristic.


def extract_addresses_from_html(html: str) -> set:
    """Extract physical street addresses strictly from HTML pages."""
    addresses = set()
    soup = BeautifulSoup(html, "lxml")

    # 1. JSON-LD Schema.org PostalAddress
    for tag in soup.find_all("script", type="application/ld+json"):
        if not tag.string:
            continue
        try:
            data = json.loads(tag.string)
            def find_address(obj):
                if isinstance(obj, dict):
                    if obj.get("@type") in ("PostalAddress", "Place") or "addressLocality" in obj or "streetAddress" in obj:
                        parts = [obj.get("streetAddress"), obj.get("addressLocality"), obj.get("addressRegion"), obj.get("postalCode"), obj.get("addressCountry")]
                        clean_parts = [str(p).strip() for p in parts if p]
                        if clean_parts:
                            addresses.add(", ".join(clean_parts))
                    for k, v in obj.items():
                        if k in ("address", "location") and isinstance(v, str):
                            addresses.add(v.strip())
                        else:
                            find_address(v)
                elif isinstance(obj, list):
                    for item in obj:
                        find_address(item)
            find_address(data)
        except Exception:
            pass

    # 2. HTML address containers (class/id hints like "address", "footer", "office")
    for tag in soup.find_all(["address", "div", "p", "span", "li"]):
        cls_id = (tag.get("class", []) if isinstance(tag.get("class"), list) else []) + [tag.get("id", "")]
        cls_str = " ".join(str(x) for x in cls_id).lower()
        txt = tag.get_text(" ", strip=True)

        if any(k in cls_str for k in ["address", "location", "footer", "header", "office", "headquarters"]):
            clean_txt = re.sub(r"^(our office address|office address|our location|location|address)\s*:?\s*", "", txt, flags=re.I).strip()
            if any(kw in clean_txt.lower() for kw in ADDRESS_KEYWORDS):
                if 15 <= len(clean_txt) <= 250 and not any(ign in clean_txt.lower() for ign in ["privacy", "terms", "cookie", "copyright", "all rights", "subscribe", "newsletter", "working hours", "weekdays"]):
                    addresses.add(clean_txt)

    # 3. Class-agnostic fallback: catches real addresses sitting in plain,
    # unlabeled blocks -- common on website-builder sites (GoDaddy, Wix,
    # Squarespace templates) that don't tag their contact section with any
    # "address"/"location"/"office" class name, so step 2 above misses them
    # even though the text is right there (e.g. armorcpa.com's
    # "Suite #209, 13220 St Albert Trail Northwest, Edmonton, Alberta T5L
    # 4P6, Canada", which sits in a generic, unclassed block).
    # No class check here -- instead requires a real structural signal (a
    # postal/zip code OR a house-number+street-type pattern) on top of the
    # existing keyword check, so it stays precise without the class hint.
    for tag in soup.find_all(["address", "div", "p", "span", "li", "td", "section", "footer"]):
        txt = tag.get_text(" ", strip=True)
        if not (15 <= len(txt) <= 250):
            continue
        l_txt = txt.lower()
        if any(ign in l_txt for ign in IGNORE_ADDRESS_PATTERNS):
            continue
        if any(ign in l_txt for ign in ["privacy", "terms", "cookie", "copyright", "all rights", "subscribe", "newsletter", "working hours", "weekdays"]):
            continue
        if not any(kw in l_txt for kw in ADDRESS_KEYWORDS):
            continue
        if POSTAL_OR_ZIP_REGEX.search(txt) or STREET_NUMBER_REGEX.search(txt):
            clean_txt = re.sub(r"^(our office address|office address|our location|location|address)\s*:?\s*", "", txt, flags=re.I).strip()
            addresses.add(clean_txt)

    # Clean and filter out non-address text
    clean_addresses = set()
    seen_normalized = set()
    for addr in addresses:
        addr = addr.strip()
        addr = re.sub(r"^(our office address|office address|our location|location|address)\s*:?\s*", "", addr, flags=re.I).strip()
        l_addr = addr.lower()
        if "?" in addr or (":" in addr and "office" not in l_addr):
            continue
        if any(ign in l_addr for ign in IGNORE_ADDRESS_PATTERNS):
            continue
        if addr.startswith("Contact "):
            addr = addr[8:].strip()

        norm = re.sub(r"[^\w]", "", l_addr)
        if norm not in seen_normalized and len(addr) >= 10:
            seen_normalized.add(norm)
            clean_addresses.add(addr)

    return clean_addresses


def extract_addresses_from_js(js_text: str) -> set:
    """Extract location strings from JavaScript bundle objects."""
    addresses = set()
    loc_matches = re.findall(r'(?:location|address)\s*:\s*"([^"]{3,120})"', js_text, re.I)
    for loc in loc_matches:
        if not any(ign in loc.lower() for ign in ["http", "pop", "react", "window", "null", "undefined", "function", ".png", ".jpg", ".svg", ".css", ".js"]):
            addresses.add(loc.strip())
    return addresses


# ============================================================================
# SECTION 5: SOCIAL EXTRACTOR  (was extractors/social_extractor.py)
# ============================================================================

SOCIAL_PATTERNS = {
    "linkedin":  re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in|pub)/[A-Za-z0-9_\-\.%]+", re.I),
    "facebook":  re.compile(r"https?://(?:www\.)?facebook\.com/(?:pages/)?(?:[A-Za-z0-9_\-\.]+/)?(?:profile\.php\?id=\d+|[A-Za-z0-9_\-\.]+)", re.I),
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_\-\.]+", re.I),
    "twitter":   re.compile(r"https?://(?:www\.)?(?:twitter\.com|x\.com)/[A-Za-z0-9_]+", re.I),
    "youtube":   re.compile(r"https?://(?:www\.)?youtube\.com/(?:user/|channel/|c/|@)?[A-Za-z0-9_\-\.]+", re.I),
    "github":    re.compile(r"https?://(?:www\.)?github\.com/[A-Za-z0-9_\-\.]+", re.I),
    "tiktok":    re.compile(r"https?://(?:www\.)?tiktok\.com/@[A-Za-z0-9_\-\.]+", re.I),
}

IGNORE_SOCIAL = {
    "facebook.com/sharer", "facebook.com/share", "twitter.com/intent", "twitter.com/share",
    "x.com/intent", "x.com/share", "linkedin.com/share", "linkedin.com/sharing",
    "instagram.com/p/", "github.com/features", "github.com/pricing"
}


def clean_social_url(url: str) -> str:
    url = url.rstrip("/").split("?")[0]
    return url


def extract_social_links_from_text(text: str) -> Dict[str, set]:
    found = {platform: set() for platform in SOCIAL_PATTERNS}
    for platform, pattern in SOCIAL_PATTERNS.items():
        for match in pattern.findall(text):
            cleaned = clean_social_url(match)
            if not any(ign in cleaned.lower() for ign in IGNORE_SOCIAL):
                found[platform].add(cleaned)
    return found


def extract_social_links_from_html(html: str, base_url: str) -> Dict[str, set]:
    found = extract_social_links_from_text(html)
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        full = urljoin(base_url, a["href"])
        for platform, pattern in SOCIAL_PATTERNS.items():
            if pattern.search(full):
                cleaned = clean_social_url(full)
                if not any(ign in cleaned.lower() for ign in IGNORE_SOCIAL):
                    found[platform].add(cleaned)
    return found


# ============================================================================
# SECTION 6: WHOIS EXTRACTOR  (was extractors/whois_extractor.py)
# ============================================================================

whois_log = logging.getLogger("WhoisExtractor")

# The `whois` library opens a raw socket with NO timeout of its own. Registries
# (CIRA/.ca especially) rate-limit by silently never answering, which used to hang
# the calling thread forever and stall the whole run. Two defences:
#
#   1. a hard socket timeout, so a silent registry costs WHOIS_TIMEOUT seconds
#      instead of blocking indefinitely;
#   2. a semaphore capping how many lookups are in flight at once. Threads WAIT
#      their turn here rather than being thrown away -- queueing politely is what
#      keeps registries answering us instead of blacklisting us.
socket.setdefaulttimeout(WHOIS_TIMEOUT)
_whois_gate = threading.Semaphore(WHOIS_MAX_WORKERS)


def get_domain_whois_info(domain: str) -> Dict[str, Optional[str]]:
    """
    Fetch WHOIS registration details for a domain name.

    Returns:
        domain_created_at : "YYYY-MM-DD" or None
        domain_expires_at : "YYYY-MM-DD" or None
        domain_age_days   : integer number of days since registration, or None

    Never raises and never blocks longer than WHOIS_TIMEOUT. Values are None when
    the registry refuses us, rate-limits us, hides the date, or does not support
    a machine-readable WHOIS response at all (common for .co.uk, .au, .nz).
    """
    result = {
        "domain_created_at": None,
        "domain_expires_at": None,
        "domain_age_days": None,
    }

    if not domain or not WHOIS_ENABLED:
        return result

    # Clean domain (strip www., http, ports)
    clean_domain = domain.lower().replace("http://", "").replace("https://", "").split("/")[0].split(":")[0]
    if clean_domain.startswith("www."):
        clean_domain = clean_domain[4:]

    try:
        with _whois_gate:
            w = whois.whois(clean_domain)

        if w is None:
            return result

        c_date = w.creation_date
        e_date = w.expiration_date

        if isinstance(c_date, list) and c_date:
            c_date = c_date[0]
        if isinstance(e_date, list) and e_date:
            e_date = e_date[0]

        # Timezone offset handling
        if c_date and hasattr(c_date, "tzinfo") and c_date.tzinfo is not None:
            c_date = c_date.replace(tzinfo=None)
        if e_date and hasattr(e_date, "tzinfo") and e_date.tzinfo is not None:
            e_date = e_date.replace(tzinfo=None)

        if isinstance(c_date, datetime.datetime):
            result["domain_created_at"] = c_date.strftime("%Y-%m-%d")
            # Age as a plain whole number of days -- no "years" text.
            result["domain_age_days"] = max((datetime.datetime.now() - c_date).days, 0)

        if isinstance(e_date, datetime.datetime):
            result["domain_expires_at"] = e_date.strftime("%Y-%m-%d")

    except Exception as e:
        whois_log.debug(f"WHOIS lookup failed for {clean_domain}: {e}")

    return result


# ============================================================================
# SECTION 7: MARKDOWN EXTRACTOR  (was extractors/markdown_extractor.py)
# ============================================================================

markdown_log = logging.getLogger("MarkdownExtractor")


def convert_html_to_markdown(html: str) -> str:
    """Convert homepage HTML to clean Markdown text, stripping scripts, styles, and SVG noise."""
    if not html:
        return ""

    try:
        soup = BeautifulSoup(html, "lxml")

        # Remove non-content tags
        for tag in soup(["script", "style", "svg", "noscript", "iframe"]):
            tag.decompose()

        clean_html = str(soup)
        md_text = markdownify.markdownify(
            clean_html,
            heading_style="ATX",
            strip=["script", "style", "svg"]
        )

        # Normalize whitespace and blank lines
        lines = [line.strip() for line in md_text.splitlines()]
        clean_lines = []
        blank = False
        for line in lines:
            if line:
                clean_lines.append(line)
                blank = False
            elif not blank:
                clean_lines.append("")
                blank = True

        return "\n".join(clean_lines).strip()

    except Exception as e:
        markdown_log.warning(f"Failed to convert HTML to Markdown: {e}")
        return ""


# ============================================================================
# SECTION 7b: EMAIL FORMATTING (labeled "email 1: ..., email 2: ..." style)
# ============================================================================

MAX_LABELED_EMAILS = 10


def format_emails_labeled(emails: List[str], max_n: int = MAX_LABELED_EMAILS) -> str:
    """Turn a list of emails into 'email 1: a@x.com, email 2: b@y.com, ...'
    capped at max_n entries (extras are simply dropped, not lost -- the
    database still has every email found)."""
    if not emails:
        return ""
    limited = emails[:max_n]
    return ", ".join(f"email {i}: {addr}" for i, addr in enumerate(limited, start=1))


def extract_raw_emails_from_labeled(labeled: str) -> List[str]:
    """Reverse of format_emails_labeled -- pull the raw email addresses back
    out of an 'email 1: ..., email 2: ...' string. Used by the exporters so
    the unique-email count/flat list stays correct even though the display
    column is now labeled text instead of a plain comma list."""
    if not labeled:
        return []
    return EMAIL_REGEX.findall(labeled)


# ============================================================================
# SECTION 8: DATABASE  (was database.py)
# ============================================================================

db_log = logging.getLogger("Database")
_db_lock = threading.Lock()


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def init_db():
    """Initialize SQLite tables and indexes."""
    with _db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Queue / Websites table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS websites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                domain TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                pages_crawled INTEGER DEFAULT 0,
                time_taken_sec REAL DEFAULT 0.0,
                domain_created_at TEXT,
                domain_expires_at TEXT,
                domain_age_days INTEGER,
                homepage_markdown TEXT,
                scraped_at TIMESTAMP
            );
        """)

        # Extracted Results table (EXACTLY 1 UNIQUE ROW PER WEBSITE)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS extracted_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                website_id INTEGER UNIQUE NOT NULL,
                url TEXT NOT NULL,
                status TEXT DEFAULT 'completed',
                email TEXT,
                phone TEXT,
                address TEXT,
                domain_created_at TEXT,
                domain_expires_at TEXT,
                domain_age_days INTEGER,
                homepage_markdown TEXT,
                linkedin TEXT,
                facebook TEXT,
                instagram TEXT,
                twitter TEXT,
                youtube TEXT,
                github TEXT,
                tiktok TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (website_id) REFERENCES websites(id) ON DELETE CASCADE
            );
        """)

        # Migration: Add new columns if table already exists
        for col in ["domain_created_at", "domain_expires_at", "homepage_markdown"]:
            for tbl in ("websites", "extracted_results"):
                try:
                    cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} TEXT;")
                except Exception:
                    pass

        for tbl in ("websites", "extracted_results"):
            try:
                cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN domain_age_days INTEGER;")
            except Exception:
                pass

        try:
            cursor.execute("ALTER TABLE extracted_results ADD COLUMN status TEXT DEFAULT 'completed';")
        except Exception:
            pass

        # Migration: domain age used to be stored as free text like
        # "2.5 years (897 days)". Pull the day count out into the new integer
        # column, then retire the old one.
        for tbl in ("websites", "extracted_results"):
            cols = {r["name"] for r in cursor.execute(f"PRAGMA table_info({tbl})")}
            if "domain_age_years" not in cols:
                continue
            cursor.execute(f"""
                UPDATE {tbl}
                SET domain_age_days = CAST(
                    substr(domain_age_years,
                           instr(domain_age_years, '(') + 1,
                           instr(domain_age_years, ' days') - instr(domain_age_years, '(') - 1
                    ) AS INTEGER)
                WHERE domain_age_days IS NULL
                  AND domain_age_years LIKE '%(% days)%'
            """)
            moved = cursor.rowcount
            try:
                cursor.execute(f"ALTER TABLE {tbl} DROP COLUMN domain_age_years;")
                db_log.info(f"Migrated {moved} '{tbl}' rows to domain_age_days (dropped domain_age_years)")
            except Exception:
                db_log.info(f"Migrated {moved} '{tbl}' rows to domain_age_days")

        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_websites_status ON websites(status);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_websites_url ON websites(url);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_website ON extracted_results(website_id);")

        conn.commit()
        conn.close()
        db_log.info(f"Database initialized at {DB_PATH}")


def reset_interrupted_tasks():
    """Reset any tasks left in 'processing' status back to 'pending' for seamless resume."""
    with _db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE websites SET status = 'pending' WHERE status = 'processing'")
        conn.commit()
        conn.close()


def requeue_by_status(statuses: List[str]) -> int:
    """Reset every website currently in one of the given statuses back to
    'pending' so it gets scraped again on this run -- WITHOUT touching the
    input file at all, since the URLs are already sitting in the database
    queue from a previous run.

    Meant for sites that didn't come back with data:
      - 'failed'  : the request itself failed (timeout, connection refused,
                    non-HTML response, etc.) -- exactly the case where a site
                    opens fine in a browser but the scraper's single attempt
                    happened to hit a timeout, a transient block, or a slow
                    response. A retry is often all it takes.
      - 'no_data' : the page loaded fine but nothing was extracted from it.

    Only rows with one of the given statuses are touched -- 'completed'
    rows (and their real scraped data) are never reset by this.
    Returns how many websites were requeued.
    """
    if not statuses:
        return 0
    with _db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(statuses))
        cursor.execute(f"SELECT id FROM websites WHERE status IN ({placeholders})", statuses)
        ids = [row["id"] for row in cursor.fetchall()]

        if ids:
            id_placeholders = ",".join("?" * len(ids))
            cursor.execute(
                f"UPDATE websites SET status = 'pending', error_message = NULL WHERE id IN ({id_placeholders})",
                ids
            )
            cursor.execute(
                f"UPDATE extracted_results SET status = 'pending' WHERE website_id IN ({id_placeholders})",
                ids
            )
            conn.commit()

        conn.close()
    return len(ids)


def load_urls_into_queue(urls: List[str]) -> int:
    """Insert URLs into websites and extracted_results queue with status = 'pending'.

    IMPORTANT: this is idempotent by design. A URL already known to the
    database -- in ANY status (pending, processing, completed, no_data,
    failed) -- is left completely untouched on conflict. Only genuinely new
    URLs get inserted as 'pending'.

    This matters because the input file is often an external master list
    (like a cleaned URL export) that intentionally does NOT get cleared after
    ingestion -- see read_and_consume_urls_from_file. That means the exact
    same URLs get passed to this function on every single run. An earlier
    version of this function used ON CONFLICT ... DO UPDATE SET status =
    'pending', which silently reset every already-completed site back to
    pending on each restart -- making it look like the whole scrape was
    starting over from scratch. DO NOTHING fixes that: re-ingesting the same
    list is now a safe no-op for anything already tracked.

    Returns the number of URLs that were actually NEW (freshly inserted),
    not the total passed in.
    """
    newly_added = 0
    with _db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        for url in urls:
            url = url.strip()
            if not url:
                continue
            if not url.startswith("http"):
                url = "https://" + url

            domain = urlparse(url).netloc.lower()

            # 1. Websites queue table -- DO NOTHING if this URL already exists
            #    in any status, so re-running never resets prior progress.
            cursor.execute(
                """
                INSERT INTO websites (url, domain, status, error_message, pages_crawled, time_taken_sec)
                VALUES (?, ?, 'pending', NULL, 0, 0.0)
                ON CONFLICT(url) DO NOTHING
                """,
                (url, domain)
            )
            was_new = cursor.rowcount > 0

            # Fetch website_id (needed whether this row was just inserted or
            # already existed, so extracted_results can reference it).
            cursor.execute("SELECT id FROM websites WHERE url = ?", (url,))
            row = cursor.fetchone()
            if row and was_new:
                wid = row["id"]
                # 2. Extracted results table -- only created for brand-new
                #    websites. Existing rows (and their real scraped data) are
                #    never touched here.
                cursor.execute(
                    """
                    INSERT INTO extracted_results (website_id, url, status, email, phone, address)
                    VALUES (?, ?, 'pending', '', '', '')
                    ON CONFLICT(website_id) DO NOTHING
                    """,
                    (wid, url)
                )

            if was_new:
                newly_added += 1

        conn.commit()
        conn.close()
    return newly_added


def get_pending_batch(batch_size: int = 10) -> List[Dict]:
    """Fetch next batch of pending websites and atomically set status to 'processing'."""
    batch = []
    with _db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, url, domain FROM websites WHERE status = 'pending' LIMIT ?",
            (batch_size,)
        )
        rows = cursor.fetchall()

        if rows:
            ids = [row["id"] for row in rows]
            placeholders = ",".join("?" * len(ids))
            cursor.execute(
                f"UPDATE websites SET status = 'processing' WHERE id IN ({placeholders})",
                ids
            )
            conn.commit()

            for row in rows:
                batch.append({"id": row["id"], "url": row["url"], "domain": row["domain"]})

        conn.close()
    return batch


def save_scraping_result(result: Dict):
    """Save scraping output to SQLite database (EXACTLY 1 ROW PER WEBSITE)."""
    with _db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()

        website_id = result.get("db_id")
        url = result["website"]
        status = result.get("status", "completed")
        error_msg = result.get("error_message")
        pages_crawled = result.get("pages_crawled", 0)
        time_taken = result.get("time_taken_sec", 0.0)
        scraped_at = result.get("timestamp")
        domain_created = result.get("domain_created_at")
        domain_expires = result.get("domain_expires_at")
        domain_age = result.get("domain_age_days")
        hp_md = result.get("homepage_markdown", "")

        # 1. Update websites table status & WHOIS metadata
        if website_id:
            cursor.execute(
                """
                UPDATE websites
                SET status = ?, error_message = ?, pages_crawled = ?, time_taken_sec = ?,
                    domain_created_at = ?, domain_expires_at = ?, domain_age_days = ?, homepage_markdown = ?, scraped_at = ?
                WHERE id = ?
                """,
                (status, error_msg, pages_crawled, time_taken, domain_created, domain_expires, domain_age, hp_md, scraped_at, website_id)
            )
        else:
            cursor.execute(
                """
                INSERT OR REPLACE INTO websites (url, domain, status, error_message, pages_crawled, time_taken_sec, domain_created_at, domain_expires_at, domain_age_days, homepage_markdown, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (url, result.get("domain", ""), status, error_msg, pages_crawled, time_taken, domain_created, domain_expires, domain_age, hp_md, scraped_at)
            )
            website_id = cursor.lastrowid

        # 2. Insert OR REPLACE into extracted_results table (1 SINGLE ROW PER WEBSITE)
        # Email column is stored as labeled text ("email 1: a@x.com, email 2: ...",
        # capped at MAX_LABELED_EMAILS) -- see format_emails_labeled.
        email_str = format_emails_labeled(result.get("emails", []))
        phone_str = ", ".join(result.get("phones", []))
        addr_str = " | ".join(result.get("addresses", []))
        soc = result.get("social_links", {})

        cursor.execute(
            """
            INSERT OR REPLACE INTO extracted_results
            (website_id, url, status, email, phone, address, domain_created_at, domain_expires_at, domain_age_days, homepage_markdown, linkedin, facebook, instagram, twitter, youtube, github, tiktok)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                website_id, url, status, email_str, phone_str, addr_str,
                domain_created, domain_expires, domain_age, hp_md,
                soc.get("linkedin", ""), soc.get("facebook", ""),
                soc.get("instagram", ""), soc.get("twitter", ""),
                soc.get("youtube", ""), soc.get("github", ""),
                soc.get("tiktok", "")
            )
        )

        conn.commit()
        conn.close()


def get_queue_stats() -> Dict[str, int]:
    """Get current count of pending, completed, failed, and total websites."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT status, COUNT(*) as cnt FROM websites GROUP BY status")
    rows = cursor.fetchall()
    stats = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "no_data": 0, "total": 0}
    for row in rows:
        st = row["status"]
        if st in stats:
            stats[st] = row["cnt"]
        stats["total"] += row["cnt"]

    conn.close()
    return stats


# ============================================================================
# SECTION 9: SCRAPER  (was scraper.py)
# ============================================================================

scraper_log = logging.getLogger("Scraper")

CONTACT_PATHS = [
    "/contact", "/contact-us", "/contactus",
    "/about", "/about-us", "/aboutus",
    "/team", "/our-team", "/leadership",
]

CONTACT_KEYWORDS = [
    "contact", "about", "team", "people", "leadership", "staff", "reach",
    "connect", "impressum", "legal", "touch", "help", "support"
]


# ============================================================================
# SECTION 9b: BOT-CHALLENGE / HEADLESS BROWSER FALLBACK
# ============================================================================
# Some sites sit behind an anti-bot JS challenge (Cloudflare "Just a
# moment...", "sgcaptcha" Robot Challenge Screen, etc.) that a plain
# `requests` GET can NEVER get past -- the challenge only clears once a real
# browser executes its JavaScript and satisfies it. No timeout or retry count
# fixes this; it needs an actual (headless) browser. When one of these is
# detected, we spin up a headless Chrome via Selenium ONCE for that site's
# homepage, let it clear the challenge, then carry the resulting cookies over
# to the normal `requests` calls for that site's contact-path fetches -- so
# we only pay the (slow, heavy) browser cost once per site, not once per
# page. Needs `pip install selenium` and a Chrome/Chromium browser installed;
# Selenium 4.6+ downloads the matching driver itself (no webdriver-manager
# needed). Disable with --no-selenium if Selenium isn't available.

selenium_log = logging.getLogger("SeleniumFallback")

SELENIUM_ENABLED = True
SELENIUM_MAX_WORKERS = 2         # headless Chrome is heavy -- keep this small
                                  # regardless of --threads, same idea as WHOIS_MAX_WORKERS
SELENIUM_PAGE_LOAD_TIMEOUT = 25  # seconds to wait for the initial page load
CHALLENGE_WAIT_TIMEOUT = 20      # seconds to wait for the challenge itself to clear

_selenium_gate = threading.Semaphore(SELENIUM_MAX_WORKERS)

# Lowercased substrings seen in known anti-bot / JS-challenge pages (Cloudflare,
# "sgcaptcha", generic "robot challenge" products, etc.). Matching any one of
# these against a page's text is enough to treat it as a challenge rather
# than real content.
CHALLENGE_SIGNATURES = [
    "checking the site connection security", "checking your browser before accessing",
    "just a moment", "please enable cookies", "ddos protection by", "attention required",
    "robot challenge", "sgcaptcha", "verifying you are human", "cf-chl", "__cf_chl",
    "human verification", "security check to access", "are you a human",
    "press and hold", "unusual traffic from your computer network",
]


def is_bot_challenge_page(html: str, url: str = "") -> bool:
    """True if this looks like an anti-bot JS-challenge page rather than
    real site content -- see CHALLENGE_SIGNATURES above."""
    if not html and not url:
        return False
    text = (html or "").lower()
    if any(sig in text for sig in CHALLENGE_SIGNATURES):
        return True
    if url and "sgcaptcha" in url.lower():
        return True
    return False


class _BrowserPage:
    """Tiny duck-typed stand-in for a requests.Response, just enough for
    the extraction code below (which only ever reads .text and
    .headers.get("Content-Type")) to treat browser-rendered HTML exactly
    like a normal HTTP response."""
    def __init__(self, text: str):
        self.text = text or ""
        self.headers = {"Content-Type": "text/html"}
        self.status_code = 200


def render_with_browser(url: str) -> Optional[Tuple[str, Dict[str, str]]]:
    """Load `url` in a real headless Chrome, wait for a bot-challenge (if
    any) to clear, and return (final_html, cookies_dict). Returns None if
    Selenium isn't installed/available or the browser itself fails to
    start -- callers fall back to treating the site as failed, same as
    before this feature existed."""
    if not SELENIUM_ENABLED:
        return None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        selenium_log.warning(
            "Selenium not installed -- skipping browser fallback for a bot-challenge "
            "page. Install with: pip install selenium (and make sure Chrome is installed)."
        )
        return None

    driver = None
    with _selenium_gate:
        try:
            opts = Options()
            opts.add_argument("--headless=new")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--window-size=1366,900")
            opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
            # Reduce the most obvious automation fingerprints -- doesn't
            # defeat every anti-bot product, but helps with simpler ones.
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)

            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT)
            driver.get(url)

            deadline = time.time() + CHALLENGE_WAIT_TIMEOUT
            html = driver.page_source
            while is_bot_challenge_page(html, driver.current_url) and time.time() < deadline:
                time.sleep(1)
                html = driver.page_source

            cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
            return html, cookies

        except Exception as e:
            selenium_log.debug(f"Browser fallback failed for {url}: {type(e).__name__}: {e}")
            return None
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass


def get_http(url: str, timeout: int = REQUEST_TIMEOUT, cookies: Optional[Dict[str, str]] = None):
    """Execute GET request with rotated User-Agents, WAF headers, and fast timeout.
    `cookies`, when provided, are the cookies a headless-browser challenge
    solve picked up for this site (see render_with_browser) -- passing them
    along lets subsequent plain requests to the same site get through
    without needing the browser again."""
    headers = dict(WAF_HEADERS)
    headers["User-Agent"] = random.choice(USER_AGENTS)

    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, cookies=cookies)
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError:
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False, cookies=cookies)
            r.raise_for_status()
            return r
        except Exception:
            return None
    except Exception:
        return None


def extract_js_script_urls(html: str, base_url: str) -> List[str]:
    """Find all external .js bundle URLs referenced in HTML."""
    soup = BeautifulSoup(html, "lxml")
    js_urls = []
    for script in soup.find_all("script", src=True):
        src = script.get("src")
        if src and (".js" in src or "static" in src or "chunks" in src):
            js_urls.append(urljoin(base_url, src))
    return js_urls[:5]


def find_contact_links(html: str, base_url: str) -> List[str]:
    """Discover internal contact/about page URLs from home page HTML."""
    soup = BeautifulSoup(html, "lxml")
    found_urls = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        text = a.get_text(strip=True).lower()
        full_url = urljoin(base_url, href)

        if urlparse(full_url).netloc == urlparse(base_url).netloc:
            l_href = href.lower()
            if any(kw in text or kw in l_href for kw in CONTACT_KEYWORDS):
                found_urls.append(full_url)

    seen = set()
    unique = []
    for u in found_urls:
        if u not in seen and u.rstrip("/") != base_url.rstrip("/"):
            seen.add(u)
            unique.append(u)
    return unique[:10]


def scrape_website(item: dict, depth: int = 2) -> dict:
    """
    Scrape a website domain for emails, phones, addresses, social links, WHOIS details, and homepage Markdown.
    `item` dict contains: {"id": db_id, "url": target_url, "domain": target_domain}
    """
    start_time = time.time()
    scraped_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    db_id = item.get("id")
    website = item.get("url")
    domain = item.get("domain", "")

    base = website.rstrip("/")
    parsed = urlparse(base)
    if not parsed.scheme:
        base = "https://" + base
        parsed = urlparse(base)

    all_emails: set = set()
    all_phones: set = set()
    all_addresses: set = set()
    all_social: Dict[str, set] = {p: set() for p in ["linkedin", "facebook", "instagram", "twitter", "youtube", "github", "tiktok"]}
    crawled: set = set()
    js_crawled: set = set()
    error_msg = None
    homepage_markdown = ""
    # Cookies picked up from a headless-browser challenge solve (see
    # render_with_browser), if this site turns out to need one. Threaded
    # through every subsequent get_http() call below so contact-path
    # fetches ride on the same cleared session instead of hitting the
    # challenge again.
    site_cookies: Dict[str, str] = {}
    used_browser_fallback = False

    # Fetch WHOIS domain metadata (creation date, expiration date, age in days)
    # in the BACKGROUND, not before scraping starts. WHOIS_MAX_WORKERS caps
    # how many lookups run at once, so with many scraper threads running
    # concurrently, doing this lookup synchronously first would leave most
    # threads stuck waiting in line for a WHOIS turn before they'd even begin
    # the actual site fetch -- starving the real work. Starting it here and
    # joining just before the result is built lets the HTTP scraping proceed
    # immediately in parallel with (rather than after) the WHOIS wait.
    whois_result: Dict[str, Optional[str]] = {}

    def _run_whois():
        whois_result.update(get_domain_whois_info(domain or parsed.netloc))

    whois_thread = threading.Thread(target=_run_whois, daemon=True)
    whois_thread.start()

    def fetch_and_extract(url: str):
        nonlocal homepage_markdown
        if url in crawled:
            return
        crawled.add(url)
        time.sleep(random.uniform(*DELAY_RANGE))
        r = get_http(url, cookies=site_cookies or None)
        if r and "text" in r.headers.get("Content-Type", ""):
            # Extract homepage markdown text from the main homepage
            if not homepage_markdown and url.rstrip("/") == base.rstrip("/"):
                homepage_markdown = convert_html_to_markdown(r.text)

            all_emails.update(extract_emails_from_html(r.text))
            all_phones.update(extract_phones_from_html(r.text, domain=domain))
            all_addresses.update(extract_addresses_from_html(r.text))

            site_soc = extract_social_links_from_html(r.text, base)
            for p, links in site_soc.items():
                all_social[p].update(links)

            # Scan JS script bundles (for SPA / React / Vue)
            js_urls = extract_js_script_urls(r.text, base)
            for js_url in js_urls:
                if js_url not in js_crawled:
                    js_crawled.add(js_url)
                    js_r = get_http(js_url, cookies=site_cookies or None)
                    if js_r and js_r.status_code == 200:
                        all_emails.update(extract_emails_from_text(js_r.text))
                        all_phones.update(extract_phones_from_js(js_r.text, domain=domain))
                        all_addresses.update(extract_addresses_from_js(js_r.text))
                        js_soc = extract_social_links_from_text(js_r.text)
                        for p, links in js_soc.items():
                            all_social[p].update(links)

    # 1. Scrape Homepage
    homepage_r = get_http(base)
    if not homepage_r and not parsed.netloc.startswith("www."):
        alt_base = f"https://www.{parsed.netloc}"
        alt_r = get_http(alt_base)
        if alt_r:
            base = alt_base
            homepage_r = alt_r

    # 1b. Bot-challenge fallback: if the homepage came back empty/failed, OR
    # came back but is actually an anti-bot JS-challenge page rather than
    # real content, hand it off to a real headless browser ONCE. If that
    # clears the challenge, its cookies get reused for every subsequent
    # fetch on this site (contact paths, JS bundles) so we don't need to
    # spin up a browser again for the same site.
    homepage_is_challenge = homepage_r and is_bot_challenge_page(homepage_r.text, base)
    if SELENIUM_ENABLED and (homepage_r is None or homepage_is_challenge):
        rendered = render_with_browser(base)
        if rendered:
            rendered_html, rendered_cookies = rendered
            used_browser_fallback = True
            site_cookies.update(rendered_cookies)
            if not is_bot_challenge_page(rendered_html, base):
                homepage_r = _BrowserPage(rendered_html)
            # else: browser also saw the challenge and it never cleared in
            # time -- fall through to the normal failure handling below.

    if homepage_r and "text" in homepage_r.headers.get("Content-Type", ""):
        crawled.add(base)
        if not homepage_markdown:
            homepage_markdown = convert_html_to_markdown(homepage_r.text)

        all_emails.update(extract_emails_from_html(homepage_r.text))
        all_phones.update(extract_phones_from_html(homepage_r.text, domain=domain))
        all_addresses.update(extract_addresses_from_html(homepage_r.text))

        site_soc = extract_social_links_from_html(homepage_r.text, base)
        for p, links in site_soc.items():
            all_social[p].update(links)

        # Dynamic contact links
        dynamic_contacts = find_contact_links(homepage_r.text, base)
        for d_url in dynamic_contacts:
            fetch_and_extract(d_url)
    else:
        error_msg = ("Connection failed or non-HTML response (browser fallback also failed)"
                     if used_browser_fallback else "Connection failed or non-HTML response")

    # 2. Scrape Known Contact Paths
    for path in CONTACT_PATHS:
        fetch_and_extract(base + path)

    # Format output
    social_formatted = {p: ", ".join(sorted(links)) for p, links in all_social.items()}

    # Clean and limit addresses to top 3 concise physical addresses (longest first)
    clean_addrs = sorted(all_addresses, key=len, reverse=True)[:3]

    # By now the HTTP scraping above has likely taken longer than the WHOIS
    # lookup anyway, so this join is usually instant. WHOIS_TIMEOUT + a small
    # buffer is a hard ceiling so a single stuck registry can never hang this
    # site's result forever -- if it's still not done, we just report no
    # WHOIS data for this site rather than waiting indefinitely.
    whois_thread.join(timeout=WHOIS_TIMEOUT + 2)
    whois_info = whois_result if whois_result else {
        "domain_created_at": None, "domain_expires_at": None, "domain_age_days": None
    }

    elapsed_sec = round(time.time() - start_time, 2)
    has_data = bool(all_emails or all_phones or clean_addrs or any(all_social.values()))

    status = "completed" if has_data else ("failed" if error_msg else "no_data")

    return {
        "db_id": db_id,
        "website": website,
        "domain": domain,
        "timestamp": scraped_timestamp,
        "time_taken_sec": elapsed_sec,
        "domain_created_at": whois_info.get("domain_created_at"),
        "domain_expires_at": whois_info.get("domain_expires_at"),
        "domain_age_days": whois_info.get("domain_age_days"),
        "homepage_markdown": homepage_markdown,
        "emails": sorted(all_emails),
        "phones": sorted(all_phones),
        "addresses": clean_addrs,
        "social_links": social_formatted,
        "pages_crawled": len(crawled),
        "status": status,
        "error_message": error_msg,
    }


# ============================================================================
# SECTION 10: EXPORTER  (was exporter.py)
# ============================================================================

exporter_log = logging.getLogger("Exporter")


def export_to_csv(output_csv: str = DEFAULT_OUTPUT_CSV, flat_csv: str = DEFAULT_FLAT_CSV) -> Tuple[int, int]:
    """
    Export extracted results from SQLite database to emails.csv and emails_flat.csv.
    Returns tuple: (total_rows_exported, total_unique_emails)
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT w.url as website, w.scraped_at as timestamp, w.time_taken_sec, w.pages_crawled, w.status,
               w.domain_created_at, w.domain_expires_at, w.domain_age_days, w.homepage_markdown,
               r.email, r.phone, r.address, r.linkedin, r.facebook, r.instagram, r.twitter, r.youtube, r.github, r.tiktok
        FROM websites w
        LEFT JOIN extracted_results r ON w.id = r.website_id
        ORDER BY w.id ASC
    """)
    rows = cursor.fetchall()
    conn.close()

    headers = [
        "timestamp", "website",
        "email 1", "email 2", "email 3", "email 4", "email 5",
        "email 6", "email 7", "email 8", "email 9", "email 10",
        "phone", "address",
        "domain_created_at", "domain_expires_at", "domain_age_days", "homepage_markdown",
        "linkedin", "facebook", "instagram", "twitter", "youtube", "github", "tiktok",
        "pages_crawled", "time_taken_sec", "status"
    ]

    total_rows = 0
    unique_emails = set()

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for r in rows:
            ts = r["timestamp"] or ""
            web = r["website"] or ""
            em = r["email"] or ""
            email_values = extract_raw_emails_from_labeled(em)[:MAX_LABELED_EMAILS]
            email_values += [""] * (MAX_LABELED_EMAILS - len(email_values))
            ph = r["phone"] or ""
            addr = r["address"] or ""
            c_date = r["domain_created_at"] or ""
            e_date = r["domain_expires_at"] or ""
            d_age = r["domain_age_days"] if r["domain_age_days"] is not None else ""
            hp_md = r["homepage_markdown"] or ""
            li = r["linkedin"] or ""
            fb = r["facebook"] or ""
            ig = r["instagram"] or ""
            tw = r["twitter"] or ""
            yt = r["youtube"] or ""
            gh = r["github"] or ""
            tt = r["tiktok"] or ""
            pc = r["pages_crawled"] or 0
            tt_sec = r["time_taken_sec"] or 0.0
            st = r["status"] or ""

            writer.writerow([
                ts, web, *email_values, ph, addr, c_date, e_date, d_age, hp_md,
                li, fb, ig, tw, yt, gh, tt, pc, tt_sec, st
            ])
            total_rows += 1
            # `em` is now labeled text ("email 1: a@x.com, email 2: ...") --
            # pull the raw addresses back out so the unique-email count and
            # flat list stay correct.
            for raw_email in extract_raw_emails_from_labeled(em):
                unique_emails.add(raw_email)

    # Save flat list of unique emails
    with open(flat_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["email"])
        for e in sorted(unique_emails):
            writer.writerow([e])

    exporter_log.info(f"Exported {total_rows} rows to {output_csv}")
    exporter_log.info(f"Exported {len(unique_emails)} unique emails to {flat_csv}")
    return total_rows, len(unique_emails)


# Excel's hard cap on a single cell's text length. homepage_markdown can be
# much longer than this for content-heavy sites, so it gets truncated on the
# way into the .xlsx (the .csv export above has no such limit and keeps the
# full text).
EXCEL_CELL_CHAR_LIMIT = 32000


def _excel_safe(value):
    """Make a value safe to write into an Excel cell:
    1. Strip characters that are illegal in Excel's XML format (certain
       control characters -- NUL, vertical tab, form feed, etc.). Scraped
       web content occasionally contains these, and openpyxl raises
       IllegalCharacterError and aborts the ENTIRE export if even one cell
       has one -- so this must happen on every string before it's written.
    2. Truncate anything over Excel's per-cell character limit.
    """
    if not isinstance(value, str):
        return value
    value = openpyxl.cell.cell.ILLEGAL_CHARACTERS_RE.sub("", value)
    if len(value) > EXCEL_CELL_CHAR_LIMIT:
        value = value[:EXCEL_CELL_CHAR_LIMIT] + " ...[truncated, see CSV for full text]"
    return value


def export_to_xlsx(output_xlsx: str = DEFAULT_OUTPUT_XLSX) -> int:
    """Export extracted results from SQLite database to an .xlsx workbook.
    Same columns/rows as export_to_csv. Uses openpyxl's write-only mode so
    exporting tens of thousands of rows stays fast and memory-light."""
    from openpyxl import Workbook
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Font

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT w.url as website, w.scraped_at as timestamp, w.time_taken_sec, w.pages_crawled, w.status,
               w.domain_created_at, w.domain_expires_at, w.domain_age_days, w.homepage_markdown,
               r.email, r.phone, r.address, r.linkedin, r.facebook, r.instagram, r.twitter, r.youtube, r.github, r.tiktok
        FROM websites w
        LEFT JOIN extracted_results r ON w.id = r.website_id
        ORDER BY w.id ASC
    """)
    rows = cursor.fetchall()
    conn.close()

    headers = [
        "timestamp", "website",
        "email 1", "email 2", "email 3", "email 4", "email 5",
        "email 6", "email 7", "email 8", "email 9", "email 10",
        "phone", "address",
        "domain_created_at", "domain_expires_at", "domain_age_days", "homepage_markdown",
        "linkedin", "facebook", "instagram", "twitter", "youtube", "github", "tiktok",
        "pages_crawled", "time_taken_sec", "status"
    ]

    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title="Results")

    # In write-only mode, sheet-level view settings (freeze panes, column
    # widths) must be set BEFORE any rows are appended -- setting them
    # afterward silently fails to persist when the file is saved.
    fixed_widths = [19, 32, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 18, 30, 16, 16, 14, 50, 24, 24, 24, 24, 24, 24, 24, 12, 14, 12]
    for i, width in enumerate(fixed_widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = width
    ws.freeze_panes = "A2"

    bold = Font(bold=True)
    header_cells = []
    for h in headers:
        c = WriteOnlyCell(ws, value=h)
        c.font = bold
        header_cells.append(c)
    ws.append(header_cells)

    total_rows = 0
    skipped_rows = 0
    for r in rows:
        try:
            ws.append([
                _excel_safe(r["timestamp"] or ""),
                _excel_safe(r["website"] or ""),
                *[_excel_safe(e) for e in extract_raw_emails_from_labeled(r["email"] or "")[:MAX_LABELED_EMAILS]],
                *["" for _ in range(MAX_LABELED_EMAILS - len(extract_raw_emails_from_labeled(r["email"] or "")[:MAX_LABELED_EMAILS]))],
                _excel_safe(r["phone"] or ""),
                _excel_safe(r["address"] or ""),
                _excel_safe(r["domain_created_at"] or ""),
                _excel_safe(r["domain_expires_at"] or ""),
                r["domain_age_days"] if r["domain_age_days"] is not None else "",
                _excel_safe(r["homepage_markdown"] or ""),
                _excel_safe(r["linkedin"] or ""),
                _excel_safe(r["facebook"] or ""),
                _excel_safe(r["instagram"] or ""),
                _excel_safe(r["twitter"] or ""),
                _excel_safe(r["youtube"] or ""),
                _excel_safe(r["github"] or ""),
                _excel_safe(r["tiktok"] or ""),
                r["pages_crawled"] or 0,
                r["time_taken_sec"] or 0.0,
                _excel_safe(r["status"] or ""),
            ])
            total_rows += 1
        except Exception as e:
            # Belt-and-braces: _excel_safe() above should prevent this, but if
            # a row still fails for any reason, skip just that one row rather
            # than losing the entire export. Log only the site + error TYPE --
            # never str(e) here, since some openpyxl errors embed the full
            # offending cell text in their message, which could dump an
            # entire scraped webpage into the log.
            skipped_rows += 1
            exporter_log.warning(f"  Skipped one row in Excel export ({r['website']}): {type(e).__name__}")

    if skipped_rows:
        exporter_log.warning(f"{skipped_rows} row(s) skipped in Excel export due to unwritable content "
                              f"(full data for these is still in the CSV and the database).")

    wb.save(output_xlsx)
    exporter_log.info(f"Exported {total_rows} rows to {output_xlsx}")
    return total_rows


def export_to_json(output_json: str = DEFAULT_OUTPUT_JSON) -> int:
    """Export DB data to clean JSON format."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT w.id, w.url as website, w.domain, w.status, w.scraped_at, w.time_taken_sec, w.pages_crawled,
               r.email, r.phone, r.address, r.linkedin, r.facebook, r.instagram, r.twitter, r.youtube, r.github, r.tiktok
        FROM websites w
        LEFT JOIN extracted_results r ON w.id = r.website_id
        ORDER BY w.id ASC
    """)
    rows = cursor.fetchall()
    conn.close()

    site_map = {}
    for r in rows:
        wid = r["id"]
        if wid not in site_map:
            site_map[wid] = {
                "website": r["website"],
                "domain": r["domain"],
                "status": r["status"],
                "timestamp": r["scraped_at"],
                "time_taken_sec": r["time_taken_sec"],
                "pages_crawled": r["pages_crawled"],
                "emails": [],
                "phone": r["phone"],
                "address": r["address"],
                "social_links": {
                    "linkedin": r["linkedin"], "facebook": r["facebook"],
                    "instagram": r["instagram"], "twitter": r["twitter"],
                    "youtube": r["youtube"], "github": r["github"], "tiktok": r["tiktok"]
                }
            }
        # `r["email"]` is the labeled "email 1: ..., email 2: ..." string --
        # pull the raw addresses back out for the JSON list.
        if r["email"]:
            site_map[wid]["emails"].extend(extract_raw_emails_from_labeled(r["email"]))

    data = list(site_map.values())
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return len(data)


# ============================================================================
# SECTION 10b: RECHECK / BACKFILL EXISTING DATA (no re-scraping, no network)
# ============================================================================
# For rows that were already scraped and saved BEFORE the country-detection
# fix and the labeled-email format existed. This re-derives the phone
# country code and reformats the email column using data ALREADY in the
# database (address, homepage_markdown, domain) -- it never re-fetches any
# website, so it's instant and safe to run any time.

recheck_log = logging.getLogger("Recheck")


def recheck_and_fix_existing_data() -> Tuple[int, int]:
    """Fix phone country codes and email formatting on rows already saved
    to the database. Returns (phone_rows_fixed, email_rows_reformatted)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.id, w.domain, r.email, r.phone, r.address, r.homepage_markdown
        FROM extracted_results r
        JOIN websites w ON w.id = r.website_id
    """)
    rows = cursor.fetchall()

    updated_phones = 0
    updated_emails = 0

    for r in rows:
        domain = r["domain"] or ""
        # Same context a live scrape would have had available: the saved
        # address text plus the saved homepage markdown.
        context = " ".join(filter(None, [r["address"] or "", r["homepage_markdown"] or ""]))

        # --- Re-derive phone country codes ---
        old_phone_str = r["phone"] or ""
        if old_phone_str:
            raw_numbers = [p.strip() for p in old_phone_str.split(",") if p.strip()]
            fixed = []
            changed = False
            for num in raw_numbers:
                digits_only = re.sub(r"[^\d]", "", num)
                # Peel off whatever country code is currently on it so we
                # get back to the bare local number, then re-detect fresh.
                if digits_only.startswith("91") and len(digits_only) == 12:
                    bare = digits_only[2:]
                elif digits_only.startswith("1") and len(digits_only) == 11:
                    bare = digits_only[1:]
                else:
                    bare = digits_only

                if len(bare) == 10:
                    recleaned = clean_phone_number(bare, context=context, domain=domain)
                    if recleaned and recleaned != num:
                        changed = True
                    fixed.append(recleaned or num)
                else:
                    # Not a re-derivable 10-digit local number (already
                    # international, extension, etc.) -- leave it as-is.
                    fixed.append(num)

            new_phone_str = ", ".join(dict.fromkeys(fixed))  # de-dupe, keep order
            if changed and new_phone_str != old_phone_str:
                cursor.execute("UPDATE extracted_results SET phone = ? WHERE id = ?", (new_phone_str, r["id"]))
                updated_phones += 1

        # --- Reformat email column into "email 1: ..., email 2: ..." ---
        old_email_str = r["email"] or ""
        if old_email_str:
            raw_emails = extract_raw_emails_from_labeled(old_email_str)
            if raw_emails:
                new_email_str = format_emails_labeled(raw_emails)
                if new_email_str != old_email_str:
                    cursor.execute("UPDATE extracted_results SET email = ? WHERE id = ?", (new_email_str, r["id"]))
                    updated_emails += 1

    conn.commit()
    conn.close()
    recheck_log.info(f"Recheck complete: fixed phone country code on {updated_phones} row(s), "
                      f"reformatted email column on {updated_emails} row(s).")
    return updated_phones, updated_emails


# ============================================================================
# SECTION 11: RUNNER  (was run.py)
# ============================================================================

def read_and_consume_urls_from_file(file_path: str) -> List[str]:
    """Read website URLs from CSV file, load into list, and clear the file
    (reset to just the header) after ingestion -- but ONLY if file_path is
    this project's own internal input/urls.csv 'drop zone'. If it points
    somewhere else (e.g. a master cleaned list you maintain yourself), it is
    left completely untouched -- clearing someone's real data file because
    they pointed the tool at it would be destructive and surprising."""
    urls = []
    if not os.path.exists(file_path):
        return urls

    # Only the project's own input/urls.csv is treated as disposable.
    is_internal_drop_zone = os.path.normcase(os.path.abspath(file_path)) == \
        os.path.normcase(os.path.abspath(DEFAULT_INPUT_CSV)) and \
        os.path.normcase(os.path.dirname(os.path.abspath(file_path))) == \
        os.path.normcase(os.path.abspath(INPUT_DIR))

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.lower().startswith("website") or line.lower().startswith("url"):
                    continue
                parts = [p.strip() for p in line.split(",") if p.strip()]
                for part in parts:
                    if "." in part or part.startswith("http"):
                        urls.append(part)
                        break

        if urls and is_internal_drop_zone:
            # Clear input file after reading so new URLs can be dropped in anytime
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("website\n")
            log.info(f"Read {len(urls)} URLs from '{file_path}' and cleared it.")
        elif urls:
            log.info(f"Read {len(urls)} URLs from '{file_path}'. "
                      f"It's an external file, so it was left untouched.")
    except Exception as e:
        log.error(f"Failed to read input file {file_path}: {e}")
    return urls


def run_batch_processor(threads: int = DEFAULT_THREADS, depth: int = DEFAULT_DEPTH, batch_size: int = 20,
                         output_csv: str = DEFAULT_OUTPUT_CSV, flat_csv: str = DEFAULT_FLAT_CSV,
                         output_xlsx: str = DEFAULT_OUTPUT_XLSX):
    """Run continuous batch processor pool pulling pending items from SQLite queue.

    The CSV/XLSX files are kept up to date automatically as this runs -- a
    snapshot is written periodically (every EXPORT_EVERY_N completions, at
    most once every EXPORT_MIN_INTERVAL_SEC), and always one more time right
    before exiting, whether that's a normal finish OR a Ctrl+C interrupt. So
    stopping the script at any point still leaves current CSV/XLSX files
    behind -- and since the export always reads the FULL database, re-running
    afterward naturally picks up both the old results and everything new,
    with no manual merging needed.
    """
    log.info(f"Starting continuous batch processor with {threads} worker threads...")

    stats = get_queue_stats()
    total_pending = stats["pending"]
    if total_pending == 0:
        log.info("No pending websites in queue to process.")
        return

    pbar = tqdm(total=total_pending, desc="Scraping Queue", unit="site")
    pool = ThreadPoolExecutor(max_workers=threads)

    def write_snapshot():
        try:
            export_to_csv(output_csv, flat_csv)
            export_to_xlsx(output_xlsx)
        except Exception as e:
            # Truncate defensively -- some exception types (like openpyxl's
            # IllegalCharacterError) embed the full offending string in their
            # message, which could otherwise dump an entire scraped webpage
            # into the log for what should be a one-line warning.
            msg = str(e)
            if len(msg) > 300:
                msg = msg[:300] + "...[truncated]"
            log.warning(f"Snapshot export failed ({type(e).__name__}: {msg}) -- will retry at the next checkpoint.")

    completed_since_export = 0
    last_export_time = time.time()

    try:
        active_futures = {}
        while True:
            # Dynamically top up worker pool queue
            slots = (threads * 2) - len(active_futures)
            if slots > 0:
                batch = get_pending_batch(batch_size=slots)
                for item in batch:
                    f = pool.submit(scrape_website, item, depth)
                    active_futures[f] = item

            if not active_futures:
                break

            # Process finished tasks as soon as they complete
            done_futures = [f for f in list(active_futures.keys()) if f.done()]
            if not done_futures:
                time.sleep(0.1)
                continue

            for future in done_futures:
                item = active_futures.pop(future)
                try:
                    result = future.result()
                    save_scraping_result(result)

                    site = result["website"]
                    em_cnt = len(result["emails"])
                    ph_cnt = len(result["phones"])
                    addr_cnt = len(result["addresses"])
                    dur = result["time_taken_sec"]
                    status = result["status"]

                    if status == "completed":
                        log.info(f"✓ {site} → {em_cnt} email(s), {ph_cnt} phone(s), {addr_cnt} address(es) [{dur}s]")
                    elif status == "no_data":
                        log.info(f"⚪ {site} → no contact info [{dur}s]")
                    else:
                        log.warning(f"✗ {site} → failed: {result.get('error_message')} [{dur}s]")

                except Exception as e:
                    log.error(f"Exception processing {item['url']}: {e}")
                    save_scraping_result({
                        "db_id": item["id"],
                        "website": item["url"],
                        "domain": item["domain"],
                        "status": "failed",
                        "error_message": str(e),
                        "time_taken_sec": 0.0,
                        "emails": [],
                        "phones": [],
                        "addresses": [],
                        "social_links": {},
                        "pages_crawled": 0
                    })

                pbar.update(1)
                completed_since_export += 1

            # Periodic snapshot: every EXPORT_EVERY_N completions, but not
            # more often than EXPORT_MIN_INTERVAL_SEC apart (re-dumping the
            # whole table too frequently would just slow the scrape down).
            if (completed_since_export >= EXPORT_EVERY_N and
                    time.time() - last_export_time >= EXPORT_MIN_INTERVAL_SEC):
                log.info(f"📄 Writing CSV/XLSX snapshot ({completed_since_export} new results since last save)...")
                write_snapshot()
                completed_since_export = 0
                last_export_time = time.time()

        pool.shutdown(wait=True)

    except KeyboardInterrupt:
        log.warning("\n[CTRL+C] Interrupted by user! All progress saved in SQLite database.")
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        pbar.close()
        log.info("Writing final CSV/XLSX snapshot before exiting...")
        try:
            write_snapshot()
        except KeyboardInterrupt:
            # A second Ctrl+C landed mid-export. Your scraped data is still
            # 100% safe in the database either way -- only this particular
            # CSV/XLSX snapshot may be incomplete or missing this time.
            # Re-running --export-only (or just running the scraper again)
            # will produce a complete, correct snapshot from everything
            # that's actually in the database.
            log.warning("Second interrupt received during export -- stopped immediately. "
                        "Your scraped data is safe in the database regardless. "
                        "Run with --export-only any time to get a fresh, complete CSV/XLSX.")
        sys.exit(0)
    finally:
        try:
            pbar.close()
        except Exception:
            pass

    # Final stats report + one last snapshot so the files reflect everything
    final_stats = get_queue_stats()
    log.info("─" * 50)
    log.info(f"Queue Status: Total={final_stats['total']} | Completed={final_stats['completed']} | No Data={final_stats['no_data']} | Failed={final_stats['failed']} | Pending={final_stats['pending']}")
    log.info("Writing final CSV/XLSX snapshot...")
    try:
        write_snapshot()
    except KeyboardInterrupt:
        log.warning("Interrupted during final export. Your scraped data is safe in the database. "
                    "Run with --export-only any time to get a fresh, complete CSV/XLSX.")


def main():
    parser = argparse.ArgumentParser(description="Production SQLite Bulk Email & Contact Scraper Engine (single-file edition)")
    parser.add_argument("--input", default=DEFAULT_INPUT_CSV, help="Input CSV/TXT file with website URLs (defaults to input/urls.csv inside this project)")
    parser.add_argument("--output", default=None,
                         help="Output CSV file path. If omitted, this is derived automatically from --input: "
                              "the project's own input/ folder for the internal drop zone, or a sibling "
                              "'NL_EMAIL_DATA' folder next to an external input file's folder.")
    parser.add_argument("--xlsx", default=None,
                         help="Output Excel (.xlsx) file path. Auto-derived from --input if omitted (see --output).")
    parser.add_argument("--flat", default=None,
                         help="Output flat email list CSV file path. Auto-derived from --input if omitted (see --output).")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS, help="Number of concurrent scraper threads")
    parser.add_argument("--depth", type=int, default=DEFAULT_DEPTH, help="Crawl depth per website (default: 2)")
    parser.add_argument("--batch-size", type=int, default=20, help="DB batch size per thread pool loop")
    parser.add_argument("--export", action="store_true",
                         help="(No longer needed for a final export -- CSV/Excel are now written "
                              "automatically as the scraper runs and again when it stops. Kept for "
                              "backward compatibility; has no additional effect.)")
    parser.add_argument("--export-only", action="store_true", help="Export existing SQLite database to CSV + Excel without scraping")
    parser.add_argument("--recheck-existing", action="store_true",
                         help="One-time, network-free fix for rows scraped BEFORE the phone "
                              "country-code fix and labeled-email format existed. Re-derives "
                              "each phone's country code and reformats the email column using "
                              "data already in the database (no re-scraping). Writes a fresh "
                              "CSV/Excel snapshot afterward, then exits.")
    parser.add_argument("--no-whois", action="store_true", help="Skip domain age/creation date lookups (a bit faster)")
    parser.add_argument("--assume-country", choices=["in", "ca", "us"], default="ca",
                         help="When a phone number gives NO signal at all (no domain/address/area-code "
                              "match -- e.g. an 800/855/866 toll-free number on a plain .com site), this "
                              "sets which country to guess. Default 'ca' fits this project's all-Canadian "
                              "CANGMB lead list, so ambiguous numbers default to +1 instead of +91. Pass "
                              "--assume-country in to restore the original India-default behavior.")
    parser.add_argument("--no-retry", action="store_true",
                         help="By default, every run automatically requeues websites marked 'failed' or "
                              "'no_data' from previous runs so they get scraped again. Pass this to skip "
                              "that and only process brand-new URLs from the input file.")
    parser.add_argument("--no-selenium", action="store_true",
                         help="Disable the headless-browser fallback for anti-bot JS-challenge pages "
                              "(Cloudflare 'Just a moment...', 'sgcaptcha' Robot Challenge Screen, etc.). "
                              "Use this if Selenium/Chrome isn't installed on this machine.")
    args = parser.parse_args()

    if args.no_whois:
        global WHOIS_ENABLED
        WHOIS_ENABLED = False
        log.info("WHOIS lookups disabled - domain_age_days will be empty.")

    global FALLBACK_COUNTRY
    FALLBACK_COUNTRY = args.assume_country
    if FALLBACK_COUNTRY != "in":
        log.info(f"Ambiguous phone numbers with no other signal will default to +{FALLBACK_COUNTRY_CODES[FALLBACK_COUNTRY]} ({FALLBACK_COUNTRY.upper()}).")

    if args.no_selenium:
        global SELENIUM_ENABLED
        SELENIUM_ENABLED = False
        log.info("Headless-browser fallback disabled - bot-challenge pages will be reported as failed.")

    # 0. Resolve where output lives for THIS run, based on the actual
    # --input path given (see compute_output_dir). This is what makes the
    # output folder automatically track the input: point --input at a new
    # ...\NL_OUTPUT\<subfolder>\clean_urls.csv and results land in that same
    # ...\NL_OUTPUT\NL_EMAIL_DATA\ folder next time, with no other changes.
    global DB_PATH
    run_output_dir = compute_output_dir(args.input)
    os.makedirs(run_output_dir, exist_ok=True)
    DB_PATH = os.path.join(run_output_dir, "scraper.db")

    output_csv = args.output or os.path.join(run_output_dir, "emails.csv")
    output_xlsx = args.xlsx or os.path.join(run_output_dir, "emails.xlsx")
    flat_csv = args.flat or os.path.join(run_output_dir, "emails_flat.csv")

    if run_output_dir != INPUT_DIR:
        log.info(f"Input: {args.input}")
        log.info(f"Output folder for this run: {run_output_dir}")

    # 1. Initialize SQLite Database & reset interrupted tasks
    init_db()
    reset_interrupted_tasks()

    # 1b. AUTOMATIC RETRY: every run, before doing anything else, requeue any
    # website still sitting in 'failed' (connection error/timeout/non-HTML)
    # or 'no_data' (loaded fine, nothing extracted) status back to 'pending'
    # so it gets scraped again THIS run -- no flags, no manual re-entry of
    # anything. This is exactly what covers a site like muckyduckpub.ca that
    # opens fine in a browser but happened to fail the scraper's one attempt.
    # Pass --no-retry if you ever want to skip this and only touch new URLs.
    if not args.no_retry:
        requeued = requeue_by_status(["failed", "no_data"])
        if requeued:
            log.info(f"Auto-retry: requeued {requeued} previously failed/no-data website(s) "
                      f"back to pending for another attempt this run.")

    if args.recheck_existing:
        recheck_and_fix_existing_data()
        log.info("Writing refreshed CSV/XLSX snapshot with corrected data...")
        export_to_csv(output_csv, flat_csv)
        export_to_xlsx(output_xlsx)
        return

    if args.export_only:
        export_to_csv(output_csv, flat_csv)
        export_to_xlsx(output_xlsx)
        return

    # 2. Ingest URLs into SQLite Queue & clear input file
    urls = read_and_consume_urls_from_file(args.input)
    if urls:
        added = load_urls_into_queue(urls)
        already_known = len(urls) - added
        log.info(f"Queue update: {added} brand-new URL(s) added, "
                 f"{already_known} already tracked from a previous run (left untouched).")
    else:
        log.info(f"No URLs found in '{args.input}'. Paste one website per line into that file and run again.")

    # 3. Process Batch Queue (saves results into SQLite DB).
    #    CSV/XLSX snapshots are written automatically along the way and
    #    again whenever this stops (normal finish OR Ctrl+C) -- see
    #    run_batch_processor / write_snapshot.
    run_batch_processor(threads=args.threads, depth=args.depth, batch_size=args.batch_size,
                         output_csv=output_csv, flat_csv=flat_csv, output_xlsx=output_xlsx)


if __name__ == "__main__":
    main()