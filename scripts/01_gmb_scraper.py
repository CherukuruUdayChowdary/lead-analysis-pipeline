#!/usr/bin/env python3
"""
Google Maps Scraper – production version
- Fully resumable (stops anywhere → continues from last listing)
- Format-aware KEYWORD INPUT: INPUT_FILE can be .xlsx, .xls, .csv, or
  .txt, in any folder.
- OUTPUT / PROGRESS / STATUS / BACKUPS are ALWAYS plain CSV now (see
  CHANGELOG #10 below) — this is what makes them corruption-resistant.
  They're created automatically in the SAME folder as the input file.
- Designed for 200+ keywords
- Every scraped row is appended to disk IMMEDIATELY (not batched) using
  an append-only, fsync'd write, plus a periodic full-snapshot backup
  every CHECKPOINT_EVERY records (see CHANGELOG #10)
- Checkpoint/backup counting is GLOBAL across the whole run (shared across
  every keyword and every worker thread, guarded by FILE_LOCK) — not
  per-keyword — so backups actually happen every CHECKPOINT_EVERY rows
  regardless of how many listings any single keyword has.
- Backups are ROTATED: only the most recent MAX_BACKUPS_KEEP backup files
  are kept in BACKUP_DIR. Every time a new backup is written, the oldest
  one(s) beyond that limit are deleted — so once a 6th backup is created,
  the 1st (oldest) one is removed automatically. See cleanup_old_backups().
- "Scraped At" is the FIRST column
- Auto-recovers from "Timed out receiving message from renderer" by
  restarting the browser and retrying the keyword before giving up
- Stealth: each keyword gets its own fresh Chrome instance with a
  randomized fingerprint (user agent, viewport, GPU strings, etc.)

CHANGELOG (this revision):
1. Removed the `webdriver_manager` / `ChromeDriverManager` dependency
   entirely. Selenium 4.6+ ships its own "Selenium Manager" that finds
   or downloads the matching chromedriver automatically.
2. Fixed the "opening Indian Maps" problem via explicit hl/gl params,
   blocked GPS geolocation, and pinned Accept-Language. (IP-based
   geolocation still needs a proxy in the target country — see
   PROXY_SERVER below.)
3. Added a PROXY_SERVER config slot.
4. MAPS_COUNTRY set to "au" (Australia) for this project.
5. INPUT_FILE can be a multi-sheet workbook with header auto-detection
   and sheet-name fallback.
6. The script never writes anything back into INPUT_FILE — it's
   treated as read-only. Resume status lives only in STATUS_FILE.
7. Added email extraction from each listing's website (SCRAPE_EMAILS).
8. Backup rotation: BACKUP_DIR only ever holds the last
   MAX_BACKUPS_KEEP backup files.
9. Reconfigured for the Australia (AUSGMB) project (filenames, gl=au).

10. **THIS REVISION — fixes the data-corruption / data-loss bug you hit
    on restart:**
      a) OUTPUT_FILE, PROGRESS_FILE, STATUS_FILE, and every backup are
         now ALWAYS plain CSV, regardless of what format INPUT_FILE is
         in. This matters because .xlsx is a zip archive under the
         hood — if a write is interrupted (crash, kill, power loss)
         partway through, the WHOLE file becomes unreadable, not just
         the last bit. CSV degrades gracefully: a truncated write only
         damages the last row; every row written before that stays
         intact and readable.
      b) Every write that touches an EXISTING file (STATUS_FILE,
         PROGRESS_FILE via legacy save_*, and every backup) is now
         ATOMIC: it's written to a temp file in the same folder first,
         fsync'd, and only then swapped into place with os.replace().
         os.replace() is atomic on both Windows and POSIX — the real
         file is always either fully the old version or fully the new
         version, NEVER a half-written mix, even if the process is
         killed mid-write.
      c) OUTPUT_FILE and PROGRESS_FILE are no longer rewritten from
         scratch at every checkpoint. Each new row is APPENDED (and
         fsync'd) the moment it's scraped. This means previously-
         written rows are never touched/rewritten again, so there is
         no window where a crash can wipe out earlier data — at worst
         you lose the one row that was mid-write, never anything
         before it.
      d) Loading is now corruption-AWARE instead of silently swallowing
         errors: if an existing STATUS/PROGRESS/OUTPUT file fails to
         parse cleanly, the script tries to recover as many rows as
         possible (skipping only the bad ones), logs exactly what
         happened, and copies the original file aside as a `.bak`
         before touching it again — instead of the old behavior, which
         silently treated ANY read error as "no previous data" and
         quietly started over from zero.
      e) Added FORCE_RERUN_ALL (default True, per your request): every
         keyword will be re-scraped from scratch on the next run. Any
         existing OUTPUT/PROGRESS/STATUS files are not deleted — they
         are moved into BACKUP_DIR with a timestamp first, so nothing
         already collected is lost, and then the run starts clean.
         Set this back to False once you've done this one full re-run,
         or every future run will keep starting over.

Usage:
    Edit INPUT_FILE below to point at your keyword list (.xlsx, .xls,
    .csv, or .txt, in any folder), then run:
        python gmb.py
"""

import os
import re
import csv
import time
import random
import shutil
import tempfile
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)

# Optional: pip install selenium-stealth for extra fingerprint hardening.
# The script works fine without it — falls back to manual CDP-based stealth.
try:
    from selenium_stealth import stealth as selenium_stealth_apply
    SELENIUM_STEALTH_AVAILABLE = True
except ImportError:
    SELENIUM_STEALTH_AVAILABLE = False

# ──────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────
# INPUT_FILE can be an .xlsx, .xls, .csv, or .txt file (one keyword per
# line for .txt), in any folder — just edit the path on the line below.
# Output, progress, status, and backups are all created automatically in
# the SAME folder as INPUT_FILE, and are ALWAYS written as plain CSV
# now (see CHANGELOG #10) regardless of what format INPUT_FILE is in.
INPUT_FILE          = r"C:\Users\Administrator\Documents\OLX\AUSGMB\AusGMBkeywords.xlsx"
INPUT_COLUMN        = "Search Keyword"

# Which worksheet tab to read keywords from when INPUT_FILE is a
# multi-sheet .xlsx/.xls workbook. Set to None to just use the file's
# first/active sheet — the auto-detect fallback in read_input_table
# will find the right sheet either way.
INPUT_SHEET_NAME = None

# ── FORCE EVERY KEYWORD TO RUN AGAIN ────────────────────────
# Set to True (as requested) to ignore ALL existing progress/status and
# re-scrape EVERY keyword from scratch on the next run. Nothing is
# deleted — any existing OUTPUT_FILE/PROGRESS_FILE/STATUS_FILE are
# moved into BACKUP_DIR with a timestamp first (see
# archive_existing_state_files()), then the run starts clean.
#
# IMPORTANT: set this back to False after this one run, or every future
# run will keep re-scraping everything from zero instead of resuming.
FORCE_RERUN_ALL = False

# ── Localization ────────────────────────────────────────────
# hl = UI language, gl = country bias Google is told to assume.
# gl must be a 2-letter ISO 3166-1 alpha-2 code.
MAPS_LANGUAGE = "en"
MAPS_COUNTRY  = "au"

# ── Proxy (optional but required for true country accuracy) ───────────
# Format: "http://user:pass@host:port" or "http://host:port". Leave as
# "" to run with your real IP.
PROXY_SERVER = ""

# ── Email extraction ────────────────────────────────────────
SCRAPE_EMAILS       = True
EMAIL_FETCH_TIMEOUT = 8   # seconds, per request

# ── Backup rotation ─────────────────────────────────────────
MAX_BACKUPS_KEEP = 5

_input_path = Path(INPUT_FILE).expanduser().resolve()
INPUT_EXT = _input_path.suffix.lower()
if INPUT_EXT not in (".xlsx", ".xls", ".csv", ".txt"):
    raise ValueError(
        f"Unsupported input file format '{INPUT_EXT}' for {_input_path}. "
        f"Use a .xlsx, .xls, .csv, or .txt file."
    )
INPUT_DIR  = _input_path.parent

# OUTPUT_EXT is now ALWAYS .csv (CHANGELOG #10a) — no longer derived
# from INPUT_EXT. This is what makes these files corruption-resistant:
# unlike .xlsx (a zip archive that becomes entirely unreadable if a
# write is interrupted), a CSV that's cut off mid-write still has every
# earlier row intact and parseable.
OUTPUT_EXT = ".csv"

INPUT_FILE    = str(_input_path)
OUTPUT_FILE   = str(INPUT_DIR / f"ausgmb_output{OUTPUT_EXT}")        # final combined results
PROGRESS_FILE = str(INPUT_DIR / f"progressausgmb{OUTPUT_EXT}")      # keyword + href already done
STATUS_FILE   = str(INPUT_DIR / f"statusausgmb{OUTPUT_EXT}")        # keyword → Completed / Not Completed
BACKUP_DIR    = str(INPUT_DIR / "backupsausgmb")                    # folder for timestamped backups

HEADLESS            = True
NUM_WORKERS         = 3
PAGE_LOAD_TIMEOUT   = 60
WAIT_SECONDS        = 14
DELAY_BETWEEN_SEARCHES  = 4
DELAY_BETWEEN_LISTINGS  = 2.2
SCROLL_PAUSE_SECONDS    = 1.6
MAX_SCROLL_ATTEMPTS     = 400
MAX_CLICK_ATTEMPTS      = 8
CLICK_RETRY_BACKOFF     = 0.8

# CHECKPOINT_EVERY now only controls how often a full-snapshot TIMESTAMPED
# BACKUP is taken (extra safety net on top of the per-row appends, and
# useful for rollback/rotation). It no longer controls the main
# OUTPUT_FILE/PROGRESS_FILE writes — those are appended immediately, per
# row, as soon as each listing is scraped (see CHANGELOG #10c).
#
# NOTE: CHECKPOINT_EVERY counts rows GLOBALLY across the whole run (every
# keyword, every worker thread) via CHECKPOINT_COUNTER below — not per
# keyword.
CHECKPOINT_EVERY        = 200                # take a full-snapshot backup
                                              # every N new rows
PROGRESS_PRINT_EVERY    = 25                 # print a terminal progress line
                                              # every N records, independent
                                              # of CHECKPOINT_EVERY

DRIVER_RETRIES_PER_KEYWORD = 2               # extra attempts with a fresh browser
                                              # if the driver crashes/hangs

NOT_AVAILABLE = "Not available"

DAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]

COLUMN_ORDER = [
    "Scraped At",           # ← FIRST COLUMN
    "Search Term", "Title", "Rating", "Reviews Count", "Location", "Timings",
    "Mobile Number", "Website", "Email", "Instagram", "Facebook", "YouTube",
    "Other Social Links", "Time Taken (sec)", "Status", "Google Maps URL",
]

# ──────────────────────────────────────────────────────────────
# SELECTORS (update only here if Google changes DOM)
# ──────────────────────────────────────────────────────────────
SELECTORS = {
    "search_box": "input[name='q']",
    "results_list": "div[role='feed']",
    "result_item": "div[role='feed'] a[href*='/maps/place/']",
    "panel": "div[role='main']",
    "name": "h1.DUwDvf.lfPIob, h1.DUwDvf, h1.lfPIob, div[role='main'] h1, h1",
    "rating": "div.F7nice span[aria-hidden='true']",
    "reviews_count": "div.F7nice span[aria-label*='review'], div.F7nice button span[aria-label*='review'], div.F7nice span.UY7F9",
    "hours_button": "button[data-item-id='oh']",
    "hours_summary": "button[data-item-id='oh'] .Io6YTe",
    "hours_expanded_container": "table, div.eK4R0e, div.y0skZc",
    "website_link": "a[data-item-id='authority']",
    "website_text": "a[data-item-id='authority'] .Io6YTe",
    "phone": "button[data-item-id^='phone'] .Io6YTe",
    "address": "button[data-item-id='address'] .Io6YTe",
    "all_panel_links": "a[href]",
}

SOCIAL_DOMAIN_MAP = {
    "instagram.com": "Instagram",
    "facebook.com": "Facebook",
    "fb.com": "Facebook",
    "youtube.com": "YouTube",
    "youtu.be": "YouTube",
}

OTHER_SOCIAL_DOMAINS = [
    "twitter.com", "x.com", "linkedin.com", "tiktok.com",
    "pinterest.com", "wa.me", "whatsapp.com", "snapchat.com", "threads.net",
]

TRANSIENT_DOM_EXCEPTIONS = (NoSuchElementException, StaleElementReferenceException)

# ──────────────────────────────────────────────────────────────
# STEALTH / FINGERPRINT POOLS
# ──────────────────────────────────────────────────────────────
USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

VIEWPORT_POOL = [
    (1920, 1080), (1536, 864), (1440, 900), (1366, 768), (1600, 900),
]

PLATFORM_POOL = ["Win32", "MacIntel", "Linux x86_64"]
VENDOR_POOL = ["Google Inc.", "Google Inc. (NVIDIA)", "Google Inc. (Intel)"]
WEBGL_VENDOR_POOL = ["Intel Inc.", "NVIDIA Corporation", "Google Inc. (Intel)"]
WEBGL_RENDERER_POOL = [
    "Intel Iris OpenGL Engine",
    "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Direct3D11 vs_5_0 ps_5_0)",
    "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)",
]

# ──────────────────────────────────────────────────────────────
# GLOBAL LOCKS + SHARED RUN-WIDE STATE
# ──────────────────────────────────────────────────────────────
FILE_LOCK  = threading.Lock()
PRINT_LOCK = threading.Lock()

CHECKPOINT_COUNTER = 0

def log(msg: str):
    with PRINT_LOCK:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def short_error(e):
    text = str(e).strip()
    first = text.split("\n")[0].strip()
    return first if first else type(e).__name__

# ──────────────────────────────────────────────────────────────
# DRIVER
# ──────────────────────────────────────────────────────────────
def build_driver():
    options = Options()

    user_agent = random.choice(USER_AGENT_POOL)
    width, height = random.choice(VIEWPORT_POOL)
    platform = random.choice(PLATFORM_POOL)
    vendor = random.choice(VENDOR_POOL)
    webgl_vendor = random.choice(WEBGL_VENDOR_POOL)
    webgl_renderer = random.choice(WEBGL_RENDERER_POOL)

    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument(f"--window-size={width},{height}")
    else:
        options.add_argument(f"--window-size={width},{height}")

    options.add_argument(f"--user-agent={user_agent}")
    options.add_argument(f"--lang={MAPS_LANGUAGE}")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-background-timer-throttling")

    if PROXY_SERVER:
        options.add_argument(f"--proxy-server={PROXY_SERVER}")

    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.geolocation": 2,
        "intl.accept_languages": f"{MAPS_LANGUAGE},en-US,en",
    })

    service = Service()  # Selenium Manager auto-resolves the driver binary
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    apply_stealth(
        driver,
        user_agent=user_agent,
        platform=platform,
        vendor=vendor,
        webgl_vendor=webgl_vendor,
        webgl_renderer=webgl_renderer,
    )

    return driver


def apply_stealth(driver, user_agent, platform, vendor, webgl_vendor, webgl_renderer):
    if SELENIUM_STEALTH_AVAILABLE:
        try:
            selenium_stealth_apply(
                driver,
                languages=[MAPS_LANGUAGE, "en-US", "en"],
                vendor=vendor,
                platform=platform,
                webgl_vendor=webgl_vendor,
                renderer=webgl_renderer,
                fix_hairline=True,
            )
            return
        except Exception as e:
            log(f"[warn] selenium-stealth failed, falling back to manual patch: {short_error(e)}")

    stealth_js = f"""
        Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
        Object.defineProperty(navigator, 'languages', {{get: () => ['{MAPS_LANGUAGE}', 'en-US', 'en']}});
        Object.defineProperty(navigator, 'plugins', {{get: () => [1, 2, 3, 4, 5]}});
        Object.defineProperty(navigator, 'platform', {{get: () => '{platform}'}});
        window.chrome = window.chrome || {{ runtime: {{}} }};
        const originalQuery = window.navigator.permissions ? window.navigator.permissions.query : null;
        if (originalQuery) {{
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications'
                    ? Promise.resolve({{ state: Notification.permission }})
                    : originalQuery(parameters)
            );
        }}
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {{
            if (parameter === 37445) return '{webgl_vendor}';
            if (parameter === 37446) return '{webgl_renderer}';
            return getParameter.apply(this, [parameter]);
        }};
    """
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument", {"source": stealth_js}
        )
    except Exception as e:
        log(f"[warn] manual stealth patch failed (continuing without it): {short_error(e)}")

# ──────────────────────────────────────────────────────────────
# SAFE ELEMENT HELPERS
# ──────────────────────────────────────────────────────────────
def safe_text(driver, css):
    try:
        el = driver.find_element(By.CSS_SELECTOR, css)
        return el.text.strip()
    except TRANSIENT_DOM_EXCEPTIONS:
        return ""

def safe_js_text(driver, css):
    try:
        el = driver.find_element(By.CSS_SELECTOR, css)
        text = driver.execute_script("return arguments[0].textContent;", el)
        return (text or "").strip()
    except TRANSIENT_DOM_EXCEPTIONS:
        return ""

def safe_attr(driver, css, attr):
    try:
        el = driver.find_element(By.CSS_SELECTOR, css)
        return (el.get_attribute(attr) or "").strip()
    except TRANSIENT_DOM_EXCEPTIONS:
        return ""

def or_na(value):
    if value is None:
        return NOT_AVAILABLE
    value = str(value).strip()
    return value if value else NOT_AVAILABLE

# ──────────────────────────────────────────────────────────────
# TIMINGS
# ──────────────────────────────────────────────────────────────
def normalize_time_range(text):
    if not text:
        return ""
    t = text.strip()
    t = t.replace("\u2009", " ").replace("\u202f", " ")
    t = re.sub(r":00", "", t)
    t = re.sub(r"\s*(to|-|\u2013|\u2014)\s*", "–", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\bAM\b", "am", t)
    t = re.sub(r"\bPM\b", "pm", t)
    return t

def get_timings(driver):
    try:
        today_name = datetime.now().strftime("%A")
        day_hours = {}

        try:
            btn = driver.find_element(By.CSS_SELECTOR, SELECTORS["hours_button"])
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1.4)

            container_text = ""
            for sel in SELECTORS["hours_expanded_container"].split(", "):
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    candidate = el.text.strip()
                    if candidate:
                        container_text = candidate
                        break
                except TRANSIENT_DOM_EXCEPTIONS:
                    continue

            if container_text:
                lines = [l.strip() for l in container_text.split("\n") if l.strip()]
                day_positions = [(i, line) for i, line in enumerate(lines) if line in DAY_NAMES]
                for idx, (line_no, day) in enumerate(day_positions):
                    end = day_positions[idx + 1][0] if idx + 1 < len(day_positions) else len(lines)
                    hours_text = " ".join(lines[line_no + 1:end]).strip()
                    if hours_text:
                        day_hours[day] = hours_text

            try:
                driver.execute_script("arguments[0].click();", btn)
            except Exception:
                pass
        except TRANSIENT_DOM_EXCEPTIONS:
            pass

        if day_hours:
            open_days = {d: h for d, h in day_hours.items() if h.lower() != "closed"}
            unique = set(open_days.values())
            if len(unique) == 1:
                return normalize_time_range(next(iter(unique)))
            if today_name in day_hours:
                return normalize_time_range(day_hours[today_name])

        summary = safe_text(driver, SELECTORS["hours_summary"])
        return normalize_time_range(summary.replace("\n", " ").strip())
    except Exception:
        return ""

# ──────────────────────────────────────────────────────────────
# SOCIAL LINKS
# ──────────────────────────────────────────────────────────────
def get_social_links(driver):
    found = {
        "Instagram": set(),
        "Facebook": set(),
        "YouTube": set(),
        "Other Social Links": set(),
    }
    try:
        links = driver.find_elements(By.CSS_SELECTOR, SELECTORS["all_panel_links"])
        for link in links:
            try:
                href = (link.get_attribute("href") or "").strip()
            except StaleElementReferenceException:
                continue
            if not href:
                continue
            href_lower = href.lower()
            matched = False
            for domain, label in SOCIAL_DOMAIN_MAP.items():
                if domain in href_lower:
                    found[label].add(href)
                    matched = True
                    break
            if not matched:
                for domain in OTHER_SOCIAL_DOMAINS:
                    if domain in href_lower:
                        found["Other Social Links"].add(href)
                        break
    except Exception:
        pass

    return {
        k: (" | ".join(sorted(v)) if v else NOT_AVAILABLE)
        for k, v in found.items()
    }

# ──────────────────────────────────────────────────────────────
# PANEL SCRAPING
# ──────────────────────────────────────────────────────────────
DATA_FIELDS = [
    "Title", "Rating", "Reviews Count", "Location", "Timings", "Mobile Number", "Website",
    "Email", "Instagram", "Facebook", "YouTube", "Other Social Links", "Google Maps URL",
]

# ──────────────────────────────────────────────────────────────
# EMAIL EXTRACTION (plain HTTP fetch — no browser needed)
# ──────────────────────────────────────────────────────────────
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

EMAIL_JUNK_DOMAINS = (
    "example.com", "sentry.io", "wixpress.com", "godaddy.com",
    "schema.org", "yourdomain.com", "domain.com", "email.com",
    "wordpress.com", "gravatar.com", "w3.org", "sentry-next.wixpress.com",
)
EMAIL_JUNK_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js")

def _clean_email_candidates(text):
    found = set()
    for m in EMAIL_REGEX.findall(text or ""):
        e = m.strip().strip(".,;:()[]<>\"'")
        el = e.lower()
        if any(el.endswith(ext) for ext in EMAIL_JUNK_EXTENSIONS):
            continue
        if any(el.endswith("@" + d) for d in EMAIL_JUNK_DOMAINS):
            continue
        found.add(e)
    return found

def get_email_for_website(url):
    if not SCRAPE_EMAILS or not url or url == NOT_AVAILABLE:
        return NOT_AVAILABLE

    headers = {"User-Agent": random.choice(USER_AGENT_POOL)}
    proxies = {"http": PROXY_SERVER, "https": PROXY_SERVER} if PROXY_SERVER else None

    def fetch(u):
        try:
            resp = requests.get(
                u, headers=headers, timeout=EMAIL_FETCH_TIMEOUT,
                proxies=proxies, allow_redirects=True,
            )
            return resp.text if resp.ok else ""
        except Exception:
            return ""

    html = fetch(url)
    if not html:
        return NOT_AVAILABLE

    mailtos = {
        m.strip() for m in re.findall(r"mailto:([^\"'?&\s]+)", html, flags=re.IGNORECASE)
    }
    mailtos = {m for m in mailtos if EMAIL_REGEX.fullmatch(m)}
    if mailtos:
        return sorted(mailtos)[0]

    found = _clean_email_candidates(html)
    if found:
        return sorted(found)[0]

    try:
        contact_match = re.search(
            r'href=["\']([^"\']*contact[^"\']*)["\']', html, flags=re.IGNORECASE
        )
        if contact_match:
            contact_url = urljoin(url, contact_match.group(1))
            if contact_url.lower().startswith("http"):
                contact_html = fetch(contact_url)
                found2 = _clean_email_candidates(contact_html)
                if found2:
                    return sorted(found2)[0]
    except Exception:
        pass

    return NOT_AVAILABLE

def blank_row():
    return {f: NOT_AVAILABLE for f in DATA_FIELDS}

def read_panel_h1(driver):
    for sel in SELECTORS["name"].split(", "):
        text = safe_js_text(driver, sel)
        if text:
            return text
    return ""

def get_title(driver, fallback_title=None):
    text = read_panel_h1(driver)
    if text:
        return text
    aria = safe_attr(driver, SELECTORS["panel"], "aria-label")
    if aria:
        return aria
    return fallback_title or ""

def get_reviews_count(driver):
    for sel in SELECTORS["reviews_count"].split(", "):
        raw = safe_text(driver, sel) or safe_attr(driver, sel, "aria-label")
        if not raw:
            continue
        digits = re.sub(r"[^\d]", "", raw)
        if digits:
            return digits
    return ""

def scrape_current_panel(driver, fallback_title=None):
    social = get_social_links(driver)
    website = or_na(
        safe_attr(driver, SELECTORS["website_link"], "href")
        or safe_text(driver, SELECTORS["website_text"])
    )
    return {
        "Title": or_na(get_title(driver, fallback_title=fallback_title)),
        "Rating": or_na(safe_text(driver, SELECTORS["rating"])),
        "Reviews Count": or_na(get_reviews_count(driver)),
        "Location": or_na(safe_text(driver, SELECTORS["address"])),
        "Timings": or_na(get_timings(driver)),
        "Mobile Number": or_na(safe_text(driver, SELECTORS["phone"])),
        "Website": website,
        "Email": or_na(get_email_for_website(website)),
        "Instagram": social["Instagram"],
        "Facebook": social["Facebook"],
        "YouTube": social["YouTube"],
        "Other Social Links": social["Other Social Links"],
        "Google Maps URL": driver.current_url,
    }

# ──────────────────────────────────────────────────────────────
# SCROLL + CONSENT
# ──────────────────────────────────────────────────────────────
def scroll_results_feed(driver):
    try:
        feed = driver.find_element(By.CSS_SELECTOR, SELECTORS["results_list"])
    except NoSuchElementException:
        return

    last_count = last_height = 0
    for _ in range(MAX_SCROLL_ATTEMPTS):
        listings = driver.find_elements(By.CSS_SELECTOR, SELECTORS["result_item"])
        current_count = len(listings)
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
        time.sleep(SCROLL_PAUSE_SECONDS)
        new_height = driver.execute_script("return arguments[0].scrollHeight", feed)
        if current_count == last_count and new_height == last_height:
            break
        last_count, last_height = current_count, new_height

CONSENT_BUTTON_TEXTS = [
    "Accept all", "I agree", "Alle akzeptieren", "Ich stimme zu",
    "Tout accepter", "Aceptar todo", "Accept", "Alles accepteren",
    "Ik ga akkoord",
]

def dismiss_consent_dialog(driver, timeout=5):
    deadline = time.time() + timeout

    def try_click():
        for text in CONSENT_BUTTON_TEXTS:
            try:
                btn = driver.find_element(
                    By.XPATH,
                    f"//button[.//*[contains(text(), '{text}')] or contains(text(), '{text}')]",
                )
                driver.execute_script("arguments[0].click();", btn)
                return True
            except NoSuchElementException:
                continue
        return False

    try:
        if try_click():
            time.sleep(1)
            return
        for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
            if time.time() > deadline:
                break
            try:
                driver.switch_to.frame(iframe)
                clicked = try_click()
            except Exception:
                clicked = False
            finally:
                driver.switch_to.default_content()
            if clicked:
                time.sleep(1)
                return
    except Exception:
        driver.switch_to.default_content()

# ──────────────────────────────────────────────────────────────
# CLICK + WAIT (atomic JS)
# ──────────────────────────────────────────────────────────────
_JS_FIND_AND_CLICK = """
    var href = arguments[0];
    var el = document.querySelector('a[href="' + href + '"]');
    if (!el) return null;
    el.scrollIntoView({block: 'center'});
    var label = el.getAttribute('aria-label') || '';
    el.click();
    return label;
"""

_JS_EXISTS = """
    return !!document.querySelector('a[href="' + arguments[0] + '"]');
"""

def listing_exists(driver, href):
    try:
        return bool(driver.execute_script(_JS_EXISTS, href))
    except Exception:
        return False

def js_find_and_click_listing(driver, href):
    try:
        return driver.execute_script(_JS_FIND_AND_CLICK, href)
    except Exception:
        return None

def title_from_aria_label(label):
    if not label:
        return None
    return label.split(" · ")[0].strip() or None

def wait_for_panel_loaded(driver, previous_title=None, expected_title=None, timeout=WAIT_SECONDS):
    def _condition(d):
        text = read_panel_h1(d)
        if not text:
            return False
        if expected_title:
            exp = expected_title.strip().lower()
            txt = text.lower()
            return exp in txt or txt in exp
        if previous_title and text == previous_title:
            return False
        return True

    WebDriverWait(
        driver,
        timeout,
        ignored_exceptions=TRANSIENT_DOM_EXCEPTIONS,
    ).until(_condition)

# ──────────────────────────────────────────────────────────────
# KEYWORD INPUT I/O — reads .xlsx, .xls, .csv, or .txt transparently.
# (This is ONLY for reading the keyword list from INPUT_FILE. It is
# unrelated to OUTPUT/PROGRESS/STATUS, which are handled by the
# corruption-resistant CSV helpers further down — see CHANGELOG #10.)
# ──────────────────────────────────────────────────────────────
_TXT_HEADER_NAMES = {
    "keywords", "keyword", "search term", "search terms", "searchterm",
    "searchterms", "query", "queries", "term", "terms", "search", "kw",
}

def read_table(path):
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    if ext == ".txt":
        with open(path, "r", encoding="utf-8-sig") as f:
            lines = [line.strip() for line in f.readlines()]
        lines = [line for line in lines if line]
        if lines and lines[0].lower() in _TXT_HEADER_NAMES:
            lines = lines[1:]
        return pd.DataFrame({"keywords": lines})
    return pd.read_excel(path)

# ──────────────────────────────────────────────────────────────
# COLUMN AUTO-DETECTION for the keyword input file
# ──────────────────────────────────────────────────────────────
KEYWORD_COLUMN_ALIASES = [
    "keywords", "keyword", "search term", "search terms", "searchterm",
    "searchterms", "query", "queries", "term", "terms", "search", "kw",
]

def _find_header_row(raw_df, alias_set, max_rows=20):
    for i in range(min(max_rows, len(raw_df))):
        row_values = [str(v).strip().lower() for v in raw_df.iloc[i].tolist()]
        if any(v in alias_set for v in row_values):
            return i
    return None

def read_input_table(path, sheet_name=None):
    ext = Path(path).suffix.lower()
    if ext not in (".xlsx", ".xls"):
        return read_table(path)

    alias_set = {a.lower() for a in ([INPUT_COLUMN.lower()] + KEYWORD_COLUMN_ALIASES)}
    available_sheets = pd.ExcelFile(path).sheet_names

    chosen_sheet = None
    if sheet_name:
        norm_target = sheet_name.strip().lower()
        for s in available_sheets:
            if s.strip().lower() == norm_target:
                chosen_sheet = s
                break
        if chosen_sheet is None:
            for s in available_sheets:
                sl = s.strip().lower()
                if norm_target in sl or sl in norm_target:
                    chosen_sheet = s
                    break
        if chosen_sheet is None:
            log(f"[warn] Sheet '{sheet_name}' not found in {path}. "
                f"Available sheets: {available_sheets}. Auto-detecting instead.")

    header_row = None
    if chosen_sheet is not None:
        raw = pd.read_excel(path, sheet_name=chosen_sheet, header=None, nrows=20)
        header_row = _find_header_row(raw, alias_set)

    if chosen_sheet is None or header_row is None:
        for s in available_sheets:
            raw = pd.read_excel(path, sheet_name=s, header=None, nrows=20)
            hr = _find_header_row(raw, alias_set)
            if hr is not None:
                chosen_sheet, header_row = s, hr
                break

    if chosen_sheet is None:
        chosen_sheet, header_row = available_sheets[0], 0

    log(f"[info] Reading keywords from sheet '{chosen_sheet}' (header row {header_row + 1})")
    df = pd.read_excel(path, sheet_name=chosen_sheet, header=header_row)
    df.columns = df.columns.astype(str).str.strip()
    return df

def detect_keyword_column(df, preferred=INPUT_COLUMN):
    normalized = {str(c).strip().lower(): c for c in df.columns}

    candidates = [preferred.strip().lower()] + KEYWORD_COLUMN_ALIASES
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]

    if len(df.columns) == 1:
        return df.columns[0]

    raise ValueError(
        f"Could not find a keyword column (tried '{preferred}' and common "
        f"alternates like {KEYWORD_COLUMN_ALIASES}). "
        f"Available columns: {list(df.columns)}. "
        f"Rename one of your columns to 'keywords', or leave the file "
        f"with just a single column."
    )

# ──────────────────────────────────────────────────────────────
# CORRUPTION-RESISTANT CSV I/O  (CHANGELOG #10)
# All of OUTPUT_FILE / PROGRESS_FILE / STATUS_FILE / backups go through
# these helpers. Two techniques, used together:
#   1. ATOMIC full-file writes (temp file + fsync + os.replace) for
#      files that get fully rewritten (STATUS_FILE, each backup).
#   2. APPEND-ONLY writes (open in "a" mode + fsync, never rewrite
#      earlier bytes) for files that grow row-by-row during the run
#      (OUTPUT_FILE, PROGRESS_FILE) — this is what stops a crash from
#      ever wiping out data collected earlier in the run.
# ──────────────────────────────────────────────────────────────
def atomic_write_csv(df, path):
    """
    Writes `df` to `path` as CSV without ever leaving a half-written or
    corrupted file behind. Writes to a temp file in the SAME directory
    first, fsyncs it, then swaps it into place with os.replace(), which
    is atomic on both Windows and POSIX — `path` is always either fully
    the old version or fully the new version, never a mix, even if the
    process is killed mid-write.
    """
    path = str(path)
    directory = os.path.dirname(path) or "."
    Path(directory).mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", suffix=".csv", dir=directory)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            df.to_csv(f, index=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

def append_rows_to_output(rows):
    """
    Appends `rows` (list of dicts) to OUTPUT_FILE. Never touches bytes
    already written for earlier rows — a crash mid-write can, at worst,
    leave the LAST row incomplete; every row written before it stays
    intact. Flushes + fsyncs after every call so rows are actually on
    disk, not just buffered, before this function returns.
    """
    if not rows:
        return
    path = OUTPUT_FILE
    file_exists = os.path.exists(path) and os.path.getsize(path) > 0
    Path(os.path.dirname(path) or ".").mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMN_ORDER, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for row in rows:
            clean = {
                k: (row.get(k) if str(row.get(k, "")).strip() != "" else NOT_AVAILABLE)
                for k in COLUMN_ORDER
            }
            writer.writerow(clean)
        f.flush()
        os.fsync(f.fileno())

def append_progress_row(keyword, href):
    """Same append-only, fsync'd approach as append_rows_to_output, but
    for the (keyword, href) 'already done' progress log."""
    path = PROGRESS_FILE
    file_exists = os.path.exists(path) and os.path.getsize(path) > 0
    Path(os.path.dirname(path) or ".").mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["keyword", "href"])
        writer.writerow([keyword, href])
        f.flush()
        os.fsync(f.fileno())

def _quarantine_corrupt_file(path):
    """Renames an unparseable file aside (never deletes it) so nothing
    is lost even in the worst case, then lets the run start fresh."""
    if not os.path.exists(path):
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    quarantine_path = f"{path}.corrupt_{ts}.bak"
    try:
        os.replace(path, quarantine_path)
        log(f"[recovery] Could not parse {path} — moved it to {quarantine_path} "
            f"so nothing is lost. A fresh file will be started.")
    except Exception as e:
        log(f"[warn] Could not quarantine corrupt file {path}: {short_error(e)}")

def read_csv_with_recovery(path):
    """
    Reads a CSV, recovering as much data as possible instead of
    silently returning nothing on any error (the old bug). Tries a
    normal read first; if that fails, retries while skipping only the
    malformed rows (typically just a truncated final line from an old
    interrupted run) and logs how many rows were recovered. If the file
    can't be parsed at all, it's quarantined (renamed, never deleted)
    and an empty frame is returned so the run can continue.
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        log(f"[warn] {path} did not parse cleanly ({short_error(e)}); "
            f"attempting recovery by skipping bad rows...")

    try:
        df = pd.read_csv(path, engine="python", on_bad_lines="skip")
        log(f"[recovery] Recovered {len(df)} row(s) from {path}.")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_backup = f"{path}.recovered_{ts}.bak"
        try:
            shutil.copy2(path, raw_backup)
            log(f"[recovery] Original file preserved at {raw_backup}")
        except Exception:
            pass
        return df
    except Exception as e2:
        log(f"[error] {path} could not be parsed even with recovery "
            f"({short_error(e2)}). Quarantining it and starting fresh.")
        _quarantine_corrupt_file(path)
        return pd.DataFrame()

# ──────────────────────────────────────────────────────────────
# STATE MANAGEMENT (output/progress/status — colocated with INPUT_FILE)
# ──────────────────────────────────────────────────────────────
def ensure_backup_dir():
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)

def order_columns(df):
    cols = [c for c in COLUMN_ORDER if c in df.columns] + \
           [c for c in df.columns if c not in COLUMN_ORDER]
    return df[cols]

def load_all_results():
    """Returns dict: keyword → list of row dicts, recovering as much as
    possible from OUTPUT_FILE if it was left in a bad state."""
    df = read_csv_with_recovery(OUTPUT_FILE)
    if df.empty:
        return {}
    df = df.fillna(NOT_AVAILABLE)
    result = {}
    for row in df.to_dict(orient="records"):
        q = str(row.get("Search Term", ""))
        result.setdefault(q, []).append(row)
    return result

def load_progress():
    """Returns set of (keyword, href) that are already done, recovering
    as much as possible from PROGRESS_FILE if it was left in a bad state."""
    df = read_csv_with_recovery(PROGRESS_FILE)
    if df.empty or "keyword" not in df.columns or "href" not in df.columns:
        return set()
    return set(zip(df["keyword"].astype(str), df["href"].astype(str)))

def load_status():
    """Returns dict keyword → status, recovering as much as possible
    from STATUS_FILE if it was left in a bad state."""
    df = read_csv_with_recovery(STATUS_FILE)
    if df.empty or "keyword" not in df.columns or "status" not in df.columns:
        return {}
    return dict(zip(df["keyword"].astype(str), df["status"].astype(str)))

def save_status(status_dict):
    """Full rewrite, but ATOMIC (temp file + os.replace) — STATUS_FILE
    is small (one row per keyword) so a full rewrite is cheap, and
    atomicity means a crash mid-write can never corrupt it."""
    df = pd.DataFrame(list(status_dict.items()), columns=["keyword", "status"])
    atomic_write_csv(df, STATUS_FILE)

def update_status(status_dict, query, status_text):
    """
    Records resume status for a keyword. Deliberately does NOT touch
    INPUT_FILE — the original workbook is treated as read-only. Status
    lives only in the separate STATUS_FILE next to it.
    """
    status_dict[query] = status_text
    save_status(status_dict)

def cleanup_old_backups():
    """
    Keeps only the MAX_BACKUPS_KEEP most recent backup files in
    BACKUP_DIR, deleting older ones. Backup filenames are
    "backup_YYYYMMDD_HHMMSS.csv", so a plain sorted() already puts them
    in chronological order (oldest first).
    """
    try:
        backup_files = sorted(
            f for f in os.listdir(BACKUP_DIR)
            if f.startswith("backup_") and os.path.isfile(os.path.join(BACKUP_DIR, f))
        )
    except FileNotFoundError:
        return

    excess = len(backup_files) - MAX_BACKUPS_KEEP
    if excess <= 0:
        return

    for old_name in backup_files[:excess]:
        old_path = os.path.join(BACKUP_DIR, old_name)
        try:
            os.remove(old_path)
            log(f"[backup] Removed oldest backup (keeping last {MAX_BACKUPS_KEEP}): {old_path}")
        except Exception as e:
            log(f"[warn] Could not remove old backup {old_path}: {short_error(e)}")

def create_timestamped_backup(all_results):
    """Writes a full snapshot backup, ATOMICALLY (see atomic_write_csv),
    so even the backup itself can never end up half-written."""
    ensure_backup_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(BACKUP_DIR, f"backup_{ts}{OUTPUT_EXT}")
    rows = []
    for rlist in all_results.values():
        rows.extend(rlist)
    if rows:
        df = order_columns(pd.DataFrame(rows)).fillna(NOT_AVAILABLE)
        atomic_write_csv(df, path)
        log(f"Backup written → {path}")
        cleanup_old_backups()
    return path

def archive_existing_state_files():
    """
    Used when FORCE_RERUN_ALL is True. Moves any existing
    OUTPUT_FILE/PROGRESS_FILE/STATUS_FILE into BACKUP_DIR with a
    timestamp — never deletes them — so nothing already collected is
    lost, then the run starts from a clean slate and re-scrapes every
    keyword.
    """
    ensure_backup_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    for path in (OUTPUT_FILE, PROGRESS_FILE, STATUS_FILE):
        if os.path.exists(path):
            dest = os.path.join(
                BACKUP_DIR, f"{Path(path).stem}_archived_{ts}{Path(path).suffix}"
            )
            try:
                os.replace(path, dest)
                log(f"[force-rerun] Archived existing {path} → {dest}")
            except Exception as e:
                log(f"[warn] Could not archive {path}: {short_error(e)}")

# ──────────────────────────────────────────────────────────────
# CORE SCRAPE LOGIC
# ──────────────────────────────────────────────────────────────
def scrape_search_term(driver, query, all_results, progress_set):
    global CHECKPOINT_COUNTER

    with FILE_LOCK:
        results = all_results.setdefault(query, [])
        already_done = {href for (k, href) in progress_set if k == query}

    if results or already_done:
        log(f"[{query}] Resuming – {len(already_done)} listing(s) already processed")

    driver.get(f"https://www.google.com/maps?hl={MAPS_LANGUAGE}&gl={MAPS_COUNTRY}")
    dismiss_consent_dialog(driver)

    try:
        box = WebDriverWait(driver, WAIT_SECONDS).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS["search_box"]))
        )
        box.clear()
        box.send_keys(query)
        box.send_keys(Keys.ENTER)
    except TimeoutException:
        log(f"[!] [{query}] Search box did not appear")
        return results, False

    time.sleep(3.5)

    # ── Case 1: single business page ──────────────────────────
    try:
        wait_for_panel_loaded(driver, previous_title=None, timeout=6)
        if not driver.find_elements(By.CSS_SELECTOR, SELECTORS["results_list"]):
            if "__SINGLE__" in already_done:
                return results, True

            start = time.time()
            row = blank_row()
            try:
                row.update(scrape_current_panel(driver))
            except Exception as e:
                log(f"  → [{query}] panel read warning: {short_error(e)}")

            row["Search Term"] = query
            row["Time Taken (sec)"] = round(time.time() - start, 2)
            row["Scraped At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["Status"] = "Success" if row.get("Title") != NOT_AVAILABLE else "Partial"

            with FILE_LOCK:
                results.append(row)
                # Durable the moment it's scraped — append-only + fsync,
                # never a full rewrite (see CHANGELOG #10c).
                append_rows_to_output([row])
                if (query, "__SINGLE__") not in progress_set:
                    progress_set.add((query, "__SINGLE__"))
                    append_progress_row(query, "__SINGLE__")

                CHECKPOINT_COUNTER += 1
                if CHECKPOINT_COUNTER >= CHECKPOINT_EVERY:
                    create_timestamped_backup(all_results)
                    CHECKPOINT_COUNTER = 0

            return results, True
    except TimeoutException:
        pass

    # ── Case 2: results feed ──────────────────────────────────
    try:
        WebDriverWait(driver, WAIT_SECONDS).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS["results_list"]))
        )
    except TimeoutException:
        log(f"[!] [{query}] No results found")
        return results, True

    scroll_results_feed(driver)

    listings = driver.find_elements(By.CSS_SELECTOR, SELECTORS["result_item"])
    seen = set()
    unique_hrefs = []
    for el in listings:
        href = el.get_attribute("href")
        if href and href not in seen:
            seen.add(href)
            unique_hrefs.append(href)

    pending = [h for h in unique_hrefs if h not in already_done]
    log(f"[{query}] {len(unique_hrefs)} total listings – "
        f"{len(unique_hrefs) - len(pending)} already done, {len(pending)} remaining")

    previous_title = None
    rows_written_this_keyword = 0

    for idx, href in enumerate(pending, start=1):
        start = time.time()
        fallback_title = None
        clicked_ok = False

        for attempt in range(1, MAX_CLICK_ATTEMPTS + 1):
            label = js_find_and_click_listing(driver, href)
            if label is None:
                if not listing_exists(driver, href):
                    scroll_results_feed(driver)
                log(f"  → [{query}] listing not ready (attempt {attempt}), retrying…")
                time.sleep(CLICK_RETRY_BACKOFF * attempt)
                continue

            fallback_title = title_from_aria_label(label) or fallback_title
            time.sleep(1.1)

            try:
                wait_for_panel_loaded(
                    driver, previous_title, expected_title=fallback_title
                )
                clicked_ok = True
                break
            except TimeoutException as e:
                log(f"  → [{query}] panel wait failed (attempt {attempt}): {short_error(e)}")
                time.sleep(CLICK_RETRY_BACKOFF * attempt)
            except Exception as e:
                log(f"  → [{query}] click/wait issue – best effort: {short_error(e)}")
                break

        if not clicked_ok:
            log(f"  → [{query}] proceeding best-effort after {MAX_CLICK_ATTEMPTS} attempts")

        row = blank_row()
        try:
            row.update(scrape_current_panel(driver, fallback_title=fallback_title))
        except Exception as e:
            log(f"  → [{query}] panel fields warning: {short_error(e)}")
            if fallback_title:
                row["Title"] = fallback_title

        row["Search Term"] = query
        row["Time Taken (sec)"] = round(time.time() - start, 2)
        row["Scraped At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row["Status"] = "Success" if row.get("Title") != NOT_AVAILABLE else "Partial"

        with FILE_LOCK:
            results.append(row)
            # Append-only + fsync, immediately — this row is durable on
            # disk before the lock is released, and every row before it
            # is untouched (see CHANGELOG #10c). No more batching this
            # behind CHECKPOINT_EVERY like the old xlsx version did.
            append_rows_to_output([row])
            if (query, href) not in progress_set:
                progress_set.add((query, href))
                append_progress_row(query, href)

            CHECKPOINT_COUNTER += 1
            rows_written_this_keyword += 1
            running_total = sum(len(r) for r in all_results.values())

            if CHECKPOINT_COUNTER >= CHECKPOINT_EVERY:
                create_timestamped_backup(all_results)
                CHECKPOINT_COUNTER = 0

        previous_title = row.get("Title")

        is_last_for_keyword = (idx == len(pending))
        if idx % PROGRESS_PRINT_EVERY == 0 or is_last_for_keyword:
            done_for_keyword = len(unique_hrefs) - len(pending) + idx
            log(f"[{query}] {done_for_keyword}/{len(unique_hrefs)} records "
                f"— {running_total} total record(s) collected so far")

        time.sleep(DELAY_BETWEEN_LISTINGS)

    # Take a backup at the end of this keyword too (every row was
    # already durably appended as it was scraped, so this is just the
    # extra full-snapshot safety net, not a "flush").
    if rows_written_this_keyword > 0:
        with FILE_LOCK:
            create_timestamped_backup(all_results)
            CHECKPOINT_COUNTER = 0

    return results, True

# ──────────────────────────────────────────────────────────────
# WORKER
# ──────────────────────────────────────────────────────────────
def worker_task(query, all_results, progress_set, status_dict, total_keywords):
    rows, completed_ok = [], False
    last_error = None

    # IMPORTANT:
    # build_driver() is intentionally INSIDE the try block.
    # This catches Chrome startup failures such as:
    # "session not created: Chrome instance exited"
    # and allows the worker to retry with a fresh browser.
    for attempt in range(1, DRIVER_RETRIES_PER_KEYWORD + 2):  # first try + retries
        driver = None

        try:
            driver = build_driver()

            rows, completed_ok = scrape_search_term(
                driver,
                query,
                all_results,
                progress_set
            )

            last_error = None
            break

        except WebDriverException as e:
            last_error = e

            if attempt <= DRIVER_RETRIES_PER_KEYWORD:
                log(
                    f"[!] [{query}] driver error on attempt {attempt}: "
                    f"{short_error(e)} — restarting browser and retrying"
                )
            else:
                log(
                    f"[!] [{query}] driver error on final attempt {attempt}: "
                    f"{short_error(e)}"
                )

        except Exception as e:
            last_error = e

            log(
                f"[!] [{query}] unexpected error on attempt {attempt}: "
                f"{short_error(e)}"
            )

            if attempt > DRIVER_RETRIES_PER_KEYWORD:
                break

        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

        if last_error is not None and attempt <= DRIVER_RETRIES_PER_KEYWORD:
            time.sleep(2 * attempt)

    with FILE_LOCK:
        if rows:
            total = sum(len(r) for r in all_results.values())
            log(
                f"[{query}] finished – {len(rows)} rows for this keyword "
                f"(total across all: {total})"
            )
        else:
            log(f"[!] [{query}] No rows scraped")

        status_text = "Completed" if completed_ok else "Not Completed"
        update_status(status_dict, query, status_text)

        completed_count = sum(
            1 for v in status_dict.values() if v == "Completed"
        )
        total_records = sum(
            len(r) for r in all_results.values()
        )

        log(
            f"[PROGRESS] {completed_count}/{total_keywords} keywords completed | "
            f"{total_records} total record(s) collected"
        )

    time.sleep(DELAY_BETWEEN_SEARCHES)
    return query, completed_ok

# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
def main():
    global INPUT_COLUMN

    ensure_backup_dir()

    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} not found. Create it with a column named '{INPUT_COLUMN}'.")

    log(f"Input file:  {INPUT_FILE}")
    log(f"Working dir: {INPUT_DIR}  (output/progress/status/backups all go here, always as CSV)")
    log(f"Localization: hl={MAPS_LANGUAGE} gl={MAPS_COUNTRY}"
        f"{'  proxy=' + PROXY_SERVER if PROXY_SERVER else '  (no proxy — real IP location still influences ranking)'}")
    log(f"Backup rotation: keeping the last {MAX_BACKUPS_KEEP} backup file(s) in {BACKUP_DIR}")

    if FORCE_RERUN_ALL:
        log("[force-rerun] FORCE_RERUN_ALL is True — archiving any existing "
            "output/progress/status and re-scraping EVERY keyword from scratch. "
            "Set FORCE_RERUN_ALL = False in the config to go back to normal "
            "resume behavior for future runs.")
        archive_existing_state_files()

    sheet_for_read = INPUT_SHEET_NAME if INPUT_EXT in (".xlsx", ".xls") else None
    df_input = read_input_table(INPUT_FILE, sheet_name=sheet_for_read)
    df_input.columns = df_input.columns.astype(str).str.strip()
    if sheet_for_read:
        log(f"Sheet:       '{sheet_for_read}'")

    detected_column = detect_keyword_column(df_input, preferred=INPUT_COLUMN)
    if detected_column != INPUT_COLUMN:
        log(f"[info] Using column '{detected_column}' as the keyword column "
            f"(no exact '{INPUT_COLUMN}' column found)")
        INPUT_COLUMN = detected_column

    all_queries = df_input[INPUT_COLUMN].dropna().astype(str).str.strip().tolist()
    all_queries = [q for q in all_queries if q]
    log(f"Loaded {len(all_queries)} search terms from {INPUT_FILE}")
    log("Note: INPUT_FILE is read-only — status/progress are tracked in "
        "separate files next to it, so its other sheets/formatting are never touched.")

    status_dict = load_status()
    for q in all_queries:
        if q not in status_dict:
            status_dict[q] = "Not Completed"
    save_status(status_dict)

    all_results = load_all_results()
    progress_set = load_progress()

    pending_queries = []
    for q in all_queries:
        if status_dict.get(q) == "Completed":
            log(f"[skip] Already completed: {q}")
        else:
            pending_queries.append(q)

    if not pending_queries:
        log("Nothing left to scrape – every keyword is already marked Completed.")
        return

    log(f"Will scrape {len(pending_queries)} keyword(s) with {NUM_WORKERS} worker(s) "
        f"(headless={HEADLESS})")

    total_keywords = len(all_queries)
    overall_start = time.time()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(
                worker_task, q, all_results, progress_set, status_dict, total_keywords
            ): q
            for q in pending_queries
        }

        for future in as_completed(futures):
            q = futures[future]
            try:
                future.result()
            except Exception as e:
                log(f"[!] [{q}] worker crashed: {short_error(e)}")
                with FILE_LOCK:
                    update_status(status_dict, q, "Not Completed")

    create_timestamped_backup(all_results)

    total_time = round(time.time() - overall_start, 1)
    final_completed = sum(1 for v in status_dict.values() if v == "Completed")
    final_records = sum(len(r) for r in all_results.values())
    log(f"Done. Total runtime: {total_time}s "
        f"(avg {round(total_time / max(len(pending_queries), 1), 1)}s per keyword)")
    log(f"[SUMMARY] {final_completed}/{total_keywords} keywords completed | "
        f"{final_records} total record(s) collected")
    log(f"Results → {OUTPUT_FILE}")
    log(f"Progress → {PROGRESS_FILE}")
    log(f"Status   → {STATUS_FILE}")
    log(f"Backups  → {BACKUP_DIR}/")

if __name__ == "__main__":
    main()