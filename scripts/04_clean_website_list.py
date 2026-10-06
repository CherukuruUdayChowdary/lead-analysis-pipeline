"""
Clean Website List Before Scraping
------------------------------------
Takes the output of the website-status-checker (one or more CSVs with
Website / Status Code / Status Note / Available columns) and produces a
clean, deduplicated urls.csv ready to drop into the scraper's input/ folder.

WHAT IT REMOVES:
    1. Dead sites            -> Available != "Yes" (404, timeout, error, etc.)
    2. Social/platform links -> facebook.com, instagram.com, linkedin.com,
                                 youtube.com, twitter.com/x.com, tiktok.com,
                                 sites.google.com, wa.me, pinterest.com, etc.
                                 (these aren't business websites the scraper
                                 can pull contact info FROM — they're social
                                 profiles ON a business)
    3. Duplicate domains      -> the same domain often appears more than once
                                 with different sub-pages or tracking query
                                 strings (e.g. .../contact and
                                 .../?utm_campaign=...). Only the first
                                 occurrence of each domain is kept, trimmed
                                 down to just the domain root (scheme://domain)
                                 — the scraper already visits /contact, /about,
                                 etc. on its own, so the extra path is useless
                                 and just causes duplicate crawling.

USAGE:
    python clean_website_list.py
    (edit INPUT_FILES / OUTPUT_FILE below first)
"""

import os
import re
import csv
from urllib.parse import urlparse

# =========== EDIT THESE BEFORE RUNNING ===========
# List every status-checker CSV you want to combine and clean.
INPUT_FILES = [
    r"C:\Users\Administrator\Documents\LEAD_ANALYSI\cleaning_raw_data\AUS_CLEANED_DATA\final_output_WEBSITE_CHECKED.csv"
]

# Folder where both output files get saved. Created automatically if it
# doesn't exist yet.
OUTPUT_FOLDER = r"C:\Users\Administrator\Documents\LEAD_ANALYSI\cleaning_raw_data\AUS_CLEANED_DATA\AUS_CLEAN_WEBSITE_URLS"

# Where the clean list gets written. "website" header, one column — matches
# what the scraper's input/urls.csv expects, so you can copy this straight in.
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "clean_urls.csv")

# A second, more detailed file — keeps the domain + which source row it came
# from, useful for double-checking what got kept/dropped.
DETAIL_OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "clean_urls_detail.csv")
# ===================================================

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Domains that are social/platform profiles, not standalone business sites.
# The scraper crawls a domain's homepage + /contact + /about etc. looking for
# that business's own contact info — running it against facebook.com itself
# wastes time and returns nothing useful.
SOCIAL_PLATFORM_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "x.com", "youtube.com",
    "linkedin.com", "tiktok.com", "pinterest.com", "yelp.com",
    "sites.google.com", "g.page", "goo.gl", "maps.google.com",
    "business.google.com", "wa.me", "api.whatsapp.com", "t.me",
    "wikipedia.org", "amazon.com", "ebay.com",
}


def get_bare_domain(url: str) -> str:
    """Return the lowercase domain with 'www.' stripped, or '' if unparseable."""
    try:
        netloc = urlparse(url).netloc.lower()
    except Exception:
        return ""
    if not netloc:
        return ""
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def is_social_platform(domain: str) -> bool:
    return any(domain == d or domain.endswith("." + d) for d in SOCIAL_PLATFORM_DOMAINS)


def clean_root_url(url: str) -> str:
    """Reduce a URL down to scheme://netloc (drop path, query, fragment)."""
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return ""


def main():
    seen_domains = set()
    clean_rows = []  # (root_url, domain, source_file, original_url)

    total_input = 0
    missing_files = []

    for path in INPUT_FILES:
        if not os.path.exists(path):
            missing_files.append(path)
            continue

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            # Be forgiving about column-name casing/spacing
            fieldmap = {c.strip().lower(): c for c in reader.fieldnames or []}
            website_col = fieldmap.get("website")
            available_col = fieldmap.get("available")

            if not website_col or not available_col:
                print(f"  Skipping {path} — couldn't find 'Website'/'Available' columns "
                      f"(found: {reader.fieldnames})")
                continue

            for row in reader:
                total_input += 1
                url = (row.get(website_col) or "").strip()
                available = (row.get(available_col) or "").strip().lower()

                if not url or available != "yes":
                    continue

                domain = get_bare_domain(url)
                if not domain:
                    continue

                if is_social_platform(domain):
                    continue

                if domain in seen_domains:
                    continue

                root_url = clean_root_url(url)
                if not root_url:
                    continue

                seen_domains.add(domain)
                clean_rows.append((root_url, domain, os.path.basename(path), url))

    if missing_files:
        print("Warning: could not find these input files (check INPUT_FILES paths):")
        for m in missing_files:
            print(f"  - {m}")
        print()

    # Write the simple output: just what the scraper needs
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["website"])
        for root_url, _, _, _ in clean_rows:
            writer.writerow([root_url])

    # Write the detailed output for reference
    with open(DETAIL_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["website", "domain", "source_file", "original_url"])
        for root_url, domain, source, original in clean_rows:
            writer.writerow([root_url, domain, source, original])

    print(f"Read {total_input} total rows across {len(INPUT_FILES) - len(missing_files)} file(s).")
    print(f"Kept {len(clean_rows)} clean, unique, non-social, available websites.")
    print(f"\nSaved:")
    print(f"  {OUTPUT_FILE}          (ready to copy into the scraper's input/urls.csv)")
    print(f"  {DETAIL_OUTPUT_FILE}   (same list + domain/source columns, for review)")


if __name__ == "__main__":
    main()