# LEAD_ANALYSI – Lead Generation Pipeline

Five Python scripts that turn a list of search keywords into cleaned business leads
(phone, website, email, address, socials, domain age). Run them in order:

| Step | Script | What it does | Reads | Writes |
|---|---|---|---|---|
| 1 | `scripts/01_gmb_scraper.py` | Scrapes Google Maps listings for every keyword (Selenium, resumable, rotated backups, email from website) | keyword list (`.xlsx/.csv/.txt`) | `ausgmb_output.csv` + progress/status/backups next to the input |
| 2 | `scripts/02_dedupe_leads.py` | Drops rows with no website **and** no phone, then dedupes by phone → website → phone+website | scraper output CSV | `final_output.csv` |
| 3 | `scripts/03_website_status_checker.py` | Checks every website (HEAD → GET fallback, 30 threads, resumable) and adds Status Code / Status Note / Available | `final_output.csv` | `final_output_WEBSITE_CHECKED.csv` |
| 4 | `scripts/04_clean_website_list.py` | Keeps only live sites, removes social/platform links, one root URL per domain | `..._WEBSITE_CHECKED.csv` | `clean_urls.csv`, `clean_urls_detail.csv` |
| 5 | `scripts/05_email_scraper.py` | Crawls each site for emails (up to 10, one column each), phones, address, socials, WHOIS domain age; SQLite-backed and resumable | `clean_urls.csv` | `scraper.db`, `emails.csv`, `emails.xlsx`, `emails_flat.csv` in `<parent>/AUS_EMAIL_DATA/` |

## Setup

```bash
pip install -r requirements.txt
```

Steps 1 and 5 need Google Chrome installed (Selenium 4.6+ fetches the driver automatically).

## Configuration

Each script has a config block at the top with Windows paths
(`C:\Users\Administrator\Documents\LEAD_ANALYSI\...`). Edit those paths before running.
The current settings are for the **Australia (AUS)** run:

- `01_gmb_scraper.py`: `INPUT_FILE`, `MAPS_COUNTRY = "au"`, optional `PROXY_SERVER`
- `02_dedupe_leads.py`: `INPUT_FILE`, `OUTPUT_DIR`
- `03_website_status_checker.py`: `INPUT_FILE`
- `04_clean_website_list.py`: `INPUT_FILES`, `OUTPUT_FOLDER`
- `05_email_scraper.py`: `DEFAULT_INPUT_CSV`, `OUTPUT_FOLDER_NAME`

## Running

```bash
python scripts/01_gmb_scraper.py
python scripts/02_dedupe_leads.py
python scripts/03_website_status_checker.py
python scripts/04_clean_website_list.py
python scripts/05_email_scraper.py --export
```

Useful options for step 5: `--export-only`, `--threads 20`, `--no-whois`, `--no-selenium`,
`--no-retry`, `--assume-country ca|us|in`.

All long-running steps (1, 3, 5) can be stopped with Ctrl+C and resumed by running the same command again.

## Folder layout on the working machine

```
LEAD_ANALYSI/
├── cleaning_raw_data/
│   ├── carpetcleaning_output.csv          <- step 2 input
│   └── AUS_CLEANED_DATA/
│       ├── final_output.csv               <- step 2 output / step 3 input
│       ├── final_output_WEBSITE_CHECKED.csv
│       ├── AUS_CLEAN_WEBSITE_URLS/clean_urls.csv
│       └── AUS_EMAIL_DATA/                <- step 5 output
└── scripts/ (this repo)
```

Scraped data files (CSV/XLSX/DB) are excluded from git via `.gitignore`.
