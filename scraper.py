#!/usr/bin/env python3
"""
HKEX Application Proof & Prospectus Scraper
============================================
Fetches listing applicant documents from HKEXnews JSON API endpoints
and downloads PDFs (Application Proofs, PHIPs, Prospectuses).

Data source: https://www1.hkexnews.hk/app/appindex.html
JSON API:    https://www1.hkexnews.hk/ncms/json/eds/{filename}.json

Designed to run on GitHub Actions (scheduled or manual dispatch).
"""

import json
import os
import re
import time
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www1.hkexnews.hk"
JSON_BASE = f"{BASE_URL}/ncms/json/eds"
PDF_BASE = f"{BASE_URL}/app"

# JSON endpoint matrix: (tab, filter, board, lang) → filename
ENDPOINTS = {
    # Active Application Proofs
    "active_app_main":   "appactive_app_sehk_e.json",
    "active_app_gem":    "appactive_app_gem_e.json",
    # Active AP + PHIP
    "active_phip_main":  "appactive_appphip_sehk_e.json",
    "active_phip_gem":   "appactive_appphip_gem_e.json",
    # Listed (includes prospectuses)
    "listed_main":       "applisted_sehk_e.json",
    "listed_gem":        "applisted_gem_e.json",
}

# Document types to download (matched against 'nF' field in JSON)
DOC_TYPES_OF_INTEREST = [
    "Application Proof",
    "Post Hearing Information Pack",
    "PHIP",
    "Prospectus",
    "Supplemental",
]

DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "downloads"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "state/downloaded.json"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "4"))
RATE_LIMIT_SECONDS = float(os.environ.get("RATE_LIMIT_SECONDS", "1.0"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; HKEXProspectusScraper/1.0)",
    "Accept": "application/json, application/pdf, */*",
    "Referer": "https://www1.hkexnews.hk/app/appindex.html",
})

# ---------------------------------------------------------------------------
# State management — track what we've already downloaded
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"downloaded": {}, "last_run": None}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["last_run"] = datetime.utcnow().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Fetch JSON index
# ---------------------------------------------------------------------------

def fetch_index(endpoint_key: str) -> list[dict]:
    """Fetch one JSON endpoint, return list of applicant records."""
    filename = ENDPOINTS[endpoint_key]
    url = f"{JSON_BASE}/{filename}"
    log.info(f"Fetching index: {url}")
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        apps = data.get("app", [])
        log.info(f"  → {len(apps)} applicants from {endpoint_key}")
        return apps
    except Exception as e:
        log.error(f"  ✗ Failed to fetch {endpoint_key}: {e}")
        return []


# ---------------------------------------------------------------------------
# Parse documents from applicant records
# ---------------------------------------------------------------------------

def is_doc_of_interest(doc_name: str) -> bool:
    """Check if a document name matches our target types."""
    if not doc_name:
        return False
    name_lower = doc_name.lower()
    for dt in DOC_TYPES_OF_INTEREST:
        if dt.lower() in name_lower:
            return True
    return False


def extract_downloads(apps: list[dict], board: str) -> list[dict]:
    """
    Extract downloadable PDF entries from applicant records.
    Returns list of dicts: {applicant, app_id, doc_type, date, url, filename}
    """
    downloads = []
    for app in apps:
        app_id = app.get("id", "unknown")
        applicant = app.get("a", "Unknown Applicant")
        # Sanitize applicant name for filesystem
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', applicant).strip()[:80]

        for doc in app.get("ls", []):
            doc_type = doc.get("nF", "")
            if not is_doc_of_interest(doc_type):
                continue

            # u1 = primary PDF, u2 = sometimes HTML multi-file index
            for url_key in ("u1",):
                rel_url = doc.get(url_key, "")
                if not rel_url or not rel_url.endswith(".pdf"):
                    continue

                full_url = f"{PDF_BASE}/{rel_url}"
                pdf_filename = rel_url.split("/")[-1]
                doc_date = doc.get("d", "unknown")

                downloads.append({
                    "applicant": applicant,
                    "safe_name": safe_name,
                    "app_id": app_id,
                    "board": board,
                    "doc_type": doc_type,
                    "date": doc_date,
                    "url": full_url,
                    "filename": pdf_filename,
                })

        # Also check 'ps' (prospectus section in listed tab)
        for doc in app.get("ps", []):
            doc_type = doc.get("nF", "")
            if not doc_type:
                doc_type = doc.get("nS1", "Prospectus")
            for url_key in ("u1",):
                rel_url = doc.get(url_key, "")
                if not rel_url or not rel_url.endswith(".pdf"):
                    continue
                full_url = f"{PDF_BASE}/{rel_url}"
                pdf_filename = rel_url.split("/")[-1]
                downloads.append({
                    "applicant": applicant,
                    "safe_name": safe_name,
                    "app_id": app_id,
                    "board": board,
                    "doc_type": doc_type,
                    "date": doc.get("d", "unknown"),
                    "url": full_url,
                    "filename": pdf_filename,
                })

    return downloads


# ---------------------------------------------------------------------------
# Download PDFs
# ---------------------------------------------------------------------------

def download_pdf(item: dict, state: dict) -> dict | None:
    """Download a single PDF. Returns item dict if successful, None if skipped/failed."""
    url = item["url"]
    url_hash = hashlib.md5(url.encode()).hexdigest()

    if url_hash in state["downloaded"]:
        log.debug(f"  ⏭ Already downloaded: {item['filename']}")
        return None

    # Descriptive filename: DATE_CompanyName_DocType.pdf
    doc_date = item.get("date", "unknown")
    # Parse date from DD/MM/YYYY to YYYY-MM-DD
    try:
        from datetime import datetime as _dt
        dt = _dt.strptime(doc_date, "%d/%m/%Y")
        date_str = dt.strftime("%Y-%m-%d")
    except Exception:
        date_str = doc_date.replace("/", "-") if doc_date else "unknown"

    # Shorten doc type for filename
    doc_type = item.get("doc_type", "")
    doc_lower = doc_type.lower()
    # Extract submission number if present (e.g., "1st", "2nd")
    sub_match = re.search(r'(\d+)(?:st|nd|rd|th)', doc_lower)
    sub_num = f"_{sub_match.group(1)}" if sub_match else ""
    type_tag = f"AP{sub_num}" if "application proof" in doc_lower else \
               f"PHIP{sub_num}" if "phip" in doc_lower or "post hearing" in doc_lower else \
               f"Prospectus{sub_num}" if "prospectus" in doc_lower else \
               f"Supplemental{sub_num}" if "supplemental" in doc_lower else \
               re.sub(r'[<>:"/\\|?*]', '_', doc_type)[:20]

    safe_name = item["safe_name"]
    desc_filename = f"{date_str}_{safe_name}_{type_tag}.pdf"

    board_dir = "MainBoard" if item["board"] == "main" else "GEM"
    dest_file = DOWNLOAD_DIR / board_dir / desc_filename
    dest_file.parent.mkdir(parents=True, exist_ok=True)

    if dest_file.exists():
        log.debug(f"  ⏭ File exists: {dest_file}")
        state["downloaded"][url_hash] = {
            "url": url,
            "path": str(dest_file),
            "date": item["date"],
            "ts": datetime.utcnow().isoformat(),
        }
        return None

    if DRY_RUN:
        log.info(f"  [DRY RUN] Would download: {item['applicant']} → {item['filename']}")
        return item

    try:
        time.sleep(RATE_LIMIT_SECONDS)
        resp = SESSION.get(url, timeout=60, stream=True)
        resp.raise_for_status()

        with open(dest_file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

        size_mb = dest_file.stat().st_size / (1024 * 1024)
        log.info(f"  ✓ {item['applicant']} | {item['doc_type']} | {item['filename']} ({size_mb:.1f} MB)")

        state["downloaded"][url_hash] = {
            "url": url,
            "path": str(dest_file),
            "date": item["date"],
            "ts": datetime.utcnow().isoformat(),
        }
        return item

    except Exception as e:
        log.error(f"  ✗ Failed: {url} — {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HKEX Prospectus Scraper")
    parser.add_argument("--tabs", nargs="+",
                        default=["active"],
                        choices=["active", "listed", "all"],
                        help="Which tabs to scrape")
    parser.add_argument("--boards", nargs="+",
                        default=["main", "gem"],
                        choices=["main", "gem"],
                        help="Which boards to scrape")
    parser.add_argument("--dry-run", action="store_true", help="Don't download, just list")
    parser.add_argument("--include-phip", action="store_true",
                        help="Include PHIP documents (AP+PHIP tab)")
    args = parser.parse_args()

    if args.dry_run:
        global DRY_RUN
        DRY_RUN = True

    state = load_state()
    all_downloads = []

    # Build endpoint keys to fetch
    endpoint_keys = []
    tabs = args.tabs if "all" not in args.tabs else ["active", "listed"]

    for tab in tabs:
        for board in args.boards:
            if tab == "active":
                endpoint_keys.append((f"active_app_{board}", board))
                if args.include_phip:
                    endpoint_keys.append((f"active_phip_{board}", board))
            elif tab == "listed":
                endpoint_keys.append((f"listed_{board}", board))

    # Fetch all indexes
    for key, board in endpoint_keys:
        apps = fetch_index(key)
        downloads = extract_downloads(apps, board)
        all_downloads.extend(downloads)

    # Deduplicate by URL
    seen_urls = set()
    unique_downloads = []
    for d in all_downloads:
        if d["url"] not in seen_urls:
            seen_urls.add(d["url"])
            unique_downloads.append(d)

    log.info(f"\nTotal PDFs to process: {len(unique_downloads)}")

    # Filter out already downloaded
    new_downloads = [
        d for d in unique_downloads
        if hashlib.md5(d["url"].encode()).hexdigest() not in state["downloaded"]
    ]
    log.info(f"New PDFs to download: {len(new_downloads)}")

    if not new_downloads:
        log.info("Nothing new to download. Done.")
        save_state(state)
        return

    # Download sequentially (rate-limited to be respectful to HKEX)
    success_count = 0
    for item in new_downloads:
        result = download_pdf(item, state)
        if result:
            success_count += 1
        save_state(state)  # crash-safe: save after every download

    save_state(state)

    # Write summary for GitHub Actions
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(f"## HKEX Prospectus Scraper\n\n")
            f.write(f"- **Run time**: {datetime.utcnow().isoformat()}Z\n")
            f.write(f"- **Total candidates**: {len(unique_downloads)}\n")
            f.write(f"- **New downloads**: {success_count}\n")
            f.write(f"- **Previously downloaded**: {len(state['downloaded'])}\n\n")

            if success_count > 0:
                f.write("### New Downloads\n\n")
                f.write("| Applicant | Document | Date |\n")
                f.write("|-----------|----------|------|\n")
                for item in new_downloads:
                    if hashlib.md5(item["url"].encode()).hexdigest() in state["downloaded"]:
                        f.write(f"| {item['applicant'][:40]} | {item['doc_type'][:30]} | {item['date']} |\n")

    log.info(f"\nDone. Downloaded {success_count} new PDFs.")


if __name__ == "__main__":
    main()
