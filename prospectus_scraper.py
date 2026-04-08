#!/usr/bin/env python3
"""
HKEX Prospectus Scraper (Listed Companies)
============================================
Downloads prospectuses for newly listed companies by:
1. Downloading NLR Excel files from HKEX (stock codes + prospectus dates)
2. Mapping stock codes to HKEX internal IDs via activestock JSON
3. Using Playwright to search HKEX title search (Listing Documents category)
4. Downloading the prospectus PDFs

Requires: playwright, openpyxl, requests
"""

import json
import os
import re
import sys
import time
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass

import requests
import openpyxl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www1.hkexnews.hk"
NLR_BASE = "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/Main"
ACTIVESTOCK_URL = f"{BASE_URL}/ncms/script/eds/activestock_sehk_e.json"
INACTIVESTOCK_URL = f"{BASE_URL}/ncms/script/eds/inactivestock_sehk_e.json"
TITLE_SEARCH_URL = f"{BASE_URL}/search/titlesearch.xhtml"
# t1Code 30000 = "Listing Documents" (includes prospectuses)
LISTING_DOCS_CATEGORY = "30000"

DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "downloads/prospectuses"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "state/prospectus_downloaded.json"))
RATE_LIMIT = float(os.environ.get("RATE_LIMIT_SECONDS", "2.0"))

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml",
})


@dataclass
class ListingRecord:
    stock_code: str
    company_name: str
    prospectus_date: str       # YYYY-MM-DD
    listing_date: str          # YYYY-MM-DD
    sponsors: str
    year: int
    hkex_internal_id: int = 0  # mapped from activestock


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"downloaded": {}, "last_run": None}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["last_run"] = datetime.now().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Step 1: Download and parse NLR Excel files
# ---------------------------------------------------------------------------

def get_nlr_url(year: int) -> str:
    """Construct NLR download URL for a given year."""
    if year >= 2020:
        return f"{NLR_BASE}/NLR{year}_Eng.xlsx"
    elif year >= 2012:
        return f"{NLR_BASE}/NLR{year}_Eng.xls"
    elif year >= 2005:
        return f"{NLR_BASE}/NLR{year}_Eng.xls"
    else:
        return f"{NLR_BASE}/{year}.xls"


def download_nlr(year: int, dest_dir: Path) -> Path | None:
    """Download NLR Excel file for a given year."""
    url = get_nlr_url(year)
    ext = "xlsx" if url.endswith(".xlsx") else "xls"
    dest = dest_dir / f"NLR{year}.{ext}"
    if dest.exists():
        log.info(f"  NLR {year}: using cached {dest}")
        return dest
    log.info(f"  NLR {year}: downloading from {url}")
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(resp.content)
        return dest
    except Exception as e:
        log.error(f"  NLR {year}: download failed — {e}")
        return None


def parse_nlr(filepath: Path, year: int) -> list[ListingRecord]:
    """Parse NLR Excel file and extract listing records."""
    records = []
    try:
        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.active or wb[wb.sheetnames[0]]
    except Exception as e:
        log.error(f"  Failed to open {filepath}: {e}")
        return []

    # Find header row (contains "Stock Code")
    header_row = None
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=10, values_only=False), 1):
        for cell in row:
            if cell.value and "stock code" in str(cell.value).lower():
                header_row = row_idx
                break
        if header_row:
            break

    if not header_row:
        log.warning(f"  Could not find header row in {filepath}")
        return []

    # Parse data rows (header_row + 1 onwards)
    for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
        # Skip empty rows and note rows
        if not row[1]:  # Column B = Stock Code
            continue
        code = str(row[1]).strip().strip('"').strip("'")
        if not code or not code[0].isdigit():
            continue
        # Pad to 5 digits
        code = code.zfill(5)

        name = str(row[2] or "").strip().strip('"') if row[2] else ""
        if not name or name == '"':
            continue

        # Parse dates
        def parse_date(val):
            if hasattr(val, 'strftime'):
                return val.strftime('%Y-%m-%d')
            if val:
                s = str(val).strip()
                for fmt in ('%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d'):
                    try:
                        return datetime.strptime(s[:10], fmt).strftime('%Y-%m-%d')
                    except ValueError:
                        continue
            return ""

        prosp_date = parse_date(row[3])  # Column D
        list_date = parse_date(row[4])   # Column E
        sponsors = str(row[5] or "").strip() if len(row) > 5 else ""

        if not prosp_date:
            continue

        records.append(ListingRecord(
            stock_code=code,
            company_name=name,
            prospectus_date=prosp_date,
            listing_date=list_date,
            sponsors=sponsors,
            year=year,
        ))

    log.info(f"  NLR {year}: parsed {len(records)} listings")
    return records


# ---------------------------------------------------------------------------
# Step 2: Map stock codes to HKEX internal IDs
# ---------------------------------------------------------------------------

def load_stock_mapping() -> dict[str, int]:
    """Load stock code → HKEX internal ID mapping."""
    mapping = {}
    for url in [ACTIVESTOCK_URL, INACTIVESTOCK_URL]:
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            stocks = resp.json()
            for s in stocks:
                mapping[s["c"]] = s["i"]
        except Exception as e:
            log.warning(f"  Failed to load stock mapping from {url}: {e}")
    log.info(f"  Stock mapping loaded: {len(mapping)} entries")
    return mapping


# ---------------------------------------------------------------------------
# Step 3: Playwright — search HKEX title search for prospectus URLs
# ---------------------------------------------------------------------------

def search_prospectus_urls(records: list[ListingRecord], stock_map: dict) -> dict[str, list[str]]:
    """
    Use Playwright to search HKEX title search for each listing.
    Returns: {stock_code: [pdf_url, ...]}
    """
    from playwright.sync_api import sync_playwright

    results = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        page.set_default_timeout(45000)

        # Navigate to title search page first (establish session)
        log.info("  Opening HKEX title search...")
        page.goto(TITLE_SEARCH_URL + "?lang=EN", wait_until="networkidle")

        for i, rec in enumerate(records):
            code = rec.stock_code
            internal_id = stock_map.get(code)
            if not internal_id:
                log.warning(f"  [{i+1}/{len(records)}] {code} {rec.company_name}: no internal ID, skipping")
                continue

            log.info(f"  [{i+1}/{len(records)}] Searching {code} {rec.company_name} (prosp date: {rec.prospectus_date})...")

            try:
                # Navigate with stock + category params
                search_url = (
                    f"{TITLE_SEARCH_URL}?lang=EN&market=SEHK"
                    f"&stockId={internal_id}&category={LISTING_DOCS_CATEGORY}"
                )
                page.goto(search_url, wait_until="networkidle")

                # Dismiss disclaimer if present
                accept_btn = page.query_selector("text=ACCEPT")
                if accept_btn and accept_btn.is_visible():
                    accept_btn.click()
                    page.wait_for_timeout(1000)

                # Wait for results to render (JS-driven)
                page.wait_for_timeout(3000)

                # Click search button if results not auto-loaded
                result_panel = page.query_selector("#titleSearchResultPanel")
                panel_text = result_panel.inner_text() if result_panel else ""

                if "No matches" in panel_text or len(panel_text.strip()) < 50:
                    # Try clicking the search/apply button
                    apply_btn = page.query_selector("a.apply-btn-wrapper")
                    if not apply_btn:
                        apply_btn = page.query_selector(".filter-btn-apply")
                    if apply_btn:
                        apply_btn.click()
                        page.wait_for_timeout(4000)

                # Extract PDF links from results
                html = page.content()
                # PDF links in HKEX results follow pattern: /listedco/listconews/sehk/...pdf
                pdf_links = re.findall(
                    r'href="(/listedco/listconews/[^"]*\.pdf)"', html
                )
                # Also check for full URLs
                pdf_links += re.findall(
                    r'href="(https://www1\.hkexnews\.hk/listedco/listconews/[^"]*\.pdf)"', html
                )

                # Normalize URLs
                pdf_urls = []
                seen = set()
                for link in pdf_links:
                    if link.startswith("/"):
                        link = f"{BASE_URL}{link}"
                    if link not in seen:
                        seen.add(link)
                        pdf_urls.append(link)

                if pdf_urls:
                    results[code] = pdf_urls
                    log.info(f"    → Found {len(pdf_urls)} listing document(s)")
                else:
                    log.warning(f"    → No listing documents found")

                    # Fallback: try brute-force on the prospectus date
                    log.info(f"    → Trying brute-force on {rec.prospectus_date}...")
                    bf_urls = brute_force_prospectus(rec.prospectus_date)
                    if bf_urls:
                        results[code] = bf_urls
                        log.info(f"    → Brute-force found {len(bf_urls)} candidate(s)")

            except Exception as e:
                log.error(f"    ✗ Search failed: {e}")
                # Fallback to brute force
                log.info(f"    → Falling back to brute-force on {rec.prospectus_date}...")
                bf_urls = brute_force_prospectus(rec.prospectus_date)
                if bf_urls:
                    results[code] = bf_urls

            time.sleep(RATE_LIMIT)

        browser.close()

    return results


# ---------------------------------------------------------------------------
# Fallback: brute-force prospectus URL discovery
# ---------------------------------------------------------------------------

def brute_force_prospectus(date_str: str, min_size_bytes: int = 1_000_000) -> list[str]:
    """
    Brute-force scan odd sequence numbers on a given date.
    Returns URLs of large PDFs (likely prospectuses).
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    yyyymmdd = dt.strftime("%Y%m%d")
    yyyy = dt.strftime("%Y")
    mmdd = dt.strftime("%m%d")

    base = f"{BASE_URL}/listedco/listconews/sehk/{yyyy}/{mmdd}/{yyyymmdd}"
    candidates = []

    for seq in range(1, 200, 2):  # odd numbers 1-199
        url = f"{base}{seq:05d}.pdf"
        try:
            resp = SESSION.head(url, timeout=8, allow_redirects=True)
            if resp.status_code == 200:
                size = int(resp.headers.get("Content-Length", 0))
                if size >= min_size_bytes:
                    candidates.append(url)
            time.sleep(0.05)
        except Exception:
            continue

    return candidates


# ---------------------------------------------------------------------------
# Step 4: Download PDFs
# ---------------------------------------------------------------------------

def download_prospectus(url: str, record: ListingRecord, state: dict) -> bool:
    """Download a single prospectus PDF."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    if url_hash in state["downloaded"]:
        return False

    safe_name = re.sub(r'[<>:"/\\|?*]', '_', record.company_name).strip()[:80]
    dest_dir = DOWNLOAD_DIR / f"{record.year}" / f"{record.stock_code}_{safe_name}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1]
    dest = dest_dir / filename

    if dest.exists():
        state["downloaded"][url_hash] = {"url": url, "path": str(dest)}
        return False

    try:
        time.sleep(RATE_LIMIT)
        resp = SESSION.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(65536):
                f.write(chunk)
        size_mb = dest.stat().st_size / (1024 * 1024)
        log.info(f"  ✓ {record.stock_code} {record.company_name[:30]} → {filename} ({size_mb:.1f}MB)")
        state["downloaded"][url_hash] = {
            "url": url, "path": str(dest),
            "stock": record.stock_code,
            "date": record.prospectus_date,
        }
        return True
    except Exception as e:
        log.error(f"  ✗ Download failed {url}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HKEX Prospectus Scraper (Listed Companies)")
    parser.add_argument("--years", nargs="+", type=int, required=True,
                        help="NLR years to scrape (e.g. 2024 2025 2026)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List targets without downloading")
    parser.add_argument("--brute-force-only", action="store_true",
                        help="Skip Playwright, use brute-force only")
    parser.add_argument("--stock-codes", nargs="+", default=None,
                        help="Only process these stock codes")
    args = parser.parse_args()

    state = load_state()
    nlr_cache = Path("cache/nlr")
    nlr_cache.mkdir(parents=True, exist_ok=True)

    # Step 1: Download NLR files and parse listings
    log.info("Step 1: Downloading NLR Excel files...")
    all_records = []
    for year in args.years:
        nlr_file = download_nlr(year, nlr_cache)
        if nlr_file:
            records = parse_nlr(nlr_file, year)
            all_records.extend(records)

    log.info(f"\nTotal listings across {len(args.years)} year(s): {len(all_records)}")

    # Filter by stock codes if specified
    if args.stock_codes:
        codes_set = set(c.zfill(5) for c in args.stock_codes)
        all_records = [r for r in all_records if r.stock_code in codes_set]
        log.info(f"Filtered to {len(all_records)} records for codes: {args.stock_codes}")

    if not all_records:
        log.info("No records to process. Done.")
        return

    # Step 2: Load stock mapping
    log.info("\nStep 2: Loading stock code → HKEX ID mapping...")
    stock_map = load_stock_mapping()

    # Assign internal IDs
    unmapped = 0
    for rec in all_records:
        if rec.stock_code in stock_map:
            rec.hkex_internal_id = stock_map[rec.stock_code]
        else:
            unmapped += 1
    if unmapped:
        log.warning(f"  {unmapped} stock(s) not found in active/inactive mapping")

    if args.dry_run:
        log.info("\n[DRY RUN] Listings to process:")
        for rec in all_records:
            mapped = "✓" if rec.hkex_internal_id else "✗"
            log.info(f"  {mapped} {rec.stock_code} | {rec.prospectus_date} | {rec.company_name[:50]}")
        return

    # Step 3: Search for prospectus URLs
    if args.brute_force_only:
        log.info("\nStep 3: Brute-force scanning prospectus dates...")
        url_map = {}
        for i, rec in enumerate(all_records):
            log.info(f"  [{i+1}/{len(all_records)}] {rec.stock_code} {rec.company_name[:30]} ({rec.prospectus_date})...")
            urls = brute_force_prospectus(rec.prospectus_date)
            if urls:
                url_map[rec.stock_code] = urls
                log.info(f"    → {len(urls)} candidate(s)")
            else:
                log.warning(f"    → No large PDFs found")
    else:
        log.info("\nStep 3: Searching via Playwright...")
        url_map = search_prospectus_urls(all_records, stock_map)

    # Step 4: Download
    log.info(f"\nStep 4: Downloading prospectuses...")
    total_found = sum(len(v) for v in url_map.values())
    log.info(f"  Found URLs for {len(url_map)}/{len(all_records)} stocks ({total_found} PDFs)")

    success = 0
    for rec in all_records:
        urls = url_map.get(rec.stock_code, [])
        for url in urls:
            if download_prospectus(url, rec, state):
                success += 1

    save_state(state)

    # Summary
    log.info(f"\n{'='*60}")
    log.info(f"Done. Downloaded {success} new prospectus PDFs.")
    log.info(f"Total in state: {len(state['downloaded'])} files")

    # GitHub Actions summary
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(f"## Prospectus Scraper\n\n")
            f.write(f"- **Years**: {args.years}\n")
            f.write(f"- **Listings found**: {len(all_records)}\n")
            f.write(f"- **URLs discovered**: {total_found}\n")
            f.write(f"- **New downloads**: {success}\n")


if __name__ == "__main__":
    main()
