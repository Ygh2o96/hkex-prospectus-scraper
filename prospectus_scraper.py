#!/usr/bin/env python3
"""
HKEX Listed Company Prospectus Scraper
=======================================
Uses Playwright to automate the HKEX Title Search, querying by stock code
+ "Listing Documents" category to find prospectus PDFs.

Data flow:
  1. Download NLR Excel for year(s) → stock codes + prospectus dates
  2. Load activestock JSON → map stock codes to HKEX internal stockIds
  3. Playwright → title search per stock → extract PDF links
  4. Download PDFs

Designed for GitHub Actions with Playwright pre-installed.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

try:
    import openpyxl
except ImportError:
    os.system(f"{sys.executable} -m pip install openpyxl --break-system-packages -q")
    import openpyxl

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    os.system(f"{sys.executable} -m pip install playwright --break-system-packages -q")
    os.system(f"{sys.executable} -m playwright install chromium")
    from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www1.hkexnews.hk"
NLR_BASE = "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/Main"
ACTIVESTOCK_URL = f"{BASE_URL}/ncms/script/eds/activestock_sehk_e.json"
TITLESEARCH_URL = f"{BASE_URL}/search/titlesearch.xhtml"
LISTING_DOCS_CATEGORY = "30000"

DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "downloads/prospectuses"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "state/prospectus_downloaded.json"))
RATE_LIMIT = float(os.environ.get("RATE_LIMIT_SECONDS", "2.0"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml",
})

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"downloaded": {}, "searched": {}, "last_run": None}

def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["last_run"] = datetime.utcnow().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))

# ---------------------------------------------------------------------------
# NLR Excel
# ---------------------------------------------------------------------------

def get_nlr_url(year: int) -> str:
    if year >= 2020:
        return f"{NLR_BASE}/NLR{year}_Eng.xlsx"
    elif year >= 2005:
        return f"{NLR_BASE}/NLR{year}_Eng.xls"
    else:
        return f"{NLR_BASE}/{year}.xls"

def download_nlr(year: int) -> Path:
    url = get_nlr_url(year)
    ext = ".xlsx" if url.endswith(".xlsx") else ".xls"
    local = Path(f"/tmp/NLR{year}{ext}")
    if local.exists():
        return local
    log.info(f"Downloading NLR {year}: {url}")
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    local.write_bytes(resp.content)
    return local

def parse_nlr(filepath: Path) -> list[dict]:
    """Parse NLR Excel → [{stock_code, company, prospectus_date, listing_date}]."""
    wb = openpyxl.load_workbook(filepath, data_only=True)
    ws = wb.active or wb[wb.sheetnames[0]]
    listings = []
    for row in ws.iter_rows(min_row=3, values_only=True):
        stock_code, company, prosp_date, list_date = row[1], row[2], row[3], row[4]
        if not stock_code or not company:
            continue
        if str(stock_code).strip() in ('"', ''):
            continue
        code = str(stock_code).strip().zfill(5)
        if not re.match(r'^\d{5}$', code):
            continue

        def to_date(val):
            if hasattr(val, 'strftime'):
                return val.strftime('%Y-%m-%d')
            if isinstance(val, str):
                for fmt in ('%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d'):
                    try:
                        return datetime.strptime(val.strip(), fmt).strftime('%Y-%m-%d')
                    except ValueError:
                        continue
            return None

        listings.append({
            "stock_code": code,
            "company": str(company).strip(),
            "prospectus_date": to_date(prosp_date),
            "listing_date": to_date(list_date),
        })
    wb.close()
    # Deduplicate (NLR has merged cells that create duplicate rows)
    seen = set()
    deduped = []
    for l in listings:
        key = l["stock_code"]
        if key not in seen:
            seen.add(key)
            deduped.append(l)
    log.info(f"  Parsed {len(deduped)} listings from {filepath.name}")
    return deduped

# ---------------------------------------------------------------------------
# Stock code → HKEX internal ID
# ---------------------------------------------------------------------------

_stock_map = None

def load_stock_map() -> dict:
    global _stock_map
    if _stock_map:
        return _stock_map
    log.info("Loading activestock mapping...")
    _stock_map = {}
    for endpoint in [ACTIVESTOCK_URL,
                     ACTIVESTOCK_URL.replace("activestock", "inactivestock")]:
        try:
            data = SESSION.get(endpoint, timeout=30).json()
            for item in data:
                code = str(item.get("c", "")).strip()
                iid = item.get("i")
                if code and iid:
                    _stock_map.setdefault(code, iid)
        except Exception:
            pass
    log.info(f"  Loaded {len(_stock_map)} stock mappings")
    return _stock_map

# ---------------------------------------------------------------------------
# Playwright search
# ---------------------------------------------------------------------------

def search_listing_docs(browser, stock_code: str, internal_id: int,
                        date_from: str, date_to: str) -> list[dict]:
    """
    Automate HKEX Title Search page with Playwright.
    Returns list of {title, url, date} for PDF results.
    """
    page = browser.new_page()
    page.set_default_timeout(60000)
    results = []

    try:
        # Navigate with pre-filled stock + category
        url = (f"{TITLESEARCH_URL}?lang=EN&market=SEHK"
               f"&stockId={internal_id}&category={LISTING_DOCS_CATEGORY}")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for JS framework to initialize
        page.wait_for_timeout(4000)

        # Set the date range via JS (hidden inputs populated by client JS)
        df = datetime.strptime(date_from, "%Y%m%d")
        dt = datetime.strptime(date_to, "%Y%m%d")
        page.evaluate(f"""() => {{
            const sd = document.getElementById('startDate');
            const ed = document.getElementById('endDate');
            if (sd) sd.value = '{df.strftime("%Y-%m-%d")}';
            if (ed) ed.value = '{dt.strftime("%Y-%m-%d")}';
            // Also set the visible date fields
            const fromUi = document.querySelector('[name="titleSearchByAllResult.dateFromUi"]');
            const toUi = document.querySelector('[name="titleSearchByAllResult.dateToUi"]');
            if (fromUi) fromUi.value = '{df.strftime("%d/%m/%Y")}';
            if (toUi) toUi.value = '{dt.strftime("%d/%m/%Y")}';
        }}""")

        # Find and click the search/apply button
        btn_selectors = [
            "a.filter__btn-apply:not(.btn-disable)",
            ".filter__btn-apply:not(.btn-disable)",
            "a[class*='btn-apply']",
        ]
        for sel in btn_selectors:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                log.debug(f"  Clicked: {sel}")
                break

        # Wait for search results
        try:
            page.wait_for_selector(
                "#titleSearchResultPanel .news_headline,"
                "#titleSearchResultPanel a[href*='.pdf'],"
                ".no-match-text",
                timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(2000)

        # Extract PDF links — try multiple selector strategies
        results = page.evaluate("""() => {
            const links = [];
            // Strategy 1: direct PDF links in result panel
            document.querySelectorAll('#titleSearchResultPanel a[href]').forEach(a => {
                const href = a.getAttribute('href') || '';
                if (href.includes('.pdf')) {
                    const row = a.closest('tr');
                    const cells = row ? row.querySelectorAll('td') : [];
                    const date = cells.length > 0 ? cells[0].innerText.trim() : '';
                    links.push({
                        url: href.startsWith('/') ? 'https://www1.hkexnews.hk' + href : href,
                        title: a.innerText.trim().substring(0, 120),
                        date: date.substring(0, 10)
                    });
                }
            });
            // Strategy 2: any PDF link on page outside nav
            if (links.length === 0) {
                document.querySelectorAll('.search-results a[href*=".pdf"], .result-table a[href*=".pdf"]').forEach(a => {
                    links.push({
                        url: a.href,
                        title: a.innerText.trim().substring(0, 120),
                        date: ''
                    });
                });
            }
            return links;
        }""")

        # Check for "load more" and click if present
        loadmore = page.query_selector(".component-loadmore a:not(.btn-disable)")
        if loadmore and loadmore.is_visible() and len(results) >= 20:
            loadmore.click()
            page.wait_for_timeout(3000)
            extra = page.evaluate("""() => {
                const links = [];
                document.querySelectorAll('#titleSearchResultPanel a[href*=".pdf"]').forEach(a => {
                    links.push({
                        url: a.href.startsWith('/') ? 'https://www1.hkexnews.hk' + a.href : a.href,
                        title: a.innerText.trim().substring(0, 120),
                        date: ''
                    });
                });
                return links;
            }""")
            # Merge, dedup
            seen_urls = {r["url"] for r in results}
            for e in extra:
                if e["url"] not in seen_urls:
                    results.append(e)

        log.info(f"  {stock_code}: {len(results)} listing document(s) found")

    except Exception as e:
        log.error(f"  {stock_code}: search failed — {e}")
    finally:
        page.close()

    return results

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_pdf(url: str, dest: Path, state: dict) -> bool:
    url_hash = hashlib.md5(url.encode()).hexdigest()
    if url_hash in state["downloaded"]:
        return False
    if dest.exists():
        state["downloaded"][url_hash] = {"url": url, "path": str(dest),
                                          "ts": datetime.utcnow().isoformat()}
        return False
    if DRY_RUN:
        log.info(f"    [DRY RUN] {dest.name}")
        return True

    try:
        time.sleep(RATE_LIMIT)
        resp = SESSION.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(65536):
                f.write(chunk)
        size_mb = dest.stat().st_size / (1024 * 1024)
        log.info(f"    ✓ {dest.name} ({size_mb:.1f} MB)")
        state["downloaded"][url_hash] = {"url": url, "path": str(dest),
                                          "ts": datetime.utcnow().isoformat()}
        return True
    except Exception as e:
        log.error(f"    ✗ {dest.name} — {e}")
        return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HKEX Prospectus Scraper (Playwright)")
    parser.add_argument("--years", nargs="+", type=int, required=True,
                        help="Year(s) to scrape, e.g. --years 2024 2025 2026")
    parser.add_argument("--stock", type=str, default=None,
                        help="Single stock code to scrape (testing)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date-margin", type=int, default=7,
                        help="Days around prospectus date to search (default: 7)")
    args = parser.parse_args()

    if args.dry_run:
        global DRY_RUN
        DRY_RUN = True

    state = load_state()

    # 1. Parse NLR files
    all_listings = []
    for year in args.years:
        try:
            nlr_path = download_nlr(year)
            all_listings.extend(parse_nlr(nlr_path))
        except Exception as e:
            log.error(f"NLR {year} failed: {e}")

    if not all_listings:
        log.error("No listings found.")
        return

    if args.stock:
        code = args.stock.zfill(5)
        all_listings = [l for l in all_listings if l["stock_code"] == code]

    log.info(f"\nListings to process: {len(all_listings)}")

    # 2. Load stock map
    stock_map = load_stock_map()

    # 3. Search + download
    success = skip = fail = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )

        for i, listing in enumerate(all_listings):
            code = listing["stock_code"]
            company = listing["company"]
            prosp_date = listing["prospectus_date"]
            search_key = f"{code}_{prosp_date}"

            if search_key in state.get("searched", {}) and not args.stock:
                skip += 1
                continue

            internal_id = stock_map.get(code)
            if not internal_id:
                log.warning(f"  {code} ({company[:30]}): no ID mapping")
                fail += 1
                continue

            log.info(f"\n[{i+1}/{len(all_listings)}] {code} {company[:40]} (prosp: {prosp_date})")

            # Date range
            if prosp_date:
                dt = datetime.strptime(prosp_date, "%Y-%m-%d")
                date_from = (dt - timedelta(days=args.date_margin)).strftime("%Y%m%d")
                date_to = (dt + timedelta(days=args.date_margin)).strftime("%Y%m%d")
            else:
                date_to = datetime.now().strftime("%Y%m%d")
                date_from = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

            # Search
            docs = search_listing_docs(browser, code, internal_id, date_from, date_to)

            # Download
            safe_name = re.sub(r'[<>:"/\\|?*]', '_', company).strip()[:80]
            for doc in docs:
                url = doc["url"]
                if not url.endswith(".pdf"):
                    continue
                filename = url.split("/")[-1]
                dest = DOWNLOAD_DIR / f"{code}_{safe_name}" / filename
                if download_pdf(url, dest, state):
                    success += 1

            state.setdefault("searched", {})[search_key] = {
                "docs_found": len(docs), "ts": datetime.utcnow().isoformat()
            }
            time.sleep(RATE_LIMIT)

        browser.close()

    save_state(state)

    log.info(f"\n{'='*60}")
    log.info(f"Processed: {len(all_listings)} | Downloaded: {success} | Skipped: {skip} | Failed: {fail}")

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(f"\n## Prospectus Scraper\n\n")
            f.write(f"- **Years**: {args.years}\n")
            f.write(f"- **Listings**: {len(all_listings)} | **Downloaded**: {success}\n")

if __name__ == "__main__":
    main()
