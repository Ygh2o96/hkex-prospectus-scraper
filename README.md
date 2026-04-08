# HKEX Prospectus Scraper

Automated scraper for **Application Proofs**, **PHIPs**, and **Prospectuses** from [HKEXnews](https://www1.hkexnews.hk/app/appindex.html).

Runs on GitHub Actions (daily schedule or manual dispatch). Downloads are stored as **GitHub Actions Artifacts** (90-day retention).

## How It Works

HKEX publishes listing applicant data via JSON endpoints at:
```
https://www1.hkexnews.hk/ncms/json/eds/appactive_app_sehk_e.json   # Main Board active
https://www1.hkexnews.hk/ncms/json/eds/appactive_app_gem_e.json    # GEM active
https://www1.hkexnews.hk/ncms/json/eds/applisted_sehk_e.json       # Main Board listed
https://www1.hkexnews.hk/ncms/json/eds/applisted_gem_e.json        # GEM listed
```

The scraper:
1. Fetches the JSON index for the selected tabs/boards
2. Extracts PDF URLs for Application Proofs, PHIPs, and Prospectuses
3. Downloads new PDFs (skipping previously downloaded via state file)
4. Uploads as GitHub Actions artifacts for download
5. Commits state file to track what's been downloaded

## Usage

### Manual Dispatch (GitHub Actions UI)
Go to **Actions → HKEX Prospectus Scraper → Run workflow** and select:
- **Tabs**: `active` (AP only), `listed` (prospectuses), or `all`
- **Boards**: `main`, `gem`, or `both`
- **Include PHIP**: toggle on for Post Hearing Information Packs
- **Dry run**: preview without downloading

### Scheduled
Runs daily at 08:00 UTC (16:00 HKT) scraping active Main Board + GEM Application Proofs.

### Local
```bash
pip install requests
python scraper.py --tabs active --boards main gem
python scraper.py --tabs all --boards main gem --include-phip
python scraper.py --dry-run --tabs all
```

## Output Structure
```
downloads/
├── MainBoard/
│   ├── Baishan Cloud Holdings Limited/
│   │   └── sehk25101501235.pdf
│   └── ...
└── GEM/
    └── ...
```

## Retrieving Downloads
After a workflow run, go to **Actions → [run] → Artifacts** and download the zip file containing all PDFs.

## Cloud Storage (Optional)
To sync to Google Drive or Dropbox, add a step after the download step using `rclone`:

```yaml
- name: Sync to Google Drive
  run: |
    curl https://rclone.org/install.sh | sudo bash
    rclone copy downloads/ gdrive:HKEX_Prospectuses/ --config ${{ secrets.RCLONE_CONFIG }}
```

## Config

| Env Var | Default | Description |
|---------|---------|-------------|
| `DOWNLOAD_DIR` | `downloads` | Where to save PDFs |
| `STATE_FILE` | `state/downloaded.json` | Tracks downloaded URLs |
| `RATE_LIMIT_SECONDS` | `1.0` | Delay between requests |
| `DRY_RUN` | `false` | Skip actual downloads |
