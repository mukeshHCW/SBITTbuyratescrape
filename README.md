# SBI TT Buy Rate Scraper

Daily scraper for SBI Telegraphic Transfer Buying Rates (USD/INR, EUR/INR, GBP/INR).

These are the official reference rates mandated by Indian Income-tax Rules 1962 (Rule 115 / Rule 26) for converting foreign income to INR.

## How it works

- **Source**: [SBI Forex Card Rates PDF](https://sbi.co.in/documents/16012/1400784/FOREX_CARD_RATES.pdf) (Rs. 10-20 lakh range, "To be used as reference rates")
- **Extraction**: `pdfplumber` table extraction for reliable column/row matching
- **Schedule**: GitHub Actions runs 3x daily (10AM, 3PM, 8PM IST)
- **Output**: `ci/sbi-tt-rates-daily.csv` with date, time, and TT Buy/Sell rates

## Local usage

```bash
# Dry run (just print the rate)
python fetch_sbi_rates.py --dry-run

# Fetch and save (writes to local/ directory)
python fetch_sbi_rates.py --archive-pdf

# Set up Windows Task Scheduler for 3x daily runs
powershell -ExecutionPolicy Bypass -File setup_windows_scheduler.ps1
```

## Directory layout

```
ci/                     # GitHub Actions output (committed)
local/                  # Windows scheduler output (gitignored)
├── sbi-tt-rates-daily.csv
└── pdf-archive/
```
