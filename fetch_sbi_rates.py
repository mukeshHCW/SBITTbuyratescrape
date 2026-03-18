#!/usr/bin/env python3
"""
Fetch SBI TT Buying Rates from the official SBI Forex Card Rates PDF.

Downloads the PDF from sbi.co.in, extracts USD/INR TT Buy rate, date and time,
appends to a CSV log, and optionally archives the raw PDF.

Usage:
    python fetch_sbi_rates.py                    # fetch and append to CSV
    python fetch_sbi_rates.py --archive-pdf      # also save raw PDF by date
    python fetch_sbi_rates.py --dry-run          # print rate without writing

Source: https://sbi.co.in/documents/16012/1400784/FOREX_CARD_RATES.pdf
  - This PDF is overwritten daily (no archive maintained by SBI)
  - Contains rates for Rs. 10-20 lakh range ("To be used as reference rates")
  - These are the rates mandated by Income-tax Rules 1962 (Rule 115/Rule 26)

Design for redundancy:
  - Idempotent: same date+time combo is never duplicated in CSV
  - Runs fine on both Windows (Task Scheduler) and Linux (GitHub Actions)
  - Exit code 0 on success, 1 on fetch failure, 2 on parse failure
"""

import argparse
import csv
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

# ---------- Config ----------
SBI_PDF_URL = "https://sbi.co.in/documents/16012/1400784/FOREX_CARD_RATES.pdf"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SBI-Rate-Fetcher/1.0"

# Resolve paths relative to this script's location
SCRIPT_DIR = Path(__file__).resolve().parent

# Separate output directories for local (Windows scheduler) vs CI (GitHub Actions)
# to avoid merge conflicts when both write to the same repo.
#   local/   — Windows Task Scheduler writes here (git-ignored)
#   ci/      — GitHub Actions writes here (committed to repo)
IS_CI = os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"
OUTPUT_DIR = SCRIPT_DIR / ("ci" if IS_CI else "local")
CSV_PATH = OUTPUT_DIR / "sbi-tt-rates-daily.csv"
PDF_ARCHIVE_DIR = OUTPUT_DIR / "pdf-archive"

CSV_HEADERS = [
    "fetch_utc",       # UTC timestamp of when we fetched
    "pdf_date",        # Date printed on the PDF (DD-MM-YYYY)
    "pdf_time",        # Time printed on the PDF (e.g. "9:30 AM")
    "usd_tt_buy",      # USD/INR TT Buying Rate
    "usd_tt_sell",     # USD/INR TT Selling Rate
    "eur_tt_buy",      # EUR/INR TT Buying Rate
    "gbp_tt_buy",      # GBP/INR TT Buying Rate
]


# ---------- Download ----------
def download_pdf(url: str) -> bytes:
    """Download PDF bytes from URL."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.read()
    except URLError as e:
        print(f"ERROR: Failed to download PDF: {e}", file=sys.stderr)
        sys.exit(1)


# ---------- Parse ----------
def parse_pdf(pdf_bytes: bytes) -> dict:
    """Extract rates from SBI Forex Card Rates PDF using pdfplumber table extraction.

    pdfplumber reads the PDF table structure properly, so we match columns
    by header name ('TT BUY', 'TT SELL') and rows by currency code ('USD/INR').
    This is robust against column reordering or new currencies being added.
    """
    try:
        import pdfplumber
    except ImportError:
        print("ERROR: pdfplumber not installed. Run: pip install pdfplumber", file=sys.stderr)
        sys.exit(2)

    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(pdf_bytes)
    tmp.close()

    try:
        pdf = pdfplumber.open(tmp.name)
        page = pdf.pages[0]

        # --- Extract date and time from page text (above the table) ---
        text = page.extract_text() or ""
        date_match = re.search(r"Date\s+(\d{2}-\d{2}-\d{4})", text)
        if not date_match:
            print(f"ERROR: Could not find date in PDF text", file=sys.stderr)
            sys.exit(2)

        time_match = re.search(r"Time\s+([0-9:]+\s*[AP]M)", text)
        pdf_time = time_match.group(1).strip() if time_match else "UNKNOWN"

        # --- Extract rate table ---
        tables = page.extract_tables()
        pdf.close()
    finally:
        os.unlink(tmp.name)

    if not tables:
        print("ERROR: No tables found in PDF", file=sys.stderr)
        sys.exit(2)

    table = tables[0]
    # Header row: ['CURRENCY', None, 'TT BUY', 'TT SELL', 'BILL BUY', ...]
    headers = [str(h).strip() if h else "" for h in table[0]]

    # Find column indices by header name
    def col_index(name: str) -> int:
        for i, h in enumerate(headers):
            if name in h:
                return i
        return -1

    tt_buy_col = col_index("TT BUY")
    tt_sell_col = col_index("TT SELL")

    if tt_buy_col < 0:
        print(f"ERROR: 'TT BUY' column not found. Headers: {headers}", file=sys.stderr)
        sys.exit(2)

    # Build currency → row lookup (match on CODE/INR in any cell)
    def find_currency_row(code: str) -> list:
        for row in table[1:]:
            if any(cell and f"{code}/INR" in str(cell) for cell in row):
                return row
        return None

    def safe_get(row, col_idx):
        if row and 0 <= col_idx < len(row) and row[col_idx]:
            return str(row[col_idx]).strip()
        return ""

    usd_row = find_currency_row("USD")
    eur_row = find_currency_row("EUR")
    gbp_row = find_currency_row("GBP")

    if not usd_row:
        print("ERROR: USD/INR row not found in table", file=sys.stderr)
        sys.exit(2)

    return {
        "pdf_date": date_match.group(1),
        "pdf_time": pdf_time,
        "usd_tt_buy": safe_get(usd_row, tt_buy_col),
        "usd_tt_sell": safe_get(usd_row, tt_sell_col),
        "eur_tt_buy": safe_get(eur_row, tt_buy_col),
        "gbp_tt_buy": safe_get(gbp_row, tt_buy_col),
    }


# ---------- CSV Append ----------
def is_duplicate(csv_path: Path, pdf_date: str, pdf_time: str) -> bool:
    """Check if this date+time combo already exists in the CSV."""
    if not csv_path.exists():
        return False
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("pdf_date") == pdf_date and row.get("pdf_time") == pdf_time:
                return True
    return False


def append_csv(csv_path: Path, data: dict):
    """Append a row to the CSV file, creating it with headers if needed."""
    file_exists = csv_path.exists() and csv_path.stat().st_size > 0
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)


# ---------- PDF Archive ----------
def archive_pdf(pdf_bytes: bytes, pdf_date: str, pdf_time: str):
    """Save PDF with date-time filename for audit trail."""
    PDF_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    # Convert "17-03-2026" + "9:30 AM" → "2026-03-17_0930"
    parts = pdf_date.split("-")
    date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}"
    time_clean = pdf_time.replace(":", "").replace(" ", "")
    filename = f"SBI_FOREX_{date_iso}_{time_clean}.pdf"
    dest = PDF_ARCHIVE_DIR / filename
    if dest.exists():
        return  # Already archived
    with open(dest, "wb") as f:
        f.write(pdf_bytes)
    print(f"  Archived PDF: {dest.name}")


# ---------- Main ----------
def main():
    parser = argparse.ArgumentParser(description="Fetch SBI TT Buying Rates")
    parser.add_argument("--archive-pdf", action="store_true", help="Save raw PDF by date")
    parser.add_argument("--dry-run", action="store_true", help="Print rate without writing")
    parser.add_argument("--csv-path", type=str, help="Override CSV output path")
    args = parser.parse_args()

    csv_path = Path(args.csv_path) if args.csv_path else CSV_PATH

    # Step 1: Download
    print(f"Fetching SBI Forex Card Rates from sbi.co.in ...")
    pdf_bytes = download_pdf(SBI_PDF_URL)
    print(f"  Downloaded {len(pdf_bytes):,} bytes")

    # Step 2: Parse
    data = parse_pdf(pdf_bytes)
    fetch_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  Date: {data['pdf_date']}  Time: {data['pdf_time']}")
    print(f"  USD/INR TT Buy: {data['usd_tt_buy']}")
    print(f"  USD/INR TT Sell: {data['usd_tt_sell']}")
    print(f"  EUR/INR TT Buy: {data['eur_tt_buy']}")
    print(f"  GBP/INR TT Buy: {data['gbp_tt_buy']}")

    if args.dry_run:
        print("(dry run — not writing to CSV)")
        return

    # Step 3: Dedup check
    if is_duplicate(csv_path, data["pdf_date"], data["pdf_time"]):
        print(f"  SKIP: {data['pdf_date']} {data['pdf_time']} already in CSV")
        return

    # Step 4: Append
    row = {"fetch_utc": fetch_utc, **data}
    append_csv(csv_path, row)
    print(f"  Appended to {csv_path}")

    # Step 5: Archive PDF
    if args.archive_pdf:
        archive_pdf(pdf_bytes, data["pdf_date"], data["pdf_time"])

    print("Done.")


if __name__ == "__main__":
    main()
