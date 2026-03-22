#!/usr/bin/env python3
"""
Download and process large bulk CSV states: IN and CA.
These have too many files for YAML configs, so we handle them directly.
"""

import csv
import io
import logging
import os
import sys
import time
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_FIELDS = [
    "state", "state_abbr", "agency_name", "vendor_name", "contract_id",
    "contract_type", "description", "amount", "start_date", "end_date",
    "procurement_method", "commodity_category", "source_url",
]


def clean_amount(val):
    if not val:
        return ""
    try:
        cleaned = val.replace("$", "").replace(",", "").replace(" ", "").strip()
        if not cleaned or cleaned == "-":
            return ""
        return str(float(cleaned))
    except (ValueError, TypeError):
        return ""


def download_and_process_csv(url, state, state_abbr, field_map, writer, session, timeout=300):
    """Download a CSV file and write mapped records to the output writer."""
    logger.info(f"  Downloading: {os.path.basename(url)}")
    try:
        resp = session.get(url, timeout=timeout, stream=True)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"  Failed to download {url}: {e}")
        return 0

    content = resp.content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    count = 0
    for raw in reader:
        row = {
            "state": state,
            "state_abbr": state_abbr,
            "agency_name": raw.get(field_map.get("agency_name", ""), "").strip(),
            "vendor_name": raw.get(field_map.get("vendor_name", ""), "").strip(),
            "contract_id": raw.get(field_map.get("contract_id", ""), "").strip(),
            "contract_type": raw.get(field_map.get("contract_type", ""), "").strip(),
            "description": raw.get(field_map.get("description", ""), "").strip(),
            "amount": clean_amount(raw.get(field_map.get("amount", ""), "")),
            "start_date": raw.get(field_map.get("start_date", ""), "").strip(),
            "end_date": raw.get(field_map.get("end_date", ""), "").strip(),
            "procurement_method": "",
            "commodity_category": raw.get(field_map.get("commodity_category", ""), "").strip(),
            "source_url": url,
        }
        writer.writerow(row)
        count += 1
    return count


def run_indiana():
    """Download all IN quarterly vendor CSVs."""
    logger.info("=" * 60)
    logger.info("INDIANA - Downloading quarterly vendor CSVs from CKAN")
    logger.info("=" * 60)

    url_file = "scraper/config/states/in_urls.txt"
    if not os.path.exists(url_file):
        logger.error(f"URL file not found: {url_file}")
        return

    with open(url_file) as f:
        urls = [line.strip() for line in f if line.strip()]

    field_map = {
        "agency_name": "Agency Name",
        "vendor_name": "Vendor Name",
        "contract_id": "Voucher ID",
        "description": "Expenditure Category",
        "amount": "Amount",
        "start_date": "Journal Date",
        "commodity_category": "Function of Government",
    }

    outdir = "scraper/output/in"
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "in_contracts.csv")

    # Check for resume
    existing_count = 0
    completed_urls = set()
    if os.path.exists(outpath):
        with open(outpath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_count += 1
                completed_urls.add(row.get("source_url", ""))
        logger.info(f"  Resuming: {existing_count:,} existing records, {len(completed_urls)} URLs done")

    mode = "a" if existing_count > 0 else "w"
    session = requests.Session()
    session.headers["User-Agent"] = "StateSpendingScraper/1.0"

    total = existing_count
    with open(outpath, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SCHEMA_FIELDS)
        if mode == "w":
            writer.writeheader()

        for i, url in enumerate(urls):
            if url in completed_urls:
                logger.info(f"  [{i+1}/{len(urls)}] Already done: {os.path.basename(url)}")
                continue
            logger.info(f"  [{i+1}/{len(urls)}] Processing...")
            count = download_and_process_csv(url, "Indiana", "IN", field_map, writer, session, timeout=600)
            total += count
            logger.info(f"  Got {count:,} records (total: {total:,})")
            f.flush()
            time.sleep(2)

    logger.info(f"INDIANA COMPLETE: {total:,} total records -> {outpath}")


def run_california():
    """Download all CA monthly vendor CSVs from Azure blob."""
    logger.info("=" * 60)
    logger.info("CALIFORNIA - Downloading monthly vendor CSVs from Azure")
    logger.info("=" * 60)

    # Fetch pointer CSV
    pointer_url = "https://adwoutputfilesadlsstore.blob.core.windows.net/transparency/MonthlyVendorTransactionPointer/MonthlyVendorTransactionPointer.csv"
    resp = requests.get(pointer_url, timeout=30)
    reader = csv.DictReader(io.StringIO(resp.text))
    urls = [row["Download"].strip('"') for row in reader]
    logger.info(f"Found {len(urls)} CA vendor files")

    field_map = {
        "agency_name": "agency_name",
        "vendor_name": "VENDOR_NAME",
        "contract_id": "document_id",
        "description": "account_description",
        "amount": "monetary_amount",
        "start_date": "accounting_date",
        "commodity_category": "account_category",
        "contract_type": "program_description",
    }

    outdir = "scraper/output/ca"
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "ca_contracts.csv")

    # Check for resume
    existing_count = 0
    completed_urls = set()
    if os.path.exists(outpath):
        with open(outpath, "r") as f:
            reader_check = csv.DictReader(f)
            for row in reader_check:
                existing_count += 1
                completed_urls.add(row.get("source_url", ""))
        logger.info(f"  Resuming: {existing_count:,} existing records, {len(completed_urls)} URLs done")

    mode = "a" if existing_count > 0 else "w"
    session = requests.Session()
    session.headers["User-Agent"] = "StateSpendingScraper/1.0"

    total = existing_count
    with open(outpath, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SCHEMA_FIELDS)
        if mode == "w":
            writer.writeheader()

        for i, url in enumerate(urls):
            if url in completed_urls:
                logger.info(f"  [{i+1}/{len(urls)}] Already done: {os.path.basename(url)}")
                continue
            logger.info(f"  [{i+1}/{len(urls)}] Processing...")
            count = download_and_process_csv(url, "California", "CA", field_map, writer, session, timeout=600)
            total += count
            logger.info(f"  Got {count:,} records (total: {total:,})")
            f.flush()
            time.sleep(1)

    logger.info(f"CALIFORNIA COMPLETE: {total:,} total records -> {outpath}")


def run_oklahoma():
    """Download all OK vendor payment CSVs from CKAN."""
    logger.info("=" * 60)
    logger.info("OKLAHOMA - Downloading vendor payment CSVs from CKAN")
    logger.info("=" * 60)

    url_file = "scraper/config/states/ok_urls.txt"
    if not os.path.exists(url_file):
        logger.error(f"URL file not found: {url_file}")
        return

    with open(url_file) as f:
        urls = [line.strip() for line in f if line.strip()]

    field_map = {
        "agency_name": "OCP_AGNCY_NAME",
        "vendor_name": "NAME1",
        "contract_id": "VOUCHER_ID",
        "description": "ACCOUNT_DESCR",
        "amount": "PYMNT_AMT",
        "start_date": "TO_CHAR(A.PYMNT_DT,'DD-MON-YYYY')",
        "commodity_category": "CLASS_DESCR",
    }

    outdir = "scraper/output/ok"
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "ok_contracts.csv")

    existing_count = 0
    completed_urls = set()
    if os.path.exists(outpath):
        with open(outpath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_count += 1
                completed_urls.add(row.get("source_url", ""))
        logger.info(f"  Resuming: {existing_count:,} existing records, {len(completed_urls)} URLs done")

    mode = "a" if existing_count > 0 else "w"
    session = requests.Session()
    session.headers["User-Agent"] = "StateSpendingScraper/1.0"

    total = existing_count
    with open(outpath, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SCHEMA_FIELDS)
        if mode == "w":
            writer.writeheader()

        for i, url in enumerate(urls):
            if url in completed_urls:
                logger.info(f"  [{i+1}/{len(urls)}] Already done: {os.path.basename(url)}")
                continue
            logger.info(f"  [{i+1}/{len(urls)}] Processing...")
            count = download_and_process_csv(url, "Oklahoma", "OK", field_map, writer, session, timeout=300)
            total += count
            logger.info(f"  Got {count:,} records (total: {total:,})")
            f.flush()
            time.sleep(1)

    logger.info(f"OKLAHOMA COMPLETE: {total:,} total records -> {outpath}")


if __name__ == "__main__":
    states = sys.argv[1:] if len(sys.argv) > 1 else ["IN", "CA"]
    for s in states:
        if s.upper() == "IN":
            run_indiana()
        elif s.upper() == "CA":
            run_california()
        elif s.upper() == "OK":
            run_oklahoma()
        else:
            logger.error(f"Unknown state: {s}")
