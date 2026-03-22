#!/usr/bin/env python3
"""
FACTS Contract Bulk Scraper
Downloads all vendor contracts from Florida FACTS system (facts.fldfs.com)
by segmenting searches by agency + record type, then merging CSVs.
"""

import asyncio
import csv
import glob
import os
import time
from pathlib import Path

from playwright.async_api import async_playwright

# --- Configuration ---
BASE_URL = "https://facts.fldfs.com/Search/ContractSearch.aspx"
DOWNLOAD_DIR = Path(__file__).parent / "downloads"
OUTPUT_DIR = Path(__file__).parent / "output"
DOWNLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# All agency codes from the dropdown
AGENCIES = {
    "680000": "AGENCY FOR HEALTH CARE ADMINISTRATION",
    "670000": "AGENCY FOR PERSONS WITH DISABILITIES",
    "420000": "DEPARTMENT OF AGRICULTURE AND CONSUMER SERVICES",
    "600000": "DEPARTMENT OF CHILDREN AND FAMILIES",
    "570000": "DEPARTMENT OF CITRUS",
    "400000": "DEPARTMENT OF COMMERCE",
    "700000": "DEPARTMENT OF CORRECTIONS",
    "480000": "DEPARTMENT OF EDUCATION",
    "650000": "DEPARTMENT OF ELDER AFFAIRS",
    "370000": "DEPARTMENT OF ENVIRONMENTAL PROTECTION",
    "430000": "DEPARTMENT OF FINANCIAL SERVICES",
    "640000": "DEPARTMENT OF HEALTH",
    "760000": "DEPARTMENT OF HIGHWAY SAFETY AND MOTOR VEHICLES",
    "800000": "DEPARTMENT OF JUVENILE JUSTICE",
    "710000": "DEPARTMENT OF LAW ENFORCEMENT",
    "410000": "DEPARTMENT OF LEGAL AFFAIRS",
    "720000": "DEPARTMENT OF MANAGEMENT SERVICES",
    "620000": "DEPARTMENT OF MILITARY AFFAIRS",
    "730000": "DEPARTMENT OF REVENUE",
    "450000": "DEPARTMENT OF STATE",
    "360000": "DEPARTMENT OF THE LOTTERY",
    "550000": "DEPARTMENT OF TRANSPORTATION",
    "500000": "DEPARTMENT OF VETERANS AFFAIRS",
    "790000": "DEPT OF BUSINESS AND PROFESSIONAL REGULATION",
    "729700": "DIVISION OF ADMINISTRATIVE HEARINGS",
    "310000": "EXECUTIVE OFFICE OF THE GOVERNOR",
    "770000": "FISH AND WILDLIFE CONSERVATION COMMISSION",
    "415000": "FL GAMING CONTROL COMMISSION",
    "780000": "FLORIDA COMMISSION ON OFFENDER REVIEW",
    "489000": "FLORIDA SCHOOL FOR THE DEAF AND THE BLIND",
    "210000": "JUSTICE ADMINISTRATION",
    "110000": "LEGISLATURE",
    "610000": "PUBLIC SERVICE COMMISSION",
    "840000": "STATE BOARD OF ADMINISTRATION",
    "220000": "STATE COURTS SYSTEM",
}

# Record type radio button values (for the rblSrchOption radio group)
# Show All = "", Grant Awards Only = "G", Contracts Only = "C", Purchase Orders Only = "P"
RECORD_TYPES = {
    "contracts": "C",
    "purchase_orders": "P",
    "grant_awards": "G",
}

# Max retries per download
MAX_RETRIES = 3
# Timeout for download to complete (seconds) - large agencies can have 200K+ records
DOWNLOAD_TIMEOUT = 600


async def run_search_and_download(page, agency_code, agency_name, record_type_key, record_type_id):
    """
    Execute a single search + download for one agency + record type combo.
    Returns the path to the downloaded file, or None on failure.
    """
    safe_agency = agency_name.replace(" ", "_").replace("'", "").replace("&", "AND")[:50]
    filename = f"{safe_agency}_{record_type_key}.csv"
    dest_path = DOWNLOAD_DIR / filename

    # Skip if already downloaded
    if dest_path.exists() and dest_path.stat().st_size > 100:
        print(f"  SKIP (already exists): {filename}")
        return dest_path

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  Attempt {attempt}/{MAX_RETRIES}: {agency_name} / {record_type_key}")

            # Navigate fresh each attempt
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(2000)

            # Dismiss session timeout popup if present
            try:
                extend_btn = page.locator("#btnExtend")
                if await extend_btn.is_visible(timeout=1000):
                    await extend_btn.click()
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

            # Select agency (rendered ID is PC_ddlAgency)
            await page.select_option("#PC_ddlAgency", value=agency_code)
            await page.wait_for_timeout(500)

            # Select record type radio button by value
            # Radio group name: ctl00$PC$rblSrchOption, values: C, P, G, ""
            await page.click(f"input[name='ctl00$PC$rblSrchOption'][value='{record_type_id}']")
            await page.wait_for_timeout(500)

            # Click Search (rendered ID is PC_btnSearch)
            await page.click("#PC_btnSearch")

            # Wait for either "Download Results" link or "No records found"
            # The search uses ASP.NET UpdatePanel (AJAX), so we wait for content
            try:
                await page.wait_for_selector(
                    'a:has-text("Download Results")',
                    timeout=60000,
                )
                results_loaded = True
            except Exception:
                results_loaded = False

            body_text = await page.text_content("body")
            if not results_loaded or "No records found" in body_text:
                print(f"  NO RESULTS: {agency_name} / {record_type_key}")
                dest_path.write_text("")
                return dest_path

            # Extract result count for logging
            try:
                if "Displaying" in body_text:
                    idx = body_text.index("Displaying")
                    snippet = body_text[idx:idx+60]
                    print(f"  {snippet.strip()}")
            except Exception:
                pass

            # Click Download Results - handle the download
            # Use no_wait_after=True since __doPostBack triggers a slow server-side
            # CSV generation that Playwright mistakes for a pending navigation
            async with page.expect_download(timeout=DOWNLOAD_TIMEOUT * 1000) as download_info:
                download_link = page.locator("a:has-text('Download Results')")
                await download_link.first.click(no_wait_after=True, timeout=10000)

            download = await download_info.value
            await download.save_as(str(dest_path))

            size = dest_path.stat().st_size
            print(f"  SUCCESS: {filename} ({size:,} bytes)")
            return dest_path

        except Exception as e:
            print(f"  ERROR (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                await page.wait_for_timeout(5000)
            else:
                print(f"  FAILED after {MAX_RETRIES} attempts: {agency_name} / {record_type_key}")
                return None


NUM_WORKERS = 5  # Number of parallel browser sessions


async def worker(worker_id, browser, jobs, results, progress):
    """
    Worker coroutine that processes jobs from a shared queue.
    Each worker has its own browser context and page.
    """
    context = await browser.new_context(
        accept_downloads=True,
        viewport={"width": 1280, "height": 900},
    )
    page = await context.new_page()

    while True:
        try:
            agency_code, agency_name, record_type_key, record_type_id = jobs.pop(0)
        except IndexError:
            break  # no more jobs

        progress["done"] += 1
        total = progress["total"]
        n = progress["done"]
        print(f"\n[W{worker_id}] [{n}/{total}] {agency_name} / {record_type_key}")

        result = await run_search_and_download(
            page, agency_code, agency_name, record_type_key, record_type_id
        )

        if result is None:
            results["failed"].append(f"{agency_name}/{record_type_key}")
        elif result.stat().st_size == 0:
            results["empty"].append(f"{agency_name}/{record_type_key}")
        elif result.stat().st_size < 100:
            results["skipped"].append(f"{agency_name}/{record_type_key}")
        else:
            results["success"].append(str(result))

        # Brief pause between requests
        await page.wait_for_timeout(1000)

    await context.close()


async def main(test_mode=False):
    """
    Main scraper loop with parallel workers.
    If test_mode=True, only scrapes Department of Citrus.
    """
    print("=" * 60)
    print(f"FACTS Contract Bulk Scraper ({NUM_WORKERS} parallel workers)")
    print("=" * 60)

    agencies_to_scrape = AGENCIES
    if test_mode:
        agencies_to_scrape = {"570000": "DEPARTMENT OF CITRUS"}
        print("TEST MODE: Only scraping Department of Citrus")

    # Build job list
    jobs = []
    for agency_code, agency_name in agencies_to_scrape.items():
        for record_type_key, record_type_id in RECORD_TYPES.items():
            jobs.append((agency_code, agency_name, record_type_key, record_type_id))

    total_combos = len(jobs)
    print(f"Total download jobs: {total_combos}")
    print(f"Download directory: {DOWNLOAD_DIR}")
    print()

    results = {"success": [], "failed": [], "empty": [], "skipped": []}
    progress = {"done": 0, "total": total_combos}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )

        # Launch workers concurrently
        num = min(NUM_WORKERS, len(jobs))
        tasks = [
            asyncio.create_task(worker(i + 1, browser, jobs, results, progress))
            for i in range(num)
        ]
        await asyncio.gather(*tasks)

        await browser.close()

    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)
    print(f"Successful downloads: {len(results['success'])}")
    print(f"Empty (no records):   {len(results['empty'])}")
    print(f"Failed:               {len(results['failed'])}")
    if results["failed"]:
        print("Failed jobs:")
        for f in results["failed"]:
            print(f"  - {f}")

    return results


def merge_csvs():
    """Merge all downloaded CSVs into a single master file."""
    print("\n" + "=" * 60)
    print("MERGING CSVs")
    print("=" * 60)

    csv_files = sorted(glob.glob(str(DOWNLOAD_DIR / "*.csv")))
    csv_files = [f for f in csv_files if os.path.getsize(f) > 100]  # skip empty markers

    if not csv_files:
        print("No CSV files found to merge!")
        return

    print(f"Found {len(csv_files)} CSV files to merge")

    master_path = OUTPUT_DIR / "facts_all_contracts_master.csv"
    total_rows = 0
    header_written = False

    with open(master_path, "w", newline="", encoding="utf-8") as outfile:
        writer = None

        for csv_file in csv_files:
            try:
                with open(csv_file, "r", encoding="utf-8") as infile:
                    reader = csv.reader(infile)
                    header = next(reader)

                    if not header_written:
                        writer = csv.writer(outfile)
                        writer.writerow(header)
                        header_written = True

                    for row in reader:
                        writer.writerow(row)
                        total_rows += 1
            except Exception as e:
                print(f"  Error reading {csv_file}: {e}")

    size_mb = master_path.stat().st_size / (1024 * 1024)
    print(f"Master file: {master_path}")
    print(f"Total rows: {total_rows:,}")
    print(f"File size: {size_mb:.1f} MB")


if __name__ == "__main__":
    import sys

    if "--test" in sys.argv:
        asyncio.run(main(test_mode=True))
    elif "--merge" in sys.argv:
        merge_csvs()
    else:
        asyncio.run(main(test_mode=False))
        merge_csvs()
