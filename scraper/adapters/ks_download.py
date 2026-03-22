"""
Kansas-specific adapter that downloads vendor payment CSVs from KanView Data Download Center.
Downloads one CSV per fiscal year + quarter, then combines them.
"""

import csv
import io
import logging
import time
from pathlib import Path
from typing import Iterator

from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)


class KSDownloadAdapter:
    """Download vendor payment CSVs from KanView for all fiscal years/quarters."""

    def __init__(self, config: dict):
        self.config = config
        self.state = config["state"]
        self.state_abbr = config["abbreviation"]
        self.base_url = config["portal"]["url"]
        self.field_map = config.get("field_map", {})
        self.fiscal_years = config.get("ks_download", {}).get("fiscal_years", list(range(2020, 2026)))
        self.quarters = config.get("ks_download", {}).get("quarters", [1, 2, 3, 4])
        self.output_dir = Path("scraper/output") / self.state_abbr.lower()
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _map_record(self, raw: dict) -> ContractRecord:
        fm = self.field_map

        def get(key: str) -> str:
            field_name = fm.get(key, "")
            if not field_name:
                return ""
            return str(raw.get(field_name, "")).strip()

        amount_str = get("amount")
        amount = None
        if amount_str:
            try:
                cleaned = amount_str.replace("$", "").replace(",", "").replace(" ", "").strip()
                amount = float(cleaned) if cleaned and cleaned != "-" else None
            except (ValueError, TypeError):
                amount = None

        return ContractRecord(
            state=self.state,
            state_abbr=self.state_abbr,
            agency_name=get("agency_name"),
            vendor_name=get("vendor_name"),
            contract_id=get("contract_id"),
            description=get("description"),
            amount=amount,
            start_date=get("start_date"),
            end_date=get("end_date"),
            procurement_method=get("procurement_method"),
            contract_type=get("contract_type"),
            commodity_category=get("commodity_category"),
            source_url=self.base_url,
            raw_fields=raw,
        )

    def scrape(self) -> Iterator[ContractRecord]:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            logger.info(f"KS: Loading download center...")
            page.goto(self.base_url, wait_until="networkidle", timeout=30000)
            time.sleep(2)

            for fy in self.fiscal_years:
                for q in self.quarters:
                    logger.info(f"KS: Downloading FY{fy} Q{q}...")
                    try:
                        page.select_option("#MainContent_uxVendorYearList", str(fy))
                        time.sleep(0.5)
                        page.select_option("#MainContent_uxQtrList", str(q))
                        time.sleep(0.5)

                        with page.expect_download(timeout=120000) as download_info:
                            page.click("#MainContent_uxVendorDownloadBtn")

                        download = download_info.value
                        tmp_path = self.output_dir / f"ks_fy{fy}_q{q}.csv"
                        download.save_as(str(tmp_path))

                        # Parse and yield records
                        count = 0
                        with open(tmp_path, encoding="utf-8", errors="replace") as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                yield self._map_record(row)
                                count += 1

                        logger.info(f"KS: FY{fy} Q{q} - {count:,} records")
                        tmp_path.unlink()  # Clean up temp file
                        time.sleep(2)  # Polite delay

                    except Exception as e:
                        logger.error(f"KS: FY{fy} Q{q} failed: {e}")
                        continue

            browser.close()

    def run(self) -> Path:
        output_path = self.output_dir / f"{self.state_abbr.lower()}_contracts.csv"
        count = 0

        logger.info(f"Starting KS download scrape")

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ContractRecord.csv_headers())
            writer.writeheader()

            for record in self.scrape():
                writer.writerow(record.to_dict())
                count += 1
                if count % 10000 == 0:
                    logger.info(f"  KS: {count:,} records written...")

        logger.info(f"Completed KS: {count:,} records -> {output_path}")
        return output_path
