"""
Playwright-based adapter for JavaScript-heavy portals that can't be scraped with requests.

Handles:
- Socrata spending apps (IA, NV, SD, AK, MD, OH) with locked-down APIs
- ASP.NET WebForms with complex postback chains
- SPAs (React/Angular/Vue) that render data client-side
- Sites behind WAFs/Cloudflare that block requests

Strategy: Navigate with a real browser, intercept network requests for JSON data,
or extract data from rendered DOM tables.
"""

import csv
import json
import logging
import re
import time
from pathlib import Path
from typing import Iterator

from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)


class PlaywrightAdapter:
    """Browser-based scraper using Playwright for JS-heavy portals."""

    def __init__(self, config: dict):
        self.config = config
        self.state = config["state"]
        self.state_abbr = config["abbreviation"]
        self.base_url = config["portal"]["url"]
        self.field_map = config.get("field_map", {})
        self.pw_config = config.get("playwright", {})
        self.scrape_mode = self.pw_config.get("mode", "table")  # table, network, export
        self.table_selector = self.pw_config.get("table_selector", "table")
        self.next_button_selector = self.pw_config.get("next_button", "")
        self.export_button_selector = self.pw_config.get("export_button", "")
        self.search_actions = self.pw_config.get("search_actions", [])
        self.wait_selector = self.pw_config.get("wait_selector", "")
        self.wait_time = self.pw_config.get("wait_time", 3)
        self.max_pages = config.get("pagination", {}).get("max_pages", 0)
        self.page_delay = config.get("rate_limit", {}).get("delay_between_pages", 2)

        # Output paths
        self.output_dir = Path("scraper/output") / self.state_abbr.lower()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Network intercept storage
        self._intercepted_data = []

    def _map_record(self, raw: dict) -> ContractRecord:
        """Map raw record to unified schema."""
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
            contract_type=get("contract_type"),
            description=get("description"),
            amount=amount,
            start_date=get("start_date"),
            end_date=get("end_date"),
            procurement_method=get("procurement_method"),
            commodity_category=get("commodity_category"),
            source_url=self.base_url,
            raw_fields=raw,
        )

    def _extract_table_data(self, page) -> list[dict]:
        """Extract data from rendered HTML tables."""
        tables = page.query_selector_all(self.table_selector)
        if not tables:
            return []

        all_records = []
        for table in tables:
            rows = table.query_selector_all("tr")
            if len(rows) < 2:
                continue

            # Get headers
            header_cells = rows[0].query_selector_all("th, td")
            headers = [cell.inner_text().strip() for cell in header_cells]
            if not headers:
                continue

            # Get data rows
            for row in rows[1:]:
                cells = row.query_selector_all("td")
                if len(cells) != len(headers):
                    continue
                record = {}
                for h, c in zip(headers, cells):
                    record[h] = c.inner_text().strip()
                all_records.append(record)

        return all_records

    def _setup_network_intercept(self, page):
        """Set up network request interception to capture JSON API responses."""
        def handle_response(response):
            try:
                ct = response.headers.get("content-type", "")
                if "json" in ct and response.status == 200:
                    url = response.url
                    # Filter for data-like responses (not analytics, not tiny)
                    if any(skip in url for skip in ["analytics", "pendo", "google", "recaptcha"]):
                        return
                    body = response.json()
                    if isinstance(body, list) and len(body) > 1:
                        self._intercepted_data.append({"url": url, "data": body})
                        logger.info(f"{self.state}: Intercepted {len(body)} records from {url[:80]}")
                    elif isinstance(body, dict) and any(k in body for k in ["results", "data", "rows", "records"]):
                        for key in ["results", "data", "rows", "records"]:
                            if key in body and isinstance(body[key], list):
                                self._intercepted_data.append({"url": url, "data": body[key]})
                                logger.info(f"{self.state}: Intercepted {len(body[key])} records from {url[:80]}")
                                break
            except Exception:
                pass

        page.on("response", handle_response)

    def _perform_search_actions(self, page):
        """Execute configured search actions (click buttons, fill forms, etc.)."""
        for action in self.search_actions:
            action_type = action.get("type", "click")
            selector = action.get("selector", "")
            value = action.get("value", "")

            try:
                if action_type == "click":
                    page.click(selector, timeout=10000)
                elif action_type == "fill":
                    page.fill(selector, value, timeout=10000)
                elif action_type == "select":
                    page.select_option(selector, value, timeout=10000)
                elif action_type == "wait":
                    page.wait_for_timeout(int(value) * 1000)

                time.sleep(0.5)  # Brief pause between actions
            except Exception as e:
                logger.warning(f"{self.state}: Action failed ({action_type} {selector}): {e}")

    def _scrape_table_mode(self, page) -> Iterator[ContractRecord]:
        """Scrape data from rendered HTML tables with pagination."""
        page_num = 0
        total = 0

        while True:
            page_num += 1

            if self.wait_selector:
                try:
                    page.wait_for_selector(self.wait_selector, timeout=15000)
                except Exception:
                    logger.warning(f"{self.state}: Wait selector timeout on page {page_num}")

            time.sleep(self.wait_time)
            records = self._extract_table_data(page)

            if not records:
                logger.info(f"{self.state}: No records on page {page_num}")
                break

            for raw in records:
                yield self._map_record(raw)
                total += 1

            logger.info(f"{self.state}: Page {page_num} - {len(records)} records (total: {total:,})")

            if self.max_pages and page_num >= self.max_pages:
                break

            # Try next page
            if not self.next_button_selector:
                break

            next_btn = page.query_selector(self.next_button_selector)
            if not next_btn or not next_btn.is_visible():
                break

            try:
                next_btn.click()
                time.sleep(self.page_delay)
            except Exception as e:
                logger.warning(f"{self.state}: Next page click failed: {e}")
                break

    def _scrape_network_mode(self, page) -> Iterator[ContractRecord]:
        """Scrape by intercepting network JSON responses."""
        self._intercepted_data = []
        self._setup_network_intercept(page)

        # Navigate and perform search
        page.goto(self.base_url, wait_until="networkidle", timeout=30000)
        time.sleep(self.wait_time)

        if self.search_actions:
            self._perform_search_actions(page)
            time.sleep(self.wait_time)

        # Yield all intercepted records
        total = 0
        for batch in self._intercepted_data:
            for raw in batch["data"]:
                if isinstance(raw, dict):
                    yield self._map_record(raw)
                    total += 1

        logger.info(f"{self.state}: Network mode - {total:,} records from {len(self._intercepted_data)} responses")

    def _scrape_export_mode(self, page) -> Iterator[ContractRecord]:
        """Scrape by triggering CSV/Excel export and parsing the downloaded file."""
        if not self.export_button_selector:
            logger.error(f"{self.state}: No export button selector configured")
            return

        # Set up download handler
        with page.expect_download(timeout=60000) as download_info:
            page.click(self.export_button_selector)

        download = download_info.value
        download_path = self.output_dir / download.suggested_filename
        download.save_as(str(download_path))
        logger.info(f"{self.state}: Downloaded {download_path}")

        # Parse the downloaded file
        suffix = download_path.suffix.lower()
        if suffix == ".csv":
            with open(download_path, encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield self._map_record(row)
        elif suffix in (".xlsx", ".xls"):
            try:
                import openpyxl
                wb = openpyxl.load_workbook(str(download_path), read_only=True)
                ws = wb.active
                rows = list(ws.iter_rows(values_only=True))
                if len(rows) >= 2:
                    headers = [str(h or "").strip() for h in rows[0]]
                    for row in rows[1:]:
                        record = dict(zip(headers, [str(v or "").strip() for v in row]))
                        yield self._map_record(record)
            except ImportError:
                logger.error("openpyxl not installed for Excel parsing")

    def scrape(self) -> Iterator[ContractRecord]:
        """Main scrape method - delegates to mode-specific scraper."""
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )
            page = context.new_page()

            try:
                if self.scrape_mode == "network":
                    yield from self._scrape_network_mode(page)
                elif self.scrape_mode == "export":
                    page.goto(self.base_url, wait_until="networkidle", timeout=30000)
                    time.sleep(self.wait_time)
                    if self.search_actions:
                        self._perform_search_actions(page)
                        time.sleep(self.wait_time)
                    yield from self._scrape_export_mode(page)
                else:  # table mode
                    page.goto(self.base_url, wait_until="networkidle", timeout=30000)
                    time.sleep(self.wait_time)
                    if self.search_actions:
                        self._perform_search_actions(page)
                        time.sleep(self.wait_time)
                    yield from self._scrape_table_mode(page)
            finally:
                browser.close()

    def run(self) -> Path:
        """Execute scrape and write results to CSV."""
        output_path = self.output_dir / f"{self.state_abbr.lower()}_contracts.csv"
        count = 0

        logger.info(f"Starting Playwright scrape: {self.state} ({self.state_abbr}) mode={self.scrape_mode}")

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ContractRecord.csv_headers())
            writer.writeheader()

            for record in self.scrape():
                writer.writerow(record.to_dict())
                count += 1
                if count % 500 == 0:
                    logger.info(f"  {self.state}: {count:,} records written...")

        logger.info(f"Completed {self.state}: {count:,} records -> {output_path}")
        return output_path
