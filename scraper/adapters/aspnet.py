"""
ASP.NET WebForms adapter for states using postback-based search forms.

Handles __VIEWSTATE, __EVENTVALIDATION, and __doPostBack pagination.
Known ASP.NET states: FL, AL, KS, KY, NE, OR (SharePoint), WI
"""

import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from scraper.base import BaseScraper
from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)


class ASPNetAdapter(BaseScraper):
    """Scraper for ASP.NET WebForms with ViewState postback."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.field_map = config.get("field_map", {})
        self.aspnet_cfg = config.get("aspnet", {})
        self.search_url = config["portal"]["url"]
        self.form_id = self.aspnet_cfg.get("form_id", "form1")
        self.search_button = self.aspnet_cfg.get("search_button_id", "")
        self.next_page_target = self.aspnet_cfg.get("next_page_target", "")
        self.results_table_id = self.aspnet_cfg.get("results_table_id", "")
        self.export_link_id = self.aspnet_cfg.get("export_link_id", "")
        self.form_fields = self.aspnet_cfg.get("form_fields", {})
        self.max_pages = config.get("pagination", {}).get("max_pages", 0)

    def _extract_viewstate(self, soup: BeautifulSoup) -> dict:
        """Extract ASP.NET hidden form fields."""
        fields = {}
        for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                      "__EVENTTARGET", "__EVENTARGUMENT", "__PREVIOUSPAGE"]:
            tag = soup.find("input", {"name": name})
            if tag:
                fields[name] = tag.get("value", "")
        return fields

    def _parse_table_rows(self, soup: BeautifulSoup) -> list[dict]:
        """Parse results from an HTML table."""
        table = None
        if self.results_table_id:
            table = soup.find("table", {"id": self.results_table_id})
        if not table:
            # Try finding the main data table
            tables = soup.find_all("table", class_=re.compile(r"grid|results|data", re.I))
            if tables:
                table = tables[0]
        if not table:
            # Fallback: largest table on page
            tables = soup.find_all("table")
            table = max(tables, key=lambda t: len(t.find_all("tr")), default=None)

        if not table:
            return []

        rows = table.find_all("tr")
        if len(rows) < 2:
            return []

        # Extract headers from first row
        header_cells = rows[0].find_all(["th", "td"])
        headers = [cell.get_text(strip=True) for cell in header_cells]

        records = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) != len(headers):
                continue
            record = {}
            for h, c in zip(headers, cells):
                record[h] = c.get_text(strip=True)
            records.append(record)

        return records

    def _map_record(self, raw: dict) -> ContractRecord:
        """Map raw table row to unified schema."""
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
                cleaned = amount_str.replace("$", "").replace(",", "").strip()
                amount = float(cleaned) if cleaned else None
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
            source_url=self.search_url,
            raw_fields=raw,
        )

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there's a next page link."""
        if self.next_page_target:
            return soup.find("a", href=re.compile(re.escape(self.next_page_target))) is not None
        # Generic check for pagination "Next" links
        next_link = soup.find("a", string=re.compile(r"Next|>>|›", re.I))
        return next_link is not None

    def _get_next_page_postback(self, soup: BeautifulSoup) -> str | None:
        """Extract the __doPostBack target for next page."""
        if self.next_page_target:
            link = soup.find("a", href=re.compile(re.escape(self.next_page_target)))
        else:
            link = soup.find("a", string=re.compile(r"Next|>>", re.I))

        if not link:
            return None

        href = link.get("href", "")
        match = re.search(r"__doPostBack\('([^']+)'", href)
        if match:
            return match.group(1)
        return None

    def scrape(self) -> Iterator[ContractRecord]:
        """Scrape by submitting search form and paginating through results."""
        logger.info(f"{self.state}: Loading search page {self.search_url}")

        # Initial GET to get the form
        resp = self.get(self.search_url)
        soup = BeautifulSoup(resp.text, "html.parser")
        viewstate = self._extract_viewstate(soup)

        # Build form data for search submission
        form_data = {**viewstate}
        form_data["__EVENTTARGET"] = self.search_button.replace("_", "$")
        form_data["__EVENTARGUMENT"] = ""

        # Add configured form fields (dropdown selections, etc.)
        for field_name, field_value in self.form_fields.items():
            form_data[field_name] = field_value

        # Submit search
        logger.info(f"{self.state}: Submitting search form")
        resp = self.post(self.search_url, data=form_data)
        soup = BeautifulSoup(resp.text, "html.parser")

        page_num = 1
        total_records = 0

        while True:
            records = self._parse_table_rows(soup)

            if not records:
                logger.info(f"{self.state}: No records on page {page_num}")
                break

            for raw in records:
                yield self._map_record(raw)
                total_records += 1

            logger.info(f"{self.state}: Page {page_num} - {len(records)} records (total: {total_records:,})")

            # Check for next page
            if self.max_pages and page_num >= self.max_pages:
                logger.info(f"{self.state}: Reached max pages ({self.max_pages})")
                break

            next_target = self._get_next_page_postback(soup)
            if not next_target:
                logger.info(f"{self.state}: No more pages")
                break

            # Navigate to next page via postback
            viewstate = self._extract_viewstate(soup)
            form_data = {**viewstate}
            form_data["__EVENTTARGET"] = next_target
            form_data["__EVENTARGUMENT"] = ""

            resp = self.post(self.search_url, data=form_data)
            soup = BeautifulSoup(resp.text, "html.parser")
            page_num += 1

        logger.info(f"{self.state}: Complete - {total_records:,} records across {page_num} pages")
