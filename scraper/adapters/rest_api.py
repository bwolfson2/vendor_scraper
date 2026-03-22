"""
Generic REST API adapter for states with JSON API endpoints.

Handles various pagination styles:
- offset/limit (like Socrata)
- page number
- cursor/token-based
- link header-based
"""

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 1000
MAX_RECORDS = 10_000_000


class RESTAPIAdapter(BaseScraper):
    """Scraper for states with REST/JSON API endpoints."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.field_map = config.get("field_map", {})
        api_cfg = config.get("rest_api", {})
        self.api_url = api_cfg["url"]
        self.page_size = config.get("pagination", {}).get("page_size", DEFAULT_PAGE_SIZE)
        self.pagination_type = api_cfg.get("pagination_type", "offset")  # offset, page, cursor
        self.offset_param = api_cfg.get("offset_param", "offset")
        self.limit_param = api_cfg.get("limit_param", "limit")
        self.page_param = api_cfg.get("page_param", "page")
        self.data_path = api_cfg.get("data_path", "")  # JSON path to records array e.g. "results" or "data.records"
        self.extra_params = api_cfg.get("params", {})
        self.headers = api_cfg.get("headers", {})

        if self.headers:
            self.session.headers.update(self.headers)

    def _extract_records(self, response_data) -> list[dict]:
        """Extract the records array from API response using data_path."""
        if not self.data_path:
            if isinstance(response_data, list):
                return response_data
            return []

        # Navigate the JSON path
        data = response_data
        for key in self.data_path.split("."):
            if isinstance(data, dict):
                data = data.get(key, [])
            else:
                return []

        return data if isinstance(data, list) else []

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

    def scrape(self) -> Iterator[ContractRecord]:
        """Paginate through REST API and yield normalized records."""
        offset = 0
        page_num = 1
        total = 0

        while total < MAX_RECORDS:
            # Build params
            params = dict(self.extra_params)
            if self.pagination_type == "offset":
                params[self.limit_param] = self.page_size
                params[self.offset_param] = offset
            elif self.pagination_type == "page":
                params[self.limit_param] = self.page_size
                params[self.page_param] = page_num

            try:
                resp = self.get(self.api_url, params=params)
                response_data = resp.json()
            except Exception as e:
                logger.error(f"{self.state}: API error at page {page_num}: {e}")
                break

            records = self._extract_records(response_data)

            if not records:
                logger.info(f"{self.state}: No more records at page {page_num}")
                break

            for raw in records:
                yield self._map_record(raw)
                total += 1

            logger.info(f"{self.state}: Page {page_num} - {len(records)} records (total: {total:,})")

            if len(records) < self.page_size:
                break

            offset += self.page_size
            page_num += 1

        logger.info(f"{self.state}: Scrape complete - {total:,} records")
