"""
Socrata/SODA API adapter for states using Socrata-powered transparency portals.

Known Socrata states: CO, CT, IA, MD, MA, NV, NY, SD, WA, AK
Uses the SODA API: https://dev.socrata.com/docs/queries/

Pagination via $limit and $offset parameters.
"""

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)

# Default page size for SODA API requests
DEFAULT_PAGE_SIZE = 10000
MAX_RECORDS = 10_000_000  # safety cap


class SocrataAdapter(BaseScraper):
    """Scraper for Socrata/SODA API-backed transparency portals."""

    def __init__(self, config: dict):
        super().__init__(config)
        socrata_cfg = config.get("socrata", {})
        self.domain = socrata_cfg["domain"]
        self.dataset_id = socrata_cfg["dataset_id"]
        self.app_token = socrata_cfg.get("app_token", "")
        self.page_size = config.get("pagination", {}).get("page_size", DEFAULT_PAGE_SIZE)
        self.field_map = config.get("field_map", {})
        self.api_base = f"https://{self.domain}/resource/{self.dataset_id}.json"

        # Add app token to session headers if provided
        if self.app_token:
            self.session.headers["X-App-Token"] = self.app_token

    def _build_url(self, offset: int, where_clause: str = "") -> str:
        """Build SODA API URL with pagination."""
        params = f"$limit={self.page_size}&$offset={offset}&$order=:id"
        if where_clause:
            params += f"&$where={where_clause}"
        return f"{self.api_base}?{params}"

    def _map_record(self, raw: dict) -> ContractRecord:
        """Map a raw SODA JSON record to our unified schema."""
        fm = self.field_map

        def get(key: str, default: str = "") -> str:
            field_name = fm.get(key, "")
            if not field_name:
                return default
            return str(raw.get(field_name, default)).strip()

        # Parse amount - handle currency strings like "$   7,412.00" or "$   - 0"
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

    def get_record_count(self) -> int:
        """Get total record count from the dataset."""
        url = f"https://{self.domain}/resource/{self.dataset_id}.json?$select=count(*) as count"
        try:
            resp = self.get(url)
            data = resp.json()
            if data:
                return int(data[0].get("count", 0))
        except Exception as e:
            logger.warning(f"Could not get record count: {e}")
        return -1

    def discover_fields(self) -> list[str]:
        """Fetch one record to discover available fields."""
        url = f"{self.api_base}?$limit=1"
        try:
            resp = self.get(url)
            data = resp.json()
            if data:
                return list(data[0].keys())
        except Exception as e:
            logger.warning(f"Could not discover fields: {e}")
        return []

    def scrape(self) -> Iterator[ContractRecord]:
        """Paginate through SODA API and yield normalized records."""
        total = self.get_record_count()
        if total > 0:
            logger.info(f"{self.state}: {total:,} total records in dataset")
        else:
            logger.info(f"{self.state}: record count unknown, will paginate until empty")

        offset = 0
        records_yielded = 0

        while offset < MAX_RECORDS:
            url = self._build_url(offset)
            try:
                resp = self.get(url)
                data = resp.json()
            except Exception as e:
                logger.error(f"{self.state}: API error at offset {offset}: {e}")
                break

            if not data:
                logger.info(f"{self.state}: No more records at offset {offset}")
                break

            for raw_record in data:
                yield self._map_record(raw_record)
                records_yielded += 1

            logger.debug(f"{self.state}: Fetched {len(data)} records at offset {offset}")
            offset += self.page_size

            # If we got fewer records than page size, we've reached the end
            if len(data) < self.page_size:
                break

        logger.info(f"{self.state}: Scrape complete - {records_yielded:,} records")


class SocrataDiscovery:
    """Utility to discover Socrata datasets on a domain."""

    def __init__(self, domain: str):
        self.domain = domain
        self.session = __import__("requests").Session()
        self.session.headers["User-Agent"] = "StateSpendingScraper/1.0"

    def search_datasets(self, query: str = "expenditure", limit: int = 20) -> list[dict]:
        """Search for datasets on the domain."""
        url = f"https://api.us.socrata.com/api/catalog/v1?domains={self.domain}&search_context={self.domain}&q={query}&limit={limit}"
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            return [
                {
                    "id": r.get("resource", {}).get("id"),
                    "name": r.get("resource", {}).get("name"),
                    "description": r.get("resource", {}).get("description", "")[:100],
                    "type": r.get("resource", {}).get("type"),
                    "rows": r.get("resource", {}).get("page_views", {}).get("page_views_total"),
                    "columns": r.get("resource", {}).get("columns_name", []),
                }
                for r in results
            ]
        except Exception as e:
            logger.error(f"Dataset search failed for {self.domain}: {e}")
            return []

    def get_dataset_metadata(self, dataset_id: str) -> dict:
        """Get full metadata for a specific dataset."""
        url = f"https://{self.domain}/api/views/{dataset_id}.json"
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            meta = resp.json()
            return {
                "id": meta.get("id"),
                "name": meta.get("name"),
                "description": meta.get("description", ""),
                "columns": [
                    {"name": c.get("fieldName"), "label": c.get("name"), "type": c.get("dataTypeName")}
                    for c in meta.get("columns", [])
                ],
                "row_count": meta.get("rowCount"),
            }
        except Exception as e:
            logger.error(f"Metadata fetch failed for {dataset_id}: {e}")
            return {}
