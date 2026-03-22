"""
Adapter for Socrata/Tyler Technologies "Spending App" portals.

These use a proprietary API at /api/chart_data.json that returns vendor summaries.
Transaction-level data isn't available via API — only aggregate vendor spending.

States: AK, IA, NV, OH, SD (and others with locked-down Socrata spending apps)
"""

import logging
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)


class SocrataSpendingAppAdapter:
    """Scraper for Socrata Spending App portals (/api/chart_data.json)."""

    def __init__(self, config: dict):
        self.config = config
        self.state = config["state"]
        self.state_abbr = config["abbreviation"]
        self.base_url = config["portal"]["url"]
        self.field_map = config.get("field_map", {})

        app_cfg = config.get("spending_app", {})
        self.api_base = app_cfg.get("api_base", self.base_url.rstrip("/") + "/api")
        self.years = app_cfg.get("years", ["All Years"])
        self.batch_size = app_cfg.get("batch_size", 10000)

        from pathlib import Path
        self.output_dir = Path("scraper/output") / self.state_abbr.lower()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0",
            "Accept": "application/json",
        })

    def _fetch_vendors(self, year: str) -> list[dict]:
        """Fetch all vendor spending summaries for a year."""
        url = f"{self.api_base}/chart_data.json"
        params = {
            "child_entity": "vendor",
            "sort_field": "total",
            "year": year,
            "limit": self.batch_size,
            "black_list": "true",
        }
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("records", [])
            logger.info(f"{self.state}: Year {year} - {len(records)} vendors")
            return records
        except Exception as e:
            logger.error(f"{self.state}: Failed to fetch year {year}: {e}")
            return []

    def scrape(self) -> Iterator[ContractRecord]:
        """Yield vendor spending records for all configured years."""
        total = 0
        for year in self.years:
            vendors = self._fetch_vendors(year)
            for v in vendors:
                record = ContractRecord(
                    state=self.state,
                    state_abbr=self.state_abbr,
                    vendor_name=v.get("label", v.get("key", "")),
                    amount=v.get("total"),
                    description=f"FY {year} Total Spending",
                    source_url=self.base_url,
                    raw_fields=v,
                )
                yield record
                total += 1

        logger.info(f"{self.state}: Complete - {total:,} vendor records")

    def run(self):
        import csv
        from pathlib import Path

        output_path = self.output_dir / f"{self.state_abbr.lower()}_contracts.csv"
        count = 0

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ContractRecord.csv_headers())
            writer.writeheader()
            for record in self.scrape():
                writer.writerow(record.to_dict())
                count += 1

        logger.info(f"Completed {self.state}: {count:,} records -> {output_path}")
        return output_path
