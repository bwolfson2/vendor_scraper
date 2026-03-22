"""
Bulk download adapter for states that offer direct CSV/Excel file downloads.

Handles downloading, parsing CSV/Excel files, and normalizing to unified schema.
"""

import csv
import io
import logging
import zipfile
from pathlib import Path
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)


class BulkDownloadAdapter(BaseScraper):
    """Scraper for states with direct CSV/Excel download links."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.field_map = config.get("field_map", {})
        self.download_urls = config.get("download", {}).get("urls", [])
        self.file_format = config.get("download", {}).get("format", "csv")
        self.encoding = config.get("download", {}).get("encoding", "utf-8")
        self.skip_rows = config.get("download", {}).get("skip_rows", 0)

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
            source_url=self.base_url,
            raw_fields=raw,
        )

    def _parse_csv(self, content: str) -> Iterator[dict]:
        """Parse CSV content and yield row dicts."""
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            yield row

    def _parse_excel(self, content: bytes, url: str) -> Iterator[dict]:
        """Parse Excel content and yield row dicts."""
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            if len(rows) < 2:
                return
            headers = [str(h or "").strip() for h in rows[0]]
            for row in rows[1 + self.skip_rows:]:
                yield dict(zip(headers, [str(v or "").strip() for v in row]))
        except ImportError:
            logger.error("openpyxl not installed. Run: pip install openpyxl")

    def _parse_zip(self, content: bytes) -> Iterator[dict]:
        """Extract and parse CSV/Excel files inside a ZIP archive."""
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in zf.namelist():
                logger.info(f"{self.state}: Extracting {name} from ZIP")
                data = zf.read(name)
                if name.lower().endswith(".csv"):
                    text = data.decode(self.encoding, errors="replace")
                    yield from self._parse_csv(text)
                elif name.lower().endswith((".xlsx", ".xls")):
                    yield from self._parse_excel(data, name)

    def scrape(self) -> Iterator[ContractRecord]:
        """Download files and yield normalized records."""
        for url in self.download_urls:
            logger.info(f"{self.state}: Downloading {url}")
            try:
                resp = self.get(url)
            except Exception as e:
                logger.error(f"{self.state}: Failed to download {url}: {e}")
                continue

            if self.file_format == "csv":
                content = resp.content.decode(self.encoding, errors="replace")
                for raw in self._parse_csv(content):
                    yield self._map_record(raw)
            elif self.file_format in ("xlsx", "xls"):
                for raw in self._parse_excel(resp.content, url):
                    yield self._map_record(raw)
            elif self.file_format == "zip":
                for raw in self._parse_zip(resp.content):
                    yield self._map_record(raw)
            else:
                logger.warning(f"{self.state}: Unsupported format: {self.file_format}")
