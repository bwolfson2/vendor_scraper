"""Base scraper class with retry logic, rate limiting, and progress tracking."""

import abc
import csv
import logging
import sqlite3
import time
from pathlib import Path
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scraper.schema import ContractRecord

logger = logging.getLogger(__name__)


class BaseScraper(abc.ABC):
    """Abstract base for all state spending scrapers."""

    def __init__(self, config: dict):
        self.config = config
        self.state = config["state"]
        self.state_abbr = config["abbreviation"]
        self.base_url = config["portal"]["url"]
        self.rate_limit = config.get("rate_limit", {})
        self.req_per_sec = self.rate_limit.get("requests_per_second", 1)
        self.page_delay = self.rate_limit.get("delay_between_pages", 2)
        self._last_request_time = 0.0

        # Setup requests session with retry
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "User-Agent": "StateSpendingScraper/1.0 (public transparency research)",
            "Accept": "text/html,application/json,text/csv,*/*",
        })

        # Output paths
        self.output_dir = Path("scraper/output") / self.state_abbr.lower()
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _throttle(self):
        """Enforce rate limiting between requests."""
        if self.req_per_sec <= 0:
            return
        min_interval = 1.0 / self.req_per_sec
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    def get(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited GET request."""
        self._throttle()
        logger.debug(f"GET {url}")
        resp = self.session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def post(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited POST request."""
        self._throttle()
        logger.debug(f"POST {url}")
        resp = self.session.post(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    @abc.abstractmethod
    def scrape(self) -> Iterator[ContractRecord]:
        """Yield normalized ContractRecord objects. Subclasses must implement."""
        ...

    def run(self) -> Path:
        """Execute scrape and write results to CSV. Returns output path."""
        output_path = self.output_dir / f"{self.state_abbr.lower()}_contracts.csv"
        count = 0

        logger.info(f"Starting scrape: {self.state} ({self.state_abbr})")

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


class ProgressTracker:
    """SQLite-based progress tracker for resumable scraping."""

    def __init__(self, db_path: str = "scraper/progress.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS scrape_progress (
                state TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                records_scraped INTEGER DEFAULT 0,
                last_page INTEGER DEFAULT 0,
                last_offset INTEGER DEFAULT 0,
                started_at TEXT,
                completed_at TEXT,
                error TEXT
            )
        """)
        self.conn.commit()

    def get_status(self, state: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM scrape_progress WHERE state = ?", (state,)
        ).fetchone()
        if not row:
            return None
        cols = ["state", "status", "records_scraped", "last_page",
                "last_offset", "started_at", "completed_at", "error"]
        return dict(zip(cols, row))

    def mark_started(self, state: str):
        self.conn.execute("""
            INSERT INTO scrape_progress (state, status, started_at)
            VALUES (?, 'running', datetime('now'))
            ON CONFLICT(state) DO UPDATE SET
                status='running', started_at=datetime('now'), error=NULL
        """, (state,))
        self.conn.commit()

    def update_progress(self, state: str, records: int, page: int = 0, offset: int = 0):
        self.conn.execute("""
            UPDATE scrape_progress
            SET records_scraped=?, last_page=?, last_offset=?
            WHERE state=?
        """, (records, page, offset, state))
        self.conn.commit()

    def mark_completed(self, state: str, records: int):
        self.conn.execute("""
            UPDATE scrape_progress
            SET status='completed', records_scraped=?, completed_at=datetime('now')
            WHERE state=?
        """, (records, state))
        self.conn.commit()

    def mark_failed(self, state: str, error: str):
        self.conn.execute("""
            UPDATE scrape_progress
            SET status='failed', error=?
            WHERE state=?
        """, (error, state))
        self.conn.commit()

    def get_all_status(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM scrape_progress ORDER BY state").fetchall()
        cols = ["state", "status", "records_scraped", "last_page",
                "last_offset", "started_at", "completed_at", "error"]
        return [dict(zip(cols, row)) for row in rows]

    def close(self):
        self.conn.close()
