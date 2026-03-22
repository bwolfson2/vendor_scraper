"""
Orchestrator for running scrapers across multiple states.
Supports parallel execution with per-state rate limiting.
"""

import csv
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from scraper.base import ProgressTracker
from scraper.config.loader import load_all_configs, load_state_config, get_adapter_class
from scraper.schema import ContractRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper/logs/scraper.log"),
    ],
)
logger = logging.getLogger(__name__)


def run_single_state(state_abbr: str, config: dict, tracker: ProgressTracker) -> dict:
    """Run scraper for a single state. Returns result dict."""
    adapter_name = config.get("adapter")
    result = {
        "state": config["state"],
        "abbr": state_abbr,
        "status": "pending",
        "records": 0,
        "error": None,
        "output_path": None,
    }

    try:
        adapter_cls = get_adapter_class(adapter_name)
        scraper = adapter_cls(config)

        tracker.mark_started(state_abbr)
        logger.info(f"=== Starting {config['state']} ({state_abbr}) via {adapter_name} adapter ===")

        output_path = scraper.run()

        # Count records in output
        with open(output_path) as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            count = sum(1 for _ in reader)

        tracker.mark_completed(state_abbr, count)
        result.update(status="completed", records=count, output_path=str(output_path))
        logger.info(f"=== Completed {config['state']}: {count:,} records ===")

    except Exception as e:
        tracker.mark_failed(state_abbr, str(e))
        result.update(status="failed", error=str(e))
        logger.error(f"=== FAILED {config['state']}: {e} ===", exc_info=True)

    return result


def run_all(
    states: list[str] | None = None,
    max_workers: int = 5,
    adapter_filter: str | None = None,
):
    """
    Run scrapers for specified states (or all configured states).

    Args:
        states: List of state abbreviations to run. None = all.
        max_workers: Max parallel scrapers.
        adapter_filter: Only run states using this adapter type.
    """
    configs = load_all_configs()
    tracker = ProgressTracker()

    if states:
        configs = {k: v for k, v in configs.items() if k in states}

    if adapter_filter:
        configs = {k: v for k, v in configs.items() if v.get("adapter") == adapter_filter}

    if not configs:
        logger.warning("No matching state configs found")
        return []

    logger.info(f"Running scrapers for {len(configs)} states with {max_workers} workers")
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_single_state, abbr, config, tracker): abbr
            for abbr, config in configs.items()
        }

        for future in as_completed(futures):
            abbr = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Unexpected error for {abbr}: {e}")
                results.append({
                    "state": abbr, "abbr": abbr,
                    "status": "error", "records": 0, "error": str(e),
                })

    # Print summary
    print("\n" + "=" * 70)
    print("SCRAPE SUMMARY")
    print("=" * 70)

    completed = [r for r in results if r["status"] == "completed"]
    failed = [r for r in results if r["status"] != "completed"]
    total_records = sum(r["records"] for r in completed)

    print(f"Completed: {len(completed)}/{len(results)}")
    print(f"Total records: {total_records:,}")

    if failed:
        print(f"\nFailed ({len(failed)}):")
        for r in failed:
            print(f"  {r['abbr']}: {r['error']}")

    # Merge all outputs
    if completed:
        merge_outputs(completed)

    tracker.close()
    return results


def merge_outputs(results: list[dict]):
    """Merge all state output CSVs into a master file."""
    master_path = Path("scraper/output/master_all_states.csv")
    total = 0

    with open(master_path, "w", newline="", encoding="utf-8") as outf:
        writer = csv.DictWriter(outf, fieldnames=ContractRecord.csv_headers())
        writer.writeheader()

        for r in sorted(results, key=lambda x: x["abbr"]):
            if not r.get("output_path"):
                continue
            try:
                with open(r["output_path"], encoding="utf-8") as inf:
                    reader = csv.DictReader(inf)
                    for row in reader:
                        writer.writerow(row)
                        total += 1
            except Exception as e:
                logger.error(f"Error merging {r['abbr']}: {e}")

    size_mb = master_path.stat().st_size / (1024 * 1024)
    logger.info(f"Master file: {master_path} ({total:,} records, {size_mb:.1f} MB)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="State Spending Portal Scraper")
    parser.add_argument("--states", nargs="*", help="State abbreviations to scrape (e.g. CO IA MD)")
    parser.add_argument("--adapter", help="Only run states using this adapter (e.g. socrata)")
    parser.add_argument("--workers", type=int, default=5, help="Max parallel workers")
    parser.add_argument("--status", action="store_true", help="Show scrape progress status")
    args = parser.parse_args()

    if args.status:
        tracker = ProgressTracker()
        for s in tracker.get_all_status():
            print(f"  {s['state']}: {s['status']} ({s['records_scraped']:,} records)")
        tracker.close()
    else:
        run_all(
            states=args.states,
            max_workers=args.workers,
            adapter_filter=args.adapter,
        )
