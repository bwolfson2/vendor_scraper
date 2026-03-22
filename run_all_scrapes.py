#!/usr/bin/env python3
"""
Master script to run all configured state scrapes sequentially.
Handles timeouts gracefully and tracks progress.

Usage:
    python3 run_all_scrapes.py                    # Run all configured states
    python3 run_all_scrapes.py --states NJ VT OR  # Run specific states
    python3 run_all_scrapes.py --adapter socrata   # Run only Socrata states
    python3 run_all_scrapes.py --status            # Show progress
    python3 run_all_scrapes.py --skip-completed    # Skip already scraped states
"""

import argparse
import csv
import logging
import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from scraper.config.loader import load_all_configs, load_state_config, get_adapter_class
from scraper.schema import ContractRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper/logs/scraper.log", mode="a"),
    ],
)
logger = logging.getLogger("run_all")


def get_output_path(state_abbr: str) -> Path:
    return Path(f"scraper/output/{state_abbr.lower()}/{state_abbr.lower()}_contracts.csv")


def count_records(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path) as f:
        return sum(1 for _ in f) - 1  # minus header


def run_state(state_abbr: str, config: dict) -> dict:
    adapter_name = config.get("adapter")
    result = {"state": config["state"], "abbr": state_abbr, "status": "pending", "records": 0, "error": None}

    try:
        adapter_cls = get_adapter_class(adapter_name)
        adapter = adapter_cls(config)

        logger.info(f"=== Starting {config['state']} ({state_abbr}) via {adapter_name} ===")
        start = time.time()
        output_path = adapter.run()
        elapsed = time.time() - start

        records = count_records(output_path)
        result.update(status="completed", records=records, output_path=str(output_path))
        logger.info(f"=== Completed {config['state']}: {records:,} records in {elapsed:.0f}s ===")

    except Exception as e:
        result.update(status="failed", error=str(e))
        logger.error(f"=== FAILED {config['state']}: {e} ===", exc_info=True)

    return result


def merge_all_outputs():
    master_path = Path("scraper/output/master_all_states.csv")
    total = 0

    output_files = sorted(Path("scraper/output").glob("*/*_contracts.csv"))
    if not output_files:
        logger.warning("No output files to merge")
        return

    with open(master_path, "w", newline="", encoding="utf-8") as outf:
        writer = csv.DictWriter(outf, fieldnames=ContractRecord.csv_headers())
        writer.writeheader()

        for path in output_files:
            try:
                with open(path, encoding="utf-8") as inf:
                    reader = csv.DictReader(inf)
                    for row in reader:
                        writer.writerow(row)
                        total += 1
            except Exception as e:
                logger.error(f"Error merging {path}: {e}")

    size_mb = master_path.stat().st_size / (1024 * 1024)
    logger.info(f"Master file: {master_path} ({total:,} records, {size_mb:.1f} MB)")


def show_status():
    print(f"\n{'State':<6} {'Records':>12} {'Size':>8} {'Adapter':<15}")
    print("-" * 50)

    configs = load_all_configs()
    total_records = 0
    done = 0

    for abbr in sorted(configs.keys()):
        config = configs[abbr]
        path = get_output_path(abbr)
        if path.exists():
            records = count_records(path)
            size_mb = path.stat().st_size / (1024 * 1024)
            total_records += records
            done += 1
            print(f"{abbr:<6} {records:>12,} {size_mb:>7.1f}M {config.get('adapter',''):<15}")
        else:
            print(f"{abbr:<6} {'---':>12} {'---':>8} {config.get('adapter',''):<15}")

    print("-" * 50)
    print(f"{'TOTAL':<6} {total_records:>12,} {'':>8} {done}/{len(configs)} states")


def main():
    parser = argparse.ArgumentParser(description="Run all state spending scrapes")
    parser.add_argument("--states", nargs="*", help="Specific state abbreviations")
    parser.add_argument("--adapter", help="Only run this adapter type")
    parser.add_argument("--status", action="store_true", help="Show scrape status")
    parser.add_argument("--skip-completed", action="store_true", help="Skip states with existing output")
    parser.add_argument("--merge", action="store_true", help="Merge all outputs into master CSV")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.merge:
        merge_all_outputs()
        return

    configs = load_all_configs()

    if args.states:
        configs = {k: v for k, v in configs.items() if k in args.states}
    if args.adapter:
        configs = {k: v for k, v in configs.items() if v.get("adapter") == args.adapter}

    if not configs:
        print("No matching state configs found")
        return

    if args.skip_completed:
        configs = {k: v for k, v in configs.items() if not get_output_path(k).exists() or count_records(get_output_path(k)) == 0}

    print(f"\nRunning scrapes for {len(configs)} states: {', '.join(sorted(configs.keys()))}\n")
    results = []

    for abbr in sorted(configs.keys()):
        result = run_state(abbr, configs[abbr])
        results.append(result)

    # Summary
    completed = [r for r in results if r["status"] == "completed"]
    failed = [r for r in results if r["status"] == "failed"]
    total = sum(r["records"] for r in completed)

    print(f"\n{'='*60}")
    print(f"SCRAPE SUMMARY")
    print(f"{'='*60}")
    print(f"Completed: {len(completed)}/{len(results)}")
    print(f"Total records: {total:,}")
    if failed:
        print(f"\nFailed ({len(failed)}):")
        for r in failed:
            print(f"  {r['abbr']}: {r['error']}")

    # Auto-merge
    if completed:
        merge_all_outputs()


if __name__ == "__main__":
    main()
