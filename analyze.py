#!/usr/bin/env python3
"""
FACTS Contract Data Analysis
Comprehensive analysis of 2.16M Florida state vendor contracts.
"""

import csv
import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

DATA_FILE = Path(__file__).parent / "output" / "facts_all_contracts_master.csv"
REPORT_FILE = Path(__file__).parent / "output" / "analysis_report.json"

def parse_amount(val):
    """Parse dollar amount string to float."""
    if not val or val.strip() == "":
        return 0.0
    try:
        return float(val.replace(",", "").replace("$", "").replace("(", "-").replace(")", "").strip())
    except (ValueError, AttributeError):
        return 0.0

def parse_date(val):
    """Parse date string to datetime."""
    if not val or val.strip() == "":
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None

def main():
    print("=" * 70)
    print("FACTS CONTRACT DATA ANALYSIS")
    print("=" * 70)
    print(f"Reading: {DATA_FILE}")
    print(f"File size: {DATA_FILE.stat().st_size / (1024*1024):.1f} MB")
    print()

    # Counters and accumulators
    total_rows = 0
    type_counts = Counter()
    agency_counts = Counter()
    agency_amounts = defaultdict(float)
    vendor_counts = Counter()
    vendor_amounts = defaultdict(float)
    commodity_counts = Counter()
    commodity_amounts = defaultdict(float)
    status_counts = Counter()
    procurement_counts = Counter()
    year_counts = Counter()
    year_amounts = defaultdict(float)

    # Dollar tracking
    total_original_amount = 0.0
    total_total_amount = 0.0
    max_contract = {"amount": 0, "vendor": "", "agency": "", "title": ""}
    negative_count = 0
    zero_amount_count = 0

    # Date tracking
    earliest_date = None
    latest_date = None

    # Federal/state aid
    aid_counts = Counter()

    # Legal challenges
    legal_challenge_count = 0

    # Insourcing
    insourcing_counts = Counter()

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_rows += 1
            if total_rows % 500000 == 0:
                print(f"  Processing row {total_rows:,}...")

            # Type
            rec_type = row.get("Type", "Unknown").strip()
            type_counts[rec_type] += 1

            # Agency
            agency = row.get("Agency Name", "Unknown").strip()
            agency_counts[agency] += 1

            # Vendor
            vendor = row.get("Vendor/Grantor Name", "Unknown").strip()
            vendor_counts[vendor] += 1

            # Amounts
            orig_amt = parse_amount(row.get("Original Contract Amount", ""))
            total_amt = parse_amount(row.get("Total Amount", ""))

            total_original_amount += orig_amt
            total_total_amount += total_amt
            agency_amounts[agency] += total_amt
            vendor_amounts[vendor] += total_amt

            if total_amt < 0:
                negative_count += 1
            if total_amt == 0:
                zero_amount_count += 1

            if total_amt > max_contract["amount"]:
                max_contract = {
                    "amount": total_amt,
                    "vendor": vendor,
                    "agency": agency,
                    "title": row.get("Long Title/PO Title", "")[:100],
                    "type": rec_type,
                }

            # Commodity
            commodity = row.get("Commodity/Service Type Description", "").strip()
            if commodity:
                commodity_counts[commodity] += 1
                commodity_amounts[commodity] += total_amt

            # Status
            status = row.get("Status", "").strip()
            if status:
                status_counts[status] += 1

            # Procurement method
            procurement = row.get("Method of Procurement", "").strip()
            if procurement:
                procurement_counts[procurement] += 1

            # Date analysis - use Begin Date, Contract Execution Date, or PO Order Date
            date_str = row.get("Begin Date", "") or row.get("Contract Execution Date", "") or row.get("PO Order Date", "")
            dt = parse_date(date_str)
            if dt:
                year = dt.year
                if 1990 <= year <= 2030:  # sanity check
                    year_counts[year] += 1
                    year_amounts[year] += total_amt
                    if earliest_date is None or dt < earliest_date:
                        earliest_date = dt
                    if latest_date is None or dt > latest_date:
                        latest_date = dt

            # Federal/State aid
            aid = row.get("Contract Involves State or Federal Aid", "").strip()
            if aid:
                aid_counts[aid] += 1

            # Legal challenges
            legal = row.get("Legal Challenges to Procurement", "").strip()
            if legal and legal.upper() in ("YES", "Y"):
                legal_challenge_count += 1

            # Insourcing
            insource = row.get("Was the Contracted Functions Considered for Insourcing back to the State", "").strip()
            if insource:
                insourcing_counts[insource] += 1

    # --- Print Report ---
    print(f"\nProcessed {total_rows:,} rows")
    print()

    # 1. Overview
    print("=" * 70)
    print("1. OVERVIEW")
    print("=" * 70)
    print(f"Total records:           {total_rows:,}")
    print(f"Total Original Amount:   ${total_original_amount:,.2f}")
    print(f"Total Amount (current):  ${total_total_amount:,.2f}")
    print(f"Zero-amount records:     {zero_amount_count:,}")
    print(f"Negative-amount records: {negative_count:,}")
    if earliest_date:
        print(f"Date range:              {earliest_date.strftime('%m/%d/%Y')} - {latest_date.strftime('%m/%d/%Y')}")
    print()

    # 2. By Record Type
    print("=" * 70)
    print("2. RECORD TYPES")
    print("=" * 70)
    for t, c in type_counts.most_common():
        pct = c / total_rows * 100
        print(f"  {t:30s} {c:>10,} ({pct:.1f}%)")
    print()

    # 3. Top 15 Agencies by Count
    print("=" * 70)
    print("3. TOP 15 AGENCIES BY RECORD COUNT")
    print("=" * 70)
    for agency, count in agency_counts.most_common(15):
        amt = agency_amounts[agency]
        print(f"  {agency[:45]:45s} {count:>10,}  ${amt:>18,.2f}")
    print()

    # 4. Top 15 Agencies by Dollar Amount
    print("=" * 70)
    print("4. TOP 15 AGENCIES BY TOTAL DOLLAR AMOUNT")
    print("=" * 70)
    sorted_agency_amt = sorted(agency_amounts.items(), key=lambda x: x[1], reverse=True)
    for agency, amt in sorted_agency_amt[:15]:
        count = agency_counts[agency]
        print(f"  {agency[:45]:45s} ${amt:>18,.2f}  ({count:,} records)")
    print()

    # 5. Top 20 Vendors by Count
    print("=" * 70)
    print("5. TOP 20 VENDORS BY RECORD COUNT")
    print("=" * 70)
    for vendor, count in vendor_counts.most_common(20):
        amt = vendor_amounts[vendor]
        print(f"  {vendor[:50]:50s} {count:>8,}  ${amt:>18,.2f}")
    print()

    # 6. Top 20 Vendors by Dollar Amount
    print("=" * 70)
    print("6. TOP 20 VENDORS BY TOTAL DOLLAR AMOUNT")
    print("=" * 70)
    sorted_vendor_amt = sorted(vendor_amounts.items(), key=lambda x: x[1], reverse=True)
    for vendor, amt in sorted_vendor_amt[:20]:
        count = vendor_counts[vendor]
        print(f"  {vendor[:50]:50s} ${amt:>18,.2f}  ({count:,} records)")
    print()

    # 7. Top 15 Commodity/Service Types
    print("=" * 70)
    print("7. TOP 15 COMMODITY/SERVICE TYPES BY COUNT")
    print("=" * 70)
    for commodity, count in commodity_counts.most_common(15):
        amt = commodity_amounts[commodity]
        print(f"  {commodity[:50]:50s} {count:>8,}  ${amt:>18,.2f}")
    print()

    # 8. Contract Status
    print("=" * 70)
    print("8. CONTRACT STATUS DISTRIBUTION")
    print("=" * 70)
    for status, count in status_counts.most_common():
        pct = count / total_rows * 100
        print(f"  {status:30s} {count:>10,} ({pct:.1f}%)")
    print()

    # 9. Procurement Methods
    print("=" * 70)
    print("9. METHOD OF PROCUREMENT")
    print("=" * 70)
    for method, count in procurement_counts.most_common(15):
        pct = count / total_rows * 100
        print(f"  {method[:50]:50s} {count:>10,} ({pct:.1f}%)")
    print()

    # 10. Year-over-Year Trends
    print("=" * 70)
    print("10. YEAR-OVER-YEAR TRENDS")
    print("=" * 70)
    for year in sorted(year_counts.keys()):
        count = year_counts[year]
        amt = year_amounts[year]
        print(f"  {year}  {count:>10,} records  ${amt:>18,.2f}")
    print()

    # 11. Largest Single Contract
    print("=" * 70)
    print("11. LARGEST SINGLE CONTRACT/PO")
    print("=" * 70)
    print(f"  Amount:  ${max_contract['amount']:,.2f}")
    print(f"  Vendor:  {max_contract['vendor']}")
    print(f"  Agency:  {max_contract['agency']}")
    print(f"  Type:    {max_contract['type']}")
    print(f"  Title:   {max_contract['title']}")
    print()

    # 12. Federal/State Aid
    print("=" * 70)
    print("12. CONTRACTS INVOLVING STATE OR FEDERAL AID")
    print("=" * 70)
    for aid, count in aid_counts.most_common():
        print(f"  {aid:30s} {count:>10,}")
    print()

    # 13. Legal Challenges
    print("=" * 70)
    print("13. LEGAL CHALLENGES TO PROCUREMENT")
    print("=" * 70)
    print(f"  Records with legal challenges: {legal_challenge_count:,}")
    print()

    # 14. Insourcing Consideration
    print("=" * 70)
    print("14. INSOURCING CONSIDERATION")
    print("=" * 70)
    for val, count in insourcing_counts.most_common():
        print(f"  {val:30s} {count:>10,}")
    print()

    print("=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
