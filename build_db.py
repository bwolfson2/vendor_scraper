#!/usr/bin/env python3
"""
Build SQLite database from FACTS contract data.
Creates normalized tables + vendor intelligence views for procurement officers.
"""

import csv
import hashlib
import os
import re
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DATA_FILE = Path(__file__).parent / "output" / "facts_all_contracts_master.csv"
DB_FILE = Path(__file__).parent / "output" / "facts_contracts.db"

# Increase CSV field size limit for large fields
csv.field_size_limit(10 * 1024 * 1024)


def parse_amount(val):
    if not val or val.strip() == "":
        return None
    try:
        cleaned = val.replace(",", "").replace("$", "").replace("(", "-").replace(")", "").strip()
        return round(float(cleaned), 2)
    except (ValueError, AttributeError):
        return None


def parse_date(val):
    if not val or val.strip() == "":
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(val.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def normalize_vendor(name):
    """Normalize vendor name for deduplication."""
    if not name:
        return ""
    name = name.upper().strip()
    # Remove common suffixes
    for suffix in [", INC.", " INC.", " INC", ", LLC", " LLC", ", L.L.C.", " L.L.C.",
                   ", LTD.", " LTD.", " LTD", ", LP", " LP", " L.P.", ", L.P.",
                   ", CO.", " CO.", ", CORP.", " CORP.", " CORP", ", COMPANY", " COMPANY",
                   ", DBA", " DBA", " D/B/A"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def classify_contract_type(rec_type):
    """Classify into broad categories."""
    if not rec_type:
        return "OTHER"
    t = rec_type.upper()
    if "PURCHASE ORDER" in t:
        return "PURCHASE_ORDER"
    elif "GRANT" in t:
        return "GRANT"
    elif "AGREEMENT" in t or "CONTRACT" in t:
        return "CONTRACT"
    elif "SETTLEMENT" in t:
        return "SETTLEMENT"
    elif "REVENUE" in t:
        return "REVENUE"
    else:
        return "OTHER"


def classify_status(status):
    """Classify into broad status categories."""
    if not status:
        return "UNKNOWN"
    s = status.upper().strip()
    if s in ("ACTIVE", "EXTENDED", "RENEWED"):
        return "ACTIVE"
    elif s in ("ORDERED", "RECEIVING", "APPROVED AWARD", "ANTICIPATED AWARD"):
        return "IN_PROGRESS"
    elif s in ("RECEIVED", "CLOSED OR EXPIRED", "CLOSED"):
        return "COMPLETED"
    elif s in ("CANCELED", "CANCELLED", "CANCELING", "TERMINATED"):
        return "CANCELLED"
    elif s == "INACTIVE FOR ONGOING REPORTING":
        return "INACTIVE"
    else:
        return "OTHER"


def create_schema(conn):
    """Create all database tables."""
    c = conn.cursor()

    # --- Core tables ---

    c.execute("""
    CREATE TABLE IF NOT EXISTS contracts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agency_name TEXT,
        vendor_name TEXT,
        vendor_name_normalized TEXT,
        vendor_name_line2 TEXT,
        record_type TEXT,
        record_type_broad TEXT,
        agency_contract_id TEXT,
        po_number TEXT,
        grant_award_id TEXT,
        original_amount REAL,
        total_amount REAL,
        recurring_budget_amount REAL,
        nonrecurring_budget_amount REAL,
        po_budget_amount REAL,
        commodity_code TEXT,
        commodity_description TEXT,
        title TEXT,
        short_title TEXT,
        status TEXT,
        status_broad TEXT,
        flair_contract_id TEXT,
        begin_date TEXT,
        original_end_date TEXT,
        new_end_date TEXT,
        execution_date TEXT,
        grant_award_date TEXT,
        po_order_date TEXT,
        effective_date TEXT,
        agency_service_area TEXT,
        authorized_advance_payment TEXT,
        procurement_method TEXT,
        state_term_contract_id TEXT,
        agency_reference_number TEXT,
        contract_exemption TEXT,
        statutory_authority TEXT,
        recipient_type TEXT,
        involves_state_federal_aid TEXT,
        provide_admin_cost TEXT,
        admin_cost_pct TEXT,
        provide_periodic_increase TEXT,
        periodic_increase_pct TEXT,
        business_case_done TEXT,
        business_case_date TEXT,
        legal_challenges TEXT,
        legal_challenge_desc TEXT,
        prev_done_by_state TEXT,
        considered_insourcing TEXT,
        vendor_capital_improvements TEXT,
        capital_improvement_desc TEXT,
        capital_improvement_value REAL,
        unamortized_capital_value REAL,
        comment TEXT,
        cfda_code TEXT,
        cfda_description TEXT,
        csfa_code TEXT,
        csfa_description TEXT,
        duration_days INTEGER,
        year_begin INTEGER,
        amount_change REAL,
        amount_change_pct REAL
    )
    """)

    # --- Vendor intelligence tables ---

    c.execute("""
    CREATE TABLE IF NOT EXISTS vendor_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vendor_name_normalized TEXT UNIQUE,
        vendor_name_display TEXT,
        total_records INTEGER DEFAULT 0,
        total_contracts INTEGER DEFAULT 0,
        total_purchase_orders INTEGER DEFAULT 0,
        total_grants INTEGER DEFAULT 0,
        total_original_amount REAL DEFAULT 0,
        total_current_amount REAL DEFAULT 0,
        avg_contract_amount REAL DEFAULT 0,
        median_contract_amount REAL DEFAULT 0,
        max_single_amount REAL DEFAULT 0,
        min_single_amount REAL DEFAULT 0,
        num_agencies_served INTEGER DEFAULT 0,
        agencies_list TEXT,
        num_commodity_types INTEGER DEFAULT 0,
        commodity_types_list TEXT,
        first_contract_date TEXT,
        last_contract_date TEXT,
        years_active INTEGER DEFAULT 0,
        active_contracts INTEGER DEFAULT 0,
        completed_contracts INTEGER DEFAULT 0,
        cancelled_contracts INTEGER DEFAULT 0,
        cancellation_rate REAL DEFAULT 0,
        avg_duration_days REAL DEFAULT 0,
        avg_amount_change_pct REAL DEFAULT 0,
        has_legal_challenges INTEGER DEFAULT 0,
        legal_challenge_count INTEGER DEFAULT 0,
        involves_federal_aid INTEGER DEFAULT 0,
        capital_improvements INTEGER DEFAULT 0,
        procurement_methods TEXT,
        vendor_score REAL DEFAULT 0,
        vendor_tier TEXT,
        last_updated TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS vendor_agency_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vendor_name_normalized TEXT,
        agency_name TEXT,
        total_records INTEGER DEFAULT 0,
        total_amount REAL DEFAULT 0,
        first_date TEXT,
        last_date TEXT,
        active_count INTEGER DEFAULT 0,
        completed_count INTEGER DEFAULT 0,
        cancelled_count INTEGER DEFAULT 0,
        commodity_types TEXT,
        avg_amount REAL DEFAULT 0
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS vendor_commodity_expertise (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vendor_name_normalized TEXT,
        commodity_description TEXT,
        total_records INTEGER DEFAULT 0,
        total_amount REAL DEFAULT 0,
        avg_amount REAL DEFAULT 0,
        first_date TEXT,
        last_date TEXT,
        num_agencies INTEGER DEFAULT 0
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS agency_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agency_name TEXT UNIQUE,
        total_records INTEGER DEFAULT 0,
        total_amount REAL DEFAULT 0,
        num_vendors INTEGER DEFAULT 0,
        top_vendors TEXT,
        num_commodity_types INTEGER DEFAULT 0,
        avg_contract_amount REAL DEFAULT 0,
        cancellation_rate REAL DEFAULT 0
    )
    """)

    conn.commit()


def create_indices(conn):
    """Create indices for fast querying."""
    c = conn.cursor()
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_contracts_vendor_norm ON contracts(vendor_name_normalized)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_agency ON contracts(agency_name)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_status ON contracts(status_broad)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_commodity ON contracts(commodity_description)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_begin_date ON contracts(begin_date)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_year ON contracts(year_begin)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_amount ON contracts(total_amount)",
        "CREATE INDEX IF NOT EXISTS idx_contracts_type ON contracts(record_type_broad)",
        "CREATE INDEX IF NOT EXISTS idx_vendor_profiles_name ON vendor_profiles(vendor_name_normalized)",
        "CREATE INDEX IF NOT EXISTS idx_vendor_profiles_score ON vendor_profiles(vendor_score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_vendor_profiles_tier ON vendor_profiles(vendor_tier)",
        "CREATE INDEX IF NOT EXISTS idx_vendor_agency_vendor ON vendor_agency_history(vendor_name_normalized)",
        "CREATE INDEX IF NOT EXISTS idx_vendor_agency_agency ON vendor_agency_history(agency_name)",
        "CREATE INDEX IF NOT EXISTS idx_vendor_commodity_vendor ON vendor_commodity_expertise(vendor_name_normalized)",
        "CREATE INDEX IF NOT EXISTS idx_vendor_commodity_desc ON vendor_commodity_expertise(commodity_description)",
    ]
    for idx in indices:
        c.execute(idx)
    conn.commit()
    print("  Indices created")


def create_views(conn):
    """Create helpful views for procurement officers."""
    c = conn.cursor()

    # View: Vendor quick lookup
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_vendor_lookup AS
    SELECT
        vp.vendor_name_display AS vendor,
        vp.vendor_tier AS tier,
        vp.vendor_score AS score,
        vp.total_records,
        vp.total_current_amount AS total_amount,
        vp.num_agencies_served,
        vp.num_commodity_types,
        vp.years_active,
        vp.cancellation_rate,
        vp.avg_contract_amount,
        vp.max_single_amount,
        vp.active_contracts,
        vp.completed_contracts,
        vp.has_legal_challenges,
        vp.first_contract_date,
        vp.last_contract_date,
        vp.agencies_list AS agencies,
        vp.commodity_types_list AS commodities,
        vp.procurement_methods
    FROM vendor_profiles vp
    ORDER BY vp.vendor_score DESC
    """)

    # View: Active contracts by vendor
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_active_contracts AS
    SELECT
        vendor_name_normalized AS vendor_norm,
        vendor_name AS vendor,
        agency_name AS agency,
        title,
        total_amount,
        begin_date,
        COALESCE(new_end_date, original_end_date) AS end_date,
        commodity_description AS commodity,
        status,
        procurement_method
    FROM contracts
    WHERE status_broad = 'ACTIVE' OR status_broad = 'IN_PROGRESS'
    ORDER BY total_amount DESC
    """)

    # View: Vendor performance summary
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_vendor_performance AS
    SELECT
        vp.vendor_name_display AS vendor,
        vp.vendor_tier AS tier,
        vp.total_records,
        vp.completed_contracts,
        vp.cancelled_contracts,
        vp.cancellation_rate,
        vp.avg_duration_days,
        vp.avg_amount_change_pct AS avg_cost_overrun_pct,
        vp.has_legal_challenges,
        vp.legal_challenge_count,
        CASE
            WHEN vp.cancellation_rate < 0.05 AND vp.avg_amount_change_pct < 20 THEN 'LOW RISK'
            WHEN vp.cancellation_rate < 0.15 OR vp.avg_amount_change_pct < 50 THEN 'MODERATE RISK'
            ELSE 'HIGH RISK'
        END AS risk_level
    FROM vendor_profiles vp
    WHERE vp.total_records >= 5
    ORDER BY vp.vendor_score DESC
    """)

    # View: Commodity market (who provides what)
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_commodity_market AS
    SELECT
        vce.commodity_description AS commodity,
        vce.vendor_name_normalized AS vendor_norm,
        vp.vendor_name_display AS vendor,
        vp.vendor_tier AS tier,
        vce.total_records,
        vce.total_amount,
        vce.avg_amount,
        vce.num_agencies,
        vce.first_date,
        vce.last_date
    FROM vendor_commodity_expertise vce
    JOIN vendor_profiles vp ON vp.vendor_name_normalized = vce.vendor_name_normalized
    ORDER BY vce.commodity_description, vce.total_amount DESC
    """)

    # View: Agency spending dashboard
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_agency_dashboard AS
    SELECT
        ap.agency_name AS agency,
        ap.total_records,
        ap.total_amount,
        ap.num_vendors,
        ap.avg_contract_amount,
        ap.cancellation_rate,
        ap.top_vendors
    FROM agency_profiles ap
    ORDER BY ap.total_amount DESC
    """)

    conn.commit()
    print("  Views created")


def ingest_contracts(conn):
    """Load all contracts into the database."""
    print("\n--- Ingesting contracts ---")
    c = conn.cursor()

    col_map = {
        "Agency Name": "agency_name",
        "Vendor/Grantor Name": "vendor_name",
        "Vendor/Grantor Name Line 2": "vendor_name_line2",
        "Type": "record_type",
        "Agency Contract ID": "agency_contract_id",
        "PO Number": "po_number",
        "Grant Award ID": "grant_award_id",
        "Original Contract Amount": "original_amount",
        "Total Amount": "total_amount",
        "Recurring Budgetary Amount": "recurring_budget_amount",
        "Non Recurring Budgetary Amount": "nonrecurring_budget_amount",
        "PO Budget Amount": "po_budget_amount",
        "Commodity/Service Type Code": "commodity_code",
        "Commodity/Service Type Description": "commodity_description",
        "Long Title/PO Title": "title",
        "Short Title": "short_title",
        "Status": "status",
        "FLAIR Contract ID": "flair_contract_id",
        "Begin Date": "begin_date",
        "Original End Date": "original_end_date",
        "New End Date": "new_end_date",
        "Contract Execution Date": "execution_date",
        "Grant Award Date": "grant_award_date",
        "PO Order Date": "po_order_date",
        "Agency Service Area": "agency_service_area",
        "Authorized Advanced Payment": "authorized_advance_payment",
        "Method of Procurement": "procurement_method",
        "State Term Contract ID": "state_term_contract_id",
        "Agency Reference Number": "agency_reference_number",
        "Contract Exemption Explanation": "contract_exemption",
        "Statutory Authority": "statutory_authority",
        "Recipient Type": "recipient_type",
        "Contract Involves State or Federal Aid": "involves_state_federal_aid",
        "Provide Administrative Cost": "provide_admin_cost",
        "Administrative Cost Percentage": "admin_cost_pct",
        "Provide for Periodic Increase": "provide_periodic_increase",
        "Periodic Increase Percentage": "periodic_increase_pct",
        "Business Case Study Done": "business_case_done",
        "Business Case Date": "business_case_date",
        "Legal Challenges to Procurement": "legal_challenges",
        "Legal Challenge Description": "legal_challenge_desc",
        "Was the Contracted Functions Previously Done by the State": "prev_done_by_state",
        "Was the Contracted Functions Considered for Insourcing back to the State": "considered_insourcing",
        "Did the Vendor Make Capital Improvements on State Property": "vendor_capital_improvements",
        "Capital Improvement Description": "capital_improvement_desc",
        "Value of Capital Improvements": "capital_improvement_value",
        "Value of Unamortized Capital Improvements": "unamortized_capital_value",
        "Comment": "comment",
        "CFDA Code": "cfda_code",
        "CFDA Description": "cfda_description",
        "CSFA Code": "csfa_code",
        "CSFA Description": "csfa_description",
    }

    amount_fields = {"original_amount", "total_amount", "recurring_budget_amount",
                     "nonrecurring_budget_amount", "po_budget_amount",
                     "capital_improvement_value", "unamortized_capital_value"}
    date_fields = {"begin_date", "original_end_date", "new_end_date",
                   "execution_date", "grant_award_date", "po_order_date", "business_case_date"}

    insert_cols = list(col_map.values()) + [
        "vendor_name_normalized", "record_type_broad", "status_broad",
        "effective_date", "duration_days", "year_begin", "amount_change", "amount_change_pct"
    ]
    placeholders = ",".join(["?"] * len(insert_cols))
    insert_sql = f"INSERT INTO contracts ({','.join(insert_cols)}) VALUES ({placeholders})"

    batch = []
    batch_size = 50000
    total = 0

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            if total % 250000 == 0:
                print(f"  {total:,} rows ingested...")

            values = []
            for csv_col, db_col in col_map.items():
                val = (row.get(csv_col) or "").strip()
                if db_col in amount_fields:
                    val = parse_amount(val)
                elif db_col in date_fields:
                    val = parse_date(val)
                values.append(val)

            # Derived fields
            vendor_norm = normalize_vendor(row.get("Vendor/Grantor Name", ""))
            rec_type_broad = classify_contract_type(row.get("Type", ""))
            status_broad = classify_status(row.get("Status", ""))

            # Effective date (first non-null of begin, execution, po_order, grant_award)
            eff_date = (parse_date(row.get("Begin Date", "")) or
                        parse_date(row.get("Contract Execution Date", "")) or
                        parse_date(row.get("PO Order Date", "")) or
                        parse_date(row.get("Grant Award Date", "")))

            # Duration
            begin = parse_date(row.get("Begin Date", ""))
            end = parse_date(row.get("New End Date", "")) or parse_date(row.get("Original End Date", ""))
            duration = None
            if begin and end:
                try:
                    d1 = datetime.strptime(begin, "%Y-%m-%d")
                    d2 = datetime.strptime(end, "%Y-%m-%d")
                    duration = (d2 - d1).days
                    if duration < 0:
                        duration = None
                except Exception:
                    pass

            # Year
            year_begin = None
            if eff_date:
                try:
                    year_begin = int(eff_date[:4])
                except Exception:
                    pass

            # Amount change
            orig = parse_amount(row.get("Original Contract Amount", ""))
            tot = parse_amount(row.get("Total Amount", ""))
            amount_change = None
            amount_change_pct = None
            if orig is not None and tot is not None and orig != 0:
                amount_change = round(tot - orig, 2)
                amount_change_pct = round((tot - orig) / abs(orig) * 100, 2)

            values.extend([vendor_norm, rec_type_broad, status_broad,
                           eff_date, duration, year_begin, amount_change, amount_change_pct])

            batch.append(values)
            if len(batch) >= batch_size:
                c.executemany(insert_sql, batch)
                conn.commit()
                batch = []

    if batch:
        c.executemany(insert_sql, batch)
        conn.commit()

    print(f"  {total:,} contracts ingested")
    return total


def build_vendor_profiles(conn):
    """Build vendor intelligence profiles."""
    print("\n--- Building vendor profiles ---")
    c = conn.cursor()

    c.execute("""
    INSERT INTO vendor_profiles (
        vendor_name_normalized, vendor_name_display,
        total_records, total_contracts, total_purchase_orders, total_grants,
        total_original_amount, total_current_amount,
        avg_contract_amount, max_single_amount, min_single_amount,
        num_agencies_served, num_commodity_types,
        first_contract_date, last_contract_date,
        active_contracts, completed_contracts, cancelled_contracts,
        has_legal_challenges, legal_challenge_count,
        involves_federal_aid, capital_improvements,
        last_updated
    )
    SELECT
        vendor_name_normalized,
        MAX(vendor_name) AS vendor_name_display,
        COUNT(*) AS total_records,
        SUM(CASE WHEN record_type_broad = 'CONTRACT' THEN 1 ELSE 0 END),
        SUM(CASE WHEN record_type_broad = 'PURCHASE_ORDER' THEN 1 ELSE 0 END),
        SUM(CASE WHEN record_type_broad = 'GRANT' THEN 1 ELSE 0 END),
        SUM(COALESCE(original_amount, 0)),
        SUM(COALESCE(total_amount, 0)),
        AVG(CASE WHEN total_amount IS NOT NULL AND total_amount != 0 THEN total_amount END),
        MAX(total_amount),
        MIN(CASE WHEN total_amount IS NOT NULL AND total_amount > 0 THEN total_amount END),
        COUNT(DISTINCT agency_name),
        COUNT(DISTINCT commodity_description),
        MIN(effective_date),
        MAX(effective_date),
        SUM(CASE WHEN status_broad = 'ACTIVE' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status_broad = 'COMPLETED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status_broad = 'CANCELLED' THEN 1 ELSE 0 END),
        MAX(CASE WHEN UPPER(legal_challenges) IN ('YES', 'Y') THEN 1 ELSE 0 END),
        SUM(CASE WHEN UPPER(legal_challenges) IN ('YES', 'Y') THEN 1 ELSE 0 END),
        MAX(CASE WHEN UPPER(involves_state_federal_aid) = 'Y' THEN 1 ELSE 0 END),
        MAX(CASE WHEN UPPER(vendor_capital_improvements) IN ('YES', 'Y') THEN 1 ELSE 0 END),
        datetime('now')
    FROM contracts
    WHERE vendor_name_normalized != ''
    GROUP BY vendor_name_normalized
    """)
    conn.commit()

    # Update derived fields
    c.execute("""
    UPDATE vendor_profiles SET
        cancellation_rate = CASE WHEN (completed_contracts + cancelled_contracts) > 0
            THEN ROUND(CAST(cancelled_contracts AS REAL) / (completed_contracts + cancelled_contracts), 4)
            ELSE 0 END,
        years_active = CASE
            WHEN first_contract_date IS NOT NULL AND last_contract_date IS NOT NULL
            THEN MAX(1, CAST((julianday(last_contract_date) - julianday(first_contract_date)) / 365.25 AS INTEGER))
            ELSE 0 END
    """)

    # Avg duration
    c.execute("""
    UPDATE vendor_profiles SET avg_duration_days = (
        SELECT AVG(duration_days)
        FROM contracts c
        WHERE c.vendor_name_normalized = vendor_profiles.vendor_name_normalized
        AND c.duration_days IS NOT NULL AND c.duration_days > 0 AND c.duration_days < 36500
    )
    """)

    # Avg amount change pct
    c.execute("""
    UPDATE vendor_profiles SET avg_amount_change_pct = (
        SELECT AVG(amount_change_pct)
        FROM contracts c
        WHERE c.vendor_name_normalized = vendor_profiles.vendor_name_normalized
        AND c.amount_change_pct IS NOT NULL
        AND c.amount_change_pct BETWEEN -100 AND 500
    )
    """)

    # Agencies list (top 5)
    c.execute("SELECT DISTINCT vendor_name_normalized FROM vendor_profiles")
    vendors = [r[0] for r in c.fetchall()]
    print(f"  Updating {len(vendors):,} vendor profiles...")

    batch_count = 0
    for vn in vendors:
        batch_count += 1
        if batch_count % 25000 == 0:
            print(f"    {batch_count:,}/{len(vendors):,} vendors processed...")
            conn.commit()

        # Top agencies
        c.execute("""
            SELECT agency_name, SUM(COALESCE(total_amount, 0)) as amt
            FROM contracts WHERE vendor_name_normalized = ?
            GROUP BY agency_name ORDER BY amt DESC LIMIT 10
        """, (vn,))
        agencies = [r[0] for r in c.fetchall()]

        # Top commodities
        c.execute("""
            SELECT commodity_description, COUNT(*) as cnt
            FROM contracts WHERE vendor_name_normalized = ? AND commodity_description != ''
            GROUP BY commodity_description ORDER BY cnt DESC LIMIT 10
        """, (vn,))
        commodities = [r[0] for r in c.fetchall()]

        # Procurement methods
        c.execute("""
            SELECT procurement_method, COUNT(*) as cnt
            FROM contracts WHERE vendor_name_normalized = ? AND procurement_method != ''
            GROUP BY procurement_method ORDER BY cnt DESC LIMIT 5
        """, (vn,))
        methods = [r[0][:60] for r in c.fetchall()]

        c.execute("""
            UPDATE vendor_profiles SET
                agencies_list = ?,
                commodity_types_list = ?,
                procurement_methods = ?
            WHERE vendor_name_normalized = ?
        """, (
            " | ".join(agencies),
            " | ".join(commodities),
            " | ".join(methods),
            vn
        ))

    conn.commit()

    # --- Vendor scoring ---
    print("  Computing vendor scores...")
    c.execute("""
    UPDATE vendor_profiles SET vendor_score = (
        -- Longevity (0-20 pts): years active
        MIN(20, years_active * 2) +
        -- Volume (0-20 pts): log scale of records
        MIN(20, CASE WHEN total_records > 0 THEN LOG(total_records) * 4 ELSE 0 END) +
        -- Breadth (0-15 pts): agencies served
        MIN(15, num_agencies_served * 1.5) +
        -- Reliability (0-25 pts): inverse of cancellation rate
        (25 * (1 - MIN(1, cancellation_rate * 5))) +
        -- Completion (0-10 pts): completed vs total
        CASE WHEN total_records > 0
            THEN 10.0 * completed_contracts / total_records
            ELSE 0 END +
        -- Recency (0-10 pts): last contract within recent years
        CASE
            WHEN last_contract_date >= date('now', '-1 year') THEN 10
            WHEN last_contract_date >= date('now', '-3 years') THEN 7
            WHEN last_contract_date >= date('now', '-5 years') THEN 4
            ELSE 0
        END
    )
    WHERE total_records >= 1
    """)

    # Vendor tiers
    c.execute("UPDATE vendor_profiles SET vendor_tier = 'PLATINUM' WHERE vendor_score >= 75 AND total_records >= 50")
    c.execute("UPDATE vendor_profiles SET vendor_tier = 'GOLD' WHERE vendor_tier IS NULL AND vendor_score >= 55 AND total_records >= 20")
    c.execute("UPDATE vendor_profiles SET vendor_tier = 'SILVER' WHERE vendor_tier IS NULL AND vendor_score >= 35 AND total_records >= 5")
    c.execute("UPDATE vendor_profiles SET vendor_tier = 'BRONZE' WHERE vendor_tier IS NULL AND vendor_score >= 15")
    c.execute("UPDATE vendor_profiles SET vendor_tier = 'UNRATED' WHERE vendor_tier IS NULL")

    conn.commit()

    # Print tier distribution
    c.execute("SELECT vendor_tier, COUNT(*), AVG(vendor_score) FROM vendor_profiles GROUP BY vendor_tier ORDER BY AVG(vendor_score) DESC")
    print("\n  Vendor Tier Distribution:")
    for tier, cnt, avg_score in c.fetchall():
        print(f"    {tier:12s}: {cnt:>8,} vendors  (avg score: {avg_score:.1f})")


def build_vendor_agency_history(conn):
    """Build vendor-agency relationship table."""
    print("\n--- Building vendor-agency history ---")
    c = conn.cursor()

    c.execute("""
    INSERT INTO vendor_agency_history (
        vendor_name_normalized, agency_name,
        total_records, total_amount, first_date, last_date,
        active_count, completed_count, cancelled_count, avg_amount
    )
    SELECT
        vendor_name_normalized,
        agency_name,
        COUNT(*),
        SUM(COALESCE(total_amount, 0)),
        MIN(effective_date),
        MAX(effective_date),
        SUM(CASE WHEN status_broad = 'ACTIVE' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status_broad = 'COMPLETED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status_broad = 'CANCELLED' THEN 1 ELSE 0 END),
        AVG(CASE WHEN total_amount IS NOT NULL AND total_amount != 0 THEN total_amount END)
    FROM contracts
    WHERE vendor_name_normalized != ''
    GROUP BY vendor_name_normalized, agency_name
    """)

    # Skip the expensive correlated commodity update — commodity info
    # is already available in vendor_commodity_expertise table

    conn.commit()
    c.execute("SELECT COUNT(*) FROM vendor_agency_history")
    print(f"  {c.fetchone()[0]:,} vendor-agency relationships")


def build_vendor_commodity_expertise(conn):
    """Build vendor commodity expertise table."""
    print("\n--- Building vendor commodity expertise ---")
    c = conn.cursor()

    c.execute("""
    INSERT INTO vendor_commodity_expertise (
        vendor_name_normalized, commodity_description,
        total_records, total_amount, avg_amount,
        first_date, last_date, num_agencies
    )
    SELECT
        vendor_name_normalized,
        commodity_description,
        COUNT(*),
        SUM(COALESCE(total_amount, 0)),
        AVG(CASE WHEN total_amount IS NOT NULL AND total_amount != 0 THEN total_amount END),
        MIN(effective_date),
        MAX(effective_date),
        COUNT(DISTINCT agency_name)
    FROM contracts
    WHERE vendor_name_normalized != '' AND commodity_description != ''
    GROUP BY vendor_name_normalized, commodity_description
    """)
    conn.commit()

    c.execute("SELECT COUNT(*) FROM vendor_commodity_expertise")
    print(f"  {c.fetchone()[0]:,} vendor-commodity records")


def build_agency_profiles(conn):
    """Build agency profiles."""
    print("\n--- Building agency profiles ---")
    c = conn.cursor()

    c.execute("""
    INSERT INTO agency_profiles (
        agency_name, total_records, total_amount,
        num_vendors, avg_contract_amount, cancellation_rate
    )
    SELECT
        agency_name,
        COUNT(*),
        SUM(COALESCE(total_amount, 0)),
        COUNT(DISTINCT vendor_name_normalized),
        AVG(CASE WHEN total_amount IS NOT NULL AND total_amount != 0 THEN total_amount END),
        CASE WHEN COUNT(*) > 0
            THEN CAST(SUM(CASE WHEN status_broad = 'CANCELLED' THEN 1 ELSE 0 END) AS REAL) / COUNT(*)
            ELSE 0 END
    FROM contracts
    GROUP BY agency_name
    """)

    # Top vendors per agency
    c.execute("SELECT DISTINCT agency_name FROM agency_profiles")
    agencies = [r[0] for r in c.fetchall()]
    for agency in agencies:
        c.execute("""
            SELECT vendor_name, SUM(COALESCE(total_amount, 0)) as amt
            FROM contracts WHERE agency_name = ? AND vendor_name != ''
            GROUP BY vendor_name ORDER BY amt DESC LIMIT 5
        """, (agency,))
        top = [f"{r[0][:40]} (${r[1]:,.0f})" for r in c.fetchall()]
        c.execute("UPDATE agency_profiles SET top_vendors = ?, num_commodity_types = (SELECT COUNT(DISTINCT commodity_description) FROM contracts WHERE agency_name = ?) WHERE agency_name = ?",
                  (" | ".join(top), agency, agency))

    conn.commit()
    print(f"  {len(agencies)} agency profiles")


def print_sample_queries(conn):
    """Run and display sample queries a procurement officer might use."""
    c = conn.cursor()
    print("\n" + "=" * 70)
    print("SAMPLE PROCUREMENT QUERIES")
    print("=" * 70)

    # 1. Top 10 vendors by score
    print("\n--- Top 10 Vendors by Score ---")
    c.execute("""
        SELECT vendor_name_display, vendor_tier, ROUND(vendor_score, 1),
               total_records, num_agencies_served, years_active,
               ROUND(cancellation_rate * 100, 1) || '%'
        FROM vendor_profiles
        WHERE total_records >= 50
        ORDER BY vendor_score DESC LIMIT 10
    """)
    print(f"  {'Vendor':<45} {'Tier':<10} {'Score':>6} {'Records':>8} {'Agencies':>8} {'Years':>6} {'Cancel%':>8}")
    for r in c.fetchall():
        print(f"  {r[0][:44]:<45} {r[1]:<10} {r[2]:>6} {r[3]:>8,} {r[4]:>8} {r[5]:>6} {r[6]:>8}")

    # 2. Find vendors for IT consulting
    print("\n--- Top IT Consulting Vendors ---")
    c.execute("""
        SELECT vp.vendor_name_display, vp.vendor_tier, vce.total_records,
               ROUND(vce.total_amount, 2), vce.num_agencies
        FROM vendor_commodity_expertise vce
        JOIN vendor_profiles vp ON vp.vendor_name_normalized = vce.vendor_name_normalized
        WHERE vce.commodity_description LIKE '%information technology%consultation%'
        ORDER BY vce.total_amount DESC LIMIT 10
    """)
    print(f"  {'Vendor':<45} {'Tier':<10} {'Records':>8} {'Total $':>18} {'Agencies':>8}")
    for r in c.fetchall():
        print(f"  {r[0][:44]:<45} {r[1]:<10} {r[2]:>8} ${r[3]:>17,.2f} {r[4]:>8}")

    # 3. Vendors with high cancellation rates (risk flags)
    print("\n--- High-Risk Vendors (>20% cancellation, 20+ records) ---")
    c.execute("""
        SELECT vendor_name_display, total_records, cancelled_contracts,
               ROUND(cancellation_rate * 100, 1), ROUND(vendor_score, 1)
        FROM vendor_profiles
        WHERE cancellation_rate > 0.20 AND total_records >= 20
        ORDER BY cancellation_rate DESC LIMIT 10
    """)
    print(f"  {'Vendor':<45} {'Records':>8} {'Cancelled':>10} {'Rate':>8} {'Score':>6}")
    for r in c.fetchall():
        print(f"  {r[0][:44]:<45} {r[1]:>8,} {r[2]:>10,} {r[3]:>7}% {r[4]:>6}")

    # 4. Agency spending concentration
    print("\n--- Agency Vendor Concentration ---")
    c.execute("""
        SELECT agency_name, num_vendors, total_records,
               ROUND(total_amount / 1e9, 2) || 'B'
        FROM agency_profiles
        ORDER BY total_amount DESC LIMIT 10
    """)
    print(f"  {'Agency':<50} {'Vendors':>8} {'Records':>10} {'Total $':>10}")
    for r in c.fetchall():
        print(f"  {r[0][:49]:<50} {r[1]:>8,} {r[2]:>10,} ${r[3]:>9}")

    print()


def main():
    print("=" * 70)
    print("FACTS CONTRACT DATABASE BUILDER")
    print("=" * 70)

    # Remove old DB
    if DB_FILE.exists():
        DB_FILE.unlink()
        print(f"Removed old database")

    conn = sqlite3.connect(str(DB_FILE), isolation_level="DEFERRED")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-512000")  # 512MB cache
    conn.execute("PRAGMA temp_store=MEMORY")

    print("Creating schema...")
    create_schema(conn)

    total = ingest_contracts(conn)

    print("\nCreating indices...")
    create_indices(conn)

    build_vendor_profiles(conn)
    build_vendor_agency_history(conn)
    build_vendor_commodity_expertise(conn)
    build_agency_profiles(conn)

    print("\nCreating views...")
    create_views(conn)

    # ANALYZE for query optimizer
    print("\nRunning ANALYZE...")
    conn.execute("ANALYZE")
    conn.commit()

    print_sample_queries(conn)

    # Final stats
    db_size = DB_FILE.stat().st_size / (1024 * 1024)
    print(f"\nDatabase: {DB_FILE}")
    print(f"Size: {db_size:.1f} MB")
    print(f"Records: {total:,}")

    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM vendor_profiles")
    print(f"Vendor profiles: {c.fetchone()[0]:,}")
    c.execute("SELECT COUNT(*) FROM vendor_agency_history")
    print(f"Vendor-agency relationships: {c.fetchone()[0]:,}")
    c.execute("SELECT COUNT(*) FROM vendor_commodity_expertise")
    print(f"Vendor-commodity records: {c.fetchone()[0]:,}")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
