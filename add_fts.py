#!/usr/bin/env python3
"""Add FTS5 full-text search tables to the FACTS database."""
import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).parent / "output" / "facts_contracts.db"

def main():
    conn = sqlite3.connect(str(DB_FILE))
    c = conn.cursor()

    # Check if FTS already exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='contracts_fts'")
    if c.fetchone():
        print("FTS tables already exist, dropping and rebuilding...")
        c.execute("DROP TABLE IF EXISTS contracts_fts")
        c.execute("DROP TABLE IF EXISTS vendors_fts")

    # FTS5 on contracts: searchable by title, vendor, commodity, agency, comment
    print("Building contracts FTS5 index...")
    c.execute("""
    CREATE VIRTUAL TABLE contracts_fts USING fts5(
        title,
        vendor_name,
        commodity_description,
        agency_name,
        short_title,
        comment,
        procurement_method,
        content=contracts,
        content_rowid=id
    )
    """)

    c.execute("""
    INSERT INTO contracts_fts(rowid, title, vendor_name, commodity_description, agency_name, short_title, comment, procurement_method)
    SELECT id, COALESCE(title,''), COALESCE(vendor_name,''), COALESCE(commodity_description,''),
           COALESCE(agency_name,''), COALESCE(short_title,''), COALESCE(comment,''), COALESCE(procurement_method,'')
    FROM contracts
    """)
    conn.commit()
    print("  Contracts FTS5 index built")

    # FTS5 on vendor profiles: searchable by name, agencies, commodities
    print("Building vendor FTS5 index...")
    c.execute("""
    CREATE VIRTUAL TABLE vendors_fts USING fts5(
        vendor_name_display,
        agencies_list,
        commodity_types_list,
        procurement_methods,
        content=vendor_profiles,
        content_rowid=id
    )
    """)

    c.execute("""
    INSERT INTO vendors_fts(rowid, vendor_name_display, agencies_list, commodity_types_list, procurement_methods)
    SELECT id, COALESCE(vendor_name_display,''), COALESCE(agencies_list,''),
           COALESCE(commodity_types_list,''), COALESCE(procurement_methods,'')
    FROM vendor_profiles
    """)
    conn.commit()
    print("  Vendor FTS5 index built")

    # Verify
    c.execute("SELECT COUNT(*) FROM contracts_fts")
    print(f"\nContracts FTS rows: {c.fetchone()[0]:,}")
    c.execute("SELECT COUNT(*) FROM vendors_fts")
    print(f"Vendor FTS rows: {c.fetchone()[0]:,}")

    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
