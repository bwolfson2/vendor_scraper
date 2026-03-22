#!/usr/bin/env python3
"""
FACTS Vendor Intelligence API
Flask backend for procurement officer UI with full analytics.
"""
import sqlite3
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

DB_FILE = Path(__file__).parent / "output" / "facts_contracts.db"


def get_db():
    conn = sqlite3.connect(str(DB_FILE), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA cache_size=-256000")
    return conn


# ── UI ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ── Search endpoints ──────────────────────────────────────────────────────

@app.route("/api/search/vendors")
def search_vendors():
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))
    tier = request.args.get("tier", "")
    min_score = request.args.get("min_score", "")

    conn = get_db()
    c = conn.cursor()

    if q:
        fts_query = " OR ".join(f'"{w}"' for w in q.split() if w)
        sql = """
            SELECT vp.*, vendors_fts.rank
            FROM vendors_fts
            JOIN vendor_profiles vp ON vp.id = vendors_fts.rowid
            WHERE vendors_fts MATCH ?
        """
        params = [fts_query]
        if tier:
            sql += " AND vp.vendor_tier = ?"
            params.append(tier)
        if min_score:
            sql += " AND vp.vendor_score >= ?"
            params.append(float(min_score))
        sql += " ORDER BY vendors_fts.rank LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    else:
        sql = "SELECT vp.* FROM vendor_profiles vp WHERE 1=1"
        params = []
        if tier:
            sql += " AND vp.vendor_tier = ?"
            params.append(tier)
        if min_score:
            sql += " AND vp.vendor_score >= ?"
            params.append(float(min_score))
        sql += " ORDER BY vp.vendor_score DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

    c.execute(sql, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({"results": rows, "count": len(rows)})


@app.route("/api/search/contracts")
def search_contracts():
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))
    agency = request.args.get("agency", "")
    status = request.args.get("status", "")
    record_type = request.args.get("type", "")

    conn = get_db()
    c = conn.cursor()

    if q:
        fts_query = " OR ".join(f'"{w}"' for w in q.split() if w)
        sql = """
            SELECT c.id, c.agency_name, c.vendor_name, c.title, c.short_title,
                   c.total_amount, c.original_amount, c.status, c.status_broad,
                   c.record_type, c.record_type_broad, c.commodity_description,
                   c.begin_date, c.original_end_date, c.new_end_date,
                   c.procurement_method, c.vendor_name_normalized,
                   contracts_fts.rank
            FROM contracts_fts
            JOIN contracts c ON c.id = contracts_fts.rowid
            WHERE contracts_fts MATCH ?
        """
        params = [fts_query]
    else:
        sql = """
            SELECT c.id, c.agency_name, c.vendor_name, c.title, c.short_title,
                   c.total_amount, c.original_amount, c.status, c.status_broad,
                   c.record_type, c.record_type_broad, c.commodity_description,
                   c.begin_date, c.original_end_date, c.new_end_date,
                   c.procurement_method, c.vendor_name_normalized
            FROM contracts c WHERE 1=1
        """
        params = []

    if agency:
        sql += " AND c.agency_name LIKE ?"
        params.append(f"%{agency}%")
    if status:
        sql += " AND c.status_broad = ?"
        params.append(status)
    if record_type:
        sql += " AND c.record_type_broad = ?"
        params.append(record_type)

    if q:
        sql += " ORDER BY contracts_fts.rank LIMIT ? OFFSET ?"
    else:
        sql += " ORDER BY c.total_amount DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    c.execute(sql, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({"results": rows, "count": len(rows)})


@app.route("/api/search/procure")
def search_procure():
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 30)), 100)

    if not q:
        return jsonify({"results": [], "count": 0})

    conn = get_db()
    c = conn.cursor()

    fts_query = " OR ".join(f'"{w}"' for w in q.split() if w)

    c.execute("""
        SELECT
            c.vendor_name,
            c.vendor_name_normalized,
            c.commodity_description,
            COUNT(*) as match_count,
            SUM(COALESCE(c.total_amount, 0)) as total_value,
            AVG(COALESCE(c.total_amount, 0)) as avg_value,
            GROUP_CONCAT(DISTINCT c.agency_name) as agencies,
            MIN(c.begin_date) as first_date,
            MAX(c.begin_date) as last_date
        FROM contracts_fts
        JOIN contracts c ON c.id = contracts_fts.rowid
        WHERE contracts_fts MATCH ?
        GROUP BY c.vendor_name_normalized
        ORDER BY match_count DESC
        LIMIT ?
    """, [fts_query, limit])

    vendors = []
    for row in c.fetchall():
        row_dict = dict(row)
        c2 = conn.cursor()
        c2.execute("""
            SELECT vendor_score, vendor_tier, cancellation_rate,
                   total_records, years_active, num_agencies_served,
                   completed_contracts, last_contract_date,
                   active_contracts, avg_contract_amount
            FROM vendor_profiles
            WHERE vendor_name_normalized = ?
        """, [row_dict["vendor_name_normalized"]])
        profile = c2.fetchone()
        if profile:
            row_dict.update(dict(profile))
        vendors.append(row_dict)

    conn.close()
    return jsonify({"results": vendors, "count": len(vendors)})


# ── Vendor detail ────────────────────────────────────────────────────────

@app.route("/api/vendor/<path:vendor_norm>")
def vendor_detail(vendor_norm):
    conn = get_db()
    c = conn.cursor()

    c.execute("SELECT * FROM vendor_profiles WHERE vendor_name_normalized = ?", [vendor_norm])
    profile = c.fetchone()
    if not profile:
        conn.close()
        return jsonify({"error": "Vendor not found"}), 404

    result = dict(profile)

    # Agency history
    c.execute("""
        SELECT * FROM vendor_agency_history
        WHERE vendor_name_normalized = ?
        ORDER BY total_amount DESC
    """, [vendor_norm])
    result["agency_history"] = [dict(r) for r in c.fetchall()]

    # Commodity expertise
    c.execute("""
        SELECT * FROM vendor_commodity_expertise
        WHERE vendor_name_normalized = ?
        ORDER BY total_amount DESC
    """, [vendor_norm])
    result["commodity_expertise"] = [dict(r) for r in c.fetchall()]

    # Spend over time
    c.execute("""
        SELECT year_begin as year, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val,
               SUM(CASE WHEN record_type_broad='CONTRACT' THEN COALESCE(total_amount,0) ELSE 0 END) as contract_val,
               SUM(CASE WHEN record_type_broad='PURCHASE_ORDER' THEN COALESCE(total_amount,0) ELSE 0 END) as po_val,
               SUM(CASE WHEN record_type_broad='GRANT' THEN COALESCE(total_amount,0) ELSE 0 END) as grant_val
        FROM contracts
        WHERE vendor_name_normalized = ? AND year_begin IS NOT NULL AND year_begin >= 2000
        GROUP BY year_begin ORDER BY year_begin
    """, [vendor_norm])
    result["spend_over_time"] = [dict(r) for r in c.fetchall()]

    # Recent contracts (last 25)
    c.execute("""
        SELECT id, agency_name, title, total_amount, original_amount,
               status, status_broad, record_type, record_type_broad, commodity_description,
               begin_date, original_end_date, new_end_date, procurement_method,
               amount_change, amount_change_pct, duration_days
        FROM contracts
        WHERE vendor_name_normalized = ?
        ORDER BY effective_date DESC
        LIMIT 25
    """, [vendor_norm])
    result["recent_contracts"] = [dict(r) for r in c.fetchall()]

    # Risk metrics
    c.execute("""
        SELECT
            COUNT(CASE WHEN amount_change_pct > 0 THEN 1 END) as cost_overrun_count,
            AVG(CASE WHEN amount_change_pct > 0 THEN amount_change_pct END) as avg_overrun_pct,
            COUNT(CASE WHEN new_end_date IS NOT NULL AND new_end_date != original_end_date THEN 1 END) as extension_count,
            COUNT(CASE WHEN procurement_method LIKE '%sole%' OR procurement_method LIKE '%single%' OR procurement_method LIKE '%exempt%' THEN 1 END) as sole_source_count
        FROM contracts
        WHERE vendor_name_normalized = ?
    """, [vendor_norm])
    risk = c.fetchone()
    if risk:
        result["risk_metrics"] = dict(risk)

    conn.close()
    return jsonify(result)


# ── Agency detail ────────────────────────────────────────────────────────

@app.route("/api/agency/<path:agency_name>")
def agency_detail(agency_name):
    conn = get_db()
    c = conn.cursor()

    # Overview stats
    c.execute("""
        SELECT
            COUNT(*) as total_records,
            SUM(COALESCE(total_amount, 0)) as total_amount,
            AVG(COALESCE(total_amount, 0)) as avg_amount,
            COUNT(DISTINCT vendor_name_normalized) as num_vendors,
            COUNT(DISTINCT commodity_description) as num_commodities,
            COUNT(CASE WHEN status_broad='ACTIVE' THEN 1 END) as active_count,
            COUNT(CASE WHEN status_broad='COMPLETED' THEN 1 END) as completed_count,
            COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) as cancelled_count,
            COUNT(CASE WHEN record_type_broad='CONTRACT' THEN 1 END) as contract_count,
            COUNT(CASE WHEN record_type_broad='PURCHASE_ORDER' THEN 1 END) as po_count,
            COUNT(CASE WHEN record_type_broad='GRANT' THEN 1 END) as grant_count,
            MIN(begin_date) as first_date,
            MAX(begin_date) as last_date,
            AVG(CASE WHEN duration_days > 0 THEN duration_days END) as avg_duration_days,
            AVG(CASE WHEN amount_change_pct IS NOT NULL THEN amount_change_pct END) as avg_cost_change_pct,
            COUNT(CASE WHEN amount_change_pct > 0 THEN 1 END) as cost_overrun_count,
            1.0 * COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) / MAX(1, COUNT(*)) as cancellation_rate
        FROM contracts WHERE agency_name = ?
    """, [agency_name])
    row = c.fetchone()
    if not row or row["total_records"] == 0:
        conn.close()
        return jsonify({"error": "Agency not found"}), 404
    result = dict(row)
    result["agency_name"] = agency_name

    # Spend over time
    c.execute("""
        SELECT year_begin as year, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val,
               SUM(CASE WHEN record_type_broad='CONTRACT' THEN COALESCE(total_amount,0) ELSE 0 END) as contract_val,
               SUM(CASE WHEN record_type_broad='PURCHASE_ORDER' THEN COALESCE(total_amount,0) ELSE 0 END) as po_val,
               SUM(CASE WHEN record_type_broad='GRANT' THEN COALESCE(total_amount,0) ELSE 0 END) as grant_val
        FROM contracts
        WHERE agency_name = ? AND year_begin IS NOT NULL AND year_begin >= 2000
        GROUP BY year_begin ORDER BY year_begin
    """, [agency_name])
    result["spend_over_time"] = [dict(r) for r in c.fetchall()]

    # Top vendors for this agency
    c.execute("""
        SELECT c.vendor_name, c.vendor_name_normalized,
               COUNT(*) as record_count,
               SUM(COALESCE(c.total_amount, 0)) as total_val,
               AVG(COALESCE(c.total_amount, 0)) as avg_val,
               vp.vendor_score, vp.vendor_tier, vp.cancellation_rate,
               vp.total_records, vp.completed_contracts, vp.years_active,
               vp.num_agencies_served, vp.last_contract_date
        FROM contracts c
        LEFT JOIN vendor_profiles vp ON c.vendor_name_normalized = vp.vendor_name_normalized
        WHERE c.agency_name = ?
        GROUP BY c.vendor_name_normalized
        ORDER BY total_val DESC
        LIMIT 30
    """, [agency_name])
    result["top_vendors"] = [dict(r) for r in c.fetchall()]

    # Vendor concentration (HHI)
    c.execute("""
        SELECT SUM(share * share) as hhi FROM (
            SELECT 1.0 * SUM(COALESCE(total_amount, 0)) /
                   NULLIF((SELECT SUM(COALESCE(total_amount,0)) FROM contracts WHERE agency_name = ?), 0) as share
            FROM contracts
            WHERE agency_name = ?
            GROUP BY vendor_name_normalized
        )
    """, [agency_name, agency_name])
    hhi_row = c.fetchone()
    result["hhi_index"] = round((hhi_row["hhi"] or 0) * 10000)

    # Top commodities
    c.execute("""
        SELECT commodity_description, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val,
               COUNT(DISTINCT vendor_name_normalized) as num_vendors
        FROM contracts
        WHERE agency_name = ? AND commodity_description != ''
        GROUP BY commodity_description
        ORDER BY total_val DESC
        LIMIT 20
    """, [agency_name])
    result["top_commodities"] = [dict(r) for r in c.fetchall()]

    # Procurement method distribution
    c.execute("""
        SELECT procurement_method, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val
        FROM contracts
        WHERE agency_name = ? AND procurement_method IS NOT NULL AND procurement_method != ''
        GROUP BY procurement_method
        ORDER BY total_val DESC
        LIMIT 15
    """, [agency_name])
    result["procurement_methods"] = [dict(r) for r in c.fetchall()]

    # Recent contracts
    c.execute("""
        SELECT id, vendor_name, vendor_name_normalized, title, total_amount,
               original_amount, status_broad, record_type_broad, commodity_description,
               begin_date, procurement_method, amount_change_pct
        FROM contracts WHERE agency_name = ?
        ORDER BY effective_date DESC LIMIT 20
    """, [agency_name])
    result["recent_contracts"] = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify(result)


# ── Commodity detail ─────────────────────────────────────────────────────

@app.route("/api/commodity/<path:commodity>")
def commodity_detail(commodity):
    conn = get_db()
    c = conn.cursor()

    # Overview
    c.execute("""
        SELECT
            COUNT(*) as total_records,
            SUM(COALESCE(total_amount, 0)) as total_amount,
            AVG(COALESCE(total_amount, 0)) as avg_amount,
            COUNT(DISTINCT vendor_name_normalized) as num_vendors,
            COUNT(DISTINCT agency_name) as num_agencies,
            COUNT(CASE WHEN status_broad='ACTIVE' THEN 1 END) as active_count,
            COUNT(CASE WHEN status_broad='COMPLETED' THEN 1 END) as completed_count,
            COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) as cancelled_count,
            MIN(begin_date) as first_date,
            MAX(begin_date) as last_date,
            1.0 * COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) / MAX(1, COUNT(*)) as cancellation_rate
        FROM contracts WHERE commodity_description = ?
    """, [commodity])
    row = c.fetchone()
    if not row or row["total_records"] == 0:
        conn.close()
        return jsonify({"error": "Commodity not found"}), 404
    result = dict(row)
    result["commodity_description"] = commodity

    # Spend over time
    c.execute("""
        SELECT year_begin as year, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val
        FROM contracts
        WHERE commodity_description = ? AND year_begin IS NOT NULL AND year_begin >= 2000
        GROUP BY year_begin ORDER BY year_begin
    """, [commodity])
    result["spend_over_time"] = [dict(r) for r in c.fetchall()]

    # Top vendors in this commodity
    c.execute("""
        SELECT c.vendor_name, c.vendor_name_normalized,
               COUNT(*) as record_count,
               SUM(COALESCE(c.total_amount, 0)) as total_val,
               AVG(COALESCE(c.total_amount, 0)) as avg_val,
               vp.vendor_score, vp.vendor_tier, vp.cancellation_rate,
               vp.total_records, vp.completed_contracts, vp.years_active,
               vp.num_agencies_served, vp.last_contract_date
        FROM contracts c
        LEFT JOIN vendor_profiles vp ON c.vendor_name_normalized = vp.vendor_name_normalized
        WHERE c.commodity_description = ?
        GROUP BY c.vendor_name_normalized
        ORDER BY total_val DESC
        LIMIT 30
    """, [commodity])
    result["top_vendors"] = [dict(r) for r in c.fetchall()]

    # Agencies using this commodity
    c.execute("""
        SELECT agency_name, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val,
               COUNT(DISTINCT vendor_name_normalized) as num_vendors
        FROM contracts WHERE commodity_description = ?
        GROUP BY agency_name
        ORDER BY total_val DESC LIMIT 20
    """, [commodity])
    result["agencies"] = [dict(r) for r in c.fetchall()]

    # Price benchmarking by agency
    c.execute("""
        SELECT agency_name,
               AVG(COALESCE(total_amount, 0)) as avg_val,
               MIN(COALESCE(total_amount, 0)) as min_val,
               MAX(COALESCE(total_amount, 0)) as max_val,
               COUNT(*) as cnt
        FROM contracts
        WHERE commodity_description = ? AND total_amount > 0
        GROUP BY agency_name
        HAVING cnt >= 2
        ORDER BY avg_val DESC LIMIT 15
    """, [commodity])
    result["price_benchmark"] = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify(result)


# ── Global Analytics endpoints ───────────────────────────────────────────

@app.route("/api/analytics/spend-by-agency-year")
def spend_by_agency_year():
    """Spend over time broken down by agency (top N)."""
    top_n = min(int(request.args.get("top", 10)), 20)
    conn = get_db()
    c = conn.cursor()

    # Get top agencies by spend
    c.execute("""
        SELECT agency_name FROM contracts
        GROUP BY agency_name
        ORDER BY SUM(COALESCE(total_amount, 0)) DESC
        LIMIT ?
    """, [top_n])
    top_agencies = [r[0] for r in c.fetchall()]

    # Get yearly data for each
    results = {}
    for ag in top_agencies:
        c.execute("""
            SELECT year_begin as year, SUM(COALESCE(total_amount, 0)) as total_val
            FROM contracts
            WHERE agency_name = ? AND year_begin IS NOT NULL AND year_begin >= 2005
            GROUP BY year_begin ORDER BY year_begin
        """, [ag])
        results[ag] = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify({"agencies": top_agencies, "data": results})


@app.route("/api/analytics/spend-by-vendor-year")
def spend_by_vendor_year():
    """Spend over time broken down by vendor (top N)."""
    top_n = min(int(request.args.get("top", 10)), 20)
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT vendor_name, vendor_name_normalized FROM contracts
        GROUP BY vendor_name_normalized
        ORDER BY SUM(COALESCE(total_amount, 0)) DESC
        LIMIT ?
    """, [top_n])
    top_vendors_list = [(r[0], r[1]) for r in c.fetchall()]

    results = {}
    for name, norm in top_vendors_list:
        c.execute("""
            SELECT year_begin as year, SUM(COALESCE(total_amount, 0)) as total_val
            FROM contracts
            WHERE vendor_name_normalized = ? AND year_begin IS NOT NULL AND year_begin >= 2005
            GROUP BY year_begin ORDER BY year_begin
        """, [norm])
        results[name] = {"norm": norm, "data": [dict(r) for r in c.fetchall()]}

    conn.close()
    return jsonify({"vendors": [v[0] for v in top_vendors_list], "data": results})


@app.route("/api/analytics/new-vs-returning")
def new_vs_returning():
    """New vs returning vendors per year."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        WITH first_year AS (
            SELECT vendor_name_normalized, MIN(year_begin) as first_yr
            FROM contracts
            WHERE year_begin IS NOT NULL AND year_begin >= 2005
            GROUP BY vendor_name_normalized
        ),
        yearly AS (
            SELECT c.year_begin as year, c.vendor_name_normalized, fy.first_yr
            FROM contracts c
            JOIN first_year fy ON c.vendor_name_normalized = fy.vendor_name_normalized
            WHERE c.year_begin IS NOT NULL AND c.year_begin >= 2005
            GROUP BY c.year_begin, c.vendor_name_normalized
        )
        SELECT year,
               COUNT(CASE WHEN year = first_yr THEN 1 END) as new_vendors,
               COUNT(CASE WHEN year != first_yr THEN 1 END) as returning_vendors
        FROM yearly
        GROUP BY year
        ORDER BY year
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({"results": rows})


@app.route("/api/analytics/risk-overview")
def risk_overview():
    """Risk metrics across the system."""
    conn = get_db()
    c = conn.cursor()

    # Cancellation rates by agency
    c.execute("""
        SELECT agency_name,
               1.0 * COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) / MAX(1, COUNT(*)) as cancel_rate,
               COUNT(*) as total
        FROM contracts
        GROUP BY agency_name
        HAVING total >= 50
        ORDER BY cancel_rate DESC
        LIMIT 20
    """)
    by_agency = [dict(r) for r in c.fetchall()]

    # Cost overruns by agency
    c.execute("""
        SELECT agency_name,
               AVG(CASE WHEN amount_change_pct > 0 AND amount_change_pct <= 500 THEN amount_change_pct END) as avg_overrun,
               COUNT(CASE WHEN amount_change_pct > 10 THEN 1 END) as overrun_count,
               COUNT(*) as total
        FROM contracts
        GROUP BY agency_name
        HAVING overrun_count > 0
        ORDER BY avg_overrun DESC
        LIMIT 20
    """)
    overruns = [dict(r) for r in c.fetchall()]

    # Extension rates
    c.execute("""
        SELECT agency_name,
               COUNT(CASE WHEN new_end_date IS NOT NULL AND new_end_date != '' AND new_end_date != original_end_date THEN 1 END) as extensions,
               COUNT(*) as total,
               1.0 * COUNT(CASE WHEN new_end_date IS NOT NULL AND new_end_date != '' AND new_end_date != original_end_date THEN 1 END) / MAX(1, COUNT(*)) as ext_rate
        FROM contracts
        GROUP BY agency_name
        HAVING total >= 50
        ORDER BY ext_rate DESC
        LIMIT 20
    """)
    extensions = [dict(r) for r in c.fetchall()]

    # Procurement method distribution
    c.execute("""
        SELECT procurement_method, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val
        FROM contracts
        WHERE procurement_method IS NOT NULL AND procurement_method != ''
        GROUP BY procurement_method
        ORDER BY total_val DESC
        LIMIT 20
    """)
    methods = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify({
        "cancellation_by_agency": by_agency,
        "cost_overruns": overruns,
        "extensions": extensions,
        "procurement_methods": methods
    })


@app.route("/api/analytics/commodity-trends")
def commodity_trends():
    """Fastest growing and largest commodity categories."""
    conn = get_db()
    c = conn.cursor()

    # Top commodities with YoY data
    c.execute("""
        SELECT commodity_description, year_begin as year,
               COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val
        FROM contracts
        WHERE commodity_description != '' AND year_begin IS NOT NULL AND year_begin >= 2015
        GROUP BY commodity_description, year_begin
        ORDER BY commodity_description, year_begin
    """)
    raw = {}
    for r in c.fetchall():
        cd = r["commodity_description"]
        if cd not in raw:
            raw[cd] = {"total": 0, "years": []}
        raw[cd]["years"].append({"year": r["year"], "cnt": r["cnt"], "val": r["total_val"]})
        raw[cd]["total"] += r["total_val"] or 0

    # Sort by total value, take top 15
    top = sorted(raw.items(), key=lambda x: x[1]["total"], reverse=True)[:15]
    result = [{"commodity": k, "total": v["total"], "years": v["years"]} for k, v in top]

    conn.close()
    return jsonify({"results": result})


# ── Aggregation endpoints ─────────────────────────────────────────────────

@app.route("/api/stats/overview")
def stats_overview():
    conn = get_db()
    c = conn.cursor()

    stats = {}
    c.execute("SELECT COUNT(*) FROM contracts")
    stats["total_contracts"] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM vendor_profiles")
    stats["total_vendors"] = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT agency_name) FROM contracts")
    stats["total_agencies"] = c.fetchone()[0]
    c.execute("SELECT SUM(total_amount) FROM contracts")
    stats["total_value"] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM contracts WHERE status_broad = 'ACTIVE'")
    stats["active_contracts"] = c.fetchone()[0]
    c.execute("SELECT vendor_tier, COUNT(*) FROM vendor_profiles GROUP BY vendor_tier ORDER BY COUNT(*) DESC")
    stats["tier_distribution"] = {r[0]: r[1] for r in c.fetchall()}
    c.execute("""
        SELECT record_type_broad, COUNT(*), SUM(COALESCE(total_amount, 0))
        FROM contracts GROUP BY record_type_broad ORDER BY COUNT(*) DESC
    """)
    stats["type_distribution"] = [{"type": r[0], "count": r[1], "total_value": r[2]} for r in c.fetchall()]
    c.execute("""
        SELECT year_begin, COUNT(*), SUM(COALESCE(total_amount, 0))
        FROM contracts WHERE year_begin IS NOT NULL AND year_begin >= 2000
        GROUP BY year_begin ORDER BY year_begin
    """)
    stats["yearly_trend"] = [{"year": r[0], "count": r[1], "total_value": r[2]} for r in c.fetchall()]

    conn.close()
    return jsonify(stats)


@app.route("/api/stats/top-vendors")
def top_vendors():
    sort_by = request.args.get("sort", "score")
    limit = min(int(request.args.get("limit", 25)), 100)
    conn = get_db()
    c = conn.cursor()
    order = "vendor_score DESC" if sort_by == "score" else "total_current_amount DESC"
    c.execute(f"""
        SELECT vendor_name_display, vendor_tier, vendor_score,
               total_records, total_current_amount, num_agencies_served,
               years_active, cancellation_rate, vendor_name_normalized,
               completed_contracts, last_contract_date
        FROM vendor_profiles
        WHERE total_records >= 5
        ORDER BY {order}
        LIMIT ?
    """, [limit])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({"results": rows})


@app.route("/api/stats/agencies")
def agency_stats():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM agency_profiles ORDER BY total_amount DESC")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({"results": rows})


@app.route("/api/stats/commodities")
def commodity_stats():
    limit = min(int(request.args.get("limit", 50)), 200)
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT commodity_description, COUNT(*) as cnt,
               SUM(COALESCE(total_amount, 0)) as total_val,
               COUNT(DISTINCT vendor_name_normalized) as num_vendors,
               COUNT(DISTINCT agency_name) as num_agencies
        FROM contracts WHERE commodity_description != ''
        GROUP BY commodity_description
        ORDER BY total_val DESC LIMIT ?
    """, [limit])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({"results": rows})


@app.route("/api/contract/<int:contract_id>")
def contract_detail(contract_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM contracts WHERE id = ?", [contract_id])
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Contract not found"}), 404
    return jsonify(dict(row))


# ── Performance / Grading endpoints ──────────────────────────────────────

def letter_grade(score):
    """Convert 0-100 score to A+ through F."""
    if score >= 97: return 'A+'
    if score >= 93: return 'A'
    if score >= 90: return 'A-'
    if score >= 87: return 'B+'
    if score >= 83: return 'B'
    if score >= 80: return 'B-'
    if score >= 77: return 'C+'
    if score >= 73: return 'C'
    if score >= 70: return 'C-'
    if score >= 67: return 'D+'
    if score >= 63: return 'D'
    if score >= 60: return 'D-'
    return 'F'


@app.route("/api/performance/state")
def performance_state():
    """State-level aggregate performance metrics."""
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT
            COUNT(*) as total_records,
            SUM(COALESCE(total_amount, 0)) as total_spend,
            COUNT(DISTINCT vendor_name_normalized) as total_vendors,
            COUNT(DISTINCT agency_name) as total_agencies,
            COUNT(CASE WHEN status_broad='ACTIVE' THEN 1 END) as active,
            COUNT(CASE WHEN status_broad='COMPLETED' THEN 1 END) as completed,
            COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) as cancelled,
            1.0 * COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) / MAX(1, COUNT(*)) as cancel_rate,
            AVG(CASE WHEN amount_change_pct > 0 AND amount_change_pct <= 500 THEN amount_change_pct END) as avg_cost_overrun,
            COUNT(CASE WHEN amount_change_pct > 10 THEN 1 END) as overrun_count,
            COUNT(CASE WHEN new_end_date IS NOT NULL AND new_end_date != '' AND new_end_date != original_end_date THEN 1 END) as extension_count,
            1.0 * COUNT(CASE WHEN new_end_date IS NOT NULL AND new_end_date != '' AND new_end_date != original_end_date THEN 1 END) / MAX(1, COUNT(*)) as extension_rate
        FROM contracts
    """)
    state = dict(c.fetchone())

    # Vendor tier distribution
    c.execute("SELECT vendor_tier, COUNT(*) as cnt FROM vendor_profiles GROUP BY vendor_tier")
    state["tier_dist"] = {r[0]: r[1] for r in c.fetchall()}

    # Overrun rate (% of contracts with >10% overrun) is more robust than avg
    c.execute("""
        SELECT 1.0 * COUNT(CASE WHEN amount_change_pct > 10 THEN 1 END) / MAX(1, COUNT(CASE WHEN amount_change_pct IS NOT NULL THEN 1 END))
        FROM contracts WHERE amount_change_pct IS NOT NULL
    """)
    state["overrun_rate"] = c.fetchone()[0] or 0

    # State efficiency score (0-100)
    cancel_score = max(0, 100 - state["cancel_rate"] * 500)  # 0% = 100, 20% = 0
    overrun_score = max(0, 100 - state["overrun_rate"] * 200)  # 0% = 100, 50% = 0
    ext_score = max(0, 100 - state["extension_rate"] * 200)
    plat_pct = (state["tier_dist"].get("PLATINUM", 0) + state["tier_dist"].get("GOLD", 0)) / max(1, state["total_vendors"]) * 100
    vendor_quality = min(100, plat_pct * 3)
    state["efficiency_score"] = round(cancel_score * 0.3 + overrun_score * 0.25 + ext_score * 0.2 + vendor_quality * 0.25, 1)
    state["grade"] = letter_grade(state["efficiency_score"])
    state["sub_scores"] = {
        "cancel_score": round(cancel_score, 1),
        "overrun_score": round(overrun_score, 1),
        "ext_score": round(ext_score, 1),
        "vendor_quality": round(vendor_quality, 1),
    }

    conn.close()
    return jsonify(state)


@app.route("/api/performance/departments")
def performance_departments():
    """Per-department performance grades and efficiency metrics."""
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT
            agency_name,
            COUNT(*) as total_records,
            SUM(COALESCE(total_amount, 0)) as total_spend,
            COUNT(DISTINCT vendor_name_normalized) as num_vendors,
            COUNT(CASE WHEN status_broad='ACTIVE' THEN 1 END) as active,
            COUNT(CASE WHEN status_broad='COMPLETED' THEN 1 END) as completed,
            COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) as cancelled,
            1.0 * COUNT(CASE WHEN status_broad='CANCELLED' THEN 1 END) / MAX(1, COUNT(*)) as cancel_rate,
            AVG(COALESCE(total_amount, 0)) as avg_contract_size,
            AVG(CASE WHEN amount_change_pct > 0 AND amount_change_pct <= 500 THEN amount_change_pct END) as avg_cost_overrun,
            COUNT(CASE WHEN amount_change_pct > 10 THEN 1 END) as overrun_count,
            1.0 * COUNT(CASE WHEN amount_change_pct > 10 THEN 1 END) / MAX(1, COUNT(*)) as overrun_rate,
            COUNT(CASE WHEN new_end_date IS NOT NULL AND new_end_date != '' AND new_end_date != original_end_date THEN 1 END) as extensions,
            1.0 * COUNT(CASE WHEN new_end_date IS NOT NULL AND new_end_date != '' AND new_end_date != original_end_date THEN 1 END) / MAX(1, COUNT(*)) as extension_rate,
            MIN(begin_date) as first_date,
            MAX(begin_date) as last_date
        FROM contracts
        GROUP BY agency_name
        HAVING total_records >= 10
        ORDER BY total_spend DESC
    """)
    depts = []
    for row in c.fetchall():
        d = dict(row)

        # HHI for vendor concentration
        c2 = conn.cursor()
        c2.execute("""
            SELECT SUM(share * share) as hhi FROM (
                SELECT 1.0 * SUM(COALESCE(total_amount, 0)) /
                       NULLIF((SELECT SUM(COALESCE(total_amount,0)) FROM contracts WHERE agency_name = ?), 0) as share
                FROM contracts WHERE agency_name = ?
                GROUP BY vendor_name_normalized
            )
        """, [d["agency_name"], d["agency_name"]])
        hhi_row = c2.fetchone()
        d["hhi"] = round((hhi_row["hhi"] or 0) * 10000)

        # Compute efficiency score (use overrun_rate not avg_cost_overrun to avoid outliers)
        cancel_score = max(0, 100 - d["cancel_rate"] * 500)
        overrun_score = max(0, 100 - d["overrun_rate"] * 200)  # % of contracts with >10% overrun
        ext_score = max(0, 100 - d["extension_rate"] * 200)
        competition_score = max(0, min(100, 100 - (d["hhi"] / 100)))  # Lower HHI = better

        d["efficiency_score"] = round(
            cancel_score * 0.30 +
            overrun_score * 0.25 +
            ext_score * 0.20 +
            competition_score * 0.25,
            1
        )
        d["grade"] = letter_grade(d["efficiency_score"])
        d["sub_scores"] = {
            "cancel": round(cancel_score, 1),
            "overrun": round(overrun_score, 1),
            "extensions": round(ext_score, 1),
            "competition": round(competition_score, 1),
        }
        depts.append(d)

    conn.close()
    return jsonify({"results": depts})


@app.route("/api/performance/dept-vendors/<path:agency_name>")
def performance_dept_vendors(agency_name):
    """Vendor grades within a specific department."""
    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT
            c.vendor_name,
            c.vendor_name_normalized,
            COUNT(*) as record_count,
            SUM(COALESCE(c.total_amount, 0)) as total_val,
            AVG(COALESCE(c.total_amount, 0)) as avg_val,
            COUNT(CASE WHEN c.status_broad='COMPLETED' THEN 1 END) as completed,
            COUNT(CASE WHEN c.status_broad='CANCELLED' THEN 1 END) as cancelled,
            1.0 * COUNT(CASE WHEN c.status_broad='CANCELLED' THEN 1 END) / MAX(1, COUNT(*)) as cancel_rate,
            COUNT(CASE WHEN c.status_broad='ACTIVE' THEN 1 END) as active,
            AVG(CASE WHEN c.amount_change_pct > 0 THEN c.amount_change_pct END) as avg_overrun,
            COUNT(CASE WHEN c.amount_change_pct > 10 THEN 1 END) as overrun_count,
            COUNT(CASE WHEN c.new_end_date IS NOT NULL AND c.new_end_date != '' AND c.new_end_date != c.original_end_date THEN 1 END) as extensions,
            1.0 * COUNT(CASE WHEN c.new_end_date IS NOT NULL AND c.new_end_date != '' AND c.new_end_date != c.original_end_date THEN 1 END) / MAX(1, COUNT(*)) as ext_rate,
            MIN(c.begin_date) as first_date,
            MAX(c.begin_date) as last_date,
            vp.vendor_score,
            vp.vendor_tier,
            vp.years_active,
            vp.num_agencies_served,
            vp.total_records as global_records,
            vp.completed_contracts as global_completed,
            vp.cancellation_rate as global_cancel_rate
        FROM contracts c
        LEFT JOIN vendor_profiles vp ON c.vendor_name_normalized = vp.vendor_name_normalized
        WHERE c.agency_name = ?
        GROUP BY c.vendor_name_normalized
        HAVING record_count >= 1
        ORDER BY total_val DESC
        LIMIT 50
    """, [agency_name])

    vendors = []
    for row in c.fetchall():
        v = dict(row)
        # Vendor efficiency grade within this dept
        cancel_s = max(0, 100 - v["cancel_rate"] * 500)
        overrun_rate_v = v["record_count"] > 0 and (v["overrun_count"] / v["record_count"]) or 0
        overrun_s = max(0, 100 - overrun_rate_v * 200)
        ext_s = max(0, 100 - v["ext_rate"] * 200)
        completion_s = v["record_count"] > 0 and (v["completed"] / v["record_count"] * 100) or 0
        experience_s = min(100, (v["record_count"] / 5) * 20)  # more records = more experience, caps at 25+

        v["dept_efficiency"] = round(
            cancel_s * 0.30 +
            overrun_s * 0.25 +
            ext_s * 0.15 +
            min(100, completion_s) * 0.15 +
            experience_s * 0.15,
            1
        )
        v["dept_grade"] = letter_grade(v["dept_efficiency"])
        v["sub_scores"] = {
            "cancel": round(cancel_s, 1),
            "overrun": round(overrun_s, 1),
            "extensions": round(ext_s, 1),
            "completion": round(min(100, completion_s), 1),
            "experience": round(experience_s, 1),
        }
        vendors.append(v)

    conn.close()
    return jsonify({"agency_name": agency_name, "results": vendors})


@app.route("/api/performance/vendors")
def performance_vendors():
    """Vendor-level performance grades with sortable metrics."""
    limit = min(int(request.args.get("limit", 100)), 500)
    sort = request.args.get("sort", "grade")  # grade, spend, records, cancel, overrun
    grade_filter = request.args.get("grade", "")  # A, B, C, D, F
    tier_filter = request.args.get("tier", "")
    min_records = int(request.args.get("min_records", 3))

    conn = get_db()
    c = conn.cursor()

    c.execute("""
        SELECT
            c.vendor_name,
            c.vendor_name_normalized,
            COUNT(*) as total_records,
            SUM(COALESCE(c.total_amount, 0)) as total_spend,
            AVG(COALESCE(c.total_amount, 0)) as avg_contract,
            COUNT(DISTINCT c.agency_name) as agencies_served,
            COUNT(CASE WHEN c.status_broad='COMPLETED' THEN 1 END) as completed,
            COUNT(CASE WHEN c.status_broad='CANCELLED' THEN 1 END) as cancelled,
            COUNT(CASE WHEN c.status_broad='ACTIVE' THEN 1 END) as active,
            1.0 * COUNT(CASE WHEN c.status_broad='CANCELLED' THEN 1 END) / MAX(1, COUNT(*)) as cancel_rate,
            1.0 * COUNT(CASE WHEN c.status_broad='COMPLETED' THEN 1 END) / MAX(1, COUNT(*)) as completion_rate,
            1.0 * COUNT(CASE WHEN c.amount_change_pct > 10 THEN 1 END) / MAX(1, COUNT(CASE WHEN c.amount_change_pct IS NOT NULL THEN 1 END)) as overrun_rate,
            COUNT(CASE WHEN c.amount_change_pct > 10 THEN 1 END) as overrun_count,
            AVG(CASE WHEN c.amount_change_pct > 0 AND c.amount_change_pct <= 500 THEN c.amount_change_pct END) as avg_cost_overrun_pct,
            1.0 * COUNT(CASE WHEN c.new_end_date IS NOT NULL AND c.new_end_date != '' AND c.new_end_date != c.original_end_date THEN 1 END) / MAX(1, COUNT(*)) as extension_rate,
            COUNT(CASE WHEN c.new_end_date IS NOT NULL AND c.new_end_date != '' AND c.new_end_date != c.original_end_date THEN 1 END) as extension_count,
            MIN(c.begin_date) as first_contract,
            MAX(c.begin_date) as last_contract,
            COUNT(DISTINCT c.commodity_description) as num_commodities,
            vp.vendor_tier,
            vp.vendor_score,
            vp.years_active
        FROM contracts c
        LEFT JOIN vendor_profiles vp ON c.vendor_name_normalized = vp.vendor_name_normalized
        GROUP BY c.vendor_name_normalized
        HAVING total_records >= ?
        ORDER BY total_spend DESC
        LIMIT 2000
    """, [min_records])

    vendors = []
    for row in c.fetchall():
        v = dict(row)

        # Compute sub-scores
        reliability = max(0, 100 - v["cancel_rate"] * 500)
        cost_control = max(0, 100 - v["overrun_rate"] * 200)
        timeliness = max(0, 100 - v["extension_rate"] * 200)
        delivery = min(100, v["completion_rate"] * 100)
        scale = min(100, v["total_records"] / 20 * 40 + v["agencies_served"] / 5 * 30 + (v["years_active"] or 0) / 10 * 30)

        v["efficiency_score"] = round(
            reliability * 0.30 +
            cost_control * 0.25 +
            timeliness * 0.15 +
            delivery * 0.15 +
            scale * 0.15,
            1
        )
        v["grade"] = letter_grade(v["efficiency_score"])
        v["sub_scores"] = {
            "reliability": round(reliability, 1),
            "cost_control": round(cost_control, 1),
            "timeliness": round(timeliness, 1),
            "delivery": round(delivery, 1),
            "scale": round(scale, 1),
        }

        # Apply filters
        if grade_filter and not v["grade"].startswith(grade_filter):
            continue
        if tier_filter and v["vendor_tier"] != tier_filter:
            continue

        vendors.append(v)

    # Sort
    if sort == "grade":
        vendors.sort(key=lambda x: x["efficiency_score"], reverse=True)
    elif sort == "grade-worst":
        vendors.sort(key=lambda x: x["efficiency_score"])
    elif sort == "spend":
        vendors.sort(key=lambda x: x["total_spend"], reverse=True)
    elif sort == "records":
        vendors.sort(key=lambda x: x["total_records"], reverse=True)
    elif sort == "cancel":
        vendors.sort(key=lambda x: x["cancel_rate"], reverse=True)
    elif sort == "overrun":
        vendors.sort(key=lambda x: x["overrun_rate"], reverse=True)

    # Grade distribution
    dist = {}
    for v in vendors:
        g = v["grade"][0]  # Just the letter
        dist[g] = dist.get(g, 0) + 1

    vendors = vendors[:limit]

    conn.close()
    return jsonify({"results": vendors, "grade_distribution": dist, "total": len(vendors)})


if __name__ == "__main__":
    print(f"Database: {DB_FILE} ({DB_FILE.stat().st_size / 1e9:.2f} GB)")
    app.run(debug=True, port=5111, threaded=True)
