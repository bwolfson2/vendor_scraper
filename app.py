#!/usr/bin/env python3
"""
Vendor Intelligence API — unified DuckDB backend.
Single connection to output/state_spending.duckdb (183M+ rows).
All endpoints accept ?state=FL,CA&year_start=2018&year_end=2024 global filters.
"""
from pathlib import Path

import duckdb
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

DB_FILE = Path(__file__).parent / "output" / "state_spending.duckdb"


# ── Connection helper ──────────────────────────────────────────────────────

def get_db():
    """Return a fresh read-only DuckDB connection per request."""
    return duckdb.connect(str(DB_FILE), read_only=True)


# ── Global filter helpers ──────────────────────────────────────────────────

def parse_global_filters(req):
    """
    Extract state/year filters from request args.
    Returns dict: states (list[str]), year_start (int|None), year_end (int|None).
    """
    raw_state = req.args.get("state", "").strip()
    states = [s.strip().upper() for s in raw_state.split(",") if s.strip()] if raw_state else []

    def _int_or_none(v):
        v = v.strip() if v else ""
        return int(v) if v.isdigit() else None

    return {
        "states": states,
        "year_start": _int_or_none(req.args.get("year_start", "")),
        "year_end": _int_or_none(req.args.get("year_end", "")),
    }


def apply_filters(filters, table_alias="s", state_col="state_abbr", year_col="fiscal_year"):
    """
    Build WHERE-clause fragments for state + year filters.
    Returns (clauses: list[str], params: list).
    Set year_col=None to skip year filtering (e.g. for vendor_states table).
    """
    prefix = f"{table_alias}." if table_alias else ""
    clauses, params = [], []

    if filters["states"]:
        phs = ",".join(["?"] * len(filters["states"]))
        clauses.append(f"{prefix}{state_col} IN ({phs})")
        params.extend(filters["states"])

    if year_col is not None:
        if filters["year_start"] is not None:
            clauses.append(f"{prefix}{year_col} >= ?")
            params.append(filters["year_start"])
        if filters["year_end"] is not None:
            clauses.append(f"{prefix}{year_col} <= ?")
            params.append(filters["year_end"])

    return clauses, params


def build_where(clauses):
    """Join a list of clause strings into a WHERE … fragment (or empty string)."""
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def check_rich_data(con, states):
    """
    Return True iff every state in `states` has has_rich_data = true.
    If `states` is empty → returns False (no specific rich state requested).
    """
    if not states:
        return False
    phs = ",".join(["?"] * len(states))
    rows = con.execute(
        f"SELECT state_abbr, has_rich_data FROM state_profiles WHERE state_abbr IN ({phs})",
        states,
    ).fetchall()
    rich = {r[0] for r in rows if r[1]}
    return all(s in rich for s in states)


def rows_to_dicts(rows, cols):
    """Zip column names with row tuples; serialize date/datetime to str."""
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = str(v)
        result.append(d)
    return result


def fetch_dicts(con, sql, params=None):
    """Execute SQL, return list of dicts (dates serialized)."""
    params = params or []
    res = con.execute(sql, params)
    cols = [desc[0] for desc in res.description]
    return rows_to_dicts(res.fetchall(), cols)


# ── Grading formulas ───────────────────────────────────────────────────────

def letter_grade(score):
    """Convert 0-100 score to letter grade A+ through F."""
    if score >= 97: return "A+"
    if score >= 93: return "A"
    if score >= 90: return "A-"
    if score >= 87: return "B+"
    if score >= 83: return "B"
    if score >= 80: return "B-"
    if score >= 77: return "C+"
    if score >= 73: return "C"
    if score >= 70: return "C-"
    if score >= 67: return "D+"
    if score >= 63: return "D"
    if score >= 60: return "D-"
    return "F"


# ── UI ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ── Meta / filters ─────────────────────────────────────────────────────────

@app.route("/api/meta/filters")
def meta_filters():
    """Return available states, year range, and per-state capabilities."""
    con = get_db()
    try:
        rows = fetch_dicts(con, """
            SELECT state_abbr, state, records, total_spend, has_rich_data,
                   quality_grade, quality_score, granularity,
                   earliest_date, latest_date
            FROM state_profiles
            ORDER BY total_spend DESC NULLS LAST
        """)
        yr = con.execute("""
            SELECT MIN(fiscal_year), MAX(fiscal_year)
            FROM spending
            WHERE fiscal_year IS NOT NULL AND fiscal_year BETWEEN 1990 AND 2030
        """).fetchone()
        return jsonify({
            "states": rows,
            "year_range": {"min": yr[0], "max": yr[1]},
            "capabilities": {
                "rich_data_states": [r["state_abbr"] for r in rows if r.get("has_rich_data")],
                "fields_rich": [
                    "status_broad", "record_type_broad", "original_end_date",
                    "new_end_date", "duration_days", "amount_change_pct",
                    "original_amount", "title",
                ],
            },
        })
    finally:
        con.close()


@app.route("/api/filters/agencies")
def filter_agencies():
    """Return distinct agency names, optionally filtered by state."""
    state = request.args.get("state", "").strip().upper()
    con = get_db()
    try:
        code_filter = "AND NOT REGEXP_MATCHES(agency_name_clean, '^[0-9]+$') AND NOT REGEXP_MATCHES(agency_name_clean, '^[A-Z0-9]{2,6}$')"
        if state:
            rows = con.execute(
                f"SELECT DISTINCT agency_name_clean FROM agency_states WHERE state_abbr = ? AND agency_name_clean IS NOT NULL {code_filter} ORDER BY agency_name_clean",
                [state],
            ).fetchall()
            return jsonify({"results": [r[0] for r in rows]})
        else:
            rows = con.execute(
                f"SELECT DISTINCT agency_name_clean FROM agency_states WHERE agency_name_clean IS NOT NULL {code_filter} ORDER BY agency_name_clean"
            ).fetchall()
            return jsonify({"results": [r[0] for r in rows]})
    finally:
        con.close()


# ── Search endpoints ────────────────────────────────────────────────────────

@app.route("/api/search/vendors")
def search_vendors():
    """Search vendor_states with optional global filters; LEFT JOIN fl_vendor_profiles."""
    filters = parse_global_filters(request)
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))
    tier = request.args.get("tier", "").strip()
    min_score = request.args.get("min_score", "").strip()

    con = get_db()
    try:
        # vendor_states has no year column — skip year filtering
        clauses, params = apply_filters(filters, table_alias="vs", year_col=None)

        if q:
            clauses.append("vs.vendor_name_clean ILIKE ?")
            params.append(f"%{q}%")
        if tier:
            clauses.append("fvp.vendor_tier = ?")
            params.append(tier)
        if min_score:
            clauses.append("fvp.vendor_score >= ?")
            params.append(float(min_score))

        where = build_where(clauses)

        rows = fetch_dicts(con, f"""
            SELECT vs.vendor_key, vs.vendor_name_clean, vs.state_abbr,
                   vs.transaction_count, vs.total_spend, vs.avg_spend,
                   vs.first_date, vs.last_date, vs.unique_agencies,
                   vs.top_agency, vs.top_category,
                   fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                   fvp.total_records, fvp.years_active, fvp.num_agencies_served,
                   fvp.completed_contracts, fvp.last_contract_date,
                   fvp.vendor_name_normalized
            FROM vendor_states vs
            LEFT JOIN fl_vendor_profiles fvp
                   ON vs.vendor_key = fvp.vendor_name_normalized
            {where}
            ORDER BY vs.total_spend DESC NULLS LAST
            LIMIT ? OFFSET ?
        """, params + [limit, offset])

        return jsonify({"results": rows, "count": len(rows)})
    finally:
        con.close()


@app.route("/api/search/contracts")
def search_contracts():
    """Search spending table with optional global filters."""
    filters = parse_global_filters(request)
    q = request.args.get("q", "").strip()
    status = request.args.get("status", "").strip()
    record_type = request.args.get("type", "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))

    con = get_db()
    try:
        clauses, params = apply_filters(filters, table_alias="s")

        if q:
            like = f"%{q}%"
            clauses.append(
                "(s.vendor_name_clean ILIKE ? OR s.agency_name ILIKE ? OR s.description ILIKE ?)"
            )
            params.extend([like, like, like])
        if status:
            clauses.append("s.status_broad = ?")
            params.append(status)
        if record_type:
            clauses.append("s.record_type_broad = ?")
            params.append(record_type)

        where = build_where(clauses)

        rows = fetch_dicts(con, f"""
            SELECT s.state_abbr, s.agency_name, s.vendor_name_clean,
                   s.vendor_name_raw, s.contract_id, s.contract_type,
                   s.description, s.title, s.procurement_method,
                   s.commodity_category, s.amount, s.parsed_date,
                   s.fiscal_year, s.status_broad, s.record_type_broad,
                   s.original_end_date, s.new_end_date, s.duration_days,
                   s.amount_change_pct, s.original_amount, s.source_url
            FROM spending s
            {where}
            ORDER BY s.amount DESC NULLS LAST
            LIMIT ? OFFSET ?
        """, params + [limit, offset])

        return jsonify({"results": rows, "count": len(rows)})
    finally:
        con.close()


@app.route("/api/search/procure")
def search_procure():
    """Procurement search: vendor_states enriched with fl_vendor_profiles."""
    filters = parse_global_filters(request)
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 30)), 100)

    if not q:
        return jsonify({"results": [], "count": 0})

    con = get_db()
    try:
        clauses, params = apply_filters(filters, table_alias="vs", year_col=None)
        clauses.append("vs.vendor_name_clean ILIKE ?")
        params.append(f"%{q}%")
        where = build_where(clauses)

        rows = fetch_dicts(con, f"""
            SELECT vs.vendor_key, vs.vendor_name_clean, vs.state_abbr,
                   vs.transaction_count, vs.total_spend, vs.avg_spend,
                   vs.unique_agencies, vs.top_agency, vs.top_category,
                   vs.first_date, vs.last_date,
                   fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                   fvp.total_records, fvp.years_active, fvp.num_agencies_served,
                   fvp.completed_contracts, fvp.last_contract_date,
                   fvp.active_contracts, fvp.avg_contract_amount,
                   fvp.vendor_name_normalized
            FROM vendor_states vs
            LEFT JOIN fl_vendor_profiles fvp ON vs.vendor_key = fvp.vendor_name_normalized
            {where}
            ORDER BY vs.total_spend DESC NULLS LAST
            LIMIT ?
        """, params + [limit])

        return jsonify({"results": rows, "count": len(rows)})
    finally:
        con.close()


# ── Detail endpoints ────────────────────────────────────────────────────────

@app.route("/api/vendor/<path:vendor_norm>")
def vendor_detail(vendor_norm):
    """
    Try fl_vendor_profiles first; fall back to dynamic aggregation from
    vendor_states / spending for non-FL vendors.
    """
    filters = parse_global_filters(request)
    con = get_db()
    try:
        profile_rows = fetch_dicts(con,
            "SELECT * FROM fl_vendor_profiles WHERE vendor_name_normalized = ?",
            [vendor_norm])

        if profile_rows:
            result = profile_rows[0]

            result["agency_history"] = fetch_dicts(con, """
                SELECT * FROM fl_vendor_agency_history
                WHERE vendor_name_normalized = ?
                ORDER BY total_amount DESC NULLS LAST
            """, [vendor_norm])

            result["commodity_expertise"] = fetch_dicts(con, """
                SELECT * FROM fl_vendor_commodity_expertise
                WHERE vendor_name_normalized = ?
                ORDER BY total_amount DESC NULLS LAST
            """, [vendor_norm])

            # Spend over time from spending (FL rows)
            sot_clauses = [
                "vendor_key = ?",
                "state_abbr = 'FL'",
                "fiscal_year IS NOT NULL",
                "fiscal_year BETWEEN 2000 AND 2030",
            ]
            sot_params = [vendor_norm]
            if filters["year_start"]:
                sot_clauses.append("fiscal_year >= ?")
                sot_params.append(filters["year_start"])
            if filters["year_end"]:
                sot_clauses.append("fiscal_year <= ?")
                sot_params.append(filters["year_end"])

            result["spend_over_time"] = fetch_dicts(con, f"""
                SELECT fiscal_year AS year,
                       COUNT(*) AS cnt,
                       SUM(COALESCE(amount, 0)) AS total_val,
                       SUM(CASE WHEN record_type_broad='CONTRACT'
                                THEN COALESCE(amount, 0) ELSE 0 END) AS contract_val,
                       SUM(CASE WHEN record_type_broad='PURCHASE_ORDER'
                                THEN COALESCE(amount, 0) ELSE 0 END) AS po_val,
                       SUM(CASE WHEN record_type_broad='GRANT'
                                THEN COALESCE(amount, 0) ELSE 0 END) AS grant_val
                FROM spending
                WHERE {" AND ".join(sot_clauses)}
                GROUP BY fiscal_year ORDER BY fiscal_year
            """, sot_params)

            result["recent_contracts"] = fetch_dicts(con, """
                SELECT contract_id, agency_name, title, amount, original_amount,
                       status_broad, record_type_broad, commodity_category,
                       parsed_date, original_end_date, new_end_date,
                       procurement_method, amount_change_pct, duration_days
                FROM spending
                WHERE vendor_key = ? AND state_abbr = 'FL'
                ORDER BY parsed_date DESC NULLS LAST
                LIMIT 25
            """, [vendor_norm])

            risk = fetch_dicts(con, """
                SELECT
                    COUNT(CASE WHEN amount_change_pct > 0 THEN 1 END) AS cost_overrun_count,
                    AVG(CASE WHEN amount_change_pct > 0 THEN amount_change_pct END) AS avg_overrun_pct,
                    COUNT(CASE WHEN new_end_date IS NOT NULL
                                AND new_end_date != original_end_date THEN 1 END) AS extension_count,
                    COUNT(CASE WHEN procurement_method ILIKE '%sole%'
                                 OR procurement_method ILIKE '%single%'
                                 OR procurement_method ILIKE '%exempt%' THEN 1 END) AS sole_source_count
                FROM spending
                WHERE vendor_key = ? AND state_abbr = 'FL'
            """, [vendor_norm])
            if risk:
                result["risk_metrics"] = risk[0]

        else:
            # Fallback: aggregate from vendor_states
            vs_clauses, vs_params = ["vendor_key = ?"], [vendor_norm]
            if filters["states"]:
                phs = ",".join(["?"] * len(filters["states"]))
                vs_clauses.append(f"state_abbr IN ({phs})")
                vs_params.extend(filters["states"])
            vs_where = build_where(vs_clauses)

            agg = fetch_dicts(con, f"""
                SELECT vendor_name_clean,
                       SUM(transaction_count) AS total_records,
                       SUM(total_spend) AS total_spend,
                       AVG(avg_spend) AS avg_spend,
                       SUM(unique_agencies) AS num_agencies_served,
                       MIN(first_date) AS first_date,
                       MAX(last_date) AS last_date,
                       COUNT(DISTINCT state_abbr) AS num_states
                FROM vendor_states
                {vs_where}
                GROUP BY vendor_name_clean
            """, vs_params)

            if not agg:
                return jsonify({"error": "Vendor not found"}), 404

            result = agg[0]
            result["vendor_name_normalized"] = vendor_norm
            result["source"] = "aggregated"

            result["state_breakdown"] = fetch_dicts(con, f"""
                SELECT state_abbr, transaction_count, total_spend,
                       unique_agencies, top_agency, top_category
                FROM vendor_states
                {vs_where}
                ORDER BY total_spend DESC NULLS LAST
            """, vs_params)

            # Spend over time from spending
            sp_clauses, sp_params = apply_filters(filters, table_alias="")
            sp_clauses = ["vendor_key = ?",
                          "fiscal_year IS NOT NULL",
                          "fiscal_year BETWEEN 2000 AND 2030"] + sp_clauses
            sp_params = [vendor_norm] + sp_params

            result["spend_over_time"] = fetch_dicts(con, f"""
                SELECT fiscal_year AS year, COUNT(*) AS cnt,
                       SUM(COALESCE(amount, 0)) AS total_val
                FROM spending
                WHERE {" AND ".join(sp_clauses)}
                GROUP BY fiscal_year ORDER BY fiscal_year
            """, sp_params)

        return jsonify(result)
    finally:
        con.close()


@app.route("/api/agency/<path:agency_name>")
def agency_detail(agency_name):
    """Aggregate agency detail from spending, with HHI calculation."""
    filters = parse_global_filters(request)
    con = get_db()
    try:
        f_clauses, f_params = apply_filters(filters, table_alias="s")
        clauses = ["s.agency_name = ?"] + f_clauses
        params = [agency_name] + f_params
        where = build_where(clauses)

        overview = fetch_dicts(con, f"""
            SELECT
                COUNT(*) AS total_records,
                SUM(COALESCE(s.amount, 0)) AS total_amount,
                AVG(COALESCE(s.amount, 0)) AS avg_amount,
                COUNT(DISTINCT s.vendor_key) AS num_vendors,
                COUNT(DISTINCT s.commodity_category) AS num_commodities,
                COUNT(CASE WHEN s.status_broad='ACTIVE' THEN 1 END) AS active_count,
                COUNT(CASE WHEN s.status_broad='COMPLETED' THEN 1 END) AS completed_count,
                COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS cancelled_count,
                COUNT(CASE WHEN s.record_type_broad='CONTRACT' THEN 1 END) AS contract_count,
                COUNT(CASE WHEN s.record_type_broad='PURCHASE_ORDER' THEN 1 END) AS po_count,
                COUNT(CASE WHEN s.record_type_broad='GRANT' THEN 1 END) AS grant_count,
                MIN(s.parsed_date) AS first_date,
                MAX(s.parsed_date) AS last_date,
                AVG(CASE WHEN s.duration_days > 0 THEN s.duration_days END) AS avg_duration_days,
                AVG(s.amount_change_pct) AS avg_cost_change_pct,
                COUNT(CASE WHEN s.amount_change_pct > 0 THEN 1 END) AS cost_overrun_count,
                CAST(COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS DOUBLE)
                    / GREATEST(1, COUNT(*)) AS cancellation_rate
            FROM spending s
            {where}
        """, params)

        if not overview or overview[0]["total_records"] == 0:
            return jsonify({"error": "Agency not found"}), 404

        result = overview[0]
        result["agency_name"] = agency_name

        # Spend over time
        sot_clauses = clauses + [
            "s.fiscal_year IS NOT NULL",
            "s.fiscal_year BETWEEN 2000 AND 2030",
        ]
        sot_where = build_where(sot_clauses)
        result["spend_over_time"] = fetch_dicts(con, f"""
            SELECT s.fiscal_year AS year, COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val,
                   SUM(CASE WHEN s.record_type_broad='CONTRACT'
                            THEN COALESCE(s.amount, 0) ELSE 0 END) AS contract_val,
                   SUM(CASE WHEN s.record_type_broad='PURCHASE_ORDER'
                            THEN COALESCE(s.amount, 0) ELSE 0 END) AS po_val,
                   SUM(CASE WHEN s.record_type_broad='GRANT'
                            THEN COALESCE(s.amount, 0) ELSE 0 END) AS grant_val
            FROM spending s {sot_where}
            GROUP BY s.fiscal_year ORDER BY s.fiscal_year
        """, params + [])  # sot_clauses only added literals, no new params

        # Top vendors
        result["top_vendors"] = fetch_dicts(con, f"""
            SELECT s.vendor_name_clean, s.vendor_key,
                   COUNT(*) AS record_count,
                   SUM(COALESCE(s.amount, 0)) AS total_val,
                   AVG(COALESCE(s.amount, 0)) AS avg_val,
                   fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                   fvp.total_records, fvp.completed_contracts, fvp.years_active,
                   fvp.num_agencies_served, fvp.last_contract_date
            FROM spending s
            LEFT JOIN fl_vendor_profiles fvp ON s.vendor_key = fvp.vendor_name_normalized
            {where}
            GROUP BY s.vendor_key, s.vendor_name_clean,
                     fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                     fvp.total_records, fvp.completed_contracts, fvp.years_active,
                     fvp.num_agencies_served, fvp.last_contract_date
            ORDER BY total_val DESC NULLS LAST
            LIMIT 30
        """, params)

        # HHI (vendor concentration)
        total_amt = result["total_amount"] or 0
        if total_amt > 0:
            hhi_rows = fetch_dicts(con, f"""
                SELECT SUM(share * share) AS hhi FROM (
                    SELECT CAST(SUM(COALESCE(s.amount, 0)) AS DOUBLE) / ? AS share
                    FROM spending s {where}
                    GROUP BY s.vendor_key
                )
            """, [total_amt] + params)
            result["hhi_index"] = round((hhi_rows[0]["hhi"] or 0) * 10000) if hhi_rows else 0
        else:
            result["hhi_index"] = 0

        # Top commodities
        comm_clauses = clauses + [
            "s.commodity_category IS NOT NULL",
            "s.commodity_category != ''",
        ]
        comm_where = build_where(comm_clauses)
        result["top_commodities"] = fetch_dicts(con, f"""
            SELECT s.commodity_category, COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val,
                   COUNT(DISTINCT s.vendor_key) AS num_vendors
            FROM spending s {comm_where}
            GROUP BY s.commodity_category
            ORDER BY total_val DESC NULLS LAST
            LIMIT 20
        """, params)

        # Procurement method distribution
        pm_clauses = clauses + [
            "s.procurement_method IS NOT NULL",
            "s.procurement_method != ''",
        ]
        pm_where = build_where(pm_clauses)
        result["procurement_methods"] = fetch_dicts(con, f"""
            SELECT s.procurement_method, COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val
            FROM spending s {pm_where}
            GROUP BY s.procurement_method
            ORDER BY total_val DESC NULLS LAST
            LIMIT 15
        """, params)

        # Recent contracts
        result["recent_contracts"] = fetch_dicts(con, f"""
            SELECT s.vendor_name_clean, s.vendor_key, s.title,
                   s.amount, s.original_amount, s.status_broad,
                   s.record_type_broad, s.commodity_category,
                   s.parsed_date, s.procurement_method, s.amount_change_pct
            FROM spending s {where}
            ORDER BY s.parsed_date DESC NULLS LAST
            LIMIT 20
        """, params)

        return jsonify(result)
    finally:
        con.close()


@app.route("/api/commodity/<path:commodity>")
def commodity_detail(commodity):
    """Aggregate from spending by commodity_category."""
    filters = parse_global_filters(request)
    con = get_db()
    try:
        f_clauses, f_params = apply_filters(filters, table_alias="s")
        clauses = ["s.commodity_category = ?"] + f_clauses
        params = [commodity] + f_params
        where = build_where(clauses)

        overview = fetch_dicts(con, f"""
            SELECT
                COUNT(*) AS total_records,
                SUM(COALESCE(s.amount, 0)) AS total_amount,
                AVG(COALESCE(s.amount, 0)) AS avg_amount,
                COUNT(DISTINCT s.vendor_key) AS num_vendors,
                COUNT(DISTINCT s.agency_name) AS num_agencies,
                COUNT(CASE WHEN s.status_broad='ACTIVE' THEN 1 END) AS active_count,
                COUNT(CASE WHEN s.status_broad='COMPLETED' THEN 1 END) AS completed_count,
                COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS cancelled_count,
                MIN(s.parsed_date) AS first_date,
                MAX(s.parsed_date) AS last_date,
                CAST(COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS DOUBLE)
                    / GREATEST(1, COUNT(*)) AS cancellation_rate
            FROM spending s {where}
        """, params)

        if not overview or overview[0]["total_records"] == 0:
            return jsonify({"error": "Commodity not found"}), 404

        result = overview[0]
        result["commodity_category"] = commodity

        # Spend over time
        sot_clauses = clauses + [
            "s.fiscal_year IS NOT NULL",
            "s.fiscal_year BETWEEN 2000 AND 2030",
        ]
        sot_where = build_where(sot_clauses)
        result["spend_over_time"] = fetch_dicts(con, f"""
            SELECT s.fiscal_year AS year, COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val
            FROM spending s {sot_where}
            GROUP BY s.fiscal_year ORDER BY s.fiscal_year
        """, params)

        # Top vendors
        result["top_vendors"] = fetch_dicts(con, f"""
            SELECT s.vendor_name_clean, s.vendor_key,
                   COUNT(*) AS record_count,
                   SUM(COALESCE(s.amount, 0)) AS total_val,
                   AVG(COALESCE(s.amount, 0)) AS avg_val,
                   fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                   fvp.total_records, fvp.completed_contracts, fvp.years_active,
                   fvp.num_agencies_served, fvp.last_contract_date
            FROM spending s
            LEFT JOIN fl_vendor_profiles fvp ON s.vendor_key = fvp.vendor_name_normalized
            {where}
            GROUP BY s.vendor_key, s.vendor_name_clean,
                     fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                     fvp.total_records, fvp.completed_contracts, fvp.years_active,
                     fvp.num_agencies_served, fvp.last_contract_date
            ORDER BY total_val DESC NULLS LAST
            LIMIT 30
        """, params)

        # Agencies using this commodity
        result["agencies"] = fetch_dicts(con, f"""
            SELECT s.agency_name, COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val,
                   COUNT(DISTINCT s.vendor_key) AS num_vendors
            FROM spending s {where}
            GROUP BY s.agency_name
            ORDER BY total_val DESC NULLS LAST
            LIMIT 20
        """, params)

        # Price benchmarking by agency
        pb_clauses = clauses + ["s.amount > 0"]
        pb_where = build_where(pb_clauses)
        result["price_benchmark"] = fetch_dicts(con, f"""
            SELECT s.agency_name,
                   AVG(s.amount) AS avg_val,
                   MIN(s.amount) AS min_val,
                   MAX(s.amount) AS max_val,
                   COUNT(*) AS cnt
            FROM spending s {pb_where}
            GROUP BY s.agency_name
            HAVING COUNT(*) >= 2
            ORDER BY avg_val DESC NULLS LAST
            LIMIT 15
        """, params)

        return jsonify(result)
    finally:
        con.close()


@app.route("/api/contract/<contract_id>")
def contract_detail(contract_id):
    """Single record from spending by contract_id."""
    con = get_db()
    try:
        rows = fetch_dicts(con,
            "SELECT * FROM spending WHERE contract_id = ? LIMIT 1",
            [contract_id])
        if not rows:
            return jsonify({"error": "Contract not found"}), 404
        return jsonify(rows[0])
    finally:
        con.close()


# ── Stats Dashboard ─────────────────────────────────────────────────────────

@app.route("/api/stats/overview")
def stats_overview():
    """
    Total records, vendors, agencies, value, yearly trend.
    active_contracts + tier_distribution only when has_rich_data.
    """
    filters = parse_global_filters(request)
    con = get_db()
    try:
        # Headline stats — use state_profiles (pre-aggregated, fast)
        if filters["states"]:
            phs = ",".join(["?"] * len(filters["states"]))
            sp_rows = fetch_dicts(con,
                f"SELECT * FROM state_profiles WHERE state_abbr IN ({phs})",
                filters["states"])
            total_records = sum(r.get("records") or 0 for r in sp_rows)
            total_vendors = sum(r.get("unique_vendors") or 0 for r in sp_rows)
            total_agencies = sum(r.get("unique_agencies") or 0 for r in sp_rows)
            total_value = sum(r.get("total_spend") or 0 for r in sp_rows)
        else:
            agg = con.execute("""
                SELECT SUM(records), SUM(unique_vendors), SUM(unique_agencies), SUM(total_spend)
                FROM state_profiles
            """).fetchone()
            total_records, total_vendors, total_agencies, total_value = agg

        stats = {
            "total_contracts": total_records,
            "total_vendors": total_vendors,
            "total_agencies": total_agencies,
            "total_value": total_value,
        }

        # Yearly trend from spending table
        f_clauses, f_params = apply_filters(filters, table_alias="s")
        yt_clauses = f_clauses + [
            "s.fiscal_year IS NOT NULL",
            "s.fiscal_year BETWEEN 2000 AND 2030",
        ]
        yt_where = build_where(yt_clauses)
        stats["yearly_trend"] = fetch_dicts(con, f"""
            SELECT s.fiscal_year AS year, COUNT(*) AS count,
                   SUM(COALESCE(s.amount, 0)) AS total_value
            FROM spending s {yt_where}
            GROUP BY s.fiscal_year ORDER BY s.fiscal_year
        """, f_params)

        rich = check_rich_data(con, filters["states"])
        if rich:
            act_clauses = f_clauses + ["s.status_broad = 'ACTIVE'"]
            act_where = build_where(act_clauses)
            stats["active_contracts"] = con.execute(
                f"SELECT COUNT(*) FROM spending s {act_where}", f_params
            ).fetchone()[0]

            stats["tier_distribution"] = {
                r["vendor_tier"]: r["cnt"]
                for r in fetch_dicts(con,
                    "SELECT vendor_tier, COUNT(*) AS cnt FROM fl_vendor_profiles GROUP BY vendor_tier")
            }

        return jsonify(stats)
    finally:
        con.close()


@app.route("/api/stats/top-vendors")
def top_vendors():
    """Top vendors from vendor_states + fl_vendor_profiles."""
    filters = parse_global_filters(request)
    sort_by = request.args.get("sort", "spend")
    limit = min(int(request.args.get("limit", 25)), 100)

    con = get_db()
    try:
        # vendor_states has no fiscal_year — skip year filter
        clauses, params = apply_filters(filters, table_alias="vs", year_col=None)
        where = build_where(clauses)

        order = (
            "fvp.vendor_score DESC NULLS LAST"
            if sort_by == "score"
            else "SUM(vs.total_spend) DESC NULLS LAST"
        )

        rows = fetch_dicts(con, f"""
            SELECT vs.vendor_name_clean, vs.vendor_key,
                   SUM(vs.transaction_count) AS total_records,
                   SUM(vs.total_spend) AS total_spend,
                   AVG(vs.avg_spend) AS avg_spend,
                   SUM(vs.unique_agencies) AS num_agencies,
                   MIN(vs.first_date) AS first_date,
                   MAX(vs.last_date) AS last_date,
                   fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                   fvp.years_active, fvp.completed_contracts, fvp.last_contract_date,
                   fvp.vendor_name_normalized, fvp.vendor_name_display
            FROM vendor_states vs
            LEFT JOIN fl_vendor_profiles fvp ON vs.vendor_key = fvp.vendor_name_normalized
            {where}
            GROUP BY vs.vendor_key, vs.vendor_name_clean,
                     fvp.vendor_score, fvp.vendor_tier, fvp.cancellation_rate,
                     fvp.years_active, fvp.completed_contracts, fvp.last_contract_date,
                     fvp.vendor_name_normalized, fvp.vendor_name_display
            ORDER BY {order}
            LIMIT ?
        """, params + [limit])

        return jsonify({"results": rows})
    finally:
        con.close()


@app.route("/api/stats/agencies")
def agency_stats():
    """Agency stats from agency_states."""
    filters = parse_global_filters(request)
    con = get_db()
    try:
        clauses, params = [], []
        if filters["states"]:
            phs = ",".join(["?"] * len(filters["states"]))
            clauses.append(f"state_abbr IN ({phs})")
            params.extend(filters["states"])
        where = build_where(clauses)

        rows = fetch_dicts(con, f"""
            SELECT agency_name_clean, state_abbr,
                   SUM(transaction_count) AS transaction_count,
                   SUM(total_spend) AS total_spend,
                   SUM(vendor_count) AS vendor_count
            FROM agency_states {where}
            GROUP BY agency_name_clean, state_abbr
            ORDER BY total_spend DESC NULLS LAST
        """, params)

        return jsonify({"results": rows})
    finally:
        con.close()


@app.route("/api/stats/commodities")
def commodity_stats():
    """GROUP BY commodity_category in spending."""
    filters = parse_global_filters(request)
    limit = min(int(request.args.get("limit", 50)), 200)
    con = get_db()
    try:
        f_clauses, f_params = apply_filters(filters, table_alias="s")
        clauses = f_clauses + [
            "s.commodity_category IS NOT NULL",
            "s.commodity_category != ''",
        ]
        where = build_where(clauses)

        rows = fetch_dicts(con, f"""
            SELECT s.commodity_category, COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val,
                   COUNT(DISTINCT s.vendor_key) AS num_vendors,
                   COUNT(DISTINCT s.agency_name) AS num_agencies
            FROM spending s {where}
            GROUP BY s.commodity_category
            ORDER BY total_val DESC NULLS LAST
            LIMIT ?
        """, f_params + [limit])

        return jsonify({"results": rows})
    finally:
        con.close()


# ── Analytics ───────────────────────────────────────────────────────────────

@app.route("/api/analytics/spend-by-agency-year")
def spend_by_agency_year():
    """Top agencies yearly from spending."""
    filters = parse_global_filters(request)
    top_n = min(int(request.args.get("top", 10)), 20)
    con = get_db()
    try:
        f_clauses, f_params = apply_filters(filters, table_alias="s")
        base_clauses = f_clauses + [
            "s.fiscal_year IS NOT NULL",
            "s.fiscal_year BETWEEN 2005 AND 2030",
        ]
        base_where = build_where(base_clauses)

        top = con.execute(f"""
            SELECT s.agency_name
            FROM spending s {base_where}
            GROUP BY s.agency_name
            ORDER BY SUM(COALESCE(s.amount, 0)) DESC NULLS LAST
            LIMIT ?
        """, f_params + [top_n]).fetchall()
        top_agencies = [r[0] for r in top]

        results = {}
        for ag in top_agencies:
            ag_clauses = base_clauses + ["s.agency_name = ?"]
            ag_where = build_where(ag_clauses)
            data = fetch_dicts(con, f"""
                SELECT s.fiscal_year AS year, SUM(COALESCE(s.amount, 0)) AS total_val
                FROM spending s {ag_where}
                GROUP BY s.fiscal_year ORDER BY s.fiscal_year
            """, f_params + [ag])
            results[ag] = data

        return jsonify({"agencies": top_agencies, "data": results})
    finally:
        con.close()


@app.route("/api/analytics/spend-by-vendor-year")
def spend_by_vendor_year():
    """Top vendors yearly from spending."""
    filters = parse_global_filters(request)
    top_n = min(int(request.args.get("top", 10)), 20)
    con = get_db()
    try:
        f_clauses, f_params = apply_filters(filters, table_alias="s")
        base_clauses = f_clauses + [
            "s.fiscal_year IS NOT NULL",
            "s.fiscal_year BETWEEN 2005 AND 2030",
        ]
        base_where = build_where(base_clauses)

        top = con.execute(f"""
            SELECT s.vendor_name_clean, s.vendor_key
            FROM spending s {base_where} AND s.vendor_name_clean IS NOT NULL
            GROUP BY s.vendor_key, s.vendor_name_clean
            ORDER BY SUM(COALESCE(s.amount, 0)) DESC NULLS LAST
            LIMIT ?
        """, f_params + [top_n]).fetchall()

        results = {}
        for name, key in top:
            vd_clauses = base_clauses + ["s.vendor_key = ?"]
            vd_where = build_where(vd_clauses)
            data = fetch_dicts(con, f"""
                SELECT s.fiscal_year AS year, SUM(COALESCE(s.amount, 0)) AS total_val
                FROM spending s {vd_where}
                GROUP BY s.fiscal_year ORDER BY s.fiscal_year
            """, f_params + [key])
            results[name] = {"norm": key, "data": data}

        return jsonify({"vendors": [r[0] for r in top], "data": results})
    finally:
        con.close()


@app.route("/api/analytics/new-vs-returning")
def new_vs_returning():
    """New vs returning vendors per fiscal year — CTE on spending."""
    filters = parse_global_filters(request)
    con = get_db()
    try:
        f_clauses, f_params = apply_filters(filters, table_alias="c")
        base_clauses = f_clauses + [
            "c.fiscal_year IS NOT NULL",
            "c.fiscal_year BETWEEN 2005 AND 2030",
        ]
        base_where = build_where(base_clauses)

        rows = fetch_dicts(con, f"""
            WITH first_year AS (
                SELECT vendor_key, MIN(fiscal_year) AS first_yr
                FROM spending c {base_where}
                GROUP BY vendor_key
            ),
            yearly AS (
                SELECT c.fiscal_year AS year, c.vendor_key, fy.first_yr
                FROM spending c
                JOIN first_year fy ON c.vendor_key = fy.vendor_key
                {base_where}
                GROUP BY c.fiscal_year, c.vendor_key, fy.first_yr
            )
            SELECT year,
                   COUNT(CASE WHEN year = first_yr THEN 1 END) AS new_vendors,
                   COUNT(CASE WHEN year != first_yr THEN 1 END) AS returning_vendors
            FROM yearly
            GROUP BY year ORDER BY year
        """, f_params + f_params)  # params used twice: once in CTE, once in yearly

        return jsonify({"results": rows})
    finally:
        con.close()


@app.route("/api/analytics/risk-overview")
def risk_overview():
    """
    GATED: only for states with has_rich_data (needs status_broad, amount_change_pct).
    Defaults to FL when no state filter is provided.
    """
    filters = parse_global_filters(request)
    con = get_db()
    try:
        rich = check_rich_data(con, filters["states"])
        # Default to FL when no state specified
        rich_states = filters["states"] if (filters["states"] and rich) else ["FL"]

        if not rich and filters["states"] and "FL" not in filters["states"]:
            return jsonify({
                "error": "risk-overview requires states with rich data (e.g., FL)",
                "has_rich_data": False,
            }), 200

        phs = ",".join(["?"] * len(rich_states))
        base_clauses = [f"s.state_abbr IN ({phs})"]
        base_params = list(rich_states)
        if filters["year_start"]:
            base_clauses.append("s.fiscal_year >= ?")
            base_params.append(filters["year_start"])
        if filters["year_end"]:
            base_clauses.append("s.fiscal_year <= ?")
            base_params.append(filters["year_end"])
        base_where = build_where(base_clauses)

        by_agency = fetch_dicts(con, f"""
            SELECT s.agency_name,
                   CAST(COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS DOUBLE)
                       / GREATEST(1, COUNT(*)) AS cancel_rate,
                   COUNT(*) AS total
            FROM spending s {base_where}
            GROUP BY s.agency_name
            HAVING COUNT(*) >= 50
            ORDER BY cancel_rate DESC NULLS LAST
            LIMIT 20
        """, base_params)

        overruns = fetch_dicts(con, f"""
            SELECT s.agency_name,
                   AVG(CASE WHEN s.amount_change_pct > 0 AND s.amount_change_pct <= 500
                            THEN s.amount_change_pct END) AS avg_overrun,
                   COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS overrun_count,
                   COUNT(*) AS total
            FROM spending s {base_where}
            GROUP BY s.agency_name
            HAVING COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) > 0
            ORDER BY avg_overrun DESC NULLS LAST
            LIMIT 20
        """, base_params)

        extensions = fetch_dicts(con, f"""
            SELECT s.agency_name,
                   COUNT(CASE WHEN s.new_end_date IS NOT NULL
                               AND s.new_end_date != s.original_end_date THEN 1 END) AS extensions,
                   COUNT(*) AS total,
                   CAST(COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                    AND s.new_end_date != s.original_end_date THEN 1 END) AS DOUBLE)
                       / GREATEST(1, COUNT(*)) AS ext_rate
            FROM spending s {base_where}
            GROUP BY s.agency_name
            HAVING COUNT(*) >= 50
            ORDER BY ext_rate DESC NULLS LAST
            LIMIT 20
        """, base_params)

        pm_clauses = base_clauses + [
            "s.procurement_method IS NOT NULL",
            "s.procurement_method != ''",
        ]
        pm_where = build_where(pm_clauses)
        methods = fetch_dicts(con, f"""
            SELECT s.procurement_method, COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val
            FROM spending s {pm_where}
            GROUP BY s.procurement_method
            ORDER BY total_val DESC NULLS LAST
            LIMIT 20
        """, base_params)

        return jsonify({
            "has_rich_data": True,
            "states": rich_states,
            "cancellation_by_agency": by_agency,
            "cost_overruns": overruns,
            "extensions": extensions,
            "procurement_methods": methods,
        })
    finally:
        con.close()


@app.route("/api/analytics/commodity-trends")
def commodity_trends():
    """Yearly commodity spending from spending table."""
    filters = parse_global_filters(request)
    con = get_db()
    try:
        f_clauses, f_params = apply_filters(filters, table_alias="s")
        clauses = f_clauses + [
            "s.commodity_category IS NOT NULL",
            "s.commodity_category != ''",
            "s.fiscal_year IS NOT NULL",
            "s.fiscal_year BETWEEN 2015 AND 2030",
        ]
        where = build_where(clauses)

        raw_rows = fetch_dicts(con, f"""
            SELECT s.commodity_category, s.fiscal_year AS year,
                   COUNT(*) AS cnt,
                   SUM(COALESCE(s.amount, 0)) AS total_val
            FROM spending s {where}
            GROUP BY s.commodity_category, s.fiscal_year
            ORDER BY s.commodity_category, s.fiscal_year
        """, f_params)

        raw = {}
        for r in raw_rows:
            cd = r["commodity_category"]
            if cd not in raw:
                raw[cd] = {"total": 0, "years": []}
            raw[cd]["years"].append({"year": r["year"], "cnt": r["cnt"], "val": r["total_val"]})
            raw[cd]["total"] += r["total_val"] or 0

        top = sorted(raw.items(), key=lambda x: x[1]["total"], reverse=True)[:15]
        result = [{"commodity": k, "total": v["total"], "years": v["years"]} for k, v in top]

        return jsonify({"results": result})
    finally:
        con.close()


# ── Performance — Two-tier ──────────────────────────────────────────────────

@app.route("/api/performance/state")
def performance_state():
    """
    Full grading (cancel/overrun/extension rates → efficiency_score) if has_rich_data.
    Else: data quality grade from state_profiles.
    """
    filters = parse_global_filters(request)
    con = get_db()
    try:
        rich = check_rich_data(con, filters["states"])

        if rich or not filters["states"]:
            # Full grading — default to FL when no state filter
            rich_states = filters["states"] if (filters["states"] and rich) else ["FL"]
            phs = ",".join(["?"] * len(rich_states))
            base_clauses = [f"s.state_abbr IN ({phs})"]
            base_params = list(rich_states)
            if filters["year_start"]:
                base_clauses.append("s.fiscal_year >= ?")
                base_params.append(filters["year_start"])
            if filters["year_end"]:
                base_clauses.append("s.fiscal_year <= ?")
                base_params.append(filters["year_end"])
            base_where = build_where(base_clauses)

            agg = fetch_dicts(con, f"""
                SELECT
                    COUNT(*) AS total_records,
                    SUM(COALESCE(s.amount, 0)) AS total_spend,
                    COUNT(DISTINCT s.vendor_key) AS total_vendors,
                    COUNT(DISTINCT s.agency_name) AS total_agencies,
                    COUNT(CASE WHEN s.status_broad='ACTIVE' THEN 1 END) AS active,
                    COUNT(CASE WHEN s.status_broad='COMPLETED' THEN 1 END) AS completed,
                    COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS cancelled,
                    CAST(COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS cancel_rate,
                    AVG(CASE WHEN s.amount_change_pct > 0 AND s.amount_change_pct <= 500
                             THEN s.amount_change_pct END) AS avg_cost_overrun,
                    COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS overrun_count,
                    COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                AND s.new_end_date != s.original_end_date THEN 1 END) AS extension_count,
                    CAST(COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                     AND s.new_end_date != s.original_end_date THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS extension_rate
                FROM spending s {base_where}
            """, base_params)
            state = agg[0] if agg else {}

            # Overrun rate: fraction of contracts with >10% overrun
            ov_clauses = base_clauses + ["s.amount_change_pct IS NOT NULL"]
            ov_where = build_where(ov_clauses)
            or_row = con.execute(f"""
                SELECT CAST(COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS DOUBLE)
                    / GREATEST(1, COUNT(*))
                FROM spending s {ov_where}
            """, base_params).fetchone()
            state["overrun_rate"] = or_row[0] or 0

            # Tier distribution from fl_vendor_profiles
            tier_rows = fetch_dicts(con,
                "SELECT vendor_tier, COUNT(*) AS cnt FROM fl_vendor_profiles GROUP BY vendor_tier")
            tier_dist = {r["vendor_tier"]: r["cnt"] for r in tier_rows}
            state["tier_dist"] = tier_dist

            cancel_score = max(0, 100 - state.get("cancel_rate", 0) * 500)
            overrun_score = max(0, 100 - state["overrun_rate"] * 200)
            ext_score = max(0, 100 - state.get("extension_rate", 0) * 200)
            tv = state.get("total_vendors") or 1
            plat_pct = (tier_dist.get("PLATINUM", 0) + tier_dist.get("GOLD", 0)) / max(1, tv) * 100
            vendor_quality = min(100, plat_pct * 3)

            state["efficiency_score"] = round(
                cancel_score * 0.3 + overrun_score * 0.25 +
                ext_score * 0.2 + vendor_quality * 0.25, 1
            )
            state["grade"] = letter_grade(state["efficiency_score"])
            state["sub_scores"] = {
                "cancel_score": round(cancel_score, 1),
                "overrun_score": round(overrun_score, 1),
                "ext_score": round(ext_score, 1),
                "vendor_quality": round(vendor_quality, 1),
            }
            state["has_rich_data"] = True
            return jsonify(state)

        else:
            # Basic: data quality grade from state_profiles
            phs = ",".join(["?"] * len(filters["states"]))
            sp_rows = fetch_dicts(con,
                f"SELECT * FROM state_profiles WHERE state_abbr IN ({phs})",
                filters["states"])
            return jsonify({
                "has_rich_data": False,
                "states": sp_rows,
                "note": "Full performance grading requires states with rich data (e.g., FL).",
            })
    finally:
        con.close()


@app.route("/api/performance/departments")
def performance_departments():
    """
    Per-agency grading + HHI if has_rich_data.
    Else: spending ranking from agency_states.
    """
    filters = parse_global_filters(request)
    con = get_db()
    try:
        rich = check_rich_data(con, filters["states"])

        if rich or not filters["states"]:
            rich_states = filters["states"] if (filters["states"] and rich) else ["FL"]
            phs = ",".join(["?"] * len(rich_states))
            base_clauses = [f"s.state_abbr IN ({phs})"]
            base_params = list(rich_states)
            if filters["year_start"]:
                base_clauses.append("s.fiscal_year >= ?")
                base_params.append(filters["year_start"])
            if filters["year_end"]:
                base_clauses.append("s.fiscal_year <= ?")
                base_params.append(filters["year_end"])
            base_where = build_where(base_clauses)

            raw = fetch_dicts(con, f"""
                SELECT
                    s.agency_name,
                    COUNT(*) AS total_records,
                    SUM(COALESCE(s.amount, 0)) AS total_spend,
                    COUNT(DISTINCT s.vendor_key) AS num_vendors,
                    COUNT(CASE WHEN s.status_broad='ACTIVE' THEN 1 END) AS active,
                    COUNT(CASE WHEN s.status_broad='COMPLETED' THEN 1 END) AS completed,
                    COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS cancelled,
                    CAST(COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS cancel_rate,
                    AVG(COALESCE(s.amount, 0)) AS avg_contract_size,
                    AVG(CASE WHEN s.amount_change_pct > 0 AND s.amount_change_pct <= 500
                             THEN s.amount_change_pct END) AS avg_cost_overrun,
                    COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS overrun_count,
                    CAST(COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS overrun_rate,
                    COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                AND s.new_end_date != s.original_end_date THEN 1 END) AS extensions,
                    CAST(COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                     AND s.new_end_date != s.original_end_date THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS extension_rate,
                    MIN(s.parsed_date) AS first_date,
                    MAX(s.parsed_date) AS last_date
                FROM spending s {base_where}
                GROUP BY s.agency_name
                HAVING COUNT(*) >= 10
                ORDER BY total_spend DESC NULLS LAST
            """, base_params)

            depts = []
            for d in raw:
                ag = d["agency_name"]
                total_amt = d["total_spend"] or 0

                # HHI
                if total_amt > 0:
                    hhi_clauses = base_clauses + ["s.agency_name = ?"]
                    hhi_where = build_where(hhi_clauses)
                    hhi_row = con.execute(f"""
                        SELECT SUM(share * share) FROM (
                            SELECT CAST(SUM(COALESCE(s.amount, 0)) AS DOUBLE) / ? AS share
                            FROM spending s {hhi_where}
                            GROUP BY s.vendor_key
                        )
                    """, [total_amt] + base_params + [ag]).fetchone()
                    d["hhi"] = round((hhi_row[0] or 0) * 10000)
                else:
                    d["hhi"] = 0

                cancel_score = max(0, 100 - d["cancel_rate"] * 500)
                overrun_score = max(0, 100 - d["overrun_rate"] * 200)
                ext_score = max(0, 100 - d["extension_rate"] * 200)
                competition_score = max(0, min(100, 100 - (d["hhi"] / 100)))

                d["efficiency_score"] = round(
                    cancel_score * 0.30 + overrun_score * 0.25 +
                    ext_score * 0.20 + competition_score * 0.25, 1
                )
                d["grade"] = letter_grade(d["efficiency_score"])
                d["sub_scores"] = {
                    "cancel": round(cancel_score, 1),
                    "overrun": round(overrun_score, 1),
                    "extensions": round(ext_score, 1),
                    "competition": round(competition_score, 1),
                }
                depts.append(d)

            return jsonify({"has_rich_data": True, "results": depts})

        else:
            # Basic: spending ranking from agency_states
            clauses, params = [], []
            if filters["states"]:
                phs = ",".join(["?"] * len(filters["states"]))
                clauses.append(f"state_abbr IN ({phs})")
                params.extend(filters["states"])
            where = build_where(clauses)

            rows = fetch_dicts(con, f"""
                SELECT agency_name_clean, state_abbr,
                       SUM(transaction_count) AS transaction_count,
                       SUM(total_spend) AS total_spend,
                       SUM(vendor_count) AS vendor_count
                FROM agency_states {where}
                GROUP BY agency_name_clean, state_abbr
                ORDER BY total_spend DESC NULLS LAST
            """, params)
            return jsonify({"has_rich_data": False, "results": rows})
    finally:
        con.close()


@app.route("/api/performance/dept-vendors/<path:agency_name>")
def performance_dept_vendors(agency_name):
    """
    Vendor grades within a department if has_rich_data.
    Else: vendor spend ranking.
    """
    filters = parse_global_filters(request)
    con = get_db()
    try:
        rich = check_rich_data(con, filters["states"])

        if rich or not filters["states"]:
            rich_states = filters["states"] if (filters["states"] and rich) else ["FL"]
            phs = ",".join(["?"] * len(rich_states))
            base_clauses = [f"s.state_abbr IN ({phs})", "s.agency_name = ?"]
            base_params = list(rich_states) + [agency_name]
            if filters["year_start"]:
                base_clauses.append("s.fiscal_year >= ?")
                base_params.append(filters["year_start"])
            if filters["year_end"]:
                base_clauses.append("s.fiscal_year <= ?")
                base_params.append(filters["year_end"])
            base_where = build_where(base_clauses)

            raw = fetch_dicts(con, f"""
                SELECT
                    s.vendor_name_clean, s.vendor_key,
                    COUNT(*) AS record_count,
                    SUM(COALESCE(s.amount, 0)) AS total_val,
                    AVG(COALESCE(s.amount, 0)) AS avg_val,
                    COUNT(CASE WHEN s.status_broad='COMPLETED' THEN 1 END) AS completed,
                    COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS cancelled,
                    CAST(COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS cancel_rate,
                    COUNT(CASE WHEN s.status_broad='ACTIVE' THEN 1 END) AS active,
                    AVG(CASE WHEN s.amount_change_pct > 0 THEN s.amount_change_pct END) AS avg_overrun,
                    COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS overrun_count,
                    COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                AND s.new_end_date != s.original_end_date THEN 1 END) AS extensions,
                    CAST(COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                     AND s.new_end_date != s.original_end_date THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS ext_rate,
                    MIN(s.parsed_date) AS first_date,
                    MAX(s.parsed_date) AS last_date,
                    fvp.vendor_score, fvp.vendor_tier, fvp.years_active,
                    fvp.num_agencies_served,
                    fvp.total_records AS global_records,
                    fvp.completed_contracts AS global_completed,
                    fvp.cancellation_rate AS global_cancel_rate
                FROM spending s
                LEFT JOIN fl_vendor_profiles fvp ON s.vendor_key = fvp.vendor_name_normalized
                {base_where}
                GROUP BY s.vendor_key, s.vendor_name_clean,
                         fvp.vendor_score, fvp.vendor_tier, fvp.years_active,
                         fvp.num_agencies_served, fvp.total_records,
                         fvp.completed_contracts, fvp.cancellation_rate
                ORDER BY total_val DESC NULLS LAST
                LIMIT 50
            """, base_params)

            vendors = []
            for v in raw:
                rc = v["record_count"] or 1
                cancel_s = max(0, 100 - v["cancel_rate"] * 500)
                overrun_rate_v = (v["overrun_count"] or 0) / rc
                overrun_s = max(0, 100 - overrun_rate_v * 200)
                ext_s = max(0, 100 - v["ext_rate"] * 200)
                completion_s = (v["completed"] or 0) / rc * 100
                experience_s = min(100, (rc / 5) * 20)

                v["dept_efficiency"] = round(
                    cancel_s * 0.30 + overrun_s * 0.25 + ext_s * 0.15 +
                    min(100, completion_s) * 0.15 + experience_s * 0.15, 1
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

            return jsonify({"has_rich_data": True, "agency_name": agency_name, "results": vendors})

        else:
            # Basic: spend ranking
            clauses = ["s.agency_name = ?"]
            params = [agency_name]
            if filters["states"]:
                phs = ",".join(["?"] * len(filters["states"]))
                clauses.append(f"s.state_abbr IN ({phs})")
                params.extend(filters["states"])
            where = build_where(clauses)

            rows = fetch_dicts(con, f"""
                SELECT s.vendor_name_clean, s.vendor_key,
                       COUNT(*) AS record_count,
                       SUM(COALESCE(s.amount, 0)) AS total_val,
                       AVG(COALESCE(s.amount, 0)) AS avg_val
                FROM spending s {where}
                GROUP BY s.vendor_key, s.vendor_name_clean
                ORDER BY total_val DESC NULLS LAST
                LIMIT 50
            """, params)
            return jsonify({"has_rich_data": False, "agency_name": agency_name, "results": rows})
    finally:
        con.close()


@app.route("/api/performance/vendors")
def performance_vendors():
    """
    5-factor scoring if has_rich_data.
    Else: spend-based ranking from vendor_states.
    """
    filters = parse_global_filters(request)
    limit = min(int(request.args.get("limit", 100)), 500)
    sort = request.args.get("sort", "grade")
    grade_filter = request.args.get("grade", "")
    tier_filter = request.args.get("tier", "")
    min_records = int(request.args.get("min_records", 3))

    con = get_db()
    try:
        rich = check_rich_data(con, filters["states"])

        if rich or not filters["states"]:
            rich_states = filters["states"] if (filters["states"] and rich) else ["FL"]
            phs = ",".join(["?"] * len(rich_states))
            base_clauses = [f"s.state_abbr IN ({phs})"]
            base_params = list(rich_states)
            if filters["year_start"]:
                base_clauses.append("s.fiscal_year >= ?")
                base_params.append(filters["year_start"])
            if filters["year_end"]:
                base_clauses.append("s.fiscal_year <= ?")
                base_params.append(filters["year_end"])
            base_where = build_where(base_clauses)

            raw = fetch_dicts(con, f"""
                SELECT
                    s.vendor_name_clean, s.vendor_key,
                    COUNT(*) AS total_records,
                    SUM(COALESCE(s.amount, 0)) AS total_spend,
                    AVG(COALESCE(s.amount, 0)) AS avg_contract,
                    COUNT(DISTINCT s.agency_name) AS agencies_served,
                    COUNT(CASE WHEN s.status_broad='COMPLETED' THEN 1 END) AS completed,
                    COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS cancelled,
                    COUNT(CASE WHEN s.status_broad='ACTIVE' THEN 1 END) AS active,
                    CAST(COUNT(CASE WHEN s.status_broad='CANCELLED' THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS cancel_rate,
                    CAST(COUNT(CASE WHEN s.status_broad='COMPLETED' THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS completion_rate,
                    CAST(COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(CASE WHEN s.amount_change_pct IS NOT NULL THEN 1 END))
                        AS overrun_rate,
                    COUNT(CASE WHEN s.amount_change_pct > 10 THEN 1 END) AS overrun_count,
                    AVG(CASE WHEN s.amount_change_pct > 0 AND s.amount_change_pct <= 500
                             THEN s.amount_change_pct END) AS avg_cost_overrun_pct,
                    CAST(COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                     AND s.new_end_date != s.original_end_date THEN 1 END) AS DOUBLE)
                        / GREATEST(1, COUNT(*)) AS extension_rate,
                    COUNT(CASE WHEN s.new_end_date IS NOT NULL
                                AND s.new_end_date != s.original_end_date THEN 1 END) AS extension_count,
                    MIN(s.parsed_date) AS first_contract,
                    MAX(s.parsed_date) AS last_contract,
                    COUNT(DISTINCT s.commodity_category) AS num_commodities,
                    fvp.vendor_tier, fvp.vendor_score, fvp.years_active
                FROM spending s
                LEFT JOIN fl_vendor_profiles fvp ON s.vendor_key = fvp.vendor_name_normalized
                {base_where}
                GROUP BY s.vendor_key, s.vendor_name_clean,
                         fvp.vendor_tier, fvp.vendor_score, fvp.years_active
                HAVING COUNT(*) >= ?
                ORDER BY total_spend DESC NULLS LAST
                LIMIT 2000
            """, base_params + [min_records])

            vendors = []
            for v in raw:
                reliability = max(0, 100 - v["cancel_rate"] * 500)
                cost_control = max(0, 100 - (v["overrun_rate"] or 0) * 200)
                timeliness = max(0, 100 - v["extension_rate"] * 200)
                delivery = min(100, v["completion_rate"] * 100)
                years_a = v["years_active"] or 0
                scale = min(100,
                    v["total_records"] / 20 * 40 +
                    v["agencies_served"] / 5 * 30 +
                    years_a / 10 * 30
                )

                v["efficiency_score"] = round(
                    reliability * 0.30 + cost_control * 0.25 +
                    timeliness * 0.15 + delivery * 0.15 + scale * 0.15, 1
                )
                v["grade"] = letter_grade(v["efficiency_score"])
                v["sub_scores"] = {
                    "reliability": round(reliability, 1),
                    "cost_control": round(cost_control, 1),
                    "timeliness": round(timeliness, 1),
                    "delivery": round(delivery, 1),
                    "scale": round(scale, 1),
                }

                if grade_filter and not v["grade"].startswith(grade_filter):
                    continue
                if tier_filter and v.get("vendor_tier") != tier_filter:
                    continue
                vendors.append(v)

            if sort == "grade":
                vendors.sort(key=lambda x: x["efficiency_score"], reverse=True)
            elif sort == "grade-worst":
                vendors.sort(key=lambda x: x["efficiency_score"])
            elif sort == "spend":
                vendors.sort(key=lambda x: x["total_spend"] or 0, reverse=True)
            elif sort == "records":
                vendors.sort(key=lambda x: x["total_records"], reverse=True)
            elif sort == "cancel":
                vendors.sort(key=lambda x: x["cancel_rate"], reverse=True)
            elif sort == "overrun":
                vendors.sort(key=lambda x: x["overrun_rate"] or 0, reverse=True)

            dist = {}
            for v in vendors:
                g = v["grade"][0]
                dist[g] = dist.get(g, 0) + 1

            total = len(vendors)
            vendors = vendors[:limit]
            return jsonify({"has_rich_data": True, "results": vendors,
                            "grade_distribution": dist, "total": total})

        else:
            # Basic: spend-based ranking from vendor_states
            clauses, params = apply_filters(filters, table_alias="", year_col=None)
            if clauses:
                # vendor_states uses bare column names
                clauses_vs = []
                params_vs = []
                if filters["states"]:
                    phs = ",".join(["?"] * len(filters["states"]))
                    clauses_vs.append(f"state_abbr IN ({phs})")
                    params_vs.extend(filters["states"])
            else:
                clauses_vs, params_vs = [], []
            where = build_where(clauses_vs)

            rows = fetch_dicts(con, f"""
                SELECT vendor_name_clean, vendor_key,
                       SUM(transaction_count) AS total_records,
                       SUM(total_spend) AS total_spend,
                       AVG(avg_spend) AS avg_spend,
                       SUM(unique_agencies) AS agencies_served,
                       MIN(first_date) AS first_date,
                       MAX(last_date) AS last_date
                FROM vendor_states {where}
                GROUP BY vendor_key, vendor_name_clean
                HAVING SUM(transaction_count) >= ?
                ORDER BY total_spend DESC NULLS LAST
                LIMIT ?
            """, params_vs + [min_records, limit])

            return jsonify({"has_rich_data": False, "results": rows})
    finally:
        con.close()


# ── Multi-State endpoints ───────────────────────────────────────────────────

@app.route("/api/states/overview")
def states_overview():
    """High-level stats for all states from state_profiles."""
    con = get_db()
    try:
        rows = fetch_dicts(con, """
            SELECT state_abbr, state, records, unique_vendors, unique_agencies,
                   total_spend, median_transaction, earliest_date, latest_date,
                   quality_grade, quality_score, granularity, has_rich_data
            FROM state_profiles
            ORDER BY total_spend DESC NULLS LAST
        """)
        total_records = sum(r.get("records") or 0 for r in rows)
        total_spend = sum(r.get("total_spend") or 0 for r in rows)
        resolved_vendors = con.execute("SELECT COUNT(*) FROM vendor_xstate").fetchone()[0]
        return jsonify({
            "states": rows,
            "total_records": total_records,
            "total_spend": total_spend,
            "state_count": len(rows),
            "resolved_vendor_entities": resolved_vendors,
        })
    finally:
        con.close()


@app.route("/api/states/<state_abbr>")
def state_detail(state_abbr):
    """Detailed analytics for a single state using pre-built tables."""
    state_abbr = state_abbr.upper()
    con = get_db()
    try:
        profile = con.execute(
            "SELECT * FROM state_profiles WHERE state_abbr = ?", [state_abbr]
        ).fetchone()
        if not profile:
            return jsonify({"error": "State not found"}), 404

        cols = [desc[0] for desc in con.description]
        result = dict(zip(cols, profile))
        for k in ("earliest_date", "latest_date"):
            if result.get(k):
                result[k] = str(result[k])

        # Top vendors
        tv = con.execute("""
            SELECT vendor_name_clean, transaction_count, total_spend, unique_agencies, top_agency
            FROM vendor_states
            WHERE state_abbr = ? AND total_spend IS NOT NULL
            ORDER BY total_spend DESC NULLS LAST LIMIT 25
        """, [state_abbr]).fetchall()
        result["top_vendors"] = [
            {"name": r[0], "transactions": r[1], "total": r[2],
             "agencies": r[3], "top_agency": r[4]} for r in tv
        ]

        # Top agencies
        ta = con.execute("""
            SELECT agency_name_clean, transaction_count, total_spend, vendor_count
            FROM agency_states
            WHERE state_abbr = ? AND total_spend IS NOT NULL
            ORDER BY total_spend DESC NULLS LAST LIMIT 25
        """, [state_abbr]).fetchall()
        result["top_agencies"] = [
            {"name": r[0], "transactions": r[1], "total": r[2], "vendors": r[3]} for r in ta
        ]

        # Spend by year
        spy = con.execute("""
            SELECT fiscal_year, COUNT(*) AS cnt,
                   SUM(CASE WHEN amount > 0 THEN amount END) AS total
            FROM spending
            WHERE state_abbr = ? AND fiscal_year IS NOT NULL
              AND fiscal_year BETWEEN 2005 AND 2026
            GROUP BY fiscal_year ORDER BY fiscal_year
        """, [state_abbr]).fetchall()
        result["spend_by_year"] = [{"year": r[0], "count": r[1], "total": r[2]} for r in spy]

        # Top categories
        tc = con.execute("""
            SELECT commodity_category, COUNT(*) AS cnt,
                   SUM(CASE WHEN amount > 0 THEN amount END) AS total
            FROM spending
            WHERE state_abbr = ? AND commodity_category IS NOT NULL AND commodity_category != ''
            GROUP BY commodity_category
            ORDER BY total DESC NULLS LAST LIMIT 15
        """, [state_abbr]).fetchall()
        result["top_categories"] = [{"name": r[0], "count": r[1], "total": r[2]} for r in tc]

        return jsonify(result)
    finally:
        con.close()


@app.route("/api/states/<state_abbr>/search")
def state_search(state_abbr):
    """Search spending records within a state."""
    state_abbr = state_abbr.upper()
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 100)), 500)
    offset = int(request.args.get("offset", 0))
    agency = request.args.get("agency", "").strip()
    min_amount = request.args.get("min_amount", "").strip()
    max_amount = request.args.get("max_amount", "").strip()

    con = get_db()
    try:
        clauses = ["state_abbr = ?"]
        params = [state_abbr]

        if q:
            like = f"%{q}%"
            clauses.append(
                "(vendor_name_clean ILIKE ? OR agency_name ILIKE ? OR description ILIKE ?)"
            )
            params.extend([like, like, like])
        if agency:
            clauses.append("agency_name ILIKE ?")
            params.append(f"%{agency}%")
        if min_amount:
            clauses.append("amount >= ?")
            params.append(float(min_amount))
        if max_amount:
            clauses.append("amount <= ?")
            params.append(float(max_amount))

        where = build_where(clauses)

        rows = con.execute(f"""
            SELECT state, state_abbr, agency_name, vendor_name_clean AS vendor_name,
                   contract_id, contract_type, description, amount,
                   parsed_date AS start_date, original_end_date AS end_date,
                   procurement_method, commodity_category, source_url
            FROM spending {where}
            ORDER BY amount DESC NULLS LAST
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()

        cols = ["state", "state_abbr", "agency_name", "vendor_name", "contract_id",
                "contract_type", "description", "amount", "start_date", "end_date",
                "procurement_method", "commodity_category", "source_url"]
        results = rows_to_dicts(rows, cols)

        total = con.execute(f"SELECT COUNT(*) FROM spending {where}", params).fetchone()[0]

        return jsonify({"results": results, "total": total, "limit": limit, "offset": offset})
    finally:
        con.close()


@app.route("/api/states/cross-state/vendors")
def cross_state_vendors():
    """Find vendors operating across multiple states (entity-resolved)."""
    min_states = int(request.args.get("min_states", 3))
    limit = min(int(request.args.get("limit", 50)), 200)
    q = request.args.get("q", "").strip()

    con = get_db()
    try:
        clauses = ["num_states >= ?"]
        params = [min_states]
        if q:
            clauses.append("vendor_name ILIKE ?")
            params.append(f"%{q.upper()}%")
        where = build_where(clauses)

        rows = con.execute(f"""
            SELECT vendor_name, num_states, states_list, total_transactions,
                   total_spend, total_agencies, first_seen, last_seen
            FROM vendor_xstate {where}
            ORDER BY total_spend DESC NULLS LAST
            LIMIT ?
        """, params + [limit]).fetchall()

        results = [
            {
                "vendor": r[0], "num_states": r[1], "states": r[2],
                "transactions": r[3], "total_spend": r[4], "agencies": r[5],
                "first_seen": str(r[6]) if r[6] else None,
                "last_seen": str(r[7]) if r[7] else None,
            }
            for r in rows
        ]
        return jsonify({"results": results, "total": len(results)})
    finally:
        con.close()


@app.route("/api/states/cross-state/search")
def cross_state_search():
    """Search for a vendor across all states with entity resolution + per-state breakdown."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Missing ?q= parameter"}), 400

    con = get_db()
    try:
        vendors = con.execute("""
            SELECT vendor_name, num_states, states_list, total_spend,
                   total_transactions, vendor_key
            FROM vendor_xstate
            WHERE vendor_name ILIKE ?
            ORDER BY total_spend DESC NULLS LAST
            LIMIT 20
        """, [f"%{q.upper()}%"]).fetchall()

        results = []
        for v in vendors:
            breakdown = con.execute("""
                SELECT state_abbr, transaction_count, total_spend, top_agency
                FROM vendor_states WHERE vendor_key = ?
                ORDER BY total_spend DESC NULLS LAST
            """, [v[5]]).fetchall()

            results.append({
                "vendor": v[0], "num_states": v[1], "states": v[2],
                "total_spend": v[3], "transactions": v[4],
                "state_breakdown": [
                    {"state": r[0], "transactions": r[1],
                     "spend": r[2], "top_agency": r[3]}
                    for r in breakdown
                ],
            })

        return jsonify({"results": results, "query": q})
    finally:
        con.close()


@app.route("/api/states/compare")
def states_compare():
    """Side-by-side comparison of up to 4 states."""
    state_list = request.args.get("states", "").upper().split(",")
    state_list = [s.strip() for s in state_list if s.strip()][:4]
    if not state_list:
        return jsonify({"error": "Missing ?states=CA,TX,NY parameter"}), 400

    con = get_db()
    try:
        phs = ",".join(["?"] * len(state_list))

        profiles = con.execute(
            f"SELECT * FROM state_profiles WHERE state_abbr IN ({phs})", state_list
        ).fetchall()
        pcols = [desc[0] for desc in con.description]
        states = []
        for p in profiles:
            d = dict(zip(pcols, p))
            for k in ("earliest_date", "latest_date"):
                if d.get(k):
                    d[k] = str(d[k])
            states.append(d)

        # Shared vendors across selected states
        shared = con.execute(f"""
            SELECT vendor_name, num_states, states_list, total_spend
            FROM vendor_xstate
            WHERE num_states >= 2
              AND list_has_any(states_list, ?)
            ORDER BY total_spend DESC NULLS LAST
            LIMIT 20
        """, [state_list]).fetchall()

        # Spend by year per state
        yearly = con.execute(f"""
            SELECT state_abbr, fiscal_year,
                   SUM(CASE WHEN amount > 0 THEN amount END) AS total
            FROM spending
            WHERE state_abbr IN ({phs})
              AND fiscal_year BETWEEN 2010 AND 2026
            GROUP BY state_abbr, fiscal_year
            ORDER BY state_abbr, fiscal_year
        """, state_list).fetchall()

        yearly_by_state = {}
        for abbr, yr, total in yearly:
            yearly_by_state.setdefault(abbr, []).append({"year": yr, "total": total})

        return jsonify({
            "states": states,
            "shared_vendors": [
                {"vendor": r[0], "num_states": r[1], "states": r[2], "total_spend": r[3]}
                for r in shared
            ],
            "spend_by_year": yearly_by_state,
        })
    finally:
        con.close()


@app.route("/api/states/analytics/top-vendors-national")
def top_vendors_national():
    """Top vendors nationwide (entity-resolved) from vendor_xstate."""
    limit = min(int(request.args.get("limit", 50)), 200)
    con = get_db()
    try:
        rows = con.execute("""
            SELECT vendor_name, num_states, states_list, total_spend,
                   total_transactions, total_agencies
            FROM vendor_xstate
            WHERE total_spend IS NOT NULL
            ORDER BY total_spend DESC NULLS LAST
            LIMIT ?
        """, [limit]).fetchall()
        results = [
            {"vendor": r[0], "num_states": r[1], "states": r[2],
             "total_spend": r[3], "transactions": r[4], "agencies": r[5]}
            for r in rows
        ]
        return jsonify({"results": results})
    finally:
        con.close()


@app.route("/api/states/sql")
def states_sql():
    """Execute read-only SQL against the spending database (max 1000 rows)."""
    sql = request.args.get("q", "").strip()
    if not sql:
        return jsonify({"error": "Missing ?q= parameter"}), 400

    forbidden = {"insert", "update", "delete", "drop", "alter", "create",
                 "attach", "copy", "export", "truncate"}
    if set(sql.lower().split()) & forbidden:
        return jsonify({"error": "Write operations not allowed"}), 403

    con = get_db()
    try:
        res = con.execute(sql)
        cols = [desc[0] for desc in res.description]
        rows = rows_to_dicts(res.fetchmany(1000), cols)
        return jsonify({"columns": cols, "rows": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    finally:
        con.close()


# ── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if DB_FILE.exists():
        size_gb = DB_FILE.stat().st_size / 1e9
        print(f"DuckDB: {DB_FILE} ({size_gb:.2f} GB)")
    else:
        print(f"WARNING: Database not found at {DB_FILE}")
    app.run(debug=True, port=5111, threaded=True)
