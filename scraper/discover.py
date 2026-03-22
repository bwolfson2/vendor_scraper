#!/usr/bin/env python3
"""
Dataset discovery tool for Socrata-powered state portals.
Searches the Socrata catalog API and domain-level APIs to find spending datasets.
"""

import json
import sys
import requests

# Known Socrata domains for state transparency portals
SOCRATA_DOMAINS = {
    "CO": "data.colorado.gov",
    "CT": "openbudget.ct.gov",
    "IA": "checkbook.iowa.gov",
    "MD": "spending.maryland.gov",
    "MA": "cthru.data.socrata.com",
    "NV": "checkbook.nv.gov",
    "NY": "data.ny.gov",
    "SD": "open.sd.gov",
    "WA": "data.wa.gov",
    "AK": "checkbook.alaska.gov",
}

# Alternative domains to try
ALT_DOMAINS = {
    "MD": ["mtp.maryland.gov", "maryland-dbm.budget.socrata.com", "vendorpayments.maryland.gov"],
    "MA": ["cthruspending.mass.gov"],
    "NY": ["wwe2.osc.state.ny.us"],
    "SD": ["southdakota.spending.socrata.com", "southdakota.budget.socrata.com"],
}

SEARCH_TERMS = ["expenditure", "spending", "contract", "vendor payment", "checkbook"]

session = requests.Session()
session.headers["User-Agent"] = "StateSpendingScraper/1.0 (transparency research)"


def search_catalog(domain: str, query: str = "expenditure", limit: int = 10) -> list:
    """Search the Socrata catalog API."""
    url = f"https://api.us.socrata.com/api/catalog/v1"
    params = {
        "domains": domain,
        "search_context": domain,
        "q": query,
        "limit": limit,
    }
    try:
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        return []


def search_domain_api(domain: str, query: str = "expenditure", limit: int = 10) -> list:
    """Search a domain's own discovery API."""
    url = f"https://{domain}/api/search/views.json"
    params = {"q": query, "limit": limit}
    try:
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception:
        pass

    # Try alternate endpoint
    url = f"https://{domain}/api/views.json"
    params = {"limit": limit}
    try:
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def get_dataset_info(domain: str, dataset_id: str) -> dict:
    """Get metadata for a specific dataset."""
    url = f"https://{domain}/api/views/{dataset_id}.json"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        meta = resp.json()
        columns = [
            {"name": c.get("fieldName"), "label": c.get("name"), "type": c.get("dataTypeName")}
            for c in meta.get("columns", [])
        ]
        return {
            "id": meta.get("id"),
            "name": meta.get("name"),
            "description": meta.get("description", "")[:200],
            "row_count": meta.get("rowCount"),
            "columns": columns,
        }
    except Exception as e:
        return {"error": str(e)}


def discover_state(state_abbr: str):
    """Run full discovery for a state."""
    domain = SOCRATA_DOMAINS.get(state_abbr)
    if not domain:
        print(f"No known Socrata domain for {state_abbr}")
        return

    print(f"\n{'='*60}")
    print(f"Discovering datasets for {state_abbr} ({domain})")
    print(f"{'='*60}")

    # Try catalog API with various search terms
    all_results = []
    for term in SEARCH_TERMS:
        results = search_catalog(domain, term)
        for r in results:
            res = r.get("resource", {})
            entry = {
                "id": res.get("id"),
                "name": res.get("name"),
                "type": res.get("type"),
                "description": res.get("description", "")[:100],
                "columns": res.get("columns_name", [])[:10],
            }
            if entry["id"] and entry not in all_results:
                all_results.append(entry)

    # Try domain API
    domain_results = search_domain_api(domain)
    for r in domain_results:
        if isinstance(r, dict):
            entry = {
                "id": r.get("id"),
                "name": r.get("name"),
                "type": r.get("type", "dataset"),
                "description": r.get("description", "")[:100],
                "columns": [c.get("fieldName", "") for c in r.get("columns", [])][:10],
            }
            if entry["id"] and entry not in all_results:
                all_results.append(entry)

    # Try alternate domains
    for alt_domain in ALT_DOMAINS.get(state_abbr, []):
        for term in SEARCH_TERMS[:2]:
            results = search_catalog(alt_domain, term)
            for r in results:
                res = r.get("resource", {})
                entry = {
                    "id": res.get("id"),
                    "name": res.get("name"),
                    "type": res.get("type"),
                    "description": res.get("description", "")[:100],
                    "columns": res.get("columns_name", [])[:10],
                    "domain": alt_domain,
                }
                if entry["id"] and entry not in all_results:
                    all_results.append(entry)

    if all_results:
        print(f"\nFound {len(all_results)} datasets:")
        for i, ds in enumerate(all_results, 1):
            print(f"\n  [{i}] {ds['name']}")
            print(f"      ID: {ds['id']}")
            print(f"      Type: {ds.get('type', 'unknown')}")
            if ds.get("domain"):
                print(f"      Domain: {ds['domain']}")
            if ds.get("columns"):
                print(f"      Columns: {', '.join(ds['columns'][:8])}")
    else:
        print(f"\nNo datasets found via API for {domain}")
        print("This portal may use Socrata frontend but not expose catalog API.")
        print(f"Try visiting https://{domain} and inspecting network requests.")

    return all_results


if __name__ == "__main__":
    states = sys.argv[1:] if len(sys.argv) > 1 else list(SOCRATA_DOMAINS.keys())
    all_discoveries = {}

    for state in states:
        state = state.upper()
        results = discover_state(state)
        if results:
            all_discoveries[state] = results

    # Write results
    output_path = "scraper/config/socrata_datasets.json"
    with open(output_path, "w") as f:
        json.dump(all_discoveries, f, indent=2)
    print(f"\n\nResults saved to {output_path}")
