#!/usr/bin/env python3
"""
Probe remaining state portals with Playwright to find and download vendor data.
Tries multiple strategies: network intercept, export buttons, CSV downloads.
"""

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_FIELDS = [
    "state", "state_abbr", "agency_name", "vendor_name", "contract_id",
    "contract_type", "description", "amount", "start_date", "end_date",
    "procurement_method", "commodity_category", "source_url",
]

# State portal configs - URL + strategy hints
STATES = {
    "AL": {
        "name": "Alabama", "url": "https://open.alabama.gov/spending_checkbook.aspx",
        "strategy": "aspnet_export",
    },
    "AR": {
        "name": "Arkansas", "url": "https://transparency.arkansas.gov/",
        "strategy": "network_intercept",
    },
    "AZ": {
        "name": "Arizona", "url": "https://openbooks.az.gov/",
        "strategy": "network_intercept",
    },
    "DC": {
        "name": "District of Columbia", "url": "https://contracts.ocp.dc.gov/",
        "strategy": "network_intercept",
    },
    "FL": {
        "name": "Florida", "url": "https://facts.fldfs.com/Search/ContractSearch.aspx",
        "strategy": "aspnet_export",
    },
    "GA": {
        "name": "Georgia", "url": "https://open.ga.gov/openga/expenditures/",
        "strategy": "network_intercept",
    },
    "HI": {
        "name": "Hawaii", "url": "https://hawaii.opengov.com/transparency/#/33/accountType=expenses&embed=n&breakdown=types&currentYearAmount=cumulative&currentYearPeriod=years&graph=bar&legendSort=desc&pr498State=true&saved_view=105&selection=DC13EC5B8D2E0EB33C1E4023DACBFCB0&projections=null&projectionType=null&highlighting=null&highlightingVariance=null&year=2024&selectedDataSetIndex=null&fiscal_start=earliest&fiscal_end=latest",
        "strategy": "network_intercept",
    },
    "ID": {
        "name": "Idaho", "url": "https://transparent.idaho.gov/",
        "strategy": "network_intercept",
    },
    "IL": {
        "name": "Illinois", "url": "https://ledger.illinoiscomptroller.gov/",
        "strategy": "network_intercept",
    },
    "KY": {
        "name": "Kentucky", "url": "https://transparency.ky.gov/Pages/default.aspx",
        "strategy": "aspnet_export",
    },
    "LA": {
        "name": "Louisiana", "url": "https://checkbook.la.gov/",
        "strategy": "network_intercept",
    },
    "MI": {
        "name": "Michigan", "url": "https://sigma.michigan.gov/EI360TransparencyApp/jsp/home",
        "strategy": "network_intercept",
    },
    "MN": {
        "name": "Minnesota", "url": "https://mn.gov/mmb/transparency-mn/",
        "strategy": "network_intercept",
    },
    "MO": {
        "name": "Missouri", "url": "https://mapyourtaxes.mo.gov/MAP/Expenditures/SearchExpenses.aspx",
        "strategy": "aspnet_export",
    },
    "MS": {
        "name": "Mississippi", "url": "https://www.transparency.ms.gov/checkbook/checkbook.aspx",
        "strategy": "aspnet_export",
    },
    "MT": {
        "name": "Montana", "url": "https://dataportal.mt.gov/t/DOA/views/BudgetTransparencyData/Expenditures",
        "strategy": "tableau",
    },
    "NE": {
        "name": "Nebraska", "url": "https://statespending.nebraska.gov/",
        "strategy": "aspnet_export",
    },
    "NH": {
        "name": "New Hampshire", "url": "https://www.nh.gov/transparentnh/",
        "strategy": "network_intercept",
    },
    "NM": {
        "name": "New Mexico", "url": "https://sunshineportalnm.com/Spending",
        "strategy": "network_intercept",
    },
    "OH": {
        "name": "Ohio", "url": "https://checkbook.ohio.gov/",
        "strategy": "network_intercept",
    },
    "PA": {
        "name": "Pennsylvania", "url": "https://www.patreasury.gov/openbookpa/e-library/",
        "strategy": "aspnet_export",
    },
    "RI": {
        "name": "Rhode Island", "url": "https://www.ri.gov/opengovernment/",
        "strategy": "network_intercept",
    },
    "SC": {
        "name": "South Carolina", "url": "https://cg.sc.gov/fiscal-transparency/spending-transparency",
        "strategy": "network_intercept",
    },
    "TN": {
        "name": "Tennessee", "url": "https://tn.gov/transparenttn/state-financial-overview/open-ecd-expenditures.html",
        "strategy": "network_intercept",
    },
    "UT": {
        "name": "Utah", "url": "https://spending.utah.gov/",
        "strategy": "network_intercept",
    },
    "WI": {
        "name": "Wisconsin", "url": "https://openbook.wi.gov/Expenditures.aspx",
        "strategy": "aspnet_export",
    },
    "WV": {
        "name": "West Virginia", "url": "https://www.wvcheckbook.gov/",
        "strategy": "opengov",
    },
    "WY": {
        "name": "Wyoming", "url": "https://www.wyopen.gov/",
        "strategy": "network_intercept",
    },
}


async def probe_state(state_abbr, config):
    """Probe a single state portal with Playwright."""
    from playwright.async_api import async_playwright

    name = config["name"]
    url = config["url"]
    strategy = config["strategy"]

    logger.info(f"[{state_abbr}] Probing {name}: {url}")

    intercepted_data = []
    json_urls = []
    csv_urls = []
    download_paths = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Intercept all responses
        async def handle_response(response):
            ct = response.headers.get("content-type", "")
            url_str = response.url

            if "json" in ct and response.status == 200:
                try:
                    body = await response.text()
                    size = len(body)
                    if size > 500:  # Skip tiny JSON responses
                        json_urls.append({"url": url_str, "size": size, "ct": ct})
                        if size < 5_000_000:  # Don't store huge responses in memory
                            intercepted_data.append({"url": url_str, "data": body[:10000], "size": size})
                except:
                    pass
            elif "csv" in ct or "excel" in ct or "spreadsheet" in ct:
                csv_urls.append({"url": url_str, "ct": ct})

        page.on("response", handle_response)

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
        except Exception as e:
            logger.warning(f"[{state_abbr}] Page load issue: {str(e)[:80]}")
            try:
                await page.wait_for_timeout(5000)
            except:
                pass

        # Get page title and check for common elements
        try:
            title = await page.title()
        except:
            title = "unknown"

        # Try clicking common navigation/data elements
        try:
            # Look for vendor/expenditure/spending links
            for selector in [
                "a:has-text('Vendor')", "a:has-text('Expenditure')",
                "a:has-text('Spending')", "a:has-text('Payment')",
                "a:has-text('Search')", "a:has-text('Download')",
                "a:has-text('Export')", "button:has-text('Search')",
            ]:
                elements = await page.query_selector_all(selector)
                if elements:
                    try:
                        await elements[0].click()
                        await page.wait_for_timeout(3000)
                        break
                    except:
                        continue
        except:
            pass

        # Look for download/export buttons
        try:
            for selector in [
                "a:has-text('Download')", "a:has-text('Export')",
                "a:has-text('CSV')", "a:has-text('Excel')",
                "button:has-text('Export')", "button:has-text('Download')",
                "[href*='csv']", "[href*='download']", "[href*='export']",
            ]:
                elements = await page.query_selector_all(selector)
                if elements:
                    for el in elements:
                        href = await el.get_attribute("href")
                        if href:
                            csv_urls.append({"url": href, "ct": "link"})
        except:
            pass

        await browser.close()

    # Report findings
    result = {
        "state": state_abbr,
        "name": name,
        "url": url,
        "title": title,
        "json_endpoints": len(json_urls),
        "csv_endpoints": len(csv_urls),
        "data_intercepted": len(intercepted_data),
        "top_json": sorted(json_urls, key=lambda x: x["size"], reverse=True)[:3],
        "csv_links": csv_urls[:5],
        "data_samples": [{
            "url": d["url"],
            "size": d["size"],
            "sample": d["data"][:500]
        } for d in intercepted_data[:3]],
    }

    return result


async def main(states_to_probe):
    results = {}
    for abbr in states_to_probe:
        if abbr in STATES:
            try:
                result = await probe_state(abbr, STATES[abbr])
                results[abbr] = result

                # Print summary
                r = result
                json_count = r["json_endpoints"]
                csv_count = r["csv_endpoints"]
                logger.info(f"[{abbr}] Title: {r['title'][:50]}")
                logger.info(f"[{abbr}] JSON endpoints: {json_count}, CSV links: {csv_count}")

                if r["top_json"]:
                    for j in r["top_json"]:
                        logger.info(f"[{abbr}]   JSON: {j['url'][:100]} ({j['size']:,} bytes)")

                if r["csv_links"]:
                    for c in r["csv_links"]:
                        logger.info(f"[{abbr}]   CSV: {c['url'][:100]}")

                if r["data_samples"]:
                    for d in r["data_samples"][:1]:
                        logger.info(f"[{abbr}]   Data sample ({d['size']:,} bytes): {d['sample'][:200]}")

                logger.info("")
            except Exception as e:
                logger.error(f"[{abbr}] Failed: {e}")

    # Save results
    with open("scraper/output/probe_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"\nSaved probe results for {len(results)} states")

    # Summary
    logger.info("\n=== SUMMARY ===")
    for abbr, r in results.items():
        has_data = "YES" if r["json_endpoints"] > 0 or r["csv_endpoints"] > 0 else "NO"
        logger.info(f"  {abbr}: {has_data} (JSON={r['json_endpoints']}, CSV={r['csv_endpoints']}) - {r['title'][:40]}")


if __name__ == "__main__":
    states = sys.argv[1:] if len(sys.argv) > 1 else list(STATES.keys())
    asyncio.run(main(states))
