#!/usr/bin/env python3
"""
Deep Playwright scraper for browser-only state portals.
Each state gets custom logic to extract vendor/spending data.
"""

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_FIELDS = [
    "state", "state_abbr", "agency_name", "vendor_name", "contract_id",
    "contract_type", "description", "amount", "start_date", "end_date",
    "procurement_method", "commodity_category", "source_url",
]


def clean_amount(val):
    if not val:
        return ""
    try:
        cleaned = str(val).replace("$", "").replace(",", "").replace(" ", "").strip()
        if not cleaned or cleaned == "-" or cleaned == "None":
            return ""
        return str(float(cleaned))
    except (ValueError, TypeError):
        return ""


def write_records(records, state_abbr, state_name):
    """Write records to CSV output file."""
    outdir = f"scraper/output/{state_abbr.lower()}"
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"{state_abbr.lower()}_contracts.csv")

    with open(outpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SCHEMA_FIELDS)
        writer.writeheader()
        count = 0
        for rec in records:
            rec["state"] = state_name
            rec["state_abbr"] = state_abbr
            writer.writerow(rec)
            count += 1

    logger.info(f"[{state_abbr}] Wrote {count:,} records to {outpath}")
    return count


async def scrape_hi(page):
    """Hawaii - OpenGov API with browser cookies."""
    url = "https://hawaii.opengov.com/transparency/#/33/accountType=expenses&embed=n&breakdown=types&currentYearAmount=cumulative&currentYearPeriod=years&graph=bar&legendSort=desc&pr498State=true&saved_view=105&selection=DC13EC5B8D2E0EB33C1E4023DACBFCB0&projections=null&projectionType=null&highlighting=null&highlightingVariance=null&year=2024&selectedDataSetIndex=null&fiscal_start=earliest&fiscal_end=latest"
    await page.goto(url, wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(5000)

    # Try to find and click "View as table" or similar
    records = []
    # Use browser context to call OpenGov API
    data = await page.evaluate("""
        async () => {
            try {
                const resp = await fetch('/api/transparency/v1/hawaii/data/expenses?fiscal_start=earliest&fiscal_end=latest&limit=50000');
                if (resp.ok) return await resp.json();
            } catch(e) {}
            // Try alternative endpoints
            try {
                const resp = await fetch('/api/transparency/v1/package/8816bf25-097b-4377-84ed-af952f497fe3');
                if (resp.ok) return await resp.json();
            } catch(e) {}
            return null;
        }
    """)

    if data:
        logger.info(f"[HI] Got API data: {type(data)}, keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
        # Parse based on structure
        if isinstance(data, dict):
            items = data.get("data", data.get("results", data.get("rows", [])))
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        records.append({
                            "agency_name": item.get("department", item.get("agency", "")),
                            "vendor_name": item.get("vendor", item.get("name", "")),
                            "contract_id": item.get("id", ""),
                            "description": item.get("description", item.get("category", "")),
                            "amount": clean_amount(item.get("amount", item.get("total", ""))),
                            "start_date": item.get("date", ""),
                            "end_date": "",
                            "contract_type": "",
                            "procurement_method": "",
                            "commodity_category": "",
                            "source_url": url,
                        })
    return records


async def scrape_oh(page):
    """Ohio - Checkbook with WAF, use search API from browser context."""
    await page.goto("https://checkbook.ohio.gov/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(5000)

    # Try to use the internal search API
    records = []
    data = await page.evaluate("""
        async () => {
            try {
                const resp = await fetch('/WebServices/BudgetSearch.ashx', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                    body: 'SearchType=Expenditures&FiscalYear=2024&PageSize=10000&PageNumber=1'
                });
                if (resp.ok) {
                    const text = await resp.text();
                    try { return JSON.parse(text); } catch(e) { return {raw: text.substring(0, 5000)}; }
                }
                return {status: resp.status};
            } catch(e) {
                return {error: e.message};
            }
        }
    """)
    if data:
        logger.info(f"[OH] API response: {str(data)[:500]}")
    return records


async def scrape_az(page):
    """Arizona - OpenBooks portal."""
    await page.goto("https://openbooks.az.gov/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(5000)

    intercepted = []
    page.on("response", lambda resp: intercepted.append(resp) if "json" in resp.headers.get("content-type", "") and resp.status == 200 else None)

    # Navigate to payments/expenditures
    try:
        await page.click("text=Payments", timeout=5000)
        await page.wait_for_timeout(3000)
    except:
        try:
            await page.click("text=Expenditures", timeout=5000)
            await page.wait_for_timeout(3000)
        except:
            pass

    records = []
    for resp in intercepted:
        try:
            body = await resp.json()
            if isinstance(body, list) and len(body) > 10:
                logger.info(f"[AZ] Found JSON list with {len(body)} items")
                for item in body:
                    if isinstance(item, dict) and ("vendor" in str(item).lower() or "amount" in str(item).lower()):
                        records.append({
                            "agency_name": item.get("agency", item.get("Agency", "")),
                            "vendor_name": item.get("vendor", item.get("Vendor", item.get("payee", ""))),
                            "contract_id": "",
                            "description": item.get("description", item.get("Description", "")),
                            "amount": clean_amount(item.get("amount", item.get("Amount", ""))),
                            "start_date": item.get("date", item.get("Date", "")),
                            "end_date": "",
                            "contract_type": "",
                            "procurement_method": "",
                            "commodity_category": "",
                            "source_url": "https://openbooks.az.gov/",
                        })
        except:
            pass
    return records


async def scrape_wi(page):
    """Wisconsin - OpenBook with ExportWebService."""
    await page.goto("https://openbook.wi.gov/Expenditures.aspx", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(5000)

    records = []
    # Try export via AJAX
    data = await page.evaluate("""
        async () => {
            try {
                // Try calling the export service
                const resp = await fetch('default.aspx', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                    body: 'ExportWebService=true&UserControl=Expenditures&ExportType=CSV&FiscalYear=2024&SearchType=&SearchString=&CategoryCode=&AgencyCode=&ObjectCode=&ProviderCode=&FundCode=&SortField=Amount&SortAscending=false&SuppressAmountColumns=false'
                });
                if (resp.ok) {
                    const text = await resp.text();
                    return {status: resp.status, contentType: resp.headers.get('content-type'), data: text.substring(0, 5000), size: text.length};
                }
                return {status: resp.status};
            } catch(e) {
                return {error: e.message};
            }
        }
    """)
    if data:
        logger.info(f"[WI] Export response: status={data.get('status')}, type={data.get('contentType')}, size={data.get('size')}")
        if data.get("data", "").startswith('"') or "," in data.get("data", "")[:100]:
            logger.info(f"[WI] CSV data! First 300: {data['data'][:300]}")
    return records


async def scrape_fl(page):
    """Florida - FACTS contract search with postback."""
    await page.goto("https://facts.fldfs.com/Search/ContractSearch.aspx", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(3000)

    records = []
    intercepted_responses = []

    async def capture(response):
        if response.status == 200:
            ct = response.headers.get("content-type", "")
            if "json" in ct or "csv" in ct or "excel" in ct:
                try:
                    body = await response.text()
                    intercepted_responses.append({"url": response.url, "body": body[:10000], "size": len(body)})
                except:
                    pass

    page.on("response", capture)

    # Click Search button without any filters to get all results
    try:
        await page.click("input[value='Search']", timeout=5000)
        await page.wait_for_timeout(5000)
    except:
        try:
            await page.click("#ctl00_PC_btnSearch", timeout=5000)
            await page.wait_for_timeout(5000)
        except:
            pass

    # Check for results table
    rows = await page.query_selector_all("table.gridview tr, table#ctl00_PC_grdContract tr, .SearchGrid tr")
    if rows:
        logger.info(f"[FL] Found {len(rows)} table rows")
        for row in rows[1:]:  # Skip header
            cells = await row.query_selector_all("td")
            if len(cells) >= 5:
                values = []
                for cell in cells:
                    text = await cell.inner_text()
                    values.append(text.strip())
                if values:
                    records.append({
                        "agency_name": values[0] if len(values) > 0 else "",
                        "vendor_name": values[1] if len(values) > 1 else "",
                        "contract_id": values[2] if len(values) > 2 else "",
                        "description": values[3] if len(values) > 3 else "",
                        "amount": clean_amount(values[4] if len(values) > 4 else ""),
                        "start_date": values[5] if len(values) > 5 else "",
                        "end_date": values[6] if len(values) > 6 else "",
                        "contract_type": "",
                        "procurement_method": "",
                        "commodity_category": "",
                        "source_url": "https://facts.fldfs.com/Search/ContractSearch.aspx",
                    })

    for resp_data in intercepted_responses:
        logger.info(f"[FL] Intercepted: {resp_data['url'][:80]} ({resp_data['size']} bytes)")

    return records


async def scrape_ga(page):
    """Georgia - open.ga.gov expenditures with disclaimer."""
    await page.goto("https://open.ga.gov/openga/expenditures/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(3000)

    # Accept disclaimer if present
    try:
        await page.click("text=Agree", timeout=3000)
        await page.wait_for_timeout(2000)
    except:
        try:
            await page.click("text=Accept", timeout=3000)
            await page.wait_for_timeout(2000)
        except:
            pass

    records = []
    intercepted = []

    async def capture(response):
        if response.status == 200 and "json" in response.headers.get("content-type", ""):
            try:
                body = await response.text()
                if len(body) > 500:
                    intercepted.append({"url": response.url, "body": body, "size": len(body)})
            except:
                pass

    page.on("response", capture)

    # Navigate to vendor list
    try:
        await page.click("text=Vendors", timeout=5000)
        await page.wait_for_timeout(5000)
    except:
        try:
            await page.click("a[href*='vendor']", timeout=5000)
            await page.wait_for_timeout(5000)
        except:
            pass

    for resp_data in intercepted:
        logger.info(f"[GA] JSON: {resp_data['url'][:80]} ({resp_data['size']} bytes)")
        try:
            data = json.loads(resp_data["body"])
            if isinstance(data, list):
                for item in data[:100]:
                    if isinstance(item, dict):
                        records.append({
                            "agency_name": item.get("agency", item.get("agencyName", "")),
                            "vendor_name": item.get("vendor", item.get("vendorName", item.get("name", ""))),
                            "contract_id": "",
                            "description": "",
                            "amount": clean_amount(item.get("amount", item.get("totalAmount", ""))),
                            "start_date": "",
                            "end_date": "",
                            "contract_type": "",
                            "procurement_method": "",
                            "commodity_category": "",
                            "source_url": "https://open.ga.gov/openga/expenditures/",
                        })
        except:
            pass

    return records


async def scrape_ar(page):
    """Arkansas - transparency.arkansas.gov with search form."""
    await page.goto("https://transparency.arkansas.gov/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(3000)

    records = []
    intercepted = []

    async def capture(response):
        ct = response.headers.get("content-type", "")
        if response.status == 200 and ("json" in ct or "csv" in ct):
            try:
                body = await response.text()
                if len(body) > 500:
                    intercepted.append({"url": response.url, "body": body[:50000], "size": len(body), "ct": ct})
            except:
                pass

    page.on("response", capture)

    # Try vendor tab
    try:
        await page.click("text=Vendors", timeout=5000)
        await page.wait_for_timeout(3000)
    except:
        try:
            await page.click("text=Expenditures", timeout=5000)
            await page.wait_for_timeout(3000)
        except:
            pass

    # Try searching with blank/wildcard
    try:
        search_input = await page.query_selector("input[type='text']")
        if search_input:
            await search_input.fill("a")
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)
    except:
        pass

    for resp_data in intercepted:
        logger.info(f"[AR] Data: {resp_data['url'][:80]} ({resp_data['size']} bytes, {resp_data['ct']})")

    return records


async def scrape_la(page):
    """Louisiana - checkbook.la.gov (ColdFusion)."""
    await page.goto("https://checkbook.la.gov/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(5000)

    records = []
    intercepted = []

    async def capture(response):
        ct = response.headers.get("content-type", "")
        if response.status == 200 and ("json" in ct):
            try:
                body = await response.text()
                if len(body) > 1000:
                    intercepted.append({"url": response.url, "body": body[:50000], "size": len(body)})
            except:
                pass

    page.on("response", capture)

    # Also try the expenditure dashboard
    await page.goto("https://expendituredashboard.doa.la.gov/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(5000)

    for resp_data in intercepted:
        logger.info(f"[LA] JSON: {resp_data['url'][:100]} ({resp_data['size']} bytes)")
        logger.info(f"[LA] Sample: {resp_data['body'][:300]}")

    return records


async def scrape_mo(page):
    """Missouri - MAP portal."""
    await page.goto("https://mapyourtaxes.mo.gov/MAP/Expenditures/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(5000)

    records = []
    intercepted = []

    async def capture(response):
        ct = response.headers.get("content-type", "")
        if response.status == 200 and ("json" in ct or "csv" in ct):
            try:
                body = await response.text()
                if len(body) > 500:
                    intercepted.append({"url": response.url, "body": body[:50000], "size": len(body)})
            except:
                pass

    page.on("response", capture)

    # Try search
    try:
        await page.click("text=Search", timeout=5000)
        await page.wait_for_timeout(5000)
    except:
        pass

    for resp_data in intercepted:
        logger.info(f"[MO] Data: {resp_data['url'][:80]} ({resp_data['size']} bytes)")

    return records


async def scrape_wy(page):
    """Wyoming - wyopen.gov with search form and CSV download."""
    await page.goto("https://www.wyopen.gov/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(3000)

    records = []

    # Fill search form with date range
    try:
        start_input = await page.query_selector("#startDate, input[name*='start'], input[placeholder*='start']")
        end_input = await page.query_selector("#endDate, input[name*='end'], input[placeholder*='end']")
        if start_input and end_input:
            await start_input.fill("07/01/2023")
            await end_input.fill("06/30/2024")
            await page.click("button[type='submit'], input[type='submit'], text=Search")
            await page.wait_for_timeout(5000)

            # Try CSV download
            csv_link = await page.query_selector("a[href*='csv'], a:has-text('CSV'), a:has-text('Download')")
            if csv_link:
                href = await csv_link.get_attribute("href")
                logger.info(f"[WY] CSV link: {href}")
    except Exception as e:
        logger.warning(f"[WY] Error: {e}")

    return records


async def scrape_id(page):
    """Idaho - transparent.idaho.gov."""
    await page.goto("https://transparent.idaho.gov/", wait_until="networkidle", timeout=60000)
    await page.wait_for_timeout(3000)

    records = []
    intercepted = []

    async def capture(response):
        ct = response.headers.get("content-type", "")
        if response.status == 200 and "json" in ct:
            try:
                body = await response.text()
                if len(body) > 1000:
                    intercepted.append({"url": response.url, "body": body[:50000], "size": len(body)})
            except:
                pass

    page.on("response", capture)

    # Navigate to vendor/expenditure data
    try:
        for text in ["Vendor", "Expenditure", "Spending", "Payment"]:
            links = await page.query_selector_all(f"a:has-text('{text}')")
            if links:
                await links[0].click()
                await page.wait_for_timeout(5000)
                break
    except:
        pass

    for resp_data in intercepted:
        logger.info(f"[ID] JSON: {resp_data['url'][:100]} ({resp_data['size']} bytes)")

    return records


# Map state abbreviations to scraper functions
SCRAPERS = {
    "HI": scrape_hi,
    "OH": scrape_oh,
    "AZ": scrape_az,
    "WI": scrape_wi,
    "FL": scrape_fl,
    "GA": scrape_ga,
    "AR": scrape_ar,
    "LA": scrape_la,
    "MO": scrape_mo,
    "WY": scrape_wy,
    "ID": scrape_id,
}

STATE_NAMES = {
    "HI": "Hawaii", "OH": "Ohio", "AZ": "Arizona", "WI": "Wisconsin",
    "FL": "Florida", "GA": "Georgia", "AR": "Arkansas", "LA": "Louisiana",
    "MO": "Missouri", "WY": "Wyoming", "ID": "Idaho",
}


async def main(states_to_scrape):
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for abbr in states_to_scrape:
            if abbr not in SCRAPERS:
                logger.warning(f"[{abbr}] No scraper implemented")
                continue

            logger.info(f"[{abbr}] Starting {STATE_NAMES.get(abbr, abbr)}...")
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = await context.new_page()

            try:
                records = await SCRAPERS[abbr](page)
                if records:
                    count = write_records(records, abbr, STATE_NAMES.get(abbr, abbr))
                    logger.info(f"[{abbr}] SUCCESS: {count:,} records")
                else:
                    logger.info(f"[{abbr}] No records extracted (may need manual investigation)")
            except Exception as e:
                logger.error(f"[{abbr}] Failed: {e}")
            finally:
                await context.close()

        await browser.close()


if __name__ == "__main__":
    states = sys.argv[1:] if len(sys.argv) > 1 else list(SCRAPERS.keys())
    asyncio.run(main(states))
