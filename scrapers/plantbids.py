import asyncio
from playwright.async_api import async_playwright
import os

AUTH_TOKEN = os.getenv("PLANETBIDS_AUTH_TOKEN")
VENDOR_ID = os.getenv("PLANETBIDS_VENDOR_ID")
VENDOR_LOGIN_ID = os.getenv("PLANETBIDS_VENDOR_LOGIN_ID")
VISIT_ID = os.getenv("PLANETBIDS_VISIT_ID")


async def run_scraper(company_id: str, name: str):

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(
            f"https://vendors.planetbids.com/portal/{company_id}/bo/bo-search"
        )

        cookies = await context.cookies()
        cookie_header = "; ".join(
            [f"{c['name']}={c['value']}" for c in cookies]
        )

        headers = {
            "accept": "application/vnd.api+json",
            "authorization": AUTH_TOKEN,
            "company-id": company_id,
            "vendor-id": VENDOR_ID,
            "vendor-login-id": VENDOR_LOGIN_ID,
            "visit-id": VISIT_ID,
            "cookie": cookie_header,
        }

        page_number = 1

        while True:
            params = {
                "cid": company_id,
                "page": page_number,
                "per_page": 30,
                "stage_id": 4,
            }

            response = await page.request.get(
                "https://api.planetbids.com/papi/bids",
                headers=headers,
                params=params
            )

            if response.status != 200:
                break

            data = await response.json()
            bids = data.get("data", [])

            if not bids:
                break

            for bid in bids:
                bid_id = bid["attributes"]["bidId"]
                details = await fetch_details(
                    page, company_id, bid_id, headers
                )

                results.append({
                    "organization": name,
                    "title": bid["attributes"].get("title"),
                    "due_date": bid["attributes"].get("bidDueDate"),
                    "contact": details.get("contactNameAndPhone") if details else None,
                    "email": details.get("contactEmail") if details else None,
                    "scope": details.get("scope") if details else None,
                })

            page_number += 1

        await browser.close()

    return results


async def fetch_details(page, company_id, bid_id, headers):

    url = f"https://api.planetbids.com/papi/bid-details/{bid_id}"

    response = await page.request.get(url, headers=headers)

    if response.status != 200:
        return None

    data = await response.json()
    return data.get("data", {}).get("attributes", {})
