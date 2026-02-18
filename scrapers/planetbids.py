# scrapers/planetbids.py
import os
import asyncio
import httpx

BASE_URL = "https://api.planetbids.com/papi"

async def run_scraper(company_id: str, name: str):
    # Load env variables here (lazy)
    AUTH_TOKEN = os.getenv("PLANETBIDS_AUTH_TOKEN")
    VENDOR_ID = os.getenv("PLANETBIDS_VENDOR_ID")
    VENDOR_LOGIN_ID = os.getenv("PLANETBIDS_VENDOR_LOGIN_ID")
    VISIT_ID = os.getenv("PLANETBIDS_VISIT_ID")

    if not all([AUTH_TOKEN, VENDOR_ID, VENDOR_LOGIN_ID, VISIT_ID]):
        raise RuntimeError("Missing PLANETBIDS env variables")

    results = []
    headers = {
        "accept": "application/vnd.api+json",
        "authorization": AUTH_TOKEN,
        "company-id": company_id,
        "vendor-id": VENDOR_ID,
        "vendor-login-id": VENDOR_LOGIN_ID,
        "visit-id": VISIT_ID,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        page_number = 1
        while True:
            params = {
                "cid": company_id,
                "page": page_number,
                "per_page": 30,
                "stage_id": 4,
            }

            try:
                response = await client.get(f"{BASE_URL}/bids", headers=headers, params=params)
                response.raise_for_status()
            except httpx.HTTPError:
                break

            data = response.json()
            bids = data.get("data", [])
            if not bids:
                break

            tasks = [
                fetch_details(client, company_id, bid["attributes"]["bidId"], AUTH_TOKEN, VENDOR_ID, VENDOR_LOGIN_ID, VISIT_ID)
                for bid in bids
            ]
            details_list = await asyncio.gather(*tasks)

            for bid, details in zip(bids, details_list):
                results.append({
                    "organization": name,
                    "title": bid["attributes"].get("title"),
                    "due_date": bid["attributes"].get("bidDueDate"),
                    "contact": details.get("contactNameAndPhone") if details else None,
                    "email": details.get("contactEmail") if details else None,
                    "scope": details.get("scope") if details else None,
                })

            page_number += 1

    print(f"Scraping complete for {name}, total bids: {len(results)}")
    return results


async def fetch_details(client, company_id, bid_id, AUTH_TOKEN, VENDOR_ID, VENDOR_LOGIN_ID, VISIT_ID):
    url = f"{BASE_URL}/bid-details/{bid_id}"
    try:
        resp = await client.get(url, headers={
            "accept": "application/vnd.api+json",
            "authorization": AUTH_TOKEN,
            "company-id": company_id,
            "vendor-id": VENDOR_ID,
            "vendor-login-id": VENDOR_LOGIN_ID,
            "visit-id": VISIT_ID,
        })
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("attributes", {})
    except httpx.HTTPError:
        return {}
