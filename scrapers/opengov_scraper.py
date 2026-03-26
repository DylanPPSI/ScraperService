import requests
import json
import time
import csv
import os
import re
from html import unescape
from datetime import datetime
from dotenv import load_dotenv

# ─── Load .env ───────────────────────────────────────────────────
load_dotenv()
EMAIL = os.getenv("OPENGOV_EMAIL")
PASSWORD = os.getenv("OPENGOV_PASSWORD")

if not EMAIL or not PASSWORD:
    print("✗ Missing OPENGOV_EMAIL or OPENGOV_PASSWORD in .env file.")
    exit(1)

# ─── Configuration ───────────────────────────────────────────────
LOGIN_URL = "https://api.procurement.opengov.com/api/v1/auth/login"
SEARCH_URL = "https://api.procurement.opengov.com/api/v1/project/search"
PROJECT_URL = "https://api.procurement.opengov.com/api/v1/project"
LIMIT = 20
DELAY = 1.0
DETAIL_DELAY = 0.5
OUTPUT_DIR = "opengov_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Origin": "https://procurement.opengov.com",
    "Referer": "https://procurement.opengov.com/",
}


# ─── HTML stripping helper ────────────────────────────────────────
def strip_html(html_str):
    """Strip HTML tags and decode entities to plain text."""
    if not html_str:
        return ""
    text = re.sub(r'<[^>]+>', ' ', html_str)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ─── Login ────────────────────────────────────────────────────────
def login(session: requests.Session) -> bool:
    """Login and capture all cookies + any tokens."""
    # First visit the main site to get any initial cookies/CSRF
    print("  Fetching initial cookies from main site...")
    try:
        session.get("https://procurement.opengov.com/login", headers={
            "User-Agent": BASE_HEADERS["User-Agent"]
        }, timeout=15)
        print(f"    Initial cookies: {list(session.cookies.get_dict().keys())}")
    except Exception as e:
        print(f"    Warning: Could not fetch initial page: {e}")

    # Now login — try multiple payload formats since API is picky
    payloads = [
        {"email": EMAIL, "password": PASSWORD},
        {"username": EMAIL, "password": PASSWORD},
        {"user": {"email": EMAIL, "password": PASSWORD}},
    ]

    resp = None
    for payload in payloads:
        print(f"  Trying payload format: {list(payload.keys())} ...")
        resp = session.post(LOGIN_URL, json=payload, headers=BASE_HEADERS, timeout=15)
        print(f"    Status: {resp.status_code}")
        if resp.status_code == 200:
            break
        # Reset cookies from failed attempt
        print(f"    ✗ {resp.status_code} — trying next format...")

    print(f"    Response cookies: {list(session.cookies.get_dict().keys())}")

    if resp is None or resp.status_code != 200:
        print(f"    ✗ Login failed with all payload formats")
        return False

    # Save full login response for debugging
    login_path = os.path.join(OUTPUT_DIR, "login_response.json")
    try:
        login_data = resp.json()
        with open(login_path, "w", encoding="utf-8") as f:
            json.dump(login_data, f, indent=2, default=str)
        print(f"    Saved login response → {login_path}")
    except Exception:
        login_data = {}

    # Check for token in response
    token = (
        login_data.get("token")
        or login_data.get("access_token")
        or login_data.get("data", {}).get("token") if isinstance(login_data.get("data"), dict) else None
    )
    if token:
        session.headers.update({"Authorization": f"Bearer {token}"})
        print(f"    ✓ Found token in response body")

    # Check for token in response headers
    auth_header = resp.headers.get("Authorization") or resp.headers.get("x-access-token")
    if auth_header:
        session.headers.update({"Authorization": auth_header})
        print(f"    ✓ Found token in response headers")

    # Print all cookies for debugging
    print(f"\n  All session cookies:")
    for name, value in session.cookies.get_dict().items():
        print(f"    {name} = {value[:50]}..." if len(value) > 50 else f"    {name} = {value}")

    print(f"\n  ✓ Logged in as {login_data.get('email', EMAIL)}")
    return True


# ─── Search (paginated) ──────────────────────────────────────────
def fetch_page(page: int, session: requests.Session) -> dict:
    params = {"page": page, "limit": LIMIT, "sort": "id", "direction": "DESC"}
    resp = session.post(SEARCH_URL, params=params, json={}, timeout=30)

    # Debug on failure
    if resp.status_code != 200:
        print(f"\n  DEBUG: Status {resp.status_code}")
        print(f"  DEBUG: Request cookies sent: {list(session.cookies.get_dict().keys())}")
        print(f"  DEBUG: Request headers: Authorization={session.headers.get('Authorization', 'NONE')}")
        print(f"  DEBUG: Response body (first 500): {resp.text[:500]}")

    resp.raise_for_status()
    return resp.json()


def scrape_all_ids(session: requests.Session) -> list[int]:
    all_ids = []
    page = 1
    total_pages = None

    while True:
        try:
            label = f"→ Page {page}" + (f" / {total_pages}" if total_pages else "")
            print(f"\n{label} ...")
            data = fetch_page(page, session)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            print(f"  ✗ HTTP {status} on page {page}")
            if status in (401, 403):
                print("  Auth issue. See debug output above.")
                return all_ids
            if status == 404:
                break
            raise
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request failed: {e} — retrying in 5s...")
            time.sleep(5)
            continue

        results = []
        if isinstance(data, dict):
            for key in ["result", "results", "data", "items", "records", "projects"]:
                if key in data and isinstance(data[key], list):
                    results = data[key]
                    break

            if total_pages is None:
                for key in ["totalPages", "total_pages", "pages", "lastPage"]:
                    if key in data:
                        total_pages = int(data[key])
                        break
                if total_pages is None:
                    for key in ["total", "totalCount", "total_count", "count"]:
                        if key in data:
                            tc = int(data[key])
                            total_pages = (tc + LIMIT - 1) // LIMIT
                            print(f"  Total records: {tc} → {total_pages} pages")
                            break
                if total_pages is None:
                    for mk in ["meta", "pagination"]:
                        if mk in data and isinstance(data[mk], dict):
                            meta = data[mk]
                            for key in ["totalPages", "total_pages", "pages"]:
                                if key in meta:
                                    total_pages = int(meta[key])
                                    break
                            if total_pages is None:
                                for key in ["total", "totalCount"]:
                                    if key in meta:
                                        total_pages = (int(meta[key]) + LIMIT - 1) // LIMIT
                                        break
                            break

            if page == 1:
                raw_path = os.path.join(OUTPUT_DIR, "page_1_raw.json")
                with open(raw_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, default=str)
                print(f"  Saved → {raw_path}")
                print(f"  Keys: {list(data.keys())}")

        elif isinstance(data, list):
            results = data

        if not results:
            print("  No results. Done!")
            break

        page_ids = []
        for r in results:
            pid = r.get("id") or r.get("projectId") or r.get("project_id")
            if pid is not None:
                page_ids.append(int(pid))
        all_ids.extend(page_ids)
        print(f"  ✓ {len(page_ids)} IDs (total: {len(all_ids)})")

        if total_pages and page >= total_pages:
            break
        if len(results) < LIMIT:
            break

        page += 1
        time.sleep(DELAY)

    return all_ids


# ─── Fetch + Extract project details ─────────────────────────────
def fetch_project_detail(pid: int, session: requests.Session) -> dict | None:
    url = f"{PROJECT_URL}/{pid}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        print(f"    ✗ HTTP {status} for project {pid}")
        return None
    except Exception as e:
        print(f"    ✗ Failed for project {pid}: {e}")
        return None


def extract_bid_fields(raw: dict) -> dict:
    """Extract core fields + pricing line items from raw project detail."""
    # Unwrap if nested
    if isinstance(raw, dict):
        for key in ["result", "data", "project"]:
            if key in raw and isinstance(raw[key], dict):
                raw = raw[key]
                break

    # ── Core fields ──
    bid = {
        "id": raw.get("id"),
        "title": raw.get("title"),
        "financial_id": raw.get("financialId"),
        "status": raw.get("status"),
        "type": raw.get("type"),

        # Dates
        "release_date": raw.get("releaseProjectDate"),
        "proposal_deadline": raw.get("proposalDeadline"),
        "qa_deadline": raw.get("qaDeadline"),
        "posted_at": raw.get("postedAt"),
        "closed_at": raw.get("closedAt"),
        "created_at": raw.get("created_at"),

        # Contact
        "contact_name": raw.get("contactFullName") or raw.get("contactDisplayName"),
        "contact_email": raw.get("contactEmail"),
        "contact_phone": raw.get("contactPhoneComplete"),
        "contact_title": raw.get("contactTitle"),
        "contact_city": raw.get("contactCity"),
        "contact_state": raw.get("contactState"),

        # Organization
        "org_name": None,
        "org_city": None,
        "org_state": None,
        "org_website": None,

        # Department
        "department_name": raw.get("departmentName"),
        "department_head": raw.get("departmentHead"),

        # Template / Classification
        "template_title": None,
        "procurement_classification": None,

        # Summary (plain text)
        "summary_text": strip_html(raw.get("summary", ""))[:2000],

        # Flags
        "is_emergency": raw.get("isEmergency"),
        "has_sealed_bid": raw.get("hasSealedBid"),
        "notified_vendors": raw.get("notifiedVendors"),
    }

    # Organization
    gov = raw.get("government", {})
    org = gov.get("organization", {}) if isinstance(gov, dict) else {}
    if org:
        bid["org_name"] = org.get("name")
        bid["org_city"] = org.get("city")
        bid["org_state"] = org.get("state")
        bid["org_website"] = org.get("website")

    # Template
    tmpl = raw.get("template", {})
    if isinstance(tmpl, dict):
        bid["template_title"] = tmpl.get("title")
        bid["procurement_classification"] = tmpl.get("procurementClassification")

    # ── Pricing line items ──
    price_tables = raw.get("priceTables", [])
    line_items = []
    for table in (price_tables or []):
        for item in (table.get("priceItems") or []):
            if item.get("isHeaderRow"):
                continue
            line_items.append({
                "line_item": item.get("lineItem"),
                "description": item.get("description"),
                "unit_of_measure": item.get("unitToMeasure"),
                "quantity": item.get("quantity"),
                "unit_price": item.get("unitPrice"),
            })

    bid["line_items"] = line_items
    bid["line_items_count"] = len(line_items)

    # Flatten line items into a readable string for CSV
    bid["line_items_summary"] = " | ".join(
        f"{li['line_item']}: {li['description']} ({li['unit_of_measure']}, qty {li['quantity']})"
        for li in line_items
    ) if line_items else ""

    return bid


def fetch_all_details(ids: list[int], session: requests.Session) -> list[dict]:
    all_details = []
    total = len(ids)
    failed = []

    print(f"\n{'=' * 60}")
    print(f"  Fetching bid details for {total} projects...")
    print(f"{'=' * 60}")

    for i, pid in enumerate(ids, 1):
        print(f"  [{i}/{total}] Project {pid} ...", end=" ")
        raw = fetch_project_detail(pid, session)

        if raw:
            bid = extract_bid_fields(raw)
            all_details.append(bid)
            print(f"✓ {bid.get('title', '?')[:60]}")
        else:
            failed.append(pid)
            print("✗")

        if i % 50 == 0:
            cp = os.path.join(OUTPUT_DIR, "details_checkpoint.json")
            with open(cp, "w", encoding="utf-8") as f:
                json.dump(all_details, f, indent=2, default=str, ensure_ascii=False)
            print(f"  ── Checkpoint ({i}/{total}) ──")

        time.sleep(DETAIL_DELAY)

    if failed:
        print(f"\n  ⚠ Failed: {len(failed)} projects")
        with open(os.path.join(OUTPUT_DIR, "failed_ids.json"), "w") as f:
            json.dump(failed, f)

    return all_details


# ─── Save ─────────────────────────────────────────────────────────
def save_to_json(data, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    print(f"  JSON → {path}")


def save_to_csv(results, filename):
    if not results:
        return
    # Exclude nested line_items list from CSV, use the summary string instead
    exclude = {"line_items"}
    all_keys = sorted({k for r in results for k in r if k not in exclude})
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for record in results:
            flat = {
                k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
                for k, v in record.items() if k not in exclude
            }
            writer.writerow(flat)
    print(f"  CSV  → {path}")


# ─── Main ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    print("=" * 60)
    print("  OpenGov Procurement Scraper")
    print("=" * 60)
    print(f"\n  Logging in as {EMAIL} ...")

    if not login(session):
        print("\n  ✗ Login failed.")
        exit(1)

    # ── Verify auth ──
    print("\n  Verifying auth against search endpoint...")
    try:
        fetch_page(1, session)
        print("  ✓ Auth verified!\n")
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        print(f"\n  ✗ Search endpoint returned HTTP {status}.")
        print(f"\n  Your cookies: {list(session.cookies.get_dict().keys())}")
        print(f"\n  TIP: If this keeps failing, try copying the full Cookie header")
        print(f"  from your browser DevTools and set it in .env as OPENGOV_COOKIE")
        print(f"  Then uncomment the manual cookie fallback in the script.")

        # ── Manual cookie fallback ──
        manual_cookie = os.getenv("OPENGOV_COOKIE")
        if manual_cookie:
            print(f"\n  Found OPENGOV_COOKIE in .env, trying manual cookie...")
            session.headers.update({"Cookie": manual_cookie})
            try:
                fetch_page(1, session)
                print("  ✓ Manual cookie works!")
            except Exception:
                print("  ✗ Manual cookie also failed. Exiting.")
                exit(1)
        else:
            exit(1)

    # ── Step 1: Scrape all project IDs ──
    start = datetime.now()
    print("── STEP 1: Collecting project IDs ──")
    project_ids = scrape_all_ids(session)
    print(f"\n  Collected {len(project_ids)} IDs")

    if not project_ids:
        print("  No IDs found. Check opengov_data/page_1_raw.json")
        exit(1)

    save_to_json(project_ids, "all_project_ids.json")

    # ── Step 2: Fetch + extract details ──
    print("\n── STEP 2: Fetching + extracting bid details ──")
    details = fetch_all_details(project_ids, session)

    elapsed = datetime.now() - start
    print(f"\n{'=' * 60}")
    print(f"  Done! {len(details)} bids in {elapsed}")
    print(f"{'=' * 60}")

    if details:
        print("\nSaving...")
        save_to_json(details, "all_bid_details.json")
        save_to_csv(details, "all_bid_details.csv")

        # Preview
        print(f"\n── Sample extracted bid ──")
        sample = {k: v for k, v in details[0].items() if k != "summary_text"}
        print(json.dumps(sample, indent=2, default=str)[:2000])