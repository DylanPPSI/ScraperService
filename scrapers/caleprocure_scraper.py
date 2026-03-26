"""
Cal eProcure Scraper v6

Phase 1: Load search page, extract 630+ event IDs from table
Phase 2: For each event, click its row (data-if-ps-clickable),
         intercept the backend detail call to get business_unit + details

Requirements:
    pip install playwright beautifulsoup4 lxml
    playwright install chromium

Usage:
    python caleprocure_scraper.py
    python caleprocure_scraper.py --headed
    python caleprocure_scraper.py --limit 10
"""

import csv
import json
import sys
import time
import re
import os
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

FRONT_DOOR = "https://caleprocure.ca.gov/pages/Events-BS3/event-search.aspx"
DETAIL_PAGE = "https://caleprocure.ca.gov/event/{bu}/{eid}"
HEADED = "--headed" in sys.argv
DEBUG = True

LIMIT = None
for i, arg in enumerate(sys.argv):
    if arg == "--limit" and i + 1 < len(sys.argv):
        LIMIT = int(sys.argv[i + 1])

# Resume support — skip events we've already scraped
RESUME_FILE = "caleprocure_progress.json"


def dbg(content, name):
    if DEBUG:
        with open(name, "w", encoding="utf-8") as f:
            f.write(content)


# ---------------------------------------------------------------------------
# Phase 1: Get event list
# ---------------------------------------------------------------------------
def get_event_list(page):
    print(f"\n[1] Loading search page...")
    page.goto(FRONT_DOOR, wait_until="domcontentloaded", timeout=60000)

    print("    Waiting for table...")
    try:
        page.wait_for_selector("td[data-if-label='tdEventId']", timeout=30000)
        print("    ✓ Table loaded")
    except PwTimeout:
        print("    Trying longer wait...")
        time.sleep(10)

    time.sleep(3)

    # Parse using the exact data-if-label attributes from the InFlight template
    rows = page.query_selector_all("tr[data-if-label^='tblBodyTr']")
    print(f"    Found {len(rows)} table rows")

    events = []
    for row in rows:
        ev = {}

        # Event ID — td with data-if-label="tdEventId"
        eid_cell = row.query_selector("td[data-if-label='tdEventId']")
        if eid_cell:
            ev["event_id"] = eid_cell.inner_text().strip()
            # Store the selector info for clicking later
            ev["_eid_id"] = eid_cell.get_attribute("id") or ""
            ev["_eid_name"] = eid_cell.get_attribute("name") or ""

        # Event Name
        name_cell = row.query_selector("td[data-if-label='tdEventName']")
        ev["event_name"] = name_cell.inner_text().strip() if name_cell else ""

        # Department
        dep_cell = row.query_selector("td[data-if-label='tdDepName']")
        ev["department"] = dep_cell.inner_text().strip() if dep_cell else ""

        # End Date
        end_cell = row.query_selector("td[data-if-label='tdEndDate']")
        ev["end_date"] = end_cell.inner_text().strip().replace("\n", " ") if end_cell else ""

        # Status
        stat_cell = row.query_selector("td[data-if-label='tdStatus']")
        ev["status"] = stat_cell.inner_text().strip() if stat_cell else ""

        # Published Date (may be hidden)
        pub_cell = row.query_selector("td[data-if-label='tdPubDate']")
        ev["published_date"] = pub_cell.inner_text().strip().replace("\n", " ") if pub_cell else ""

        if ev.get("event_id"):
            events.append(ev)

    # Deduplicate
    seen = set()
    unique = []
    for ev in events:
        if ev["event_id"] not in seen:
            seen.add(ev["event_id"])
            unique.append(ev)

    print(f"    {len(unique)} unique events")
    return unique


# ---------------------------------------------------------------------------
# Phase 2: Get details by clicking each event row
# ---------------------------------------------------------------------------
def get_all_details(page, events):
    total = len(events)
    if LIMIT:
        events = events[:LIMIT]

    # Load progress for resume support
    progress = load_progress()
    print(f"\n[3] Fetching details for {len(events)} events...")
    if progress:
        print(f"    Resuming — {len(progress)} already done")

    detailed = []
    failures = []

    for i, ev in enumerate(events):
        eid = ev["event_id"]

        # Skip if already scraped
        if eid in progress:
            detailed.append(progress[eid])
            continue

        pct = (i + 1) / len(events) * 100
        print(f"    [{i+1}/{len(events)} {pct:.0f}%] {eid} ", end="", flush=True)

        detail = dict(ev)
        detail["business_unit"] = ""
        detail["description"] = ""
        detail["contact_name"] = ""
        detail["contact_phone"] = ""
        detail["contact_email"] = ""
        detail["start_date"] = ""
        detail["event_format"] = ""
        detail["event_version"] = ""
        detail["unspsc_codes"] = ""
        detail["service_areas"] = ""

        try:
            result = click_event_and_get_detail(page, ev)
            if result:
                detail.update(result)
                bu = detail.get("business_unit", "")
                print(f"BU={bu}" if bu else "(no BU)")
            else:
                print("(no detail)")
                failures.append(eid)
        except Exception as e:
            print(f"ERROR: {e}")
            failures.append(eid)
            # Try to recover by going back to search
            try:
                page.goto(FRONT_DOOR, wait_until="domcontentloaded", timeout=30000)
                time.sleep(5)
            except Exception:
                pass

        # Remove internal keys
        detail = {k: v for k, v in detail.items() if not k.startswith("_")}
        detailed.append(detail)

        # Save progress periodically
        progress[eid] = detail
        if (i + 1) % 10 == 0:
            save_progress(progress)
            # Also save interim results
            save_csv(detailed, "caleprocure_events_partial.csv")

    save_progress(progress)

    if failures:
        print(f"\n    {len(failures)} failures: {failures[:10]}...")

    return detailed


def click_event_and_get_detail(page, ev):
    """Click an event row and intercept the detail backend call."""
    captured = {}

    def on_response(response):
        url = response.url
        if "AUC_RESP_INQ_DTL" in url and response.status == 200:
            try:
                body = response.text()
                if len(body) > 500:
                    captured["html"] = body
                    captured["url"] = url
            except Exception:
                pass
        # Also check for the detail page template load
        if "event-details" in url and response.status == 200:
            captured["detail_page"] = True

    page.on("response", on_response)

    try:
        # Click the event ID cell (InFlight handles the click via data-if-ps-clickable)
        eid = ev["event_id"]
        eid_id = ev.get("_eid_id", "")

        clicked = False

        # Method 1: Click by the cell's id attribute (escape $ for CSS)
        if eid_id:
            escaped = eid_id.replace("$", "\\$")
            cell = page.query_selector(f"td[id='{eid_id}']")
            if cell:
                cell.click()
                clicked = True

        # Method 2: Find by text content in event ID cells
        if not clicked:
            cells = page.query_selector_all("td[data-if-label='tdEventId']")
            for c in cells:
                if c.inner_text().strip() == eid:
                    c.click()
                    clicked = True
                    break

        if not clicked:
            return None

        # Wait for detail page to load
        try:
            page.wait_for_selector(
                "[data-if-label='eventName'], "
                "[data-if-label='descriptiondetails'], "
                "[data-if-label='contactName']",
                timeout=15000,
            )
        except PwTimeout:
            time.sleep(3)

        time.sleep(1.5)

        # Extract data
        result = {}

        # Get business_unit from intercepted URL
        if captured.get("url"):
            bu_match = re.search(r"BUSINESS_UNIT=(\w+)", captured["url"])
            if bu_match:
                result["business_unit"] = bu_match.group(1)

        # Also check the browser URL bar
        current_url = page.url
        url_match = re.match(r".*/event/(\w+)/(.+)", current_url)
        if url_match and not result.get("business_unit"):
            result["business_unit"] = url_match.group(1)

        # Parse from intercepted raw HTML if available
        if captured.get("html"):
            raw = parse_detail_from_raw(captured["html"])
            # Only fill in what we don't already have
            for k, v in raw.items():
                if v and not result.get(k):
                    result[k] = v

        # Parse from rendered DOM
        dom = parse_detail_from_dom(page)
        for k, v in dom.items():
            if v and not result.get(k):
                result[k] = v

        # Navigate back
        go_back(page)

        return result

    finally:
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Parse detail page
# ---------------------------------------------------------------------------
def parse_detail_from_raw(html):
    """Parse from intercepted PeopleSoft HTML."""
    soup = BeautifulSoup(html, "lxml")
    d = {}

    pairs = {
        "event_name": "[id*='AUC_HDR_ZZ_AUC_NAME'], [id*='AUC_HDR_AUC_NAME']",
        "start_date": "[id*='AUC_HDR_AUC_DTTM_START']",
        "end_date_detail": "[id='AUC_HDR_AUC_DTTM_FINISH']",
        "department": "[id*='SP_BU_GL_CLSVW_DESCR'], [id*='BUS_UNIT_TBL_FS_DESCR']",
        "event_format": "[id*='AUC_FORMAT_BIDBER'], [id*='AUC_HDR_AUC_TYPE']",
        "event_version": "[id*='AUC_HDR_AUC_VERSION']",
        "contact_name": "[id*='AUC_HDR_NAME1']",
        "contact_phone": "[id='AUC_HDR_PHONE']",
        "contact_email": "[id*='EMAILID']",
    }
    for key, sel in pairs.items():
        el = soup.select_one(sel)
        if el:
            d[key] = el.get_text(strip=True)

    # Description (can be long HTML)
    el = soup.select_one("[id*='AUC_HDR_DESCRLONG']")
    if el:
        d["description"] = el.get_text(strip=True)[:3000]

    # Business unit from combined ID field
    el = soup.select_one("[id='RESP_AUC_H0B_WK_AUC_ID_BUS_UNIT']")
    if el:
        text = el.get_text(strip=True)
        # Could be "2720 - 0000037900" or just "2720"
        if " - " in text:
            d["business_unit"] = text.split(" - ")[0].strip()
        elif text:
            d["business_unit"] = text.strip()

    # UNSPSC
    rows = soup.select("tr[id^='trZZ_UNSPSC_CD_VW2$0_row']")
    codes = []
    for row in rows:
        code_el = row.select_one("[id^='ZZ_CATGRY_CD_VW_CATEGORY_CD$']")
        desc_el = row.select_one("[id^='ZZ_CAT_DSCR_VW_DESCR254']")
        if code_el:
            codes.append({
                "code": code_el.get_text(strip=True),
                "description": desc_el.get_text(strip=True) if desc_el else "",
            })
    if codes:
        d["unspsc_codes"] = json.dumps(codes)

    # Service areas
    sa_rows = soup.select("tr[id^='trZZ_AUC_SA_TBL$0_row']")
    areas = []
    for row in sa_rows:
        sa_id = row.select_one("[id^='ZZ_AUC_SA_TBL_ZZ_SRVC_AREA_ID$']")
        county = row.select_one("[id^='ZZ_SA_VW_COUNTY$']")
        if sa_id:
            areas.append({
                "area_id": sa_id.get_text(strip=True),
                "county": county.get_text(strip=True) if county else "",
            })
    if areas:
        d["service_areas"] = json.dumps(areas)

    return d


def parse_detail_from_dom(page):
    """Parse from rendered detail page DOM using data-if-label attributes."""
    d = {}

    label_map = {
        "event_name": "eventName",
        "description": "descriptiondetails",
        "contact_name": "contactName",
        "contact_email": "emailAnchor, emffffailText",
        "start_date": "eventStartDate",
        "event_format": "format1, format2",
        "event_version": "eventVersion",
        "business_unit": "eventId",  # combined BU-EventID field
    }

    for key, labels in label_map.items():
        for label in labels.split(", "):
            el = page.query_selector(f"[data-if-label='{label}']")
            if el:
                text = el.inner_text().strip()
                if text:
                    d[key] = text
                    break

    # Phone — try specific selector
    el = page.query_selector("[data-if-label='phoneText']")
    if el:
        d["contact_phone"] = el.inner_text().strip()

    # Parse business_unit from the eventId field ("2720 - 0000037900")
    if "business_unit" in d and " - " in d["business_unit"]:
        d["business_unit"] = d["business_unit"].split(" - ")[0].strip()

    # Description might be long
    if d.get("description"):
        d["description"] = d["description"][:3000]

    return d


def go_back(page):
    """Navigate back to search results."""
    try:
        page.go_back(wait_until="domcontentloaded", timeout=15000)
        # Wait for the table to reappear
        page.wait_for_selector("td[data-if-label='tdEventId']", timeout=15000)
        time.sleep(1)
    except Exception:
        # Reload the search page
        try:
            page.goto(FRONT_DOOR, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_selector("td[data-if-label='tdEventId']", timeout=30000)
            time.sleep(3)
        except Exception:
            time.sleep(5)


# ---------------------------------------------------------------------------
# Progress / Resume
# ---------------------------------------------------------------------------
def load_progress():
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    with open(RESUME_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
FIELDS = [
    "event_id", "business_unit", "event_name", "department",
    "event_format", "start_date", "end_date", "status",
    "description", "contact_name", "contact_phone", "contact_email",
    "event_version", "published_date", "unspsc_codes", "service_areas",
]


def save_csv(events, filename="caleprocure_events.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        for ev in events:
            w.writerow({k: ev.get(k, "") for k in FIELDS})
    print(f"    {filename}")


def save_json(events, filename="caleprocure_events.json"):
    out = [{k: ev.get(k, "") for k in FIELDS} for ev in events]
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"    {filename}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Cal eProcure Scraper v6")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not HEADED,
            slow_mo=200 if HEADED else 0,
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        # Phase 1: Get event list
        events = get_event_list(page)
        if not events:
            print("    No events found.")
            browser.close()
            sys.exit(1)

        # Save basic list
        print("\n[2] Saving basic list...")
        save_csv(events, "caleprocure_events_basic.csv")
        save_json(events, "caleprocure_events_basic.json")

        # Phase 2: Get details
        detailed = get_all_details(page, events)

        browser.close()

    # Save final results
    print(f"\n[4] Saving {len(detailed)} events with details...")
    save_csv(detailed)
    save_json(detailed)

    # Clean up progress file on success
    if os.path.exists(RESUME_FILE) and not LIMIT:
        os.remove(RESUME_FILE)

    print(f"\n{'=' * 60}")
    print(f"DONE — {len(detailed)} events")
    print(f"  caleprocure_events_basic.csv  (list only)")
    print(f"  caleprocure_events.csv        (with details)")
    print(f"  caleprocure_events.json       (with details)")
    print(f"  {datetime.now().isoformat()}")
    print(f"{'=' * 60}")

    for e in detailed[:10]:
        bu = e.get("business_unit", "?")
        eid = e.get("event_id", "?")
        name = e.get("event_name", "?")[:40]
        print(f"  BU={bu:<6} {eid:>12}  {name}")


if __name__ == "__main__":
    main()