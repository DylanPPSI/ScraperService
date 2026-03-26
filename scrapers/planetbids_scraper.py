"""
PlanetBids Scraper — Backend API Service + CSV Export
"""

import asyncio
import csv
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────
BASE_API      = "https://api-external.prod.planetbids.com/papi"
PORTAL_BASE   = "https://vendors.planetbids.com/portal"
EM_VERSION    = "v11027@cb96357"
TIMEZONE      = "America/Los_Angeles"
DUE_DATE_FROM = "2026-02-25 00:00:00.000"
FETCH_DETAILS = True

OUTPUT_DIR = "planetbids_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

COMPANIES = [
    ("20314", "City of Santa Rosa"),
    ("14460", "City of Bakersfield"),
    ("49392", "City of Soledad"),
    ("16339", "Coachella Valley Water District"),
    ("39483", "City of Culver City"),
    ("43874", "County of Santa Barbara"),
    ("14599", "County of Stanislaus"),
    ("14663", "Central Contra Costa Sanitary District"),
    ("26384", "City of Chico"),
    ("14742", "City of Clovis"),
    ("39501", "San Joaquin Regional Rail Commission & SJJPA"),
    ("14769", "City of Fresno"),
    ("43728", "City of Laguna Beach"),
    ("46202", "City of Menlo Park"),
    ("46991", "City of Millbrae"),
    ("11434", "City of Ontario"),
    ("25569", "City of Palo Alto"),
    ("39475", "City of Riverside"),
    ("15300", "City of Sacramento"),
    ("22949", "City of Salinas"),
    ("17950", "City of San Diego"),
    ("14058", "Orange County Sanitation District"),
    ("13982", "Port of San Diego"),
    ("46106", "City of Burlingame"),
    ("48397", "Santa Clara Valley Water District (SCVWD)"),
    ("71685", "City of Los Altos"),
    ("43764", "Sacramento Area Sewer District (SASD)"),
    ("75302", "City of Sunnyvale"),
    ("14660", "City of Bakersfield (alt ID)"),
]

# ─────────────────────────────────────────────
#  Core competency matching
# ─────────────────────────────────────────────
KEYWORDS = """
Anaerobic Digester
Asset Condition
Barge Cleaning
Basins Inspection
Bio-solids Removal
Biosolid Removal
Biosolids Hauling
Biosolids Removal
Camera Assessment
Camera System
Catch Basin
CCTV
CCTV Inspection
CCTV Video
CIP
CIPL
CIPP
Clarifier Cleaning
Cleaning and CCTV
Cleaning and Disposal
cleaning and inspection
cleaning and inspections
Cleaning of Digester
cleaning of sewer
cleaning of the sewer
Cleaning Project
Cleaning Service
Closed Circuit Television
Combined Sewer
Condition Assessment
Confined Space
Cured in place
cured-in place
cured-in-place
Debris Removal
Defect Coding
Digester
Digester 9 Cleaning
Digester Cleaning
Digital Scanning
Drain Cleaning
Epoxy Liner
Flood Mitigation
Flow Diversion
Flow Monitoring
Force Main
GIS Mapping
GIS Utility Mapping
Grease Removal
Grit Removal
Hazardous Material Removal
Headworks
Heat Exchanger
HEX
Hydro Excavating
Hydro Excavation
inspection and cleaning
Inspection Camera
inspections and cleaning
Interceptor Cleaning
Jet Camera
Lagoon
Large Diameter
Large Diameter Cleaning
Large Diameter Pipe Cleaning
Lateral Inspection
Lateral Launch
Lidar
Lining
Mainline Inspection
Manhole Inspection
Manhole Rehab
Misc. Camera
Misc. Cleaner
Misc. Video
Mobile Sludge Dewatering
MSI
NASSCO
Oil Tanker Cleaning
Outfall
Outfall Cleaning
Outfall Inspection
Pipe Cleaning
Pipe Inspection
Pipeline Inspection
Pot Holing
Potholing
Pump Station
Re-Lining
Relining
Reservoir Cleaning
Robotic Camera
Robotic Vehicle
Root Cutting
Sediment Removal
Sewer Cleaner
Sewer Cleaning
Sewer Condition Assessment
Sewer Inspection
Sewer Rehabilitation
Ship Cleaning
SIPHON CLEANING
Sliplining
Sludge Dewatering
Sludge Removal
solids accumulation
Sonar
SONAR INSPECTION
Sonar Profiler
Spray Lining
Storm Drain
Structural Liner
Tank Cleaning
Televise
Televising
Television Inspection
Trenchless
Utility Assessment
Utility Condition
Utility Locating
Utility locating
Utility Location
Utlitiy Locating
video inspecting
Video Inspection
Video Service
Well Cleaning
Water Treatment
""".strip()

MATCH_THRESHOLD = 20
COMPETENCY_TITLE_WEIGHT = 2.0
COMPETENCY_SCOPE_WEIGHT = 1.0


def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[-_/]", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_core_keywords(raw: str) -> list[str]:
    kws, seen = [], set()
    for line in raw.splitlines():
        k = line.strip()
        if not k:
            continue
        nk = _normalize(k)
        if nk and nk not in seen:
            seen.add(nk)
            kws.append(nk)
    kws.sort(key=len, reverse=True)
    return kws


CORE_KEYWORDS = build_core_keywords(KEYWORDS)


def score_bid_core_competency(title: str, scope: str) -> dict:
    t = _normalize(title)
    s = _normalize(scope)
    matches = Counter()
    raw_points = 0.0

    for kw in CORE_KEYWORDS:
        if not kw:
            continue
        t_count = t.count(kw) if t else 0
        s_count = s.count(kw) if s else 0
        if t_count or s_count:
            matches[kw] = t_count + s_count
            raw_points += (t_count * COMPETENCY_TITLE_WEIGHT) + (s_count * COMPETENCY_SCOPE_WEIGHT)

    score = int(min(100, round(raw_points * 12)))
    return {
        "competency_score": score,
        "competency_pass": score >= MATCH_THRESHOLD,
        "competency_matches": [{"keyword": k, "count": int(c)} for k, c in matches.most_common(12)],
    }


# ─────────────────────────────────────────────
#  Load portal page, capture visit-id + auth
# ─────────────────────────────────────────────
async def load_portal(context, company_id: str) -> dict:
    session = {
        "visit-id": None, "authorization": "",
        "vendor-id": "null", "vendor-login-id": "null",
        "em-version": EM_VERSION, "timezone-name": TIMEZONE,
    }
    page = await context.new_page()

    async def on_request(req):
        if "api-external.prod.planetbids.com/papi" not in req.url:
            return
        h = req.headers
        if not session["authorization"]:
            session["authorization"] = h.get("authorization", "")
        if not session["visit-id"]:
            vid = h.get("visit-id", "")
            if vid and vid != "null":
                session["visit-id"] = vid
        session["vendor-id"] = h.get("vendor-id", "null")
        session["vendor-login-id"] = h.get("vendor-login-id", "null")
        tz = h.get("timezone-name", "")
        if tz and tz != "null":
            session["timezone-name"] = tz
        em = h.get("em-version", "")
        if em:
            session["em-version"] = em

    async def on_response(resp):
        if "/papi/visits" in resp.url and resp.request.method == "POST":
            try:
                body = await resp.json()
                vid = (body.get("data") or {}).get("id")
                if vid and not session["visit-id"]:
                    session["visit-id"] = str(vid)
            except Exception:
                pass

    page.on("request", on_request)
    page.on("response", on_response)
    await page.goto(
        f"{PORTAL_BASE}/{company_id}/bo/bo-search",
        wait_until="networkidle", timeout=45000,
    )
    if not session["visit-id"]:
        await page.wait_for_timeout(4000)
    await page.close()
    return session


def make_headers(session: dict, company_id: str) -> dict:
    return {
        "accept": "application/vnd.api+json",
        "authorization": session["authorization"],
        "company-id": company_id,
        "em-version": session["em-version"],
        "origin": "https://vendors.planetbids.com",
        "referer": "https://vendors.planetbids.com/",
        "timezone-name": session["timezone-name"],
        "vendor-id": session["vendor-id"],
        "vendor-login-id": session["vendor-login-id"],
        "visit-id": session["visit-id"] or "",
    }


async def api_get(context, url, params, session, company_id, max_retries=4):
    for attempt in range(1, max_retries + 1):
        resp = await context.request.get(
            url, headers=make_headers(session, company_id), params=params,
        )
        if resp.status == 200:
            return await resp.json()
        elif resp.status == 429:
            print(f"    [429] Rate limited. Waiting 90s (attempt {attempt}/{max_retries})")
            await asyncio.sleep(90)
        elif resp.status in (500, 502, 503, 504):
            wait = 5 * attempt
            print(f"    [{resp.status}] Server error. Waiting {wait}s")
            await asyncio.sleep(wait)
        else:
            txt = await resp.text()
            print(f"    [{resp.status}] {url}: {txt[:150]}")
            return None
    return None


async def fetch_categories(context, session, company_id):
    print("Fetching global categories...")
    data = await api_get(context, f"{BASE_API}/categories",
                         params={"cid": company_id},
                         session=session, company_id=company_id)
    if not data:
        return {}
    cats = {}
    for item in data.get("data", []):
        cid = item.get("id")
        attr = item.get("attributes", {})
        cats[cid] = {"id": cid, "name": attr.get("name", ""), "code": attr.get("code", "")}
    print(f"  {len(cats)} categories loaded.")
    return cats


async def fetch_bid_types(context, session, company_id):
    data = await api_get(context, f"{BASE_API}/bid-types",
                         params={"cid": company_id},
                         session=session, company_id=company_id)
    if not data:
        return []
    return [
        {"id": item.get("id"), "name": (item.get("attributes") or {}).get("label", "")}
        for item in data.get("data", [])
    ]


async def fetch_company_bids(context, company_id, company_name):
    page = await context.new_page()
    print(f"\n=== Fetching {company_name} ({company_id}) ===")
    await page.goto(
        f"https://vendors.planetbids.com/portal/{company_id}/bo/bo-search",
        wait_until="networkidle",
    )
    api_url = "https://api-external.prod.planetbids.com/papi/bids"
    headers = {
        "accept": "application/vnd.api+json",
        "company-id": company_id, "em-version": EM_VERSION,
        "origin": "https://vendors.planetbids.com",
        "referer": "https://vendors.planetbids.com/",
        "timezone-name": TIMEZONE,
    }
    page_number, results = 1, []
    while True:
        params = {
            "bid_type_id": 0, "cid": company_id, "dept_id": 0,
            "due_date_from": DUE_DATE_FROM, "due_date_to": "",
            "keyword": "", "page": page_number, "per_page": 30,
            "sort_by": "", "sort_order": -1, "stage_id": 0,
        }
        response = await context.request.get(api_url, headers=headers, params=params)
        if response.status != 200:
            print(f"❌ FAILED {company_name}: {response.status}")
            break
        data = (await response.json()).get("data", [])
        if not data:
            break
        results.extend(data)
        print(f"Fetched page {page_number} ({len(data)})")
        page_number += 1
    await page.close()
    print(f"Total bids for {company_name}: {len(results)}")
    return {"company_id": company_id, "company_name": company_name, "bids": results}


async def fetch_bid_detail(context, session, company_id, bid_id):
    data = await api_get(
        context, f"{BASE_API}/bid-details/{bid_id}",
        params={"cid": company_id}, session=session, company_id=company_id,
    )
    if not data:
        return {}
    return data.get("data", {}).get("attributes", {})


# ─────────────────────────────────────────────
#  Process one company
# ─────────────────────────────────────────────
async def process_company(context, company_id, name, categories):
    print(f"\n{'─'*60}\n  {name}  (id={company_id})\n{'─'*60}")

    session = await load_portal(context, company_id)
    print(f"  visit-id={session.get('visit-id')}")

    bid_types = await fetch_bid_types(context, session, company_id)
    bid_type_map = {str(bt["id"]): bt for bt in bid_types}

    result = await fetch_company_bids(context, company_id, name)
    raw_bids = result["bids"]
    print(f"  Total bids fetched: {len(raw_bids)}")

    all_bids, matched_bids = [], []

    for item in raw_bids:
        if not isinstance(item, dict):
            continue
        attrs = item.get("attributes", {}) or {}
        bid_id = attrs.get("bidId")
        if not bid_id:
            continue

        detail = {}
        if FETCH_DETAILS:
            detail = await fetch_bid_detail(context, session, company_id, bid_id)

        bid_type_id = str(attrs.get("bidTypeId") or "")
        bid_type_obj = bid_type_map.get(bid_type_id, {})

        cat_ids = attrs.get("categoryIds") or detail.get("categoryIds") or []
        cat_names = [categories.get(str(cid), {}).get("name", str(cid)) for cid in cat_ids] if isinstance(cat_ids, list) else []

        bid = {
            "bid_id":             bid_id,
            "company_id":        company_id,
            "company_name":      name,
            "title":             attrs.get("title", ""),
            "invitation_number": attrs.get("invitationNumber", ""),
            "stage":             attrs.get("stageStr", ""),
            "bid_due_date":      attrs.get("bidDueDate", ""),
            "issue_date":        attrs.get("issueDate", ""),
            "pre_bid_date":      attrs.get("preBidDate", ""),
            "bid_type_name":     bid_type_obj.get("name", ""),
            "category_names":    ", ".join(cat_names),
            "scope":             detail.get("scope", ""),
            "contact_name_phone": detail.get("contactNameAndPhone", ""),
            "contact_email":     detail.get("contactEmail", ""),
            "bid_bond_pct":      f"{detail.get('bidBond')}%" if detail.get("bidBond") else "",
            "plan_holders_count": detail.get("planHoldersCount"),
            "addenda_count":     detail.get("addendaCount"),
            "pre_bid_location":  detail.get("preBidLocation", ""),
            "deliver_to":        detail.get("deliverTo", ""),
            "estimated_value":   detail.get("estimatedValue"),
            "department":        detail.get("department", ""),
            "notes":             detail.get("notes", ""),
        }

        comp = score_bid_core_competency(bid["title"], bid["scope"])
        bid["competency_score"] = comp["competency_score"]
        bid["competency_pass"] = comp["competency_pass"]
        bid["competency_matches"] = ", ".join(
            f"{m['keyword']}({m['count']})" for m in comp["competency_matches"]
        )

        all_bids.append(bid)
        if bid["competency_pass"]:
            matched_bids.append(bid)
            print(f"    ✓ [{bid['competency_score']}%] {bid['title']}")
        else:
            print(f"    · [{bid['competency_score']}%] {bid['title']} (filtered)")

    return all_bids, matched_bids


# ─────────────────────────────────────────────
#  CSV / JSON save helpers
# ─────────────────────────────────────────────
def save_to_csv(bids: list[dict], filename: str) -> None:
    if not bids:
        print(f"  No bids to save for {filename}.")
        return

    priority = [
        "competency_score", "competency_matches", "competency_pass",
        "title", "company_name", "bid_type_name",
        "invitation_number", "stage", "bid_due_date", "issue_date",
        "contact_name_phone", "contact_email",
        "category_names", "department",
        "bid_bond_pct", "estimated_value",
        "plan_holders_count", "addenda_count",
        "scope",
    ]
    all_keys = set()
    for b in bids:
        all_keys.update(b.keys())
    ordered = [k for k in priority if k in all_keys]
    ordered += sorted(all_keys - set(ordered))

    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered, extrasaction="ignore")
        writer.writeheader()
        for bid in bids:
            flat = {}
            for k, v in bid.items():
                if isinstance(v, (dict, list)):
                    flat[k] = json.dumps(v, default=str)
                elif isinstance(v, bool):
                    flat[k] = str(v)
                else:
                    flat[k] = v
            writer.writerow(flat)
    print(f"  CSV → {path}")


def save_to_json(data, filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    print(f"  JSON → {path}")


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
async def scrape_all_companies(companies=None):
    if companies is None:
        companies = COMPANIES

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        print("Bootstrapping session...")
        bootstrap_session = await load_portal(context, companies[0][0])
        print(f"Bootstrap visit-id: {bootstrap_session['visit-id']}\n")

        categories = await fetch_categories(context, bootstrap_session, companies[0][0])

        all_bids, matched = [], []

        for company_id, name in companies:
            try:
                company_all, company_matched = await process_company(
                    context, company_id, name, categories,
                )
                all_bids.extend(company_all)
                matched.extend(company_matched)
            except Exception as e:
                print(f"  ⚠️  Error: {name}: {e}")
            await asyncio.sleep(1)

        await browser.close()

    # ── Save outputs ─────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"\n{'=' * 60}")
    print(f"  Scraping complete!")
    print(f"  Total bids: {len(all_bids)}")
    print(f"  Matched bids: {len(matched)}")
    print(f"{'=' * 60}")

    print("\nSaving matched bids...")
    save_to_csv(matched, f"matched_bids_{timestamp}.csv")

    print("Saving all bids...")
    save_to_csv(all_bids, f"all_bids_{timestamp}.csv")

    data = {
        "scraped_at":      datetime.now(timezone.utc).isoformat(),
        "source":          "planetbids",
        "total_found":     len(all_bids),
        "total_matched":   len(matched),
        "match_threshold": MATCH_THRESHOLD,
        "bids":            matched,
    }

    print("Saving JSON...")
    save_to_json(data, f"matched_bids_{timestamp}.json")

    print(f"\n  {len(matched)} matched bids → {OUTPUT_DIR}/")
    print(f"  {len(all_bids)} total bids → {OUTPUT_DIR}/")

    return data


if __name__ == "__main__":
    if len(sys.argv) > 1:
        ids = set(sys.argv[1:])
        companies = [(cid, name) for cid, name in COMPANIES if cid in ids]
    else:
        companies = COMPANIES

    asyncio.run(scrape_all_companies(companies))