"""
BidNetDirect Scraper — Async Backend JSON Service (Playwright) + CSV Export
"""

import re
import os
import csv
import json
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Tuple
import time
from dotenv import load_dotenv

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

load_dotenv()

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BASE_URL   = "https://www.bidnetdirect.com"
SEARCH_URL = f"{BASE_URL}/private/supplier/solicitations/search"

NAV_TIMEOUT_MS     = 45000
SCRAPE_TAB         = "Open"
MATCH_THRESHOLD    = 0.20
DETAIL_CONCURRENCY = 4
PAGE_DELAY_SEC     = 1.0

BIDNET_USER = os.getenv("BIDNET_USER", "")
BIDNET_PASS = os.getenv("BIDNET_PASS", "")
HEADLESS    = True

OUTPUT_DIR = "bidnet_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Date filter: today → today + 30 days
_today    = datetime.now()
DATE_FROM = _today.strftime("%m/%d/%Y")
DATE_TO   = (_today + timedelta(days=30)).strftime("%m/%d/%Y")


# ─────────────────────────────────────────────
# Keywords
# ─────────────────────────────────────────────
CORE_KEYWORDS_RAW = """
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
"""

def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", s)
    s = re.sub(r"[^a-z0-9\s\-\/&]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_keyword_list(raw: str) -> List[str]:
    seen, out = set(), []
    for line in raw.splitlines():
        k = _normalize_text(line.strip())
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out

CORE_KEYWORDS = build_keyword_list(CORE_KEYWORDS_RAW)
print(f"[init] Loaded {len(CORE_KEYWORDS)} keywords.")

def score_bid(title: str, scope: str, keywords: List[str]) -> Dict[str, Any]:
    combined = _normalize_text(f"{title} {scope}")
    matched  = [kw for kw in keywords if kw and kw in combined]
    score    = len(matched) / max(1, len(keywords))
    return {
        "match_score":      score,
        "matched_keywords": matched,
        "matched_count":    len(matched),
        "keyword_count":    max(1, len(keywords)),
    }


# ─────────────────────────────────────────────
# Cookie / login helpers
# ─────────────────────────────────────────────
async def dismiss_cookie_banner(page) -> None:
    try:
        btn = page.locator("#cookieBannerRejectBtn").first
        if await btn.is_visible(timeout=2000):
            await btn.click()
            print("  [cookie] Banner dismissed.")
    except Exception:
        pass

async def is_login_form_present(page) -> bool:
    selectors = ["#j_username", 'input[name="j_username"]', 'input[type="email"]']
    for frame in [page.main_frame] + page.frames:
        for sel in selectors:
            try:
                loc = frame.locator(sel).first
                if await loc.is_visible(timeout=500):
                    print(f"  [login-check] Login form found in frame: {frame.url[:80]}")
                    return True
            except Exception:
                pass
    return False

async def _find_in_frames(page, selectors, timeout_ms=15000):
    deadline = asyncio.get_event_loop().time() + timeout_ms / 1000
    while asyncio.get_event_loop().time() < deadline:
        for frame in [page.main_frame] + page.frames:
            for sel in selectors:
                try:
                    loc = frame.locator(sel).first
                    if await loc.is_visible(timeout=300):
                        return frame, loc
                except Exception:
                    pass
        await page.wait_for_timeout(300)
    raise PWTimeoutError(f"Selectors not found: {selectors}")

async def login(page) -> None:
    print(f"  [login] Navigating to {SEARCH_URL} ...")
    await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_timeout(2000)
    print(f"  [login] Current URL: {page.url}")
    await dismiss_cookie_banner(page)

    if not await is_login_form_present(page):
        print("  [login] No login form — assuming already logged in.")
        return
    time.sleep(1)
    print("  [login] Filling credentials ...")
    user_frame, user_loc = await _find_in_frames(
        page, ["#j_username", 'input[name="j_username"]', 'input[type="email"]']
    )
    pass_frame, pass_loc = await _find_in_frames(
        page, ["#j_password", 'input[name="j_password"]', 'input[type="password"]']
    )
    await user_loc.fill(BIDNET_USER)
    await pass_loc.fill(BIDNET_PASS)
    print("  [login] Credentials entered.")

    submitted = False
    for sel in ["#loginButton", 'button[type="submit"]', 'input[type="submit"]']:
        try:
            btn = user_frame.locator(sel).first
            if await btn.is_visible(timeout=500):
                print(f"  [login] Clicking: {sel}")
                await btn.click()
                submitted = True
                break
        except Exception:
            pass
    if not submitted:
        await pass_loc.press("Enter")

    print("  [login] Waiting for /private/ redirect ...")
    try:
        await page.wait_for_url(
            re.compile(r".*bidnetdirect\.com/private/.*"), timeout=NAV_TIMEOUT_MS
        )
        print(f"  [login] ✅ Logged in. URL: {page.url}")
    except PWTimeoutError:
        print(f"  [login] ⚠️  Redirect timeout. URL: {page.url}")
        html = await page.content()
        fname = f"bidnet_debug_login_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"  [login] Saved: {fname}")

    await dismiss_cookie_banner(page)

async def ensure_logged_in(page) -> None:
    if await is_login_form_present(page):
        print("  [session] Re-logging in ...")
        await login(page)


# ─────────────────────────────────────────────
# Overlay helper
# ─────────────────────────────────────────────
async def hide_blocking_overlays(page) -> None:
    try:
        hidden = await page.evaluate("""
            () => {
                let count = 0;
                [
                    'div.mets-tab-view-body.inner-tab.advanced-search-inner-tab',
                    'div.ui-widget-overlay',
                    'div.ui-widget-overlay.ui-front',
                ].forEach(sel => {
                    document.querySelectorAll(sel).forEach(el => {
                        if (el.style.display !== 'none') {
                            el.style.display = 'none';
                            count++;
                        }
                    });
                });
                return count;
            }
        """)
        if hidden > 0:
            print(f"  [overlay] Hid {hidden} blocking element(s) via JS.")
    except Exception as e:
        print(f"  [overlay] JS hide error: {e}")


# ─────────────────────────────────────────────
# Step 1: Select California Purchasing Group
# ─────────────────────────────────────────────
async def select_california_purchasing_group(page) -> None:
    print("  [cpg] Selecting California Purchasing Group ...")

    cb_selectors = [
        'input[data-filter-item-value="88020151"]',
        '#g_654',
        'input[title="California Purchasing Group"]',
    ]

    cb_visible = False
    for sel in cb_selectors:
        try:
            if await page.locator(sel).first.is_visible(timeout=1500):
                cb_visible = True
                break
        except Exception:
            pass

    if not cb_visible:
        print("  [cpg] Checkbox not visible — expanding purchasing group panel ...")
        panel_toggles = [
            'xpath=//span[contains(normalize-space(),"Purchasing Group")]'
            '/ancestor::div[contains(@class,"mets-panel")]'
            '//span[contains(@class,"mets-panel-header")]',
            f'xpath=//input[@data-filter-item-value="88020151"]'
            f'/ancestor::div[contains(@class,"mets-panel")]'
            f'//span[contains(@class,"mets-panel-header")]',
        ]
        for toggle_sel in panel_toggles:
            try:
                toggle = page.locator(toggle_sel).first
                if await toggle.is_visible(timeout=2000):
                    await toggle.click(force=True)
                    await page.wait_for_timeout(600)
                    print("  [cpg] Panel expanded.")
                    break
            except Exception:
                pass
    time.sleep(3)

    for sel in cb_selectors:
        try:
            cb = page.locator(sel).first
            if await cb.is_visible(timeout=3000):
                checked = await cb.is_checked()
                if not checked:
                    await cb.check(force=True)
                    print("  [cpg] ✅ California Purchasing Group checked.")
                else:
                    print("  [cpg] ✅ California Purchasing Group already checked.")
                await page.wait_for_timeout(3000)
                return
        except Exception:
            pass

    print("  [cpg] ⚠️  Could not find/check California Purchasing Group checkbox.")


# ─────────────────────────────────────────────
# Step 2: Set results per page to 100
# ─────────────────────────────────────────────
async def set_results_per_page(page, per_page: int = 100) -> None:
    print(f"  [rpp] Setting results per page to {per_page} ...")

    sel_locator = page.locator("select[id*='mets-results-per-page-select']").first

    try:
        await sel_locator.wait_for(state="visible", timeout=10000)
    except Exception as e:
        print(f"  [rpp] ⚠️  Results-per-page select not found: {e}")
        return

    first_row_before = ""
    try:
        first_row_before = await page.locator(
            "table#solicitationsTable tr.mets-table-row"
        ).first.inner_text()
    except Exception:
        pass

    try:
        await sel_locator.select_option(value=str(per_page))
        print(f"  [rpp] Selected {per_page}. Waiting for table to update ...")
    except Exception as e:
        try:
            await sel_locator.select_option(label=str(per_page))
            print(f"  [rpp] Selected {per_page} by label. Waiting ...")
        except Exception as e2:
            print(f"  [rpp] ⚠️  Could not select {per_page}: {e} / {e2}")
            return

    for _ in range(40):
        await page.wait_for_timeout(500)
        try:
            new_text = await page.locator(
                "table#solicitationsTable tr.mets-table-row"
            ).first.inner_text()
            if new_text != first_row_before:
                n = await page.locator("table#solicitationsTable tr.mets-table-row").count()
                print(f"  [rpp] ✅ Table updated — {n} rows now visible.")
                return
        except Exception:
            pass

    print("  [rpp] Table unchanged — proceeding.")


# ─────────────────────────────────────────────
# Date filter
# ─────────────────────────────────────────────
async def set_date_filter(page, date_from: str, date_to: str) -> None:
    print(f"  [date-filter] Setting closing date: {date_from} -> {date_to}")

    try:
        panel   = page.locator("#panel_closingDate")
        arrow   = panel.locator("svg.svg-arrow-right").first
        classes = await arrow.get_attribute("class") or ""
        if "expanded" not in classes:
            print("  [date-filter] Expanding panel ...")
            await panel.locator("span.mets-panel-header").first.click(force=True)
            await page.wait_for_timeout(600)
        else:
            print("  [date-filter] Panel already expanded.")
    except Exception as e:
        print(f"  [date-filter] Panel expand: {e}")

    try:
        cb = page.locator("#closingDateCheckRANGE").first
        await cb.wait_for(state="attached", timeout=8000)
        if not await cb.is_checked():
            await cb.check(force=True)
            print("  [date-filter] ✅ Checkbox checked.")
        else:
            print("  [date-filter] Checkbox already checked.")
        await page.wait_for_timeout(400)
    except Exception as e:
        print(f"  [date-filter] #closingDateCheckRANGE not found ({e}), trying span.checkbox ...")
        try:
            await page.locator("#panel_closingDate span.checkbox").first.click(force=True)
            await page.wait_for_timeout(400)
        except Exception as e2:
            print(f"  [date-filter] Checkbox fallback also failed: {e2}")

    for field_id, value, label in [
        ("closingDateRANGE1", date_from, "start"),
        ("closingDateRANGE2", date_to,   "end"),
    ]:
        try:
            result = await page.evaluate("""
                ([id, val]) => {
                    const el = document.getElementById(id);
                    if (!el) return 'not found: ' + id;
                    el.disabled = false;
                    el.removeAttribute('disabled');
                    el.removeAttribute('readonly');
                    el.value = val;
                    el.dispatchEvent(new Event('input',  { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new Event('blur',   { bubbles: true }));
                    if (typeof updateDateStatus === 'function') updateDateStatus('closingDate');
                    return 'ok: ' + el.value;
                }
            """, [field_id, value])
            print(f"  [date-filter] {label}: {result}")
        except Exception as e:
            print(f"  [date-filter] {label} JS error: {e}")

    await page.wait_for_timeout(300)

    print("  [date-filter] Clicking Search ...")
    first_row_before = ""
    try:
        first_row_before = await page.locator(
            "table#solicitationsTable tr.mets-table-row"
        ).first.inner_text()
    except Exception:
        pass

    await hide_blocking_overlays(page)

    for sel in ['button:has-text("Search")', 'input[type="submit"][value="Search"]', 'button[type="submit"]']:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=2000):
                await btn.click(force=True)
                print(f"  [date-filter] Search clicked. Waiting for table ...")
                for _ in range(30):
                    await page.wait_for_timeout(500)
                    try:
                        new_text = await page.locator(
                            "table#solicitationsTable tr.mets-table-row"
                        ).first.inner_text()
                        if new_text != first_row_before:
                            n = await page.locator("table#solicitationsTable tr.mets-table-row").count()
                            print(f"  [date-filter] ✅ Table updated — {n} rows.")
                            return
                    except Exception:
                        pass
                print("  [date-filter] Table unchanged after Search — proceeding.")
                return
        except Exception as e:
            print(f"  [date-filter] ({sel}): {e}")

    print("  [date-filter] ⚠️  Search button not found.")


# ─────────────────────────────────────────────
# Ensure rows loaded (fallback)
# ─────────────────────────────────────────────
async def row_count(page) -> int:
    try:
        return await page.locator("table#solicitationsTable tr.mets-table-row").count()
    except Exception:
        return 0

async def ensure_results_loaded(page, debug_tag: str) -> None:
    await ensure_logged_in(page)
    await dismiss_cookie_banner(page)

    print(f"  [rows] Waiting for table ... (tag={debug_tag})")
    try:
        await page.wait_for_selector("table#solicitationsTable", timeout=15000)
        print("  [rows] Table found.")
    except Exception:
        print("  [rows] ⚠️  Table not found after 15s.")

    for i in range(10):
        n = await row_count(page)
        if n > 0:
            print(f"  [rows] ✅ {n} rows on poll #{i+1}.")
            return
        await page.wait_for_timeout(500)

    html = await page.content()
    fname = f"bidnet_debug_{debug_tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  [rows] ⚠️  No rows. Debug HTML: {fname}")
    print(f"  [rows] URL: {page.url} | Title: {await page.title()}")
    print(f"  [rows] Body: {(await page.locator('body').inner_text())[:500]}")


# ─────────────────────────────────────────────
# Pagination & parsing
# ─────────────────────────────────────────────
def parse_list_page_for_links(html: str) -> List[Dict[str, str]]:
    soup  = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "solicitationsTable"})
    if not table:
        print("  [parse] ⚠️  No solicitationsTable found.")
        return []
    rows = table.find_all("tr", class_="mets-table-row")
    print(f"  [parse] {len(rows)} rows found.")
    out = []
    for row in rows:
        title_tag = row.find("a", class_="solicitationsTitleLink")
        title     = title_tag.get_text(strip=True) if title_tag else ""
        link      = title_tag.get("href", "") if title_tag else ""
        cd        = row.find("span", class_="dateValue")
        loc       = row.find("span", class_="regionValue")
        if title and link:
            out.append({
                "title":        title,
                "link":         link,
                "closing_date": cd.get_text(strip=True) if cd else "",
                "location":     loc.get_text(strip=True) if loc else "",
            })
    return out

async def paginate_and_collect(page) -> List[Dict[str, str]]:
    all_items: List[Dict[str, str]] = []
    page_num = 1

    await ensure_results_loaded(page, debug_tag="firstpage")

    while True:
        await ensure_logged_in(page)
        await dismiss_cookie_banner(page)

        print(f"  [paginate] Parsing page {page_num} ...")
        items     = parse_list_page_for_links(await page.content())
        existing  = {i["link"] for i in all_items}
        new_items = [i for i in items if i["link"] not in existing]
        all_items.extend(new_items)
        print(f"  [paginate] Page {page_num}: {len(new_items)} new. Total: {len(all_items)}")

        try:
            next_btn = page.locator('a.next.mets-pagination-page-icon[rel="next"]').first
            if not await next_btn.is_visible(timeout=2000):
                print("  [paginate] No next-page button — done.")
                break

            href = await next_btn.get_attribute("href") or ""
            if not href:
                print("  [paginate] Next button has no href — done.")
                break

            next_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            first_row_text = await page.locator(
                "table#solicitationsTable tr.mets-table-row"
            ).first.inner_text()

            print(f"  [paginate] Going to page {page_num + 1} via href ...")
            await page.goto(next_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)

            for _ in range(40):
                await page.wait_for_timeout(500)
                try:
                    new_text = await page.locator(
                        "table#solicitationsTable tr.mets-table-row"
                    ).first.inner_text()
                    if new_text and new_text != first_row_text:
                        print(f"  [paginate] Page {page_num + 1} loaded.")
                        break
                except Exception:
                    pass
            else:
                print("  [paginate] Rows unchanged — last page.")
                break

            await page.wait_for_timeout(int(PAGE_DELAY_SEC * 1000))
            page_num += 1
        except Exception as e:
            print(f"  [paginate] Ended: {e}")
            break

    return all_items


# ─────────────────────────────────────────────
# Detail scraping
# ─────────────────────────────────────────────
SCRAPE_LABELS = [
    "Reference Number", "Issuing Organization", "Solicitation Type",
    "Solicitation Number", "Title", "Source ID", "Location",
    "Purchase Type", "Piggyback Contract", "Publication", "Closing Date",
]

def soup_field_value(soup, label_text):
    label = soup.find("span", string=lambda s: s and label_text in s)
    if not label:
        return ""
    body = label.find_next("div", class_="mets-field-body")
    return body.get_text(strip=True) if body else ""

def soup_contact_info(soup) -> Tuple[str, str, str]:
    h = soup.find("h3", string=lambda s: s and "Contact Information" in s)
    if not h:
        return "", "", ""
    bodies, nxt = [], h
    while len(bodies) < 3:
        nxt = nxt.find_next("div", class_="mets-field-body")
        if not nxt:
            break
        bodies.append(nxt)
    return (
        bodies[0].get_text(strip=True) if len(bodies) > 0 else "",
        bodies[1].get_text(strip=True) if len(bodies) > 1 else "",
        bodies[2].get_text(strip=True) if len(bodies) > 2 else "",
    )

def soup_description(soup) -> str:
    label = soup.find("span", string=lambda s: s and "Description" in s)
    if not label:
        return ""
    parent = label.find_parent("div", class_="mets-field")
    if not parent:
        return ""
    desc = parent.find("span", id="descriptionText")
    return desc.get_text(strip=True) if desc else ""

def soup_bid_docs_link(soup) -> str:
    div = soup.find("div", class_="noticeExternalUrl")
    if not div:
        return ""
    a = div.find("a")
    return a.get("href", "") if a and a.has_attr("href") else ""

async def scrape_detail(page, item: Dict[str, str]) -> Dict[str, Any]:
    await ensure_logged_in(page)
    link = item["link"]
    url  = link if link.startswith("http") else f"{BASE_URL}{link}"
    print(f"    [detail] {url[:80]}")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except PWTimeoutError:
        print(f"    [detail] Timeout on goto — scraping whatever loaded.")
    except Exception as e:
        print(f"    [detail] goto error: {e}")

    await dismiss_cookie_banner(page)
    await page.wait_for_timeout(2000)

    try:
        await page.locator("a#descriptionTextReadMore.mets-command-link.read-more").first.click(timeout=1500)
        await page.wait_for_timeout(300)
    except Exception:
        pass

    soup   = BeautifulSoup(await page.content(), "html.parser")
    fields = {lbl: soup_field_value(soup, lbl) for lbl in SCRAPE_LABELS}
    contact_name, contact_number, contact_email = soup_contact_info(soup)
    description   = soup_description(soup)
    bid_docs_link = soup_bid_docs_link(soup)
    title = fields.get("Title") or item.get("title") or ""
    scope = description or ""
    match = score_bid(title, scope, CORE_KEYWORDS)
    print(f"    [detail] '{title[:50]}' score={match['match_score']:.3f}")

    return {
        "title":                title,
        "scope":                scope,
        "closing_date":         fields.get("Closing Date") or item.get("closing_date") or "",
        "location":             fields.get("Location") or item.get("location") or "",
        "link":                 url,
        "reference_number":     fields.get("Reference Number", ""),
        "issuing_organization": fields.get("Issuing Organization", ""),
        "solicitation_type":    fields.get("Solicitation Type", ""),
        "solicitation_number":  fields.get("Solicitation Number", ""),
        "source_id":            fields.get("Source ID", ""),
        "purchase_type":        fields.get("Purchase Type", ""),
        "piggyback_contract":   fields.get("Piggyback Contract", ""),
        "publication":          fields.get("Publication", ""),
        "contact_name":         contact_name,
        "contact_number":       contact_number,
        "contact_email":        contact_email,
        "bid_docs_link":        bid_docs_link,
        "match_score":          match["match_score"],
        "matched_keywords":     ", ".join(match["matched_keywords"]),
        "matched_count":        match["matched_count"],
        "keyword_count":        match["keyword_count"],
        "scraped_at":           datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────
# CSV / JSON save helpers
# ─────────────────────────────────────────────
def save_to_csv(bids: List[Dict], filename: str) -> None:
    if not bids:
        print(f"  No bids to save for {filename}.")
        return

    # Priority column order — most useful fields first
    priority = [
        "match_score", "matched_count", "matched_keywords",
        "title", "issuing_organization", "solicitation_type",
        "solicitation_number", "reference_number",
        "closing_date", "location", "contact_name",
        "contact_email", "contact_number",
        "link", "bid_docs_link",
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
                else:
                    flat[k] = v
            writer.writerow(flat)
    print(f"  CSV → {path}")


def save_to_json(data: Any, filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    print(f"  JSON → {path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def scrape_bidnetdirect() -> Dict[str, Any]:
    print("Bootstrapping session...")
    print(f"[main] Date filter: {DATE_FROM} -> {DATE_TO}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        print("[main] Logging in ...")
        await login(page)

        print("[main] Selecting California Purchasing Group ...")
        await select_california_purchasing_group(page)

        print(f"[main] Applying date filter ({DATE_FROM} -> {DATE_TO}) ...")
        await set_date_filter(page, DATE_FROM, DATE_TO)

        print("[main] Setting results per page to 100 ...")
        await set_results_per_page(page, 100)

        print(f"[main] Collecting solicitations ...")
        items = await paginate_and_collect(page)
        print(f"[main] Found {len(items)} solicitations.")

        if not items:
            await browser.close()
            return {
                "scraped_at":      datetime.now(timezone.utc).isoformat(),
                "source":          "bidnetdirect",
                "tab":             SCRAPE_TAB,
                "date_from":       DATE_FROM,
                "date_to":         DATE_TO,
                "total_found":     0,
                "total_matched":   0,
                "match_threshold": MATCH_THRESHOLD,
                "bids":            [],
            }

        print(f"[main] Scraping {len(items)} detail pages ...")
        sem     = asyncio.Semaphore(DETAIL_CONCURRENCY)
        results: List[Dict[str, Any]] = []

        async def worker(it):
            async with sem:
                dp = await context.new_page()
                try:
                    return await scrape_detail(dp, it)
                except Exception as e:
                    print(f"    [detail] ⚠️  {it.get('link','?')}: {e}")
                    return {"error": str(e), "scraped_at": datetime.now(timezone.utc).isoformat()}
                finally:
                    await dp.close()

        tasks = [asyncio.create_task(worker(it)) for it in items]
        for coro in asyncio.as_completed(tasks):
            bid = await coro
            if bid:
                results.append(bid)

        await browser.close()

    matched = [
        b for b in results
        if isinstance(b, dict) and b.get("match_score", 0.0) >= MATCH_THRESHOLD
    ]
    all_scraped = [b for b in results if isinstance(b, dict) and "error" not in b]

    print(f"[main] Done. {len(results)} scraped, {len(matched)} matched.")

    # ── Save outputs ─────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\nSaving matched bids...")
    save_to_csv(matched, f"matched_bids_{timestamp}.csv")

    print("Saving all scraped bids...")
    save_to_csv(all_scraped, f"all_bids_{timestamp}.csv")

    data = {
        "scraped_at":      datetime.now(timezone.utc).isoformat(),
        "source":          "bidnetdirect",
        "tab":             SCRAPE_TAB,
        "date_from":       DATE_FROM,
        "date_to":         DATE_TO,
        "total_found":     len(results),
        "total_matched":   len(matched),
        "match_threshold": MATCH_THRESHOLD,
        "bids":            matched,
    }

    print("Saving JSON...")
    save_to_json(data, f"matched_bids_{timestamp}.json")

    print(f"\n{'=' * 60}")
    print(f"  {len(matched)} matched bids saved to {OUTPUT_DIR}/")
    print(f"  {len(all_scraped)} total bids saved to {OUTPUT_DIR}/")
    print(f"{'=' * 60}")

    return data


if __name__ == "__main__":
    result = asyncio.run(scrape_bidnetdirect())