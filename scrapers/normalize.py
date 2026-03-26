"""
scrapers/normalize.py
─────────────────────────────────────────
Unified bid schema and per-source field mappers.
Every scraper's raw output passes through normalize_bids()
which returns a list of NormalizedBid dicts with identical keys.
"""

from datetime import datetime, timezone
from typing import Any


# ── Canonical field names ────────────────────────────────────────
NORMALIZED_FIELDS = [
    "title",
    "source",               # planetbids | bidnet | caleprocure | biddingo | opengov
    "organization",          # issuing agency / buyer / department
    "bid_number",            # solicitation number, event ID, financial ID
    "bid_type",              # RFP, IFB, RFQ, etc.
    "status",                # open, closed, awarded, etc.
    "description",           # scope / summary text
    "closing_date",          # due date as ISO string or original string
    "posted_date",           # issue / publication date
    "location",              # city, region, or state
    "contact_name",
    "contact_email",
    "contact_phone",
    "link",                  # URL to the bid detail page
    "match_score",           # numeric relevance score (0-100)
    "matched_keywords",      # comma-separated or list of matched terms
    "scraped_at",            # ISO timestamp
    "raw",                   # original dict preserved for debugging
]


def _clean(val: Any) -> str:
    """Coerce to stripped string, None → ''."""
    if val is None:
        return ""
    if isinstance(val, (list, dict)):
        return str(val)
    return str(val).strip()


def _clean_score(val: Any, scale: float = 1.0) -> float:
    """Normalize a score to 0-100 float."""
    try:
        v = float(val or 0)
        return round(min(100.0, v * scale), 2)
    except (ValueError, TypeError):
        return 0.0


# ═════════════════════════════════════════════════════════════════
#  PlanetBids
# ═════════════════════════════════════════════════════════════════
def _normalize_planetbids(bid: dict) -> dict:
    return {
        "title":            _clean(bid.get("title")),
        "source":           "planetbids",
        "organization":     _clean(bid.get("company_name")),
        "bid_number":       _clean(bid.get("invitation_number") or bid.get("bid_id")),
        "bid_type":         _clean(bid.get("bid_type_name")),
        "status":           _clean(bid.get("stage")),
        "description":      _clean(bid.get("scope")),
        "closing_date":     _clean(bid.get("bid_due_date")),
        "posted_date":      _clean(bid.get("issue_date")),
        "location":         _clean(bid.get("department")),
        "contact_name":     _clean(bid.get("contact_name_phone")),
        "contact_email":    _clean(bid.get("contact_email")),
        "contact_phone":    "",  # embedded in contact_name_phone
        "link":             f"https://vendors.planetbids.com/portal/{bid.get('company_id', '')}/bo/{bid.get('bid_id', '')}",
        "match_score":      _clean_score(bid.get("competency_score")),
        "matched_keywords": _clean(bid.get("competency_matches")),
        "scraped_at":       _clean(bid.get("scraped_at") or datetime.now(timezone.utc).isoformat()),
        "raw":              bid,
    }


# ═════════════════════════════════════════════════════════════════
#  BidNet Direct
# ═════════════════════════════════════════════════════════════════
def _normalize_bidnet(bid: dict) -> dict:
    # match_score from bidnet is 0.0–1.0 fractional
    return {
        "title":            _clean(bid.get("title")),
        "source":           "bidnet",
        "organization":     _clean(bid.get("issuing_organization")),
        "bid_number":       _clean(bid.get("solicitation_number") or bid.get("reference_number")),
        "bid_type":         _clean(bid.get("solicitation_type")),
        "status":           "",  # not provided by bidnet scraper
        "description":      _clean(bid.get("scope")),
        "closing_date":     _clean(bid.get("closing_date")),
        "posted_date":      _clean(bid.get("publication")),
        "location":         _clean(bid.get("location")),
        "contact_name":     _clean(bid.get("contact_name")),
        "contact_email":    _clean(bid.get("contact_email")),
        "contact_phone":    _clean(bid.get("contact_number")),
        "link":             _clean(bid.get("link")),
        "match_score":      _clean_score(bid.get("match_score"), scale=100),
        "matched_keywords": _clean(bid.get("matched_keywords")),
        "scraped_at":       _clean(bid.get("scraped_at") or datetime.now(timezone.utc).isoformat()),
        "raw":              bid,
    }


# ═════════════════════════════════════════════════════════════════
#  Cal eProcure
# ═════════════════════════════════════════════════════════════════
def _normalize_caleprocure(bid: dict) -> dict:
    eid = _clean(bid.get("event_id"))
    bu = _clean(bid.get("business_unit"))
    link = f"https://caleprocure.ca.gov/event/{bu}/{eid}" if bu and eid else ""

    return {
        "title":            _clean(bid.get("event_name")),
        "source":           "caleprocure",
        "organization":     _clean(bid.get("department")),
        "bid_number":       eid,
        "bid_type":         _clean(bid.get("event_format")),
        "status":           _clean(bid.get("status")),
        "description":      _clean(bid.get("description")),
        "closing_date":     _clean(bid.get("end_date")),
        "posted_date":      _clean(bid.get("published_date") or bid.get("start_date")),
        "location":         _clean(bid.get("service_areas")),
        "contact_name":     _clean(bid.get("contact_name")),
        "contact_email":    _clean(bid.get("contact_email")),
        "contact_phone":    _clean(bid.get("contact_phone")),
        "link":             link,
        "match_score":      0.0,  # caleprocure doesn't score — score downstream if needed
        "matched_keywords": "",
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
        "raw":              bid,
    }


# ═════════════════════════════════════════════════════════════════
#  Biddingo
# ═════════════════════════════════════════════════════════════════
def _normalize_biddingo(bid: dict) -> dict:
    bid_id = _clean(bid.get("bidId") or bid.get("tenderNumber"))
    return {
        "title":            _clean(bid.get("tenderName")),
        "source":           "biddingo",
        "organization":     _clean(bid.get("buyerName")),
        "bid_number":       bid_id,
        "bid_type":         _clean(bid.get("bidType")),
        "status":           _clean(bid.get("status")),
        "description":      "",  # biddingo list endpoint doesn't return descriptions
        "closing_date":     _clean(bid.get("closingDate")),
        "posted_date":      _clean(bid.get("postedDate")),
        "location":         _clean(bid.get("regionName")),
        "contact_name":     "",
        "contact_email":    "",
        "contact_phone":    "",
        "link":             "",  # would need to construct from bid ID + portal URL
        "match_score":      _clean_score(bid.get("match_score")),
        "matched_keywords": "",
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
        "raw":              bid,
    }


# ═════════════════════════════════════════════════════════════════
#  OpenGov
# ═════════════════════════════════════════════════════════════════
def _normalize_opengov(bid: dict) -> dict:
    pid = bid.get("id") or bid.get("financial_id") or ""
    link = f"https://procurement.opengov.com/portal/project/{pid}" if pid else ""

    return {
        "title":            _clean(bid.get("title")),
        "source":           "opengov",
        "organization":     _clean(bid.get("org_name") or bid.get("department_name")),
        "bid_number":       _clean(bid.get("financial_id") or bid.get("id")),
        "bid_type":         _clean(bid.get("type") or bid.get("template_title") or bid.get("procurement_classification")),
        "status":           _clean(bid.get("status")),
        "description":      _clean(bid.get("summary_text")),
        "closing_date":     _clean(bid.get("proposal_deadline") or bid.get("closed_at")),
        "posted_date":      _clean(bid.get("release_date") or bid.get("posted_at")),
        "location":         ", ".join(filter(None, [
            _clean(bid.get("contact_city")),
            _clean(bid.get("contact_state") or bid.get("org_state")),
        ])),
        "contact_name":     _clean(bid.get("contact_name")),
        "contact_email":    _clean(bid.get("contact_email")),
        "contact_phone":    _clean(bid.get("contact_phone")),
        "link":             link,
        "match_score":      0.0,
        "matched_keywords": "",
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
        "raw":              bid,
    }


# ═════════════════════════════════════════════════════════════════
#  Public API
# ═════════════════════════════════════════════════════════════════

_MAPPERS = {
    "planetbids":   _normalize_planetbids,
    "bidnet":       _normalize_bidnet,
    "caleprocure":  _normalize_caleprocure,
    "biddingo":     _normalize_biddingo,
    "opengov":      _normalize_opengov,
}


def normalize_bid(source: str, raw_bid: dict) -> dict:
    """Normalize a single bid dict from the given source."""
    mapper = _MAPPERS.get(source)
    if not mapper:
        raise ValueError(f"Unknown source: {source}. Valid: {list(_MAPPERS.keys())}")
    return mapper(raw_bid)


def normalize_bids(source: str, raw_bids: list[dict]) -> list[dict]:
    """Normalize a list of bid dicts from the given source."""
    mapper = _MAPPERS.get(source)
    if not mapper:
        raise ValueError(f"Unknown source: {source}. Valid: {list(_MAPPERS.keys())}")
    normalized = []
    for bid in raw_bids:
        try:
            normalized.append(mapper(bid))
        except Exception as e:
            print(f"  [normalize] Skipping bid from {source}: {e}")
            continue
    return normalized


def normalize_scraper_result(source: str, result: dict) -> dict:
    """
    Takes a full scraper result dict (with 'bids' list + metadata)
    and returns the same structure with bids normalized.
    Preserves source-level metadata like total_found, total_matched, etc.
    """
    raw_bids = result.get("bids", [])
    normalized = normalize_bids(source, raw_bids)

    return {
        "source":          source,
        "scraped_at":      result.get("scraped_at", datetime.now(timezone.utc).isoformat()),
        "total_found":     result.get("total_found", len(raw_bids)),
        "total_matched":   len(normalized),
        "bids":            normalized,
    }
