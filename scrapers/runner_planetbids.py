"""
scrapers/planetbids_runner.py
Wraps the existing planetbids_scraper and normalizes output.
"""

import asyncio
from datetime import datetime, timezone
from scrapers.normalize import normalize_scraper_result


async def run() -> dict:
    """Run PlanetBids scraper and return normalized results."""
    # Import here to avoid loading Playwright at module level
    from scrapers.planetbids_scraper import scrape_all_companies

    print("[planetbids_runner] Starting scrape...")

    try:
        # scrape_all_companies is already async and returns a dict with:
        #   { scraped_at, source, total_found, total_matched, bids: [...] }
        raw_result = await scrape_all_companies()
    except Exception as e:
        print(f"[planetbids_runner] Scraper error: {e}")
        return {
            "source": "planetbids",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "total_found": 0,
            "total_matched": 0,
            "bids": [],
            "error": str(e),
        }

    normalized = normalize_scraper_result("planetbids", raw_result)
    print(f"[planetbids_runner] Done. {normalized['total_matched']} normalized bids.")
    return normalized
