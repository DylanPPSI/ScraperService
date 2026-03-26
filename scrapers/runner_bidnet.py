"""
scrapers/bidnet_runner.py
Wraps the existing bidnet_scraper and normalizes output.
"""

from datetime import datetime, timezone
from scrapers.normalize import normalize_scraper_result


async def run() -> dict:
    """Run BidNet Direct scraper and return normalized results."""
    from scrapers.bidnet_scraper import scrape_bidnetdirect

    print("[bidnet_runner] Starting scrape...")

    try:
        # scrape_bidnetdirect is already async and returns a dict with:
        #   { scraped_at, source, tab, date_from, date_to, total_found, total_matched, bids }
        raw_result = await scrape_bidnetdirect()
    except Exception as e:
        print(f"[bidnet_runner] Scraper error: {e}")
        return {
            "source": "bidnet",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "total_found": 0,
            "total_matched": 0,
            "bids": [],
            "error": str(e),
        }

    normalized = normalize_scraper_result("bidnet", raw_result)
    print(f"[bidnet_runner] Done. {normalized['total_matched']} normalized bids.")
    return normalized
