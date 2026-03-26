"""
scrapers/caleprocure_runner.py
Wraps the existing caleprocure_scraper (sync) and normalizes output.
"""

import asyncio
from datetime import datetime, timezone
from scrapers.normalize import normalize_bids


async def run() -> dict:
    """Run Cal eProcure scraper in a thread and return normalized results."""
    from scrapers.caleprocure_scraper import main as caleprocure_main

    print("[caleprocure_runner] Starting scrape...")

    try:
        # caleprocure's main() is synchronous and uses sync Playwright.
        # We need to modify it slightly to return data. See note below.
        # Run in executor to avoid blocking the event loop.
        loop = asyncio.get_event_loop()
        raw_events = await loop.run_in_executor(None, _run_sync)
    except Exception as e:
        print(f"[caleprocure_runner] Scraper error: {e}")
        return {
            "source": "caleprocure",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "total_found": 0,
            "total_matched": 0,
            "bids": [],
            "error": str(e),
        }

    normalized = normalize_bids("caleprocure", raw_events)

    result = {
        "source": "caleprocure",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_found": len(raw_events),
        "total_matched": len(normalized),
        "bids": normalized,
    }
    print(f"[caleprocure_runner] Done. {len(normalized)} normalized bids.")
    return result


def _run_sync() -> list[dict]:
    """
    Runs the caleprocure scraper synchronously and returns the event list.

    IMPORTANT: You need to make ONE small change to your caleprocure_scraper.py:
    At the end of main(), add `return detailed` before the final print block.
    The existing main() prints results but doesn't return them.

    If you haven't made that change yet, this falls back to reading
    the saved JSON file.
    """
    import json
    import os

    try:
        from scrapers.caleprocure_scraper import main as caleprocure_main
        result = caleprocure_main()

        # If main() returns the list, use it directly
        if isinstance(result, list):
            return result

        # Fallback: read the JSON file the scraper saves
        json_path = "caleprocure_events.json"
        if os.path.exists(json_path):
            print("[caleprocure_runner] Reading from saved JSON file...")
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)

        return []

    except Exception as e:
        print(f"[caleprocure_runner] sync error: {e}")
        raise
