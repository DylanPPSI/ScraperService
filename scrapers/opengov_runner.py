"""
scrapers/opengov_runner.py
Wraps the existing opengov_scraper (sync) and normalizes output.
"""

import asyncio
from datetime import datetime, timezone
from scrapers.normalize import normalize_bids


async def run() -> dict:
    """Run OpenGov scraper in a thread and return normalized results."""
    print("[opengov_runner] Starting scrape...")

    try:
        loop = asyncio.get_event_loop()
        raw_bids = await loop.run_in_executor(None, _run_sync)
    except Exception as e:
        print(f"[opengov_runner] Scraper error: {e}")
        return {
            "source": "opengov",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "total_found": 0,
            "total_matched": 0,
            "bids": [],
            "error": str(e),
        }

    normalized = normalize_bids("opengov", raw_bids)

    result = {
        "source": "opengov",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_found": len(raw_bids),
        "total_matched": len(normalized),
        "bids": normalized,
    }
    print(f"[opengov_runner] Done. {len(normalized)} normalized bids.")
    return result


def _run_sync() -> list[dict]:
    """
    Runs the opengov scraper synchronously.

    The opengov scraper's __main__ block does login → scrape IDs →
    fetch details → save. We replicate that flow here to capture
    the details list directly.
    """
    import json
    import os
    import glob
    import requests

    try:
        from scrapers.opengov_scraper import (
            login, scrape_all_ids, fetch_all_details,
            BASE_HEADERS,
        )

        session = requests.Session()
        session.headers.update(BASE_HEADERS)

        if not login(session):
            raise RuntimeError("OpenGov login failed")

        project_ids = scrape_all_ids(session)
        if not project_ids:
            return []

        details = fetch_all_details(project_ids, session)
        return details

    except ImportError:
        # If individual functions aren't importable, try running main
        # and reading the output file
        try:
            from scrapers.opengov_scraper import main as opengov_main
            opengov_main()
        except Exception:
            pass

        # Read saved output
        json_path = "opengov_data/all_bid_details.json"
        if os.path.exists(json_path):
            print(f"[opengov_runner] Reading from {json_path}...")
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)

        return []

    except Exception as e:
        print(f"[opengov_runner] sync error: {e}")
        raise
