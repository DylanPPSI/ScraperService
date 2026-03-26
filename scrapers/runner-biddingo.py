"""
scrapers/biddingo_runner.py
Wraps the existing biddingo_scraper (sync) and normalizes output.
"""

import asyncio
from datetime import datetime, timezone
from scrapers.normalize import normalize_bids


async def run() -> dict:
    """Run Biddingo scraper in a thread and return normalized results."""
    print("[biddingo_runner] Starting scrape...")

    try:
        loop = asyncio.get_event_loop()
        raw_result = await loop.run_in_executor(None, _run_sync)
    except Exception as e:
        print(f"[biddingo_runner] Scraper error: {e}")
        return {
            "source": "biddingo",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "total_found": 0,
            "total_matched": 0,
            "bids": [],
            "error": str(e),
        }

    all_bids = raw_result.get("all_bids", [])
    matched_bids = raw_result.get("matched_bids", [])

    # Normalize the matched bids (they have match_score already)
    normalized = normalize_bids("biddingo", matched_bids)

    result = {
        "source": "biddingo",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_found": len(all_bids),
        "total_matched": len(normalized),
        "bids": normalized,
    }
    print(f"[biddingo_runner] Done. {len(normalized)} normalized bids.")
    return result


def _run_sync() -> dict:
    """
    Runs the biddingo scraper synchronously.

    IMPORTANT: You need to make ONE small change to your biddingo_scraper.py:
    At the end of main(), add:
        return {"all_bids": all_bids, "matched_bids": matched}

    If you haven't made that change yet, this falls back to reading
    the saved JSON file.
    """
    import json
    import os
    import glob

    try:
        from scrapers.biddingo_scraper import main as biddingo_main
        result = biddingo_main()

        if isinstance(result, dict) and "all_bids" in result:
            return result
        if isinstance(result, dict) and "bids" in result:
            return {"all_bids": result.get("bids", []), "matched_bids": result.get("bids", [])}

        # Fallback: read the most recent JSON the scraper saved
        pattern = "biddingo_data/matched_bids_*.json"
        files = sorted(glob.glob(pattern), reverse=True)
        if files:
            print(f"[biddingo_runner] Reading from {files[0]}...")
            with open(files[0], "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "all_bids": data.get("bids", []),
                "matched_bids": data.get("bids", []),
            }

        return {"all_bids": [], "matched_bids": []}

    except Exception as e:
        print(f"[biddingo_runner] sync error: {e}")
        raise
