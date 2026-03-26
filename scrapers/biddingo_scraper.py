import os
import csv
import json
import requests
from datetime import datetime, timezone

# Common request headers
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.biddingo.com",
    "Referer": "https://www.biddingo.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
}

# ── Matching config ──────────────────────────────────────────────
MATCH_THRESHOLD = 1

KEYWORDS = [
    "construction", "maintenance", "janitorial",
    "plumbing", "electrical", "HVAC", "landscaping",
]

OUTPUT_DIR = "biddingo_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def keyword_score(bid, keywords):
    searchable = " ".join([
        bid.get("tenderName") or "",
        bid.get("buyerName") or "",
        bid.get("bidType") or "",
        bid.get("regionName") or "",
    ]).lower()
    return sum(1 for kw in keywords if kw.lower() in searchable)


def fetch_bids(base_url, org_id, step=100, max_pages=50):
    all_bids = []
    start = 0

    for _ in range(max_pages):
        payload = {
            "startResult": start,
            "maxRow": step,
            "filterRegionId": [],
            "filterCategoryId": [],
            "filterStatus": [],
            "closingDateStart": "",
            "closingDateEnd": "",
            "postedDateStart": "",
            "postedDateEnd": "",
            "selectedRegionId": [],
            "showOnlyResearchBid": False,
            "searchString": "",
            "startDate": "",
            "endDate": "",
            "searchType": "closing",
            "selectedChildOrgIdList": [],
            "sortType": "",
        }

        url = f"{base_url}/restapi/bidding/list/noauthorize/1/{org_id}"
        response = requests.post(url, headers=HEADERS, json=payload)

        if response.status_code != 200:
            print(f"Error: {response.status_code} → {response.text}")
            break

        data = response.json()
        bids = data.get("bidInfoList", [])
        if not bids:
            break

        all_bids.extend(bids)
        print(f"Fetched {len(bids)} bids (start={start})")
        start += 1

    return all_bids


def save_to_csv(bids, filename):
    if not bids:
        print("  No bids to save.")
        return

    # Collect all keys across all bid dicts, put common ones first
    priority_keys = [
        "match_score", "tenderName", "buyerName", "bidType",
        "regionName", "closingDate", "postedDate", "status",
        "tenderNumber", "bidId",
    ]
    all_keys = set()
    for b in bids:
        all_keys.update(b.keys())

    # Ordered: priority keys first, then the rest alphabetically
    ordered = [k for k in priority_keys if k in all_keys]
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


def main():
    biddingo_base = "https://api.biddingo.com"
    biddingousa_base = "https://api.biddingousa.com"

    san_jose_bids = fetch_bids(biddingo_base, "41183311", step=10)
    santa_clara_bids = fetch_bids(biddingousa_base, "41284411", step=10)
    all_bids = san_jose_bids + santa_clara_bids

    print(f"\nTotal bids fetched: {len(all_bids)}")

    # ── Keyword matching ─────────────────────────────────────────
    matched = []
    for bid in all_bids:
        score = keyword_score(bid, KEYWORDS)
        if score >= MATCH_THRESHOLD:
            bid["match_score"] = score
            matched.append(bid)

    print(f"Matched bids: {len(matched)} / {len(all_bids)}")

    # ── Save CSVs ────────────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\nSaving matched bids...")
    save_to_csv(matched, f"matched_bids_{timestamp}.csv")

    print("Saving all bids...")
    save_to_csv(all_bids, f"all_bids_{timestamp}.csv")

    # ── Save JSON too ────────────────────────────────────────────
    json_path = os.path.join(OUTPUT_DIR, f"matched_bids_{timestamp}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": "biddingo",
            "total_found": len(all_bids),
            "total_matched": len(matched),
            "match_threshold": MATCH_THRESHOLD,
            "keywords": KEYWORDS,
            "bids": matched,
        }, f, indent=2, default=str, ensure_ascii=False)
    print(f"  JSON → {json_path}")

    print(f"\nDone! {len(matched)} matched bids saved to biddingo_data/")


if __name__ == "__main__":
    main()