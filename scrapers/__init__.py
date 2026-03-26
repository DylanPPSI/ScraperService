"""
scrapers/
─────────────────────────────────────────
Package exposing each scraper as an async function that returns
normalized bid data in a unified schema.

Directory layout:
  scrapers/
    __init__.py              ← this file
    normalize.py             ← field mapping + NormalizedBid schema
    planetbids_runner.py     ← wrapper for planetbids_scraper.py
    bidnet_runner.py         ← wrapper for bidnet_scraper.py
    caleprocure_runner.py    ← wrapper for caleprocure_scraper.py
    biddingo_runner.py       ← wrapper for biddingo_scraper.py
    opengov_runner.py        ← wrapper for opengov_scraper.py
    planetbids_scraper.py    ← your existing file (unchanged)
    bidnet_scraper.py        ← your existing file (unchanged)
    caleprocure_scraper.py   ← your existing file (minor return added)
    biddingo_scraper.py      ← your existing file (minor return added)
    opengov_scraper.py       ← your existing file (unchanged)
"""

from scrapers.planetbids_runner import run as run_planetbids
from scrapers.bidnet_runner import run as run_bidnet
from scrapers.caleprocure_runner import run as run_caleprocure
from scrapers.biddingo_runner import run as run_biddingo
from scrapers.opengov_runner import run as run_opengov

SCRAPERS = {
    "planetbids":   run_planetbids,
    "bidnet":       run_bidnet,
    "caleprocure":  run_caleprocure,
    "biddingo":     run_biddingo,
    "opengov":      run_opengov,
}

__all__ = [
    "SCRAPERS",
    "run_planetbids",
    "run_bidnet",
    "run_caleprocure",
    "run_biddingo",
    "run_opengov",
]