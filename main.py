"""
main.py — FastAPI Scraper Service
─────────────────────────────────────────
Runs Python scrapers via the scrapers/ package, tracks progress via SSE,
and returns normalized bid data to the Express backend.

Start with:
    uvicorn main:app --port 8000 --reload
"""

import asyncio
import uuid
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Import the scrapers package ──────────────────────────────────
from scrapers import SCRAPERS

app = FastAPI(title="PipelineIQ Scraper Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory job store ──────────────────────────────────────────
# In production, swap for Redis or a DB table
jobs: dict[str, dict[str, Any]] = {}


class ScrapeRequest(BaseModel):
    sources: list[str]  # e.g. ["planetbids", "bidnet", "caleprocure", "biddingo", "opengov"]


# ── Health ───────────────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "running", "service": "pipelineiq-scraper"}


# ── Start scrape job ─────────────────────────────────────────────
@app.post("/api/scrape")
async def start_scrape(req: ScrapeRequest, background_tasks: BackgroundTasks):
    if not req.sources:
        raise HTTPException(400, "No sources provided")

    valid = set(SCRAPERS.keys())
    invalid = set(req.sources) - valid
    if invalid:
        raise HTTPException(400, f"Unknown sources: {invalid}. Valid: {sorted(valid)}")

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "progress": 0,
        "sources_total": len(req.sources),
        "sources_completed": 0,
        "current_source": None,
        "results": {},
        "error": None,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    }

    background_tasks.add_task(run_scrape_job, job_id, req.sources)
    return {"job_id": job_id, "status": "pending"}


# ── Poll job status ──────────────────────────────────────────────
@app.get("/api/scrape/{job_id}")
async def get_job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    # Strip raw dicts from the status response to keep it light
    return _strip_raw(job)


# ── SSE progress stream ─────────────────────────────────────────
@app.get("/api/scrape/{job_id}/stream")
async def stream_job_progress(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    async def event_generator():
        last_progress = -1
        last_status = ""
        while True:
            j = jobs.get(job_id, {})
            current_progress = j.get("progress", 0)
            current_status = j.get("status", "")

            if current_progress != last_progress or current_status != last_status:
                data = json.dumps({
                    "job_id": job_id,
                    "status": current_status,
                    "progress": current_progress,
                    "current_source": j.get("current_source"),
                    "sources_completed": j.get("sources_completed", 0),
                    "sources_total": j.get("sources_total", 0),
                })
                yield f"data: {data}\n\n"
                last_progress = current_progress
                last_status = current_status

            if current_status in ("completed", "failed"):
                # Send final summary (without full bid data to keep stream light)
                summary = _strip_raw(j)
                # Include per-source counts instead of full bids
                summary_results = {}
                for src, res in j.get("results", {}).items():
                    summary_results[src] = {
                        "total_found": res.get("total_found", 0),
                        "total_matched": res.get("total_matched", 0),
                        "error": res.get("error"),
                    }
                summary["results"] = summary_results
                yield f"data: {json.dumps(summary)}\n\n"
                break

            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


# ── Get full results for a specific source ───────────────────────
@app.get("/api/scrape/{job_id}/results/{source}")
async def get_source_results(job_id: str, source: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    results = job.get("results", {}).get(source)
    if results is None:
        raise HTTPException(404, f"No results for source '{source}'")
    # Return full normalized bids (with raw stripped to save bandwidth)
    return _strip_raw_from_result(results)


# ── Get ALL results across all sources (merged) ─────────────────
@app.get("/api/scrape/{job_id}/results")
async def get_all_results(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] != "completed":
        raise HTTPException(400, f"Job status is '{job['status']}', not 'completed'")

    all_bids = []
    per_source = {}

    for source, result in job.get("results", {}).items():
        bids = result.get("bids", [])
        # Strip raw from each bid to reduce payload
        clean_bids = [{k: v for k, v in b.items() if k != "raw"} for b in bids]
        all_bids.extend(clean_bids)
        per_source[source] = {
            "total_found": result.get("total_found", 0),
            "total_matched": result.get("total_matched", 0),
            "error": result.get("error"),
        }

    return {
        "job_id": job_id,
        "scraped_at": job.get("finished_at"),
        "total_bids": len(all_bids),
        "per_source": per_source,
        "bids": all_bids,
    }


# ── Background scrape runner ─────────────────────────────────────
async def run_scrape_job(job_id: str, sources: list[str]):
    job = jobs[job_id]
    job["status"] = "running"

    for i, source in enumerate(sources):
        job["current_source"] = source
        job["progress"] = int((i / len(sources)) * 100)

        try:
            runner = SCRAPERS.get(source)
            if not runner:
                job["results"][source] = {"error": f"No scraper for '{source}'"}
                continue

            print(f"[job {job_id}] Running {source}...")
            result = await runner()
            job["results"][source] = result
            print(f"[job {job_id}] {source} done: {result.get('total_matched', 0)} matched bids")

        except Exception as e:
            print(f"[job {job_id}] Error scraping {source}: {e}")
            job["results"][source] = {
                "source": source,
                "error": str(e),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "total_found": 0,
                "total_matched": 0,
                "bids": [],
            }

        job["sources_completed"] = i + 1
        job["progress"] = int(((i + 1) / len(sources)) * 100)

    job["status"] = "completed"
    job["current_source"] = None
    job["progress"] = 100
    job["finished_at"] = datetime.now(timezone.utc).isoformat()


# ── List available scrapers ──────────────────────────────────────
@app.get("/api/scrapers")
def list_scrapers():
    return {
        "scrapers": [
            {"id": "planetbids",   "label": "PlanetBids"},
            {"id": "bidnet",       "label": "BidNet Direct"},
            {"id": "caleprocure",  "label": "Cal eProcure"},
            {"id": "biddingo",     "label": "Biddingo"},
            {"id": "opengov",      "label": "OpenGov"},
        ]
    }


# ── Helpers ──────────────────────────────────────────────────────
def _strip_raw(job: dict) -> dict:
    """Remove 'raw' and full 'bids' from job dict for lightweight responses."""
    copy = {k: v for k, v in job.items() if k != "results"}
    copy["results"] = {}
    for src, res in job.get("results", {}).items():
        copy["results"][src] = {
            "source": res.get("source", src),
            "total_found": res.get("total_found", 0),
            "total_matched": res.get("total_matched", 0),
            "error": res.get("error"),
        }
    return copy


def _strip_raw_from_result(result: dict) -> dict:
    """Strip 'raw' from each bid in a source result."""
    copy = {k: v for k, v in result.items() if k != "bids"}
    copy["bids"] = [
        {k: v for k, v in bid.items() if k != "raw"}
        for bid in result.get("bids", [])
    ]
    return copy