import asyncio
from supabase import create_client
from plantbids import run_scraper
from bidnet import run_scraper as run_bidnet
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

async def process_job(job):
    job_id = job["id"]

    await supabase.table("scraper_jobs")\
        .update({"status": "running", "progress": 10})\
        .eq("id", job_id).execute()

    try:
        if job["scraper"] == "planetbids":
            await run_scraper(job["company_id"], job["company_name"])

        if job["scraper"] == "bidnet":
            await run_bidnet()

        await supabase.table("scraper_jobs")\
            .update({"status": "completed", "progress": 100})\
            .eq("id", job_id).execute()

    except Exception as e:
        await supabase.table("scraper_jobs")\
            .update({"status": "failed", "message": str(e)})\
            .eq("id", job_id).execute()


async def worker_loop():
    while True:
        response = supabase.table("scraper_jobs")\
            .select("*")\
            .eq("status", "pending")\
            .limit(5)\
            .execute()

        jobs = response.data

        if jobs:
            await asyncio.gather(*(process_job(job) for job in jobs))

        await asyncio.sleep(5)


asyncio.run(worker_loop())
