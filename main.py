# main.py
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from scrapers import planetbids

app = FastAPI()

class ScrapeRequest(BaseModel):
    company_id: str
    name: str

@app.get("/")
def health():
    return {"status": "running"}

@app.post("/start/planetbids")
async def start_scrape(data: ScrapeRequest, background_tasks: BackgroundTasks):
    # Run the scraper in background, return immediately
    background_tasks.add_task(
        planetbids.run_scraper,
        data.company_id,
        data.name
    )
    return {"status": "started"}
