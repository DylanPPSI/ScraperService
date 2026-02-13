from fastapi import FastAPI
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
async def start_scrape(data: ScrapeRequest):
    result = await planetbids.run_scraper(
        company_id=data.company_id,
        name=data.name
    )
    return {"status": "complete", "records": len(result)}
