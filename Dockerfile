# Dockerfile for PipelineIQ FastAPI Scraper Service
# Uses Playwright's official image which includes Chromium + all system deps

FROM mcr.microsoft.com/playwright/python:v1.49.0-noble

WORKDIR /app

# Copy requirements first for Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY scrapers/ ./scrapers/

# Playwright browsers are pre-installed in the base image,
# but ensure they're available
RUN playwright install chromium

# Create output directories the scrapers expect
RUN mkdir -p planetbids_data bidnet_data biddingo_data opengov_data caleprocure_data

# Render sets PORT env var automatically
ENV PORT=8000

EXPOSE 8000

# Start FastAPI with uvicorn
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT}