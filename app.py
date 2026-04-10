import os
import re
import time
import logging
from datetime import datetime, timezone
from serpapi import GoogleSearch
from supabase import create_client, Client
from flask import Flask, jsonify
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

SEARCH_QUERIES = [
    "OneStream developer jobs USA",
    "OneStream consultant jobs United States",
    "OneStream architect jobs USA",
    "OneStream EPM jobs United States",
    "OneStream financial systems jobs USA",
    "OneStream implementation jobs United States",
    "OneStream administrator jobs USA",
    "OneStream planning analyst jobs United States",
    "CPM OneStream jobs USA",
    "OneStream finance transformation jobs",
]

INDUSTRY_MAP = {
    "bank": "Financial Services", "financial": "Financial Services", "insurance": "Insurance",
    "capital": "Financial Services", "investment": "Financial Services", "asset": "Financial Services",
    "pharma": "Life Sciences", "biotech": "Life Sciences", "health": "Healthcare",
    "medical": "Healthcare", "hospital": "Healthcare", "clinical": "Life Sciences",
    "manufactur": "Manufacturing", "industrial": "Manufacturing", "aerospace": "Aerospace",
    "defense": "Aerospace", "automotive": "Automotive", "vehicle": "Automotive",
    "tech": "Technology", "software": "Technology", "digital": "Technology",
    "retail": "Retail", "consumer": "Consumer Goods", "food": "Food and Beverage",
    "energy": "Energy", "oil": "Energy", "gas": "Energy", "utility": "Energy",
    "consult": "Professional Services", "advisory": "Professional Services",
    "media": "Media", "entertainment": "Media", "publishing": "Media",
    "logistics": "Logistics", "transport": "Logistics", "supply": "Logistics",
    "real estate": "Real Estate", "property": "Real Estate",
    "education": "Education", "university": "Education",
    "government": "Government", "federal": "Government", "agency": "Government",
}

REGION_MAP = {
    "new york": "New York", "ny ": "New York", ", ny": "New York", "nyc": "New York",
    "new jersey": "New Jersey", "nj ": "New Jersey", ", nj": "New Jersey",
    "connecticut": "Connecticut", ", ct": "Connecticut", "ct ": "Connecticut",
    "pennsylvania": "Pennsylvania", ", pa": "Pennsylvania", "philadelphia": "Philadelphia",
    "california": "California", ", ca": "California", "san francisco": "California",
    "los angeles": "California", "chicago": "Midwest", "illinois": "Midwest",
    "texas": "Texas", ", tx": "Texas", "dallas": "Texas", "houston": "Texas",
    "florida": "Southeast", ", fl": "Southeast", "miami": "Southeast",
    "georgia": "Southeast", "atlanta": "Southeast",
    "massachusetts": "Northeast", "boston": "Northeast",
    "virginia": "Mid-Atlantic", ", va": "Mid-Atlantic", "washington dc": "Mid-Atlantic",
    "north carolina": "Southeast", "ohio": "Midwest", "michigan": "Midwest",
    "remote": "Remote / National",
}

def detect_industry(text):
    text_lower = text.lower()
    for keyword, industry in INDUSTRY_MAP.items():
        if keyword in text_lower:
            return industry
    return "Enterprise"

def detect_region(text):
    text_lower = text.lower()
    for keyword, region in REGION_MAP.items():
        if keyword in text_lower:
            return region
    return "National"

def extract_company(result):
    return result.get("company_name") or result.get("detected_extensions", {}).get("company", "Unknown")

from apify_client import ApifyClient

# Add this near your other OS gets
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
apify_client = ApifyClient(APIFY_TOKEN)

def scrape_jobs():
    logger.info(f"Starting multi-engine scrape at {datetime.now(timezone.utc)}")
    new_signals = []
    seen_urls = set()

    try:
        existing = supabase.table("signals").select("source_url").execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except Exception as e:
        logger.warning(f"Could not fetch existing URLs: {e}")

    for query in SEARCH_QUERIES:
        # --- ENGINE 1: GOOGLE JOBS ---
        try:
            token = None
            for page in range(3):
                params = {
                    "engine": "google_jobs",
                    "q": query,
                    "api_key": SERPAPI_KEY,
                    "country": "us"
                }
                if token:
                    params["next_page_token"] = token
                
                search = GoogleSearch(params)
                res_dict = search.get_dict()
                token = res_dict.get("serpapi_pagination", {}).get("next_page_token")
                jobs = res_dict.get("jobs_results", [])
                
                if not jobs: break
                for job in jobs:
                    url = job.get("share_link") or job.get("related_links", [{}])[0].get("link", "")
                    if url and url not in seen_urls:
                        process_and_add_job(job, url, "Google Jobs", new_signals, seen_urls)
                if not token: break
        except Exception as e:
            logger.error(f"Google error: {e}")

        # --- ENGINE 2: INDEED ---
        try:
            for page in range(2):
                params = {
                    "engine": "indeed",
                    "q": query,
                    "api_key": SERPAPI_KEY,
                    "start": page * 25,
                    "l": "United States"
                }
                res = GoogleSearch(params).get_dict().get("jobs_results", [])
                if not res: break
                for job in res:
                    url = job.get("link")
                    if url and url not in seen_urls:
                        process_and_add_job(job, url, "Indeed", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"Indeed error: {e}")

if new_signals:
    try:
        # The on_conflict="source_url" is the key to stopping duplicates
        supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()
        logger.info(f"Saved {len(new_signals)} signals total.")
    except Exception as e:
        logger.error(f"Supabase save error: {e}")
    
    return len(new_signals)

def process_and_add_job(job, url, source_name, signal_list, seen_set):
    title = job.get("title", "Unknown Role")
    company = extract_company(job)
    
    # --- THIS IS STEP 4 ---
    # We look through the list of jobs we JUST found in this scrape.
    # If we already have a job with the same company AND title, we stop here.
    for existing_job in signal_list:
        if existing_job['company'] == company and existing_job['job_title'] == title:
            return  # This exits the function so the job is NOT added to the list
    # -----------------------

    location = job.get("location", "Remote/USA")
    description = job.get("description", "")[:500]
    
    full_text = f"{title} {company} {description}"
    industry = detect_industry(full_text)
    region = detect_region(location + " " + full_text)

    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": industry,
        "region": region,
        "signal_type": "role",
        "detail": f"{title} at {company} ({source_name}).",
        "source_url": url,
        "source": source_name,
        "location": location,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    seen_set.add(url)

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "SA Intelligence API running"})

@app.route("/signals", methods=["GET"])
def process_and_add_job(job, url, source_name, signal_list, seen_set):
    # These lines must be indented 4 spaces
    title = job.get("title", "Unknown Role")
    company = extract_company(job)
    
    # Check for duplicates in the current list
    for existing_job in signal_list:
        if existing_job['company'] == company and existing_job['job_title'] == title:
            return 

    location = job.get("location", "Remote/USA")
    description = job.get("description", "")[:500]
    
    full_text = f"{title} {company} {description}"
    industry = detect_industry(full_text)
    region = detect_region(location + " " + full_text)

    # Adding the job to the list
    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": industry,
        "region": region,
        "signal_type": "role",
        "detail": f"{title} at {company} ({source_name}).",
        "source_url": url,
        "source": source_name,
        "location": location,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    seen_set.add(url)

@app.route("/refresh", methods=["POST"])
def manual_refresh():
    try:
        count = scrape_jobs()
        return jsonify({"status": "ok", "new_signals": count})
    except Exception as e:
        logger.error(f"Error during manual refresh: {e}")
        return jsonify({"error": str(e)}), 500

scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    logger.info("Running initial scrape on startup...")
    scrape_jobs()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
