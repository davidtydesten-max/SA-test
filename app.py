import os
import re
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from serpapi import GoogleSearch
from supabase import create_client, Client
from flask import Flask, jsonify, request
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
    "manufactur": "Manufacturing", "industrial": "Manufacturing", "tech": "Technology", 
    "software": "Technology", "digital": "Technology", "retail": "Retail", 
    "consult": "Professional Services", "advisory": "Professional Services",
}

REGION_MAP = {
    "new york": "New York", "ny ": "New York", ", ny": "New York", "nyc": "New York",
    "new jersey": "New Jersey", "nj ": "New Jersey", ", nj": "New Jersey",
    "connecticut": "Connecticut", ", ct": "Connecticut", "ct ": "Connecticut",
    "pennsylvania": "Pennsylvania", ", pa": "Pennsylvania", "remote": "Remote / National",
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

def process_and_add_job(job, url, source_name, signal_list, seen_set):
    # Normalize URL: Strip tracking parameters
    clean_url = url.split('?')[0].split('#')[0].strip()
    title = job.get("title", "Unknown Role").strip()
    company = extract_company(job).strip()
    
    # Strict Deduplication
    if clean_url in seen_set:
        return

    for existing in signal_list:
        if (existing['company'].lower() == company.lower() and 
            existing['job_title'].lower() == title.lower()):
            return 

    location = job.get("location", "Remote/USA")
    description = job.get("description", "")[:500]
    full_text = f"{title} {company} {description}"

    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": detect_industry(full_text),
        "region": detect_region(location + " " + full_text),
        "signal_type": "role",
        "detail": f"{title} at {company} ({source_name}).",
        "source_url": clean_url,
        "source": source_name,
        "location": location,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    seen_set.add(clean_url)

def scrape_jobs():
    logger.info("Starting master scrape (Google, Indeed, HiringCafe)...")
    
    # 1. Purge jobs older than 30 days
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        supabase.table("signals").delete().lt("created_at", cutoff).execute()
        logger.info("Stale jobs purged.")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

    new_signals = []
    seen_urls = set()

    # 2. Fetch existing URLs from Supabase to prevent duplicates
    try:
        existing = supabase.table("signals").select("source_url").limit(5000).execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except Exception as e:
        logger.warning(f"Database sync warning: {e}")

    for query in SEARCH_QUERIES:
        # --- Google Jobs ---
        try:
            search = GoogleSearch({"engine": "google_jobs", "q": query, "api_key": SERPAPI_KEY, "country": "us"})
            for job in search.get_dict().get("jobs_results", []):
                url = job.get("share_link") or job.get("related_links", [{}])[0].get("link")
                if url: process_and_add_job(job, url, "Google Jobs", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"Google error: {e}")

        # --- Indeed ---
        try:
            search = GoogleSearch({"engine": "indeed", "q": query, "api_key": SERPAPI_KEY, "l": "United States"})
            for job in search.get_dict().get("jobs_results", []):
                url = job.get("link")
                if url: process_and_add_job(job, url, "Indeed", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"Indeed error: {e}")

        # --- HiringCafe (Direct API Attempt) ---
        try:
            hc_response = requests.post(
                "https://hiring.cafe/api/jobs/search", 
                json={"query": query, "location": "USA"}, 
                timeout=10
            )
            if hc_response.status_code == 200:
                for job in hc_response.json().get("jobs", []):
                    url = job.get("apply_url")
                    if url: process_and_add_job(job, url, "HiringCafe", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"HiringCafe error: {e}")

    # 3. Batch Upsert
    if new_signals:
        try:
            supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()
            logger.info(f"Saved {len(new_signals)} new unique signals.")
        except Exception as e:
            logger.error(f"Save error: {e}")
    
    return len(new_signals)

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "SA Intelligence Live"})

@app.route("/signals", methods=["GET"])
def get_signals():
    try:
        # Override default Supabase limit
        query = supabase.table("signals").select("*").order("created_at", desc=True).limit(1000)
        
        region = request.args.get('region')
        if region:
            query = query.eq("region", region)
            
        result = query.execute()
        return jsonify({"signals": result.data, "count": len(result.data)})
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/refresh", methods=["GET", "POST"])
def manual_refresh():
    try:
        scheduler.add_job(scrape_jobs, 'date', run_date=datetime.now(timezone.utc))
        return jsonify({"status": "ok", "message": "Background scrape started."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
