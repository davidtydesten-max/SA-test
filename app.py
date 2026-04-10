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

def scrape_jobs():
    logger.info(f"Starting scrape at {datetime.now(timezone.utc)}")
    new_signals = []
    seen_urls = set()

    try:
        existing = supabase.table("signals").select("source_url").execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except Exception as e:
        logger.warning(f"Could not fetch existing URLs: {e}")

    for query in SEARCH_QUERIES:
        try:
            params = {
                "engine": "google_jobs",
                "q": query,
                "api_key": SERPAPI_KEY,
                "num": 50,
                "country": "us",
            }
            search = GoogleSearch(params)
            results = search.get_dict()
            jobs = results.get("jobs_results", [])
            logger.info(f"Query '{query}': {len(jobs)} results")

            for job in jobs:
                url = job.get("share_link") or job.get("related_links", [{}])[0].get("link", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                title = job.get("title", "")
                company = extract_company(job)
                location = job.get("location", "")
                description = job.get("description", "")[:500]
                date_posted = job.get("detected_extensions", {}).get("posted_at", "")

                full_text = f"{title} {company} {location} {description}"
                industry = detect_industry(full_text)
                region = detect_region(location + " " + full_text)

                detail = f"{title} role at {company}."
                if location:
                    detail += f" Based in {location}."
                if description:
                    first_sentence = description.split(".")[0].strip()
                    if len(first_sentence) > 20:
                        detail += f" {first_sentence}."

                signal = {
                    "company": company,
                    "job_title": title,
                    "industry": industry,
                    "region": region,
                    "signal_type": "role",
                    "detail": detail,
                    "contact": "See job posting",
                    "source_url": url,
                    "source": "Google Jobs",
                    "location": location,
                    "date_posted": date_posted,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                new_signals.append(signal)

            time.sleep(1)

        except Exception as e:
            logger.error(f"Error on query '{query}': {e}")
            continue

    if new_signals:
        try:
            supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()
            logger.info(f"Saved {len(new_signals)} signals to Supabase")
        except Exception as e:
            logger.error(f"Error saving to Supabase: {e}")
    else:
        logger.info("No new signals found")

    return len(new_signals)


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "SA Intelligence API running"})

@app.route("/signals", methods=["GET"])
def get_signals():
    try:
        result = supabase.table("signals") \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(200) \
            .execute()
        return jsonify({"signals": result.data, "count": len(result.data)})
    except Exception as e:
        logger.error(f"Error fetching signals: {e}")
        return jsonify({"error": str(e)}), 500

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
