import os
import re
import time
import logging
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
    logger.info(f"Starting multi-engine scrape at {datetime.now(timezone.utc)}")
    
    # --- CLEANUP STALE JOBS ---
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        supabase.table("signals").delete().lt("created_at", cutoff).execute()
        logger.info(f"Cleaned up jobs older than {cutoff}")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

    new_signals = []
    seen_urls = set()

    # Load existing URLs to avoid duplicates
    try:
        existing = supabase.table("signals").select("source_url").execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except Exception as e:
        logger.warning(f"Could not fetch existing URLs: {e}")

    for query in SEARCH_QUERIES:
        # Google
