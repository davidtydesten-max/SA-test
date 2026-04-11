import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from serpapi import GoogleSearch
from supabase import create_client, Client
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Credentials
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

SEARCH_QUERIES = [
    "OneStream developer jobs", 
    "OneStream consultant hiring", 
    "OneStream architect", 
    "OneStream administrator",
    "OneStream EPM implementation"
]
DISCOVERY_QUERIES = [
    "OneStream implementation case study", 
    "OneStream selects software", 
    "OneStream customer success story"
]

STATES = {'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'}

def get_state(text, loc):
    search_zone = f"{text} {loc}".upper()
    if any(k in search_zone for k in ["REMOTE", "WORK FROM HOME", "VIRTUAL"]):
        return "Remote"
    for name, code in STATES.items():
        if re.search(rf"\b{code}\b", search_zone) or name.upper() in search_zone:
            return code
    if any(city in search_zone for city in ["NYC", "MANHATTAN", "BROOKLYN", "ALBANY"]): return "NY"
    if any(city in search_zone for city in ["NEWARK", "JERSEY CITY", "TRENTON"]): return "NJ"
    if any(city in search_zone for city in ["STAMFORD", "HARTFORD"]): return "CT"
    return "USA"

def clean_company(job, snippet=""):
    name = job.get("company_name")
    if not name or name.lower() in ["unmasked", "view listing", "company unknown"]:
        match = re.search(r"(?:at|by|from|@)\s+([A-Z][\w\s&',.]+)", snippet)
        if match: name = match.group(1).split("...")[0].split(" - ")[0].strip()
    if not name:
        via = job.get("via", "")
        name = via.replace("via ", "").strip() if via else "Company Name Pending"
    return name

def scrape_engine(mode='jobs'):
    logger.info(f"Starting {mode} scrape")
    new_data = []
    seen_urls = set()
    
    try:
        res = supabase.table("signals").select("source_url").execute()
        seen_urls = {r["source_url"] for r in res.data}
    except: pass

    queries = SEARCH_QUERIES if mode == 'jobs' else DISCOVERY_QUERIES
    
    for q in queries:
        try:
            search_type = "google_jobs" if mode == 'jobs' else "google"
            # Increased num to 20 to get more results
            params = {"engine": search_type, "q": q, "api_key": SERPAPI_KEY, "gl": "us", "num": 20}
            res = GoogleSearch(params).get_dict()
            
            items = res.get("jobs_results", []) if mode == 'jobs' else res.get("organic_results", [])
            
            for item in items:
                url = item.get("related_links", [{}])[0].get("link") or item.get("link") or item.get("share_link")
                if not url: continue
                url = url.split('?')[0].strip()
                
                if url in seen_urls: continue

                snippet = item.get("snippet", item.get("description", ""))
                company = clean_company(item, snippet)
                title = item.get("title", "OneStream Project")
                state = get_state(title + " " + snippet, item.get("location", "USA"))

                new_data.append({
                    "company": company,
                    "job_title": title[:100],
                    "region": state,
                    "source": "Google Jobs" if mode == 'jobs' else "Market Intel",
                    "source_url": url,
                    "posted_at": datetime.now(timezone.utc).isoformat(),
                    "signal_type": "role" if mode == 'jobs' else "install",
                    "industry": "Enterprise", 
                    "location": item.get("location", "USA"), 
                    "updated_at": datetime.now(timezone.utc).isoformat()
                })
                seen_urls.add(url)
        except Exception as e:
            logger.error(f"Error on query {q}: {e}")
        time.sleep(1)

    if new_data:
        supabase.table("signals").upsert(new_data, on_conflict="source_url").execute()
        logger.info(f"Saved {len(new_data)} items")

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SA Intelligence Hub</title>
        <style>
            body { font-family: 'Segoe UI', sans-serif; background: #f0f2f5; margin: 0; padding: 20px; }
            .container { max-width: 1240px; margin: auto; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
            .tabs { display: flex; background: #f8f9fa; border-bottom: 1px solid #dee2e6; }
            .tab { padding: 18px 28px; cursor: pointer; border: none; background: none; font-weight: 600; color: #65676b; }
            .tab.active { color: #1877f2; border-bottom: 3px solid #1877f2; background: white; }
            .content { padding: 30px; }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; font-size: 11px; color: #888; text-transform: uppercase; padding: 12px; background: #fafafa; }
