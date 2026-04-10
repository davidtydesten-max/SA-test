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

# Connections
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

SEARCH_QUERIES = [
    "OneStream developer jobs USA",
    "OneStream consultant jobs United States",
    "OneStream architect jobs USA",
    "OneStream EPM jobs USA",
    "OneStream financial systems jobs",
    "OneStream administrator jobs",
    "CPM OneStream jobs",
    "OneStream finance transformation"
]

# State lookup for extraction
STATES = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 
    'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 
    'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 
    'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
}

def parse_date_safely(text):
    now = datetime.now(timezone.utc)
    if not text or not isinstance(text, str): return now.isoformat()
    text = text.lower()
    try:
        num = re.search(r'(\d+)', text)
        val = int(num.group(1)) if num else 1
        if 'hour' in text: return (now - timedelta(hours=val)).isoformat()
        if 'day' in text: return (now - timedelta(days=val)).isoformat()
        if 'week' in text: return (now - timedelta(weeks=val)).isoformat()
        if 'month' in text: return (now - timedelta(days=val*30)).isoformat()
    except: pass
    return now.isoformat()

def detect_state(text, location):
    combined = (text + " " + location).upper()
    # Check for "Remote" first
    if "REMOTE" in combined or "WORK FROM HOME" in combined:
        return "Remote"
    # Check for State Abbreviations (e.g., " NY", ", NY")
    for full_name, abbrev in STATES.items():
        if f" {abbrev}" in combined or f", {abbrev}" in combined or full_name.upper() in combined:
            return abbrev
    return "USA (National)"

def extract_company(job):
    name = job.get("company_name")
    if not name or name.lower() in ["view listing", "unmasked"]:
        via = job.get("via", "")
        if via.lower().startswith("via "):
            name = via[4:].strip()
    return name if name else "Company Unmasked"

def process_and_add_job(job, url, source_name, signal_list, seen_set):
    clean_url = url.split('?')[0].split('#')[0].strip()
    if clean_url in seen_set: return

    title = job.get("title", "Unknown Role").strip()
    company = extract_company(job)
    location = job.get("location", "USA")
    
    state = detect_state(title + " " + job.get("description", ""), location)
    
    raw_date = job.get("detected_extensions", {}).get("posted_at") or job.get("date")
    posted_at = parse_date_safely(raw_date)

    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": "Enterprise",
        "region": state,
        "signal_type": "role",
        "detail": f"{title} at {company}.",
        "source_url": clean_url,
        "source": source_name,
        "location": location,
        "posted_at": posted_at,
        "updated_at": datetime.now(timezone.utc).isoformat()
    })
    seen_set.add(clean_url)

def scrape_jobs():
    logger.info("Weekly Scrape Initiated")
    new_signals = []
    seen_urls = set()
    try:
        existing = supabase.table("signals").select("source_url").limit(5000).execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except: pass

    for query in SEARCH_QUERIES:
        try:
            # 1. Google Jobs
            search = GoogleSearch({"engine": "google_jobs", "q": query, "api_key": SERPAPI_KEY, "gl": "us"})
            results = search.get_dict().get("jobs_results", [])
            for job in results:
                url = job.get("related_links", [{}])[0].get("link") or job.get("share_link")
                if url: process_and_add_job(job, url, "Google Jobs", new_signals, seen_urls)
            
            # 2. Organic (Indeed)
            search_org = GoogleSearch({"engine": "google", "q": f"{query} site:indeed.com", "api_key": SERPAPI_KEY, "gl": "us", "num": 5})
            for res in search_org.get_dict().get("organic_results", []):
                snippet = res.get("snippet", "")
                co_match = re.search(r"(?:at|by|from)\s+([A-Z][\w\s&]+)", snippet)
                job_data = {
                    "title": res.get("title", "").split(" - ")[0],
                    "company_name": co_match.group(1).strip() if co_match else "Unmasked",
                    "date": "1 day ago",
                    "location": "USA",
                    "description": snippet
                }
                process_and_add_job(job_data, res.get("link"), "Indeed", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"Error on {query}: {e}")
        time.sleep(2)

    if new_signals:
        supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SA Intel | OneStream</title>
        <style>
            body { font-family: sans-serif; padding: 25px; background: #f0f2f5; color: #1c1e21; }
            .card { max-width: 1200px; margin: auto; background: white; padding: 25px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            header { display: flex; justify-content: space-between; border-bottom: 1px solid #ddd; padding-bottom: 15px; margin-bottom: 15px; }
            button { background: #1877f2; color: white; border: none; padding: 10px 18px; border-radius: 5px; cursor: pointer; font-weight: bold; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; padding: 12px; background: #f5f6f7; border-bottom: 1px solid #ddd; font-size: 12px; color: #606770; }
            td { padding: 12px; border-bottom: 1px solid #eee; font-size: 14px; }
            .date { color: #8d949e; font-size: 12px; }
            .source-tag { background: #e7f3ff; color: #1877f2; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
            a { color: #1877f2; text-decoration: none; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="card">
            <header>
                <h2>OneStream National Intelligence</h2>
                <button onclick="run()">Refresh Leads</button>
            </header>
            <table>
                <thead>
                    <tr><th>Posted</th><th>Company</th><th>Job Role</th><th>State/Region</th><th>Source</th></tr>
                </thead>
                <tbody id="rows"></tbody>
            </table>
        </div>
        <script>
            async function load() {
                const r = await fetch('/signals');
                const d = await r.json();
                document.getElementById('rows').innerHTML = d.signals.map(s => `
                    <tr>
                        <td class="date">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td><strong>${s.company}</strong></td>
                        <td><a href="${s.source_url}" target="_blank">${s.job_title}</a></td>
                        <td>${s.region}</td>
                        <td><span class="source-tag">${s.source}</span></td>
                    </tr>
                `).join('');
            }
            async function run() {
                alert("Scrape in progress. Refresh page in 45 seconds.");
                await fetch('/refresh', {method:'POST'});
            }
            load();
        </script>
    </body>
    </html>
    """)

@app.route("/signals")
def get_signals():
    res = supabase.table("signals").select("*").order("posted_at", desc=True).limit(200).execute()
    return jsonify({"signals": res.data})

@app.route("/refresh", methods=["POST"])
def manual_refresh():
    scheduler.add_job(scrape_jobs, 'date', run_date=datetime.now(timezone.utc))
    return jsonify({"status": "ok"})

@app.route("/")
def health(): return "Dashboard at /dashboard"

scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", day_of_week="mon", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
