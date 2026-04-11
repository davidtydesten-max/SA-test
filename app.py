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
    "OneStream developer", "OneStream consultant", "OneStream architect", 
    "OneStream EPM", "OneStream administrator", "CPM OneStream"
]

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

def parse_date(text):
    now = datetime.now(timezone.utc)
    if not text: return now.isoformat()
    text = text.lower()
    try:
        num = int(re.search(r'(\d+)', text).group(1)) if re.search(r'(\d+)', text) else 1
        if 'hour' in text: return (now - timedelta(hours=num)).isoformat()
        if 'day' in text: return (now - timedelta(days=num)).isoformat()
        if 'week' in text: return (now - timedelta(weeks=num)).isoformat()
    except: pass
    return now.isoformat()

def get_state(text, loc):
    combined = (str(text) + " " + str(loc)).upper()
    if any(k in combined for k in ["REMOTE", "WORK FROM HOME", "VIRTUAL"]): return "Remote"
    for name, code in STATES.items():
        if f" {code}" in combined or f", {code}" in combined or name.upper() in combined:
            return code
    return "USA"

def clean_company(job_res, snippet=""):
    # Priority 1: Google Jobs direct field
    name = job_res.get("company_name")
    # Priority 2: Extract from snippet (Indeed/Organic)
    if not name or name.lower() in ["unmasked", "view listing"]:
        match = re.search(r"(?:at|by|from)\s+([A-Z][\w\s&',.]+)", snippet)
        if match: name = match.group(1).split("...")[0].strip()
    # Priority 3: Via field
    if not name:
        via = job_res.get("via", "")
        name = via.replace("via ", "").strip() if via else "Company Unknown"
    return name

def process_job(job, url, source, signals, seen):
    link = url.split('?')[0].strip()
    if link in seen: return
    
    title = job.get("title", "OneStream Role")
    snippet = job.get("snippet", job.get("description", ""))
    company = clean_company(job, snippet)
    loc = job.get("location", "USA")
    
    signals.append({
        "company": company,
        "job_title": title,
        "region": get_state(title + " " + snippet, loc),
        "source": source,
        "source_url": link,
        "posted_at": parse_date(job.get("detected_extensions", {}).get("posted_at") or job.get("date")),
        "industry": "Enterprise", "signal_type": "role", "location": loc, "updated_at": datetime.now(timezone.utc).isoformat()
    })
    seen.add(link)

def scrape_jobs():
    logger.info("Starting Scrape")
    new_data, seen_urls = [], set()
    try:
        obs = supabase.table("signals").select("source_url").execute()
        seen_urls = {r["source_url"] for r in obs.data}
    except: pass

    for q in SEARCH_QUERIES:
        # Engine 1: Google Jobs (High Quality)
        try:
            res = GoogleSearch({"engine": "google_jobs", "q": q, "api_key": SERPAPI_KEY, "gl": "us"}).get_dict()
            for j in res.get("jobs_results", []):
                u = j.get("related_links", [{}])[0].get("link") or j.get("share_link")
                if u: process_job(j, u, "Google Jobs", new_data, seen_urls)
        except: pass

        # Engine 2: Organic (Indeed Focus)
        try:
            res = GoogleSearch({"engine": "google", "q": f"{q} site:indeed.com", "api_key": SERPAPI_KEY, "gl": "us", "num": 10}).get_dict()
            for r in res.get("organic_results", []):
                process_job(r, r.get("link"), "Indeed", new_data, seen_urls)
        except: pass
        time.sleep(2)

    if new_data:
        supabase.table("signals").upsert(new_data, on_conflict="source_url").execute()
    logger.info(f"Done. Saved {len(new_data)}")

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SystemsAccountants Intel</title>
        <style>
            body { font-family: sans-serif; background: #f8f9fa; padding: 40px; }
            .box { max-width: 1200px; margin: auto; background: #fff; padding: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
            header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #eee; margin-bottom: 20px; padding-bottom: 10px; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; font-size: 12px; color: #666; text-transform: uppercase; padding: 12px; border-bottom: 2px solid #eee; }
            td { padding: 12px; border-bottom: 1px solid #eee; font-size: 14px; }
            .st { background: #eef2ff; color: #4338ca; padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 11px; }
            .src { color: #059669; font-weight: bold; font-size: 11px; }
            a { color: #2563eb; text-decoration: none; font-weight: 600; }
        </style>
    </head>
    <body>
        <div class="box">
            <header><h2>OneStream National Leads</h2><button onclick="run()">Scrape Now</button></header>
            <table>
                <thead><tr><th>Date</th><th>Company</th><th>Role</th><th>State</th><th>Source</th></tr></thead>
                <tbody id="rows"></tbody>
            </table>
        </div>
        <script>
            async function load() {
                const r = await fetch('/signals');
                const d = await r.json();
                document.getElementById('rows').innerHTML = d.signals.map(s => `
                    <tr>
                        <td>${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td><strong>${s.company}</strong></td>
                        <td><a href="${s.source_url}" target="_blank">${s.job_title}</a></td>
                        <td><span class="st">${s.region}</span></td>
                        <td><span class="src">${s.source}</span></td>
                    </tr>
                `).join('');
            }
            async function run() { alert("Starting. Wait 60s."); fetch('/refresh',{method:'POST'}); }
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
def health(): return "Visit /dashboard"

scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", day_of_week="mon", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
