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

SEARCH_QUERIES = ["OneStream developer", "OneStream consultant", "OneStream architect", "OneStream administrator"]
DISCOVERY_QUERIES = ["OneStream implementation case study", "OneStream selects software", "OneStream customer success"]

STATES = {'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'}

def get_state(text, loc):
    combined = (str(text) + " " + str(loc)).upper()
    if any(k in combined for k in ["REMOTE", "WORK FROM HOME"]): return "Remote"
    for name, code in STATES.items():
        if f" {code}" in combined or f", {code}" in combined or name.upper() in combined: return code
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
    new_data, seen_urls = [], set()
    queries = SEARCH_QUERIES if mode == 'jobs' else DISCOVERY_QUERIES
    
    try:
        res = supabase.table("signals").select("source_url, company, job_title").execute()
        seen_urls = {r["source_url"] for r in res.data}
    except: pass

    for q in queries:
        try:
            search_type = "google_jobs" if mode == 'jobs' else "google"
            params = {"engine": search_type, "q": q, "api_key": SERPAPI_KEY, "gl": "us"}
            res = GoogleSearch(params).get_dict()
            
            items = res.get("jobs_results", []) if mode == 'jobs' else res.get("organic_results", [])
            for item in items:
                url = item.get("related_links", [{}])[0].get("link") or item.get("link") or item.get("share_link")
                if not url: continue
                url = url.split('?')[0].strip()
                
                snippet = item.get("snippet", item.get("description", ""))
                company = clean_company(item, snippet)
                
                # Deduplication logic
                is_dupe = False
                for existing in new_data:
                    # If we find a match, check if the new one has a better name
                    if existing.get('job_title') == item.get('title'):
                        if existing.get('company') == "Company Name Pending" and company != "Company Name Pending":
                            existing['company'] = company # Update the existing entry with the better name
                        is_dupe = True
                        break
                
                if url not in seen_urls and not is_dupe:
                    new_data.append({
                        "company": company,
                        "job_title": item.get("title", "OneStream Project")[:100],
                        "region": get_state(item.get("title","") + snippet, item.get("location", "USA")),
                        "source": "Google Jobs" if mode == 'jobs' else "Market Intel",
                        "source_url": url,
                        "posted_at": datetime.now(timezone.utc).isoformat(),
                        "signal_type": "role" if mode == 'jobs' else "install",
                        "industry": "Enterprise", "location": item.get("location", "USA"), "updated_at": datetime.now(timezone.utc).isoformat()
                    })
                    seen_urls.add(url)
        except Exception as e: logger.error(f"Error: {e}")
        time.sleep(1)

    if new_data:
        supabase.table("signals").upsert(new_data, on_conflict="source_url").execute()

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SA Intelligence Dashboard</title>
        <style>
            body { font-family: 'Segoe UI', sans-serif; background: #f0f2f5; margin: 0; padding: 20px; }
            .container { max-width: 1200px; margin: auto; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
            .tabs { display: flex; background: #f8f9fa; border-bottom: 1px solid #dee2e6; }
            .tab { padding: 15px 25px; cursor: pointer; border: none; background: none; font-weight: 600; color: #65676b; transition: 0.3s; }
            .tab.active { color: #1877f2; border-bottom: 3px solid #1877f2; background: white; }
            .content { padding: 25px; }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; font-size: 11px; color: #888; text-transform: uppercase; padding: 12px; border-bottom: 2px solid #eee; }
            td { padding: 14px; border-bottom: 1px solid #f0f0f0; font-size: 14px; }
            .btn { background: #1877f2; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; font-weight: bold; }
            .state-tag { background: #e7f3ff; color: #1877f2; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
            .pending { color: #d93025; font-style: italic; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="tabs">
                <button class="tab active" onclick="switchTab('role')">Live Job Leads</button>
                <button class="tab" onclick="switchTab('install')">Verified Install Base</button>
            </div>
            <div class="content">
                <header>
                    <h2 id="title">Active Hiring Signals</h2>
                    <button class="btn" onclick="refresh()">Update All Data</button>
                </header>
                <table>
                    <thead><tr><th>Date</th><th>Company</th><th>Description / Title</th><th>State</th><th>Link</th></tr></thead>
                    <tbody id="rows"></tbody>
                </table>
            </div>
        </div>
        <script>
            let currentType = 'role';
            async function load() {
                const r = await fetch('/signals');
                const d = await r.json();
                const filtered = d.signals.filter(s => s.signal_type === currentType);
                document.getElementById('rows').innerHTML = filtered.map(s => `
                    <tr>
                        <td style="color:#888">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td class="${s.company === 'Company Name Pending' ? 'pending' : ''}"><strong>${s.company}</strong></td>
                        <td>${s.job_title}</td>
                        <td><span class="state-tag">${s.region}</span></td>
                        <td><a href="${s.source_url}" target="_blank" style="text-decoration:none; color:#1877f2; font-weight:bold;">View</a></td>
                    </tr>
                `).join('');
            }
            function switchTab(type) {
                currentType = type;
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                event.target.classList.add('active');
                document.getElementById('title').innerText = type === 'role' ? 'Active Hiring Signals' : 'Verified Install Base';
                load();
            }
            async function refresh() {
                alert("Scraping started. Please refresh page in 60 seconds.");
                fetch('/refresh', {method:'POST'});
            }
            load();
        </script>
    </body>
    </html>
    """)

@app.route("/signals")
def get_signals():
    res = supabase.table("signals").select("*").order("posted_at", desc=True).limit(500).execute()
    return jsonify({"signals": res.data})

@app.route("/refresh", methods=["POST"])
def manual_refresh():
    scheduler.add_job(scrape_engine, 'date', run_date=datetime.now(timezone.utc), args=['jobs'])
    scheduler.add_job(scrape_engine, 'date', run_date=datetime.now(timezone.utc) + timedelta(seconds=30), args=['discovery'])
    return jsonify({"status": "ok"})

@app.route("/")
def health(): return "Visit /dashboard"

scheduler = BackgroundScheduler()
scheduler.add_job(lambda: scrape_engine('jobs'), "cron", day_of_week="mon", hour=7, minute=0)
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
