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

# Expanded queries to ensure we catch everything
SEARCH_QUERIES = [
    "OneStream jobs",
    "OneStream Consultant",
    "OneStream Architect",
    "OneStream Administrator",
    "OneStream Developer",
    "OneStream Analyst",
    "OneStream implementation"
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

def scrape_jobs():
    logger.info("Starting fresh scrape")
    new_data = []
    seen_urls = set()
    
    # Still check for URLs currently in DB to avoid exact duplicates
    try:
        res = supabase.table("signals").select("source_url").execute()
        seen_urls = {r["source_url"] for r in res.data}
    except: pass

    for q in SEARCH_QUERIES:
        try:
            # num: 100 pulls way more results
            params = {"engine": "google_jobs", "q": q, "api_key": SERPAPI_KEY, "gl": "us", "num": 100}
            search = GoogleSearch(params)
            res = search.get_dict()
            items = res.get("jobs_results", [])
            
            logger.info(f"Query '{q}' found {len(items)} items")

            for item in items:
                # Try multiple fields for a link
                url = item.get("related_links", [{}])[0].get("link") or item.get("share_link")
                if not url: continue
                url = url.split('?')[0].strip()
                
                if url in seen_urls: continue

                snippet = item.get("snippet", item.get("description", ""))
                company = clean_company(item, snippet)
                title = item.get("title", "OneStream Role")
                state = get_state(title + " " + snippet, item.get("location", "USA"))

                new_data.append({
                    "company": company,
                    "job_title": title[:100],
                    "region": state,
                    "source": item.get("via", "Google Jobs"),
                    "source_url": url,
                    "posted_at": datetime.now(timezone.utc).isoformat(),
                    "signal_type": "role",
                    "location": item.get("location", "USA"),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                })
                seen_urls.add(url)
        except Exception as e:
            logger.error(f"Error on query {q}: {e}")
        time.sleep(1)

    if new_data:
        supabase.table("signals").upsert(new_data, on_conflict="source_url").execute()
        logger.info(f"Successfully saved {len(new_data)} new leads.")

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>OneStream Lead Intel</title>
        <style>
            body { font-family: 'Segoe UI', sans-serif; background: #f0f2f5; margin: 0; padding: 20px; }
            .container { max-width: 1240px; margin: auto; background: white; border-radius: 12px; padding: 30px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px; border-bottom: 1px solid #eee; padding-bottom: 15px; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; font-size: 11px; color: #888; text-transform: uppercase; padding: 12px; background: #fafafa; }
            td { padding: 15px; border-bottom: 1px solid #f0f0f0; font-size: 14px; }
            .btn { background: #1877f2; color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; font-weight: bold; }
            .state-tag { background: #e7f3ff; color: #1877f2; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: 800; }
            .pending { color: #d93025; font-style: italic; }
            a { text-decoration: none; color: #1877f2; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h2>OneStream Live Leads</h2>
                <button class="btn" onclick="refresh()">Update Leads</button>
            </header>
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
                        <td style="color:#888">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td class="${s.company === 'Company Name Pending' ? 'pending' : ''}"><strong>${s.company}</strong></td>
                        <td>${s.job_title}</td>
                        <td><span class="state-tag">${s.region}</span></td>
                        <td><a href="${s.source_url}" target="_blank">View Listing</a></td>
                    </tr>
                `).join('');
            }
            async function refresh() {
                alert("Update started. Please wait 60 seconds and refresh page.");
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
    scheduler.add_job(scrape_jobs, 'date', run_date=datetime.now(timezone.utc))
    return jsonify({"status": "ok"})

@app.route("/")
def health(): return "Dashboard at /dashboard"

scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", day_of_week="mon", hour=7, minute=0)
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
