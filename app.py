import os
import re
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from serpapi import GoogleSearch
from supabase import create_client, Client
from flask import Flask, jsonify, request, render_template_string
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

def parse_relative_date(text):
    """Converts '3 days ago' or '2 weeks ago' into a real date."""
    now = datetime.now(timezone.utc)
    if not text: return now.isoformat()
    text = text.lower()
    
    number = re.search(r'(\d+)', text)
    count = int(number.group(1)) if number else 1
    
    if 'hour' in text: return (now - timedelta(hours=count)).isoformat()
    if 'day' in text: return (now - timedelta(days=count)).isoformat()
    if 'week' in text: return (now - timedelta(weeks=count)).isoformat()
    if 'month' in text: return (now - timedelta(days=count*30)).isoformat()
    return now.isoformat()

def process_and_add_job(job, url, source_name, signal_list, seen_set):
    clean_url = url.split('?')[0].split('#')[0].strip()
    title = job.get("title", "Unknown Role").strip()
    company = job.get("company_name", "Unknown").strip()
    
    if clean_url in seen_set: return

    # Extract Posted Date
    raw_date = job.get("detected_extensions", {}).get("posted_at") or job.get("date")
    posted_at = parse_relative_date(raw_date) if raw_date else datetime.now(timezone.utc).isoformat()

    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": "Enterprise",
        "region": "National",
        "signal_type": "role",
        "detail": f"{title} at {company} ({source_name}).",
        "source_url": clean_url,
        "source": source_name,
        "location": job.get("location", "USA"),
        "posted_at": posted_at, # This is the REAL job date
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    seen_set.add(clean_url)

def scrape_jobs():
    logger.info("Starting master scrape...")
    new_signals = []
    seen_urls = set()

    try:
        existing = supabase.table("signals").select("source_url").limit(5000).execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except: pass

    for query in SEARCH_QUERIES:
        # 1. Google Jobs
        try:
            search = GoogleSearch({"engine": "google_jobs", "q": query, "api_key": SERPAPI_KEY, "hl": "en", "gl": "us"})
            results = search.get_dict()
            for job in results.get("jobs_results", []):
                url = job.get("related_links", [{}])[0].get("link") or job.get("share_link")
                if url: process_and_add_job(job, url, "Google Jobs", new_signals, seen_urls)
        except: pass

        # 2. Organic (Indeed/HiringCafe)
        try:
            search = GoogleSearch({"engine": "google", "q": f"OneStream jobs on Indeed or hiring.cafe", "api_key": SERPAPI_KEY, "gl": "us"})
            results = search.get_dict()
            for result in results.get("organic_results", []):
                url = result.get("link")
                if "indeed.com" in url or "hiring.cafe" in url:
                    snippet = result.get("snippet", "")
                    job = {
                        "title": result.get("title", "").split(" - ")[0],
                        "company_name": (re.search(r"(?:at|by|from)\s+([A-Z][\w\s&]+)", snippet) or [None, "View Listing"])[1],
                        "date": re.search(r"(\d+\s\w+\sago)", snippet).group(1) if re.search(r"(\d+\s\w+\sago)", snippet) else "1 day ago",
                        "location": "USA",
                        "description": snippet
                    }
                    process_and_add_job(job, url, "Indeed/HiringCafe", new_signals, seen_urls)
        except: pass
        time.sleep(1)

    if new_signals:
        supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()
    return len(new_signals)

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SA Intelligence</title>
        <style>
            body { font-family: -apple-system, sans-serif; padding: 20px; background: #f4f7f6; }
            .container { max-width: 1100px; margin: auto; background: white; padding: 25px; border-radius: 12px; }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            button { background: #0066ff; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; }
            table { width: 100%; border-collapse: collapse; }
            th, td { text-align: left; padding: 12px; border-bottom: 1px solid #eee; }
            .date-tag { color: #666; font-size: 12px; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>OneStream Leads (Sorted by Freshness)</h1>
                <button onclick="refreshSignals()" id="btn">Refresh Signals</button>
            </header>
            <table>
                <thead>
                    <tr><th>Posted</th><th>Company</th><th>Role</th><th>Source</th></tr>
                </thead>
                <tbody id="list"></tbody>
            </table>
        </div>
        <script>
            async function load() {
                const res = await fetch('/signals');
                const data = await res.json();
                document.getElementById('list').innerHTML = data.signals.map(s => `
                    <tr>
                        <td class="date-tag">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td><strong>${s.company}</strong></td>
                        <td><a href="${s.source_url}" target="_blank">${s.job_title}</a></td>
                        <td>${s.source}</td>
                    </tr>
                `).join('');
            }
            async function refreshSignals() {
                document.getElementById('btn').textContent = 'Scraping... wait 30s';
                await fetch('/refresh', {method:'POST'});
                setTimeout(() => { load(); document.getElementById('btn').textContent = 'Refresh Signals'; }, 30000);
            }
            load();
        </script>
    </body>
    </html>
    """)

@app.route("/signals")
def get_signals():
    # ORDER BY POSTED_AT (The real job date) instead of created_at (the scrape date)
    result = supabase.table("signals").select("*").order("posted_at", desc=True).limit(500).execute()
    return jsonify({"signals": result.data})

@app.route("/refresh", methods=["POST"])
def manual_refresh():
    scheduler.add_job(scrape_jobs, 'date', run_date=datetime.now(timezone.utc))
    return jsonify({"status": "ok"})

scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
