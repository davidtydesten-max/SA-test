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
    "OneStream implementation jobs",
    "OneStream administrator jobs",
    "OneStream planning analyst",
    "CPM OneStream jobs",
    "OneStream finance transformation"
]

def parse_date_safely(text):
    """Prevents 1970 errors by defaulting to 'Now' if parsing fails."""
    now = datetime.now(timezone.utc)
    if not text or not isinstance(text, str):
        return now.isoformat()
    
    text = text.lower()
    try:
        num = re.search(r'(\d+)', text)
        val = int(num.group(1)) if num else 1
        
        if 'hour' in text: return (now - timedelta(hours=val)).isoformat()
        if 'day' in text: return (now - timedelta(days=val)).isoformat()
        if 'week' in text: return (now - timedelta(weeks=val)).isoformat()
        if 'month' in text: return (now - timedelta(days=val*30)).isoformat()
    except:
        pass
    return now.isoformat()

def extract_company(job):
    """Deep search for company name across multiple JSON fields."""
    # 1. Primary field
    name = job.get("company_name")
    
    # 2. Source field (e.g., 'via LinkedIn') - Clean it up
    if not name or name == "View Listing":
        via = job.get("via", "")
        if via.lower().startswith("via "):
            name = via[4:].strip()
            
    # 3. Last resort from extensions
    if not name:
        ext = job.get("detected_extensions", {})
        name = ext.get("company_name", "Company Unmasked")
        
    return name if name else "Company Unmasked"

def process_and_add_job(job, url, source_name, signal_list, seen_set):
    clean_url = url.split('?')[0].split('#')[0].strip()
    if clean_url in seen_set: return

    title = job.get("title", "Unknown Role").strip()
    company = extract_company(job)
    location = job.get("location", "USA")
    
    # Classification Logic (Simplified National)
    is_remote = any(k in (title + location).lower() for k in ["remote", "virtual", "anywhere"])
    region = "Remote" if is_remote else "US National"
    
    # Date Logic
    raw_date = job.get("detected_extensions", {}).get("posted_at") or job.get("date")
    posted_at = parse_date_safely(raw_date)

    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": "Enterprise",
        "region": region,
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
    logger.info("Weekly Scrape Triggered")
    new_signals = []
    seen_urls = set()
    try:
        existing = supabase.table("signals").select("source_url").limit(5000).execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except: pass

    for query in SEARCH_QUERIES:
        try:
            # Google Jobs Engine
            search = GoogleSearch({"engine": "google_jobs", "q": query, "api_key": SERPAPI_KEY, "gl": "us"})
            results = search.get_dict().get("jobs_results", [])
            for job in results:
                url = job.get("related_links", [{}])[0].get("link") or job.get("share_link")
                if url: process_and_add_job(job, url, "Google Jobs", new_signals, seen_urls)
            
            # Organic Engine (Indeed/HiringCafe)
            search_org = GoogleSearch({"engine": "google", "q": f"{query} site:indeed.com", "api_key": SERPAPI_KEY, "gl": "us", "num": 5})
            for res in search_org.get_dict().get("organic_results", []):
                snippet = res.get("snippet", "")
                co_match = re.search(r"(?:at|by|from)\s+([A-Z][\w\s&]+)", snippet)
                job_data = {
                    "title": res.get("title", "").split(" - ")[0],
                    "company_name": co_match.group(1).strip() if co_match else "Unmasked",
                    "date": "1 day ago",
                    "location": "USA"
                }
                process_and_add_job(job_data, res.get("link"), "Indeed", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"Error on query {query}: {e}")
        time.sleep(2)

    if new_signals:
        supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SystemsAccountants Intelligence</title>
        <style>
            body { font-family: sans-serif; padding: 30px; background: #f4f7f6; }
            .container { max-width: 1100px; margin: auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            header { display: flex; justify-content: space-between; border-bottom: 2px solid #eee; padding-bottom: 20px; margin-bottom: 20px; }
            button { background: #0066ff; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; padding: 12px; background: #fafafa; border-bottom: 2px solid #eee; color: #666; font-size: 12px; }
            td { padding: 12px; border-bottom: 1px solid #eee; font-size: 14px; }
            .date { color: #888; font-weight: bold; }
            a { color: #0066ff; text-decoration: none; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>OneStream Weekly Leads</h1>
                <button onclick="refresh()">Run Scrape Now</button>
            </header>
            <table id="tbl"><thead><tr><th>Posted</th><th>Company</th><th>Job Role</th><th>Region</th></tr></thead><tbody id="bdy"></tbody></table>
        </div>
        <script>
            async function load() {
                const r = await fetch('/signals');
                const d = await r.json();
                document.getElementById('bdy').innerHTML = d.signals.map(s => `
                    <tr>
                        <td class="date">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td><strong>${s.company}</strong></td>
                        <td><a href="${s.source_url}" target="_blank">${s.job_title}</a></td>
                        <td>${s.region}</td>
                    </tr>
                `).join('');
            }
            async function refresh() {
                alert("Scrape started. Wait 30 seconds then refresh page.");
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
def health(): return "Live. Use /dashboard"

# WEEKLY SCHEDULER: Monday at 7:00 AM EST
scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", day_of_week="mon", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
