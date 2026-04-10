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
    "OneStream finance transformation jobs"
]

def detect_region(text, location):
    text_lower = (text + " " + location).lower()
    remote_keywords = ["remote", "virtual", "home-based", "anywhere", "work from home"]
    if any(k in text_lower for k in remote_keywords):
        return "Remote"
    return "US National"

def parse_relative_date(text):
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
    if clean_url in seen_set: return
    title = job.get("title", "Unknown Role").strip()
    company = job.get("company_name", "View Listing").strip()
    location = job.get("location", "USA")
    raw_date = job.get("detected_extensions", {}).get("posted_at") or job.get("date")
    posted_at = parse_relative_date(raw_date)
    region = detect_region(title + " " + job.get("description", ""), location)
    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": "Enterprise",
        "region": region,
        "signal_type": "role",
        "detail": f"{title} at {company} ({source_name}).",
        "source_url": clean_url,
        "source": source_name,
        "location": location,
        "posted_at": posted_at,
        "updated_at": datetime.now(timezone.utc).isoformat()
    })
    seen_set.add(clean_url)

def scrape_jobs():
    logger.info("Starting weekly scrape sequence...")
    new_signals = []
    seen_urls = set()
    try:
        existing = supabase.table("signals").select("source_url").limit(5000).execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except Exception as e:
        logger.error(f"DB sync error: {e}")

    for query in SEARCH_QUERIES:
        try:
            # 1. Google Jobs
            search = GoogleSearch({"engine": "google_jobs", "q": query, "api_key": SERPAPI_KEY, "hl": "en", "gl": "us"})
            for job in search.get_dict().get("jobs_results", []):
                url = job.get("related_links", [{}])[0].get("link") or job.get("share_link")
                if url: process_and_add_job(job, url, "Google Jobs", new_signals, seen_urls)
            
            # 2. Organic (Indeed/HiringCafe)
            # site: filter is efficient for credits
            search_org = GoogleSearch({"engine": "google", "q": f"{query} site:indeed.com OR site:hiring.cafe", "api_key": SERPAPI_KEY, "gl": "us", "num": 5})
            for result in search_org.get_dict().get("organic_results", []):
                url = result.get("link")
                if "indeed.com" in url or "hiring.cafe" in url:
                    snippet = result.get("snippet", "")
                    co_match = re.search(r"(?:at|by|from)\s+([A-Z][\w\s&]+)", snippet)
                    job_data = {
                        "title": result.get("title", "").split(" - ")[0],
                        "company_name": co_match.group(1).strip() if co_match else "View Listing",
                        "date": re.search(r"(\d+\s\w+\sago)", snippet).group(1) if re.search(r"(\d+\s\w+\sago)", snippet) else "1 day ago",
                        "location": "USA",
                        "description": snippet
                    }
                    process_and_add_job(job_data, url, "Indeed/HiringCafe", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"Search error: {e}")
        time.sleep(2)

    if new_signals:
        try:
            supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()
            logger.info(f"Saved {len(new_signals)} weekly leads.")
        except Exception as e:
            logger.error(f"Upsert Error: {e}")

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <title>SA Intelligence</title>
        <style>
            body { font-family: -apple-system, sans-serif; padding: 20px; background: #f4f7f6; color: #333; }
            .container { max-width: 1100px; margin: auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }
            header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #eee; padding-bottom: 20px; margin-bottom: 20px; }
            button { background: #0066ff; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-weight: 600; }
            button:disabled { background: #aab; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; padding: 15px; background: #fafafa; border-bottom: 2px solid #eee; font-size: 13px; text-transform: uppercase; color: #666; }
            td { padding: 15px; border-bottom: 1px solid #eee; }
            .date-cell { color: #888; font-size: 13px; font-weight: 600; }
            .badge { background: #eff6ff; color: #1e40af; padding: 4px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; border: 1px solid #dbeafe; text-transform: uppercase; }
            a { color: #0066ff; text-decoration: none; font-weight: 600; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>OneStream Weekly Lead Intelligence</h1>
                <button id="refresh-btn" onclick="refreshSignals()">Refresh Signals</button>
            </header>
            <table>
                <thead>
                    <tr><th>Posted Date</th><th>Company</th><th>Job Role</th><th>Classification</th><th>Source</th></tr>
                </thead>
                <tbody id="signals-body"></tbody>
            </table>
        </div>
        <script>
            async function loadSignals() {
                const res = await fetch('/signals');
                const data = await res.json();
                document.getElementById('signals-body').innerHTML = data.signals.map(s => `
                    <tr>
                        <td class="date-cell">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td><strong>${s.company}</strong></td>
                        <td><a href="${s.source_url}" target="_blank">${s.job_title}</a></td>
                        <td>${s.region}</td>
                        <td><span class="badge">${s.source}</span></td>
                    </tr>
                `).join('');
            }
            async function refreshSignals() {
                const btn = document.getElementById('refresh-btn');
                btn.textContent = 'Scraping... wait 30s'; btn.disabled = true;
                await fetch('/refresh', { method: 'POST' });
                setTimeout(async () => { await loadSignals(); btn.textContent = 'Refresh Signals'; btn.disabled = false; }, 30000);
            }
            loadSignals();
        </script>
    </body>
    </html>
    """)

@app.route("/signals")
def get_signals():
    result = supabase.table("signals").select("*").order("posted_at", desc=True).limit(500).execute()
    return jsonify({"signals": result.data})

@app.route("/refresh", methods=["POST"])
def manual_refresh():
    scheduler.add_job(scrape_jobs, 'date', run_date=datetime.now(timezone.utc))
    return jsonify({"status": "ok"})

@app.route("/")
def health(): return "Visit /dashboard"

# --- WEEKLY SCHEDULER ---
# Runs every Monday at 7:00 AM
scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", day_of_week="mon", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
