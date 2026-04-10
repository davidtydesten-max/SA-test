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

# Environment Variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# Updated Search Queries
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
    "manufactur": "Manufacturing", "industrial": "Manufacturing", "tech": "Technology", 
    "software": "Technology", "digital": "Technology", "retail": "Retail", 
    "consult": "Professional Services", "advisory": "Professional Services",
}

REGION_MAP = {
    "new york": "New York", "ny ": "New York", ", ny": "New York", "nyc": "New York",
    "new jersey": "New Jersey", "nj ": "New Jersey", ", nj": "New Jersey",
    "connecticut": "Connecticut", ", ct": "Connecticut", "ct ": "Connecticut",
    "pennsylvania": "Pennsylvania", ", pa": "Pennsylvania", "remote": "Remote / National",
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

def process_and_add_job(job, url, source_name, signal_list, seen_set):
    clean_url = url.split('?')[0].split('#')[0].strip()
    title = job.get("title", "Unknown Role").strip()
    company = extract_company(job).strip()
    
    if clean_url in seen_set:
        return

    for existing in signal_list:
        if (existing['company'].lower() == company.lower() and 
            existing['job_title'].lower() == title.lower()):
            return 

    location = job.get("location", "Remote/USA")
    description = job.get("description", "")[:500]
    full_text = f"{title} {company} {description}"

    signal_list.append({
        "company": company,
        "job_title": title,
        "industry": detect_industry(full_text),
        "region": detect_region(location + " " + full_text),
        "signal_type": "role",
        "detail": f"{title} at {company} ({source_name}).",
        "source_url": clean_url,
        "source": source_name,
        "location": location,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    seen_set.add(clean_url)

def scrape_jobs():
    logger.info("Starting master scrape...")
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        supabase.table("signals").delete().lt("created_at", cutoff).execute()
        logger.info("Stale jobs purged.")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

    new_signals = []
    seen_urls = set()

    try:
        existing = supabase.table("signals").select("source_url").limit(5000).execute()
        seen_urls = {r["source_url"] for r in existing.data if r.get("source_url")}
    except Exception as e:
        logger.warning(f"Database sync warning: {e}")

    for query in SEARCH_QUERIES:
        # 1. Google Jobs Engine (Broad)
        try:
            search = GoogleSearch({
                "engine": "google_jobs",
                "q": query,
                "api_key": SERPAPI_KEY,
                "hl": "en",
                "gl": "us"
            })
            results = search.get_dict()
            for job in results.get("jobs_results", []):
                url = job.get("related_links", [{}])[0].get("link") or job.get("share_link")
                if url:
                    process_and_add_job(job, url, "Google Jobs", new_signals, seen_urls)
        except Exception as e:
            logger.error(f"Google Jobs error: {e}")

        # 2. Specifically targeting Indeed & HiringCafe via Organic Search
        # We use a broader query here to help Google find the index pages
        special_queries = [f"OneStream jobs on Indeed", f"OneStream hiring.cafe listings"]
        for sq in special_queries:
            try:
                search = GoogleSearch({
                    "engine": "google",
                    "q": sq,
                    "api_key": SERPAPI_KEY,
                    "gl": "us"
                })
                results = search.get_dict()
                for result in results.get("organic_results", []):
                    url = result.get("link")
                    source = "Indeed" if "indeed.com" in url else "HiringCafe" if "hiring.cafe" in url else None
                    if source:
                        job = {
                            "title": result.get("title", "Job Posting"),
                            "company_name": "Check listing",
                            "location": "USA",
                            "description": result.get("snippet", "")
                        }
                        process_and_add_job(job, url, source, new_signals, seen_urls)
            except Exception as e:
                logger.error(f"Organic search error: {e}")

        time.sleep(1)

    if new_signals:
        try:
            supabase.table("signals").upsert(new_signals, on_conflict="source_url").execute()
            logger.info(f"Saved {len(new_signals)} new unique signals.")
        except Exception as e:
            logger.error(f"Save error: {e}")
    
    return len(new_signals)

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "SA Intelligence Live. Visit /dashboard to view leads."})

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>SA Intelligence Dashboard</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; padding: 20px; background: #f4f7f6; color: #333; }
            .container { max-width: 1100px; margin: auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
            header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #f0f0f0; margin-bottom: 25px; padding-bottom: 15px; }
            h1 { margin: 0; color: #1a1a1a; font-size: 24px; }
            button { background: #0066ff; color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; font-weight: 600; transition: background 0.2s; }
            button:hover { background: #0052cc; }
            button:disabled { background: #aab; cursor: not-allowed; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th { text-align: left; padding: 15px; background: #fafafa; font-weight: 600; border-bottom: 2px solid #eee; }
            td { padding: 15px; border-bottom: 1px solid #eee; vertical-align: top; }
            tr:hover { background: #fcfcfc; }
            .source-tag { background: #eff6ff; color: #1e40af; padding: 4px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; text-transform: uppercase; border: 1px solid #dbeafe; }
            a { color: #0066ff; text-decoration: none; font-weight: 500; }
            a:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>OneStream & EPM Intelligence</h1>
                <button id="refresh-btn" onclick="refreshSignals()">Refresh Signals</button>
            </header>
            <table id="signals-table">
                <thead>
                    <tr>
                        <th style="width: 25%;">Company</th>
                        <th style="width: 40%;">Job Role</th>
                        <th style="width: 20%;">Region</th>
                        <th style="width: 15%;">Source</th>
                    </tr>
                </thead>
                <tbody id="signals-body">
                    <tr><td colspan="4">Loading leads from database...</td></tr>
                </tbody>
            </table>
        </div>
        <script>
            async function loadSignals() {
                try {
                    const response = await fetch('/signals');
                    const data = await response.json();
                    const body = document.getElementById('signals-body');
                    body.innerHTML = '';
                    if (!data.signals || data.signals.length === 0) {
                        body.innerHTML = '<tr><td colspan="4">No leads found. Click refresh to start a scrape.</td></tr>';
                        return;
                    }
                    data.signals.forEach(sig => {
                        const row = `<tr>
                            <td><strong>${sig.company}</strong></td>
                            <td><a href="${sig.source_url}" target="_blank">${sig.job_title}</a></td>
                            <td>${sig.region}</td>
                            <td><span class="source-tag">${sig.source}</span></td>
                        </tr>`;
                        body.innerHTML += row;
                    });
                } catch (e) {
                    console.error('Load error:', e);
                }
            }
            async function refreshSignals() {
                const btn = document.getElementById('refresh-btn');
                btn.textContent = 'Scraping... Please Wait 30s';
                btn.disabled = true;
                try {
                    await fetch('/refresh', { method: 'POST' });
                    setTimeout(async () => {
                        await loadSignals();
                        btn.textContent = 'Refresh Signals';
                        btn.disabled = false;
                    }, 30000);
                } catch(e) {
                    alert('Refresh failed. Check Render logs.');
                    btn.textContent = 'Refresh Signals';
                    btn.disabled = false;
                }
            }
            loadSignals();
        </script>
    </body>
    </html>
    """)

@app.route("/signals", methods=["GET"])
def get_signals():
    try:
        query = supabase.table("signals").select("*").order("created_at", desc=True).limit(1000)
        result = query.execute()
        return jsonify({"signals": result.data, "count": len(result.data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/refresh", methods=["GET", "POST"])
def manual_refresh():
    try:
        scheduler.add_job(scrape_jobs, 'date', run_date=datetime.now(timezone.utc))
        return jsonify({"status": "ok", "message": "Background scrape started."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

scheduler = BackgroundScheduler()
scheduler.add_job(scrape_jobs, "cron", hour=7, minute=0, timezone="America/New_York")
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
