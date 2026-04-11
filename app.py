import os
import re
import time
import logging
from datetime import datetime, timezone
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

# Broadened keywords to catch hidden ads
SEARCH_QUERIES = [
    "OneStream",
    "OneStream Consultant",
    "OneStream Developer",
    "OneStream Architect",
    "OneStream EPM jobs",
    "SystemsAccountants OneStream"
]

def get_state(text, loc):
    zone = f"{text} {loc}".upper()
    if any(k in zone for k in ["REMOTE", "WORK FROM HOME", "VIRTUAL"]): return "Remote"
    if any(c in zone for c in ["NY", "NEW YORK", "NYC"]): return "NY"
    if any(c in zone for c in ["NJ", "NEW JERSEY"]): return "NJ"
    if any(c in zone for c in ["CT", "CONNECTICUT"]): return "CT"
    if any(c in zone for c in ["PA", "PENNSYLVANIA", "PHILLY"]): return "PA"
    return "USA"

def scrape_jobs():
    logger.info("Starting Deep Scrape...")
    raw_results = []
    
    for q in SEARCH_QUERIES:
        # Loop through pages 0, 10, and 20 to get 30+ results per query
        for page_offset in [0, 10, 20]:
            try:
                params = {
                    "engine": "google_jobs",
                    "q": q,
                    "api_key": SERPAPI_KEY,
                    "gl": "us",
                    "hl": "en",
                    "start": page_offset 
                }
                search = GoogleSearch(params)
                res = search.get_dict()
                jobs = res.get("jobs_results", [])
                
                if not jobs:
                    break # Stop if a page comes back empty

                for j in jobs:
                    apply_links = j.get("apply_options", [])
                    url = apply_links[0].get("link") if apply_links else j.get("share_link")
                    if not url: continue

                    raw_results.append({
                        "company": j.get("company_name", "Unknown"),
                        "job_title": j.get("title", "OneStream Role"),
                        "region": get_state(j.get("title", "") + j.get("description", ""), j.get("location", "")),
                        "source": j.get("via", "Google"),
                        "source_url": url,
                        "posted_at": datetime.now(timezone.utc).isoformat(),
                        "signal_type": "role",
                        "location": j.get("location", "USA"),
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    })
                time.sleep(1) # Ethical delay between pages
            except Exception as e:
                logger.error(f"Error on {q} page {page_offset}: {e}")

    # Locally deduplicate by URL
    unique_data = {}
    for item in raw_results:
        unique_data[item["source_url"]] = item
    
    final_payload = list(unique_data.values())

    if final_payload:
        try:
            supabase.table("signals").upsert(final_payload, on_conflict="source_url").execute()
            logger.info(f"Deep search complete. Synced {len(final_payload)} unique leads.")
        except Exception as e:
            logger.error(f"Database sync error: {e}")

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>OneStream Leads</title>
        <style>
            body { font-family: 'Segoe UI', sans-serif; background: #f4f7f6; padding: 20px; }
            .container { max-width: 1100px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            table { width: 100%; border-collapse: collapse; }
            th { text-align: left; padding: 12px; border-bottom: 2px solid #eee; background: #fcfcfc; color: #666; font-size: 13px; }
            td { padding: 12px; border-bottom: 1px solid #eee; font-size: 14px; }
            .btn { background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: bold; }
            .tag { background: #eef6ff; color: #007bff; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 11px; }
            a { color: #007bff; text-decoration: none; font-weight: 600; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h2>OneStream Market Intelligence</h2>
                <button class="btn" onclick="update()">Deep Refresh</button>
            </header>
            <table>
                <thead><tr><th>Date</th><th>Company</th><th>Role</th><th>State</th><th>Action</th></tr></thead>
                <tbody id="rows"></tbody>
            </table>
        </div>
        <script>
            async function load() {
                const r = await fetch('/signals');
                const d = await r.json();
                document.getElementById('rows').innerHTML = d.map(s => `
                    <tr>
                        <td style="color: #888">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td><strong>${s.company}</strong></td>
                        <td>${s.job_title}</td>
                        <td><span class="tag">${s.region}</span></td>
                        <td><a href="${s.source_url}" target="_blank">View Post</a></td>
                    </tr>
                `).join('');
            }
            async function update() {
                alert("Deep search triggered. This may take up to 60 seconds.");
                await fetch('/refresh', {method:'POST'});
            }
            load();
            setInterval(load, 5000);
        </script>
    </body>
    </html>
    """)

@app.route("/signals")
def get_signals():
    res = supabase.table("signals").select("*").order("posted_at", desc=True).limit(200).execute()
    return jsonify(res.data)

@app.route("/refresh", methods=["POST"])
def refresh():
    scrape_jobs()
    return jsonify({"status": "done"})

@app.route("/")
def home(): return "Dashboard at /dashboard"

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
