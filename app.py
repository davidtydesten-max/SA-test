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

# Broadened queries for maximum volume
SEARCH_QUERIES = [
    "OneStream Consultant",
    "OneStream Developer",
    "OneStream Administrator",
    "OneStream Architect",
    "OneStream implementation"
]

def get_state(text, loc):
    zone = f"{text} {loc}".upper()
    if any(k in zone for k in ["REMOTE", "WORK FROM HOME", "VIRTUAL"]): return "Remote"
    # Tri-State Quick Check
    if any(c in zone for c in ["NY", "NEW YORK", "NYC", "BROOKLYN"]): return "NY"
    if any(c in zone for c in ["NJ", "NEW JERSEY", "NEWARK", "JERSEY CITY"]): return "NJ"
    if any(c in zone for c in ["CT", "CONNECTICUT", "STAMFORD", "HARTFORD"]): return "CT"
    # Generic USA catch-all
    return "USA"

def scrape_jobs():
    logger.info("Starting Scrape...")
    new_data = []
    
    for q in SEARCH_QUERIES:
        try:
            params = {
                "engine": "google_jobs",
                "q": q,
                "api_key": SERPAPI_KEY,
                "gl": "us",
                "hl": "en"
            }
            res = GoogleSearch(params).get_dict()
            jobs = res.get("jobs_results", [])
            
            for j in jobs:
                # FIX: Find the REAL link inside apply_options
                apply_links = j.get("apply_options", [])
                job_url = ""
                if apply_links:
                    job_url = apply_links[0].get("link") # Take the first direct apply link
                else:
                    job_url = j.get("related_links", [{}])[0].get("link") or j.get("share_link")

                if not job_url: continue
                
                # Cleanup title and company
                title = j.get("title", "OneStream Role")
                company = j.get("company_name", "Company Name Pending")
                snippet = j.get("description", "")
                location = j.get("location", "USA")
                
                new_data.append({
                    "company": company,
                    "job_title": title,
                    "region": get_state(title + snippet, location),
                    "source": j.get("via", "Google Jobs"),
                    "source_url": job_url,
                    "posted_at": datetime.now(timezone.utc).isoformat(),
                    "signal_type": "role",
                    "location": location,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                })
        except Exception as e:
            logger.error(f"Error scraping {q}: {e}")
        time.sleep(1)

    if new_data:
        # Using upsert with on_conflict to prevent duplicates while allowing new entries
        supabase.table("signals").upsert(new_data, on_conflict="source_url").execute()
        logger.info(f"Saved {len(new_data)} leads.")

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>OneStream Lead Hub</title>
        <style>
            body { font-family: 'Segoe UI', sans-serif; background: #f4f7f6; margin: 0; padding: 40px; }
            .container { max-width: 1100px; margin: auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); padding: 20px; }
            header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th { text-align: left; color: #666; font-size: 12px; border-bottom: 2px solid #eee; padding: 10px; }
            td { padding: 15px; border-bottom: 1px solid #eee; font-size: 14px; }
            .tag { background: #007bff; color: white; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
            .btn { background: #28a745; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; }
            a { color: #007bff; text-decoration: none; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h2>OneStream Live Leads</h2>
                <button class="btn" onclick="update()">Refresh Data</button>
            </header>
            <table>
                <thead>
                    <tr><th>Date</th><th>Company</th><th>Position</th><th>State</th><th>Action</th></tr>
                </thead>
                <tbody id="data-rows"></tbody>
            </table>
        </div>
        <script>
            async function fetchLeads() {
                const res = await fetch('/signals');
                const data = await res.json();
                document.getElementById('data-rows').innerHTML = data.signals.map(s => `
                    <tr>
                        <td style="color: #999">${new Date(s.posted_at).toLocaleDateString()}</td>
                        <td><strong>${s.company}</strong></td>
                        <td>${s.job_title}</td>
                        <td><span class="tag">${s.region}</span></td>
                        <td><a href="${s.source_url}" target="_blank">View Post</a></td>
                    </tr>
                `).join('');
            }
            async function update() {
                alert('Scrape triggered. Please wait 30 seconds.');
                await fetch('/refresh', {method: 'POST'});
            }
            fetchLeads();
            setInterval(fetchLeads, 10000); // Auto-refresh table every 10s
        </script>
    </body>
    </html>
    """)

@app.route("/signals")
def get_signals():
    res = supabase.table("signals").select("*").order("posted_at", desc=True).limit(100).execute()
    return jsonify({"signals": res.data})

@app.route("/refresh", methods=["POST"])
def manual_refresh():
    scheduler.add_job(scrape_jobs, 'date', run_date=datetime.now(timezone.utc))
    return jsonify({"status": "triggered"})

@app.route("/")
def home(): return "Use /dashboard"

scheduler = BackgroundScheduler()
scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
