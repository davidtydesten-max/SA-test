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

# Credentials
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SERPAPI_KEY = os.environ.get("SERPAPI_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def get_state(text, loc):
    zone = f"{text} {loc}".upper()
    if any(k in zone for k in ["REMOTE", "WORK FROM HOME", "VIRTUAL"]): return "Remote"
    if any(c in zone for c in ["NY", "NEW YORK", "NYC"]): return "NY"
    if any(c in zone for c in ["NJ", "NEW JERSEY"]): return "NJ"
    if any(c in zone for c in ["CT", "CONNECTICUT"]): return "CT"
    return "USA"

def scrape_jobs():
    logger.info("Scrape started...")
    new_data = []
    
    # We only use one high-performing query to ensure we get results fast
    params = {
        "engine": "google_jobs",
        "q": "OneStream",
        "api_key": SERPAPI_KEY,
        "gl": "us"
    }
    
    try:
        search = GoogleSearch(params)
        res = search.get_dict()
        jobs = res.get("jobs_results", [])
        
        for j in jobs:
            # Prioritize apply_options for valid links
            apply_links = j.get("apply_options", [])
            url = apply_links[0].get("link") if apply_links else j.get("share_link")
            
            if not url: continue

            new_data.append({
                "company": j.get("company_name", "Unknown"),
                "job_title": j.get("title", "OneStream Role"),
                "region": get_state(j.get("title", "") + j.get("description", ""), j.get("location", "")),
                "source": j.get("via", "Google"),
                "source_url": url,
                "posted_at": datetime.now(timezone.utc).isoformat(),
                "signal_type": "role"
            })
            
        if new_data:
            # We use a simple insert. If it fails due to a duplicate URL, it will just skip that one.
            for record in new_data:
                try:
                    supabase.table("signals").insert(record).execute()
                except:
                    continue # Skip duplicates
        logger.info(f"Scrape finished. Found {len(new_data)} items.")
    except Exception as e:
        logger.error(f"Scrape failed: {e}")

@app.route("/dashboard")
def dashboard():
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>OneStream Lead Hub</title>
        <style>
            body { font-family: sans-serif; background: #f4f7f6; padding: 40px; }
            .container { max-width: 1000px; margin: auto; background: white; padding: 20px; border-radius: 8px; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; }
            th { text-align: left; padding: 10px; border-bottom: 2px solid #eee; }
            td { padding: 10px; border-bottom: 1px solid #eee; }
            .btn { background: #28a745; color: white; border: none; padding: 10px; cursor: pointer; border-radius: 4px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h2>OneStream Live Leads</h2>
            <button class="btn" onclick="fetch('/refresh', {method:'POST'}); alert('Updating...');">Update Now</button>
            <table id="table">
                <thead><tr><th>Company</th><th>Role</th><th>State</th><th>Link</th></tr></thead>
                <tbody id="rows"></tbody>
            </table>
        </div>
        <script>
            async function load() {
                const r = await fetch('/signals');
                const d = await r.json();
                document.getElementById('rows').innerHTML = d.map(s => `
                    <tr>
                        <td><strong>${s.company}</strong></td>
                        <td>${s.job_title}</td>
                        <td>${s.region}</td>
                        <td><a href="${s.source_url}" target="_blank">View</a></td>
                    </tr>
                `).join('');
            }
            load();
            setInterval(load, 5000);
        </script>
    </body>
    </html>
    """)

@app.route("/signals")
def get_signals():
    res = supabase.table("signals").select("*").order("posted_at", desc=True).limit(50).execute()
    return jsonify(res.data)

@app.route("/refresh", methods=["POST"])
def refresh():
    scrape_jobs()
    return jsonify({"status": "done"})

@app.route("/")
def home(): return "Visit /dashboard"

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
