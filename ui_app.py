# ui_app.py
from flask import Flask, render_template, jsonify
import asyncio
from agent import LOG_BUFFER
import httpx
import os

AIRFLOW_API = os.getenv("AIRFLOW_API", "http://host.docker.internal:8080/api/v1")
AUTH = (os.getenv("AIRFLOW_USER", "admin"), os.getenv("AIRFLOW_PASS", "admin"))

app = Flask(__name__, template_folder="ui_templates", static_folder="static")

async def fetch_json(url):
    async with httpx.AsyncClient(timeout=10) as client:
        res = await client.get(url, auth=AUTH)
        res.raise_for_status()
        return res.json()

@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/api/stats")
async def api_stats():
    """Returns metrics for the dashboard cards + graphs."""
    dags = await fetch_json(f"{AIRFLOW_API}/dags?limit=1000")
    dags = dags.get("dags", [])

    active = sum(1 for d in dags if not d.get("is_paused"))
    paused = sum(1 for d in dags if d.get("is_paused"))

    # Fetch scheduler slots (workers)
    try:
        workers = await fetch_json(f"{AIRFLOW_API}/pools")
        pools = workers.get("pools", [])
        slots_total = sum(int(p["slots"]) for p in pools)
        slots_used = sum(int(p["occupied_slots"]) for p in pools)
    except:
        slots_total, slots_used = 0, 0

    slots_free = max(0, slots_total - slots_used)

    # Fetch failures for graph
    dag_runs_resp = await fetch_json(f"{AIRFLOW_API}/dagRuns?limit=200")
    dag_runs = dag_runs_resp.get("dag_runs", [])

    failed = sum(1 for r in dag_runs if r["state"] == "failed")
    succeeded = sum(1 for r in dag_runs if r["state"] == "success")

    return jsonify({
        "card_stats": {
            "active_dags": active,
            "paused_dags": paused,
            "failed_runs": failed,
            "succeeded_runs": succeeded,
            "slots_total": slots_total,
            "slots_used": slots_used,
            "slots_free": slots_free,
        }
    })

@app.route("/api/logs")
def api_logs():
    """Returns agent logs."""
    return jsonify(list(reversed(LOG_BUFFER[-200:])))

@app.route("/api/failures")
async def api_failures():
    """Recent failed tasks table."""
    try:
        data = await fetch_json(f"{AIRFLOW_API}/dagRuns?limit=50&order_by=-execution_date")
        runs = data.get("dag_runs", [])
        failed = [r for r in runs if r["state"] == "failed"]
    except:
        failed = []
    return jsonify(failed[:15])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
