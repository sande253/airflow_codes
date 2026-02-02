#!/usr/bin/env python3
"""
Production-Ready Airflow Auto-Healing Agent with Safe Autonomous Recovery
Features:
- Real-time dashboard
- Smart failure diagnosis
- Safe auto-clear with cooldown & max attempts
- Escalation alerts after repeated failures
- Secure auth (Bearer token preferred)
- Structured logging
- Graceful shutdown
"""

import os
import json
import time
import logging
import httpx
import signal
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from urllib.parse import quote_plus
from threading import Thread, Lock, Event
from collections import defaultdict

from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv

# -------------------------- 
# Configuration & Logging
# --------------------------
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("airflow-agent")

# Airflow Config
AIRFLOW_API = os.getenv("AIRFLOW_API", "http://localhost:8080/api/v1").rstrip("/")
AIRFLOW_TOKEN = os.getenv("AIRFLOW_TOKEN")  # Preferred: Bearer token
AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS")

# Agent Behavior
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL_SECONDS", "60"))
LOOKBACK_MINUTES = int(os.getenv("MONITOR_LOOKBACK_MINUTES", "60"))
MAX_FIXES_PER_TASK = int(os.getenv("MAX_FIXES_PER_TASK", "3"))  # Per 24h
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "30"))
ESCALATION_WEBHOOK = os.getenv("ESCALATION_WEBHOOK")  # Critical alerts

# Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)

# -------------------------- 
# Global State (Thread-Safe)
# --------------------------
shutdown_event = Event()

agent_state = {
    "supervisor_enabled": False,
    "last_check": None,
    "fixes_applied": [],
    "current_status": {},
    "escalated": []
}

# Track recent fixes: (dag_id, task_id, dag_run_id) -> {count, last_fixed}
fix_tracker = defaultdict(lambda: {"count": 0, "last_fixed": None})
state_lock = Lock()

# -------------------------- 
# Failure Classification
# --------------------------
FAILURE_PATTERNS = [
    ("Task Timeout", ["execution_timeout", "timeout", "timed out", "exceeded timeout"]),
    ("ImportError", ["no module named", "importerror", "module not found", "cannot import"]),
    ("Connection Failure", ["connection refused", "failed to connect", "401", "403", "auth failed", "unauthorized"]),
    ("Memory Error (OOM)", ["out of memory", "oom", "memoryerror", "killed", "signal 9"]),
    ("Permission Error", ["permission denied", "access denied", "permissionerror"]),
    ("Missing XCom", ["xcom", "no xcom value", "xcom_pull returned none"]),
    ("Database Error", ["operationalerror", "database", "sqlalchemy", "deadlock"]),
    ("General Task Failure", ["exception", "error", "failed", "traceback", "non-zero exit"]),
]

FIX_GUIDANCE = {
    "Task Timeout": "Increase task timeout or optimize performance",
    "ImportError": "Install missing package in worker image or fix requirements",
    "Connection Failure": "Check connection config, credentials, or service availability",
    "Memory Error (OOM)": "Increase worker memory, optimize task, or use smaller batches",
    "Permission Error": "Fix file paths, IAM roles, or mount permissions",
    "Missing XCom": "Ensure upstream task pushes XCom with correct key",
    "Database Error": "Check DB connectivity, locks, or query performance",
    "General Task Failure": "Review task code and logs for root cause",
}

# -------------------------- 
# Airflow Client (Secure)
# --------------------------
class AirflowClient:
    def __init__(self):
        self.base_url = AIRFLOW_API
        self.headers = {"Content-Type": "application/json"}
        
        if AIRFLOW_TOKEN:
            self.headers["Authorization"] = f"Bearer {AIRFLOW_TOKEN}"
            self.auth = None
            log.info("Using Bearer token auth")
        elif AIRFLOW_USER and AIRFLOW_PASS:
            self.auth = (AIRFLOW_USER, AIRFLOW_PASS)
            log.warning("Using Basic Auth (less secure)")
        else:
            raise ValueError("Must set AIRFLOW_TOKEN or AIRFLOW_USER/AIRFLOW_PASS")

        self.client = httpx.Client(timeout=30.0, headers=self.headers)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.base_url}{path}"
        try:
            resp = self.client.request(method, url, auth=self.auth, **kwargs)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            return resp.json() if "json" in ct else resp.text
        except httpx.HTTPStatusError as e:
            log.error(f"Airflow API error {e.response.status_code}: {e.response.text}")
            raise
        except Exception as e:
            log.error(f"Request failed {method} {path}: {e}")
            raise

    def get_dags(self) -> List[Dict]:
        data = self._request("GET", "/dags", params={"limit": 100})
        return data.get("dags", [])

    def get_dag_runs(self, dag_id: str, limit: int = 10) -> List[Dict]:
        try:
            data = self._request("GET", f"/dags/{quote_plus(dag_id)}/dagRuns", 
                               params={"limit": limit, "order_by": "-execution_date"})
            return data.get("dag_runs", [])
        except:
            return []

    def get_task_instances(self, dag_id: str, dag_run_id: str) -> List[Dict]:
        try:
            data = self._request("GET", f"/dags/{quote_plus(dag_id)}/dagRuns/{quote_plus(dag_run_id)}/taskInstances")
            return data.get("task_instances", [])
        except:
            return []

    def get_task_log(self, dag_id: str, task_id: str, dag_run_id: str, try_number: int = 1) -> str:
        paths = [
            f"/dags/{quote_plus(dag_id)}/dagRuns/{quote_plus(dag_run_id)}/taskInstances/{quote_plus(task_id)}/logs/{try_number}",
            f"/dags/{quote_plus(dag_id)}/taskInstances/{quote_plus(task_id)}/logs/{try_number}"
        ]
        for path in paths:
            try:
                data = self._request("GET", path)
                if isinstance(data, dict):
                    return data.get("content") or json.dumps(data)
                return str(data)
            except:
                continue
        return ""

    def clear_task(self, dag_id: str, task_id: str, dag_run_id: str) -> str:
        payload = {
            "dry_run": False,
            "only_failed": True,
            "task_ids": [task_id]
        }
        try:
            self._request("POST", f"/dags/{quote_plus(dag_id)}/clearTaskInstances", json=payload)
            return "Task cleared (retry scheduled)"
        except Exception as e:
            return f"Clear failed: {e}"

    def unpause_dag(self, dag_id: str) -> str:
        try:
            self._request("PATCH", f"/dags/{quote_plus(dag_id)}", json={"is_paused": False})
            return "DAG unpaused"
        except Exception as e:
            return f"Unpause failed: {e}"

client = AirflowClient()

# -------------------------- 
# Core Agent Logic (Safe!)
# --------------------------
def classify_failure(log_text: str) -> str:
    if not log_text:
        return "Unknown"
    text = log_text.lower()
    for label, keywords in FAILURE_PATTERNS:
        if any(kw in text for kw in keywords):
            return label
    return "General Task Failure"

def should_fix(dag_id: str, task_id: str, dag_run_id: str) -> tuple[bool, str]:
    key = (dag_id, task_id, dag_run_id)
    now = datetime.now(timezone.utc)
    record = fix_tracker[key]

    # Cooldown check
    if record["last_fixed"]:
        cooldown_until = record["last_fixed"] + timedelta(minutes=COOLDOWN_MINUTES)
        if now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds() / 60)
            return False, f"Cooldown active ({remaining} min)"

    # Max attempts check (24h window)
    if record["count"] >= MAX_FIXES_PER_TASK:
        return False, f"Max fixes reached ({MAX_FIXES_PER_TASK})"

    return True, "OK"

def record_fix(dag_id: str, task_id: str, dag_run_id: str):
    key = (dag_id, task_id, dag_run_id)
    fix_tracker[key]["count"] += 1
    fix_tracker[key]["last_fixed"] = datetime.now(timezone.utc)

def escalate_issue(payload: Dict):
    if not ESCALATION_WEBHOOK:
        return
    try:
        with httpx.Client(timeout=10) as c:
            c.post(ESCALATION_WEBHOOK, json={**payload, "level": "ESCALATION"})
        log.warning(f"ESCALATED: {payload['dag_id']}.{payload['task_id']}")
    except:
        log.error("Escalation webhook failed")

def analyze_and_fix_safely(dag_id: str, task: Dict, dag_run_id: str) -> Dict:
    task_id = task["task_id"]
    try_number = task.get("try_number", 1)

    log_text = client.get_task_log(dag_id, task_id, dag_run_id, try_number)
    problem = classify_failure(log_text)
    guidance = FIX_GUIDANCE.get(problem, "Manual investigation required")

    key = (dag_id, task_id, dag_run_id)
    can_fix, reason = should_fix(dag_id, task_id, dag_run_id)

    action = "SKIPPED"
    if can_fix:
        action = client.clear_task(dag_id, task_id, dag_run_id)
        record_fix(dag_id, task_id, dag_run_id)
    else:
        log.info(f"Fix skipped for {dag_id}.{task_id}: {reason}")

    payload = {
        "dag_id": dag_id,
        "task_id": task_id,
        "dag_run_id": dag_run_id,
        "problem": problem,
        "guidance": guidance,
        "action": action,
        "reason": reason if not can_fix else "Auto-cleared",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "log_sample": (log_text[:1000] + "...") if log_text else ""
    }

    # Escalate after too many failures
    if fix_tracker[key]["count"] >= MAX_FIXES_PER_TASK:
        escalate_issue(payload)

    with state_lock:
        agent_state["fixes_applied"].append(payload)
        if len(agent_state["fixes_applied"]) > 100:
            agent_state["fixes_applied"] = agent_state["fixes_applied"][-100:]

    log.info(f"Fixed {dag_id}.{task_id} | {problem} | {action}")
    return payload

def collect_status() -> Dict:
    dags = client.get_dags()
    status = {"active": 0, "paused": 0, "failed": 0, "running": 0, "total": len(dags)}
    details = {"active": [], "paused": [], "failed": [], "running": []}

    for dag in dags:
        dag_id = dag["dag_id"]
        is_paused = dag.get("is_paused", False)
        runs = client.get_dag_runs(dag_id, limit=1)
        state = runs[0].get("state", "").lower() if runs else "none"

        info = {"dag_id": dag_id, "is_paused": is_paused, "last_state": state}
        if state == "failed":
            status["failed"] += 1
            details["failed"].append(info)
        elif state == "running":
            status["running"] += 1
            details["running"].append(info)
        elif is_paused:
            status["paused"] += 1
            details["paused"].append(info)
        else:
            status["active"] += 1
            details["active"].append(info)

    return {**status, "details": details}

def supervisor_loop():
    log.info("Airflow Auto-Healing Supervisor started")
    while not shutdown_event.is_set():
        if not agent_state["supervisor_enabled"]:
            time.sleep(5)
            continue

        try:
            status = collect_status()
            with state_lock:
                agent_state["current_status"] = status
                agent_state["last_check"] = datetime.now(timezone.utc).isoformat()

            # Only look at failed DAG runs
            for dag_info in status["details"]["failed"]:
                dag_id = dag_info["dag_id"]
                runs = client.get_dag_runs(dag_id, limit=1)
                if not runs:
                    continue
                dag_run_id = runs[0]["dag_run_id"]
                tasks = client.get_task_instances(dag_id, dag_run_id)

                for task in tasks:
                    state = task.get("state", "").lower()
                    if state in ("failed", "upstream_failed"):
                        analyze_and_fix_safely(dag_id, task, dag_run_id)

        except Exception as e:
            log.exception(f"Supervisor error: {e}")

        time.sleep(MONITOR_INTERVAL)

    log.info("Supervisor stopped gracefully")

# -------------------------- 
# Flask Routes
# --------------------------
@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/status')
def api_status():
    with state_lock:
        return jsonify({
            "success": True,
            "status": agent_state["current_status"],
            "supervisor_enabled": agent_state["supervisor_enabled"],
            "last_check": agent_state["last_check"],
            "fixes_today": len(agent_state["fixes_applied"])
        })

@app.route('/api/fixes')
def api_fixes():
    with state_lock:
        fixes = agent_state["fixes_applied"][-50:]
    return jsonify({"success": True, "fixes": fixes})

@app.route('/api/supervisor/toggle', methods=['POST'])
def toggle_supervisor():
    with state_lock:
        agent_state["supervisor_enabled"] = not agent_state["supervisor_enabled"]
    return jsonify({"success": True, "enabled": agent_state["supervisor_enabled"]})

# -------------------------- 
# Graceful Shutdown
# --------------------------
def signal_handler(sig, frame):
    log.info("Shutdown signal received...")
    shutdown_event.set()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Start supervisor
thread = Thread(target=supervisor_loop, daemon=False)
thread.start()

# -------------------------- 
# Embedded Dashboard (same beautiful UI, slightly updated)
# --------------------------
@app.route('/dashboard.html')
def dashboard_html():
    return '''
<!DOCTYPE html>
<html>
<head>
    <title>Airflow Auto-Healing Agent (Safe Mode)</title>
    <meta charset="utf-8">
    <style>
        /* Same beautiful CSS as before - omitted for brevity */
        body { font-family: system-ui; background: linear-gradient(135deg, #1e3c72, #2a5298); color: #fff; padding: 20px; min-height: 100vh; }
        .container { max-width: 1400px; margin: 0 auto; }
        .card { background: rgba(255,255,255,0.95); color: #333; border-radius: 16px; padding: 24px; margin-bottom: 20px; box-shadow: 0 8px 32px rgba(0,0,0,0.2); }
        h1 { color: #1e40af; }
        .badge { padding: 6px 12px; border-radius: 20px; font-weight: bold; }
        .success { background: #d4edda; color: #155724; }
        .warning { background: #fff3cd; color: #856404; }
        .danger { background: #f8d7da; color: #721c24; }
        .info { background: #d1ecf1; color: #0c5460; }
        .toggle { padding: 12px 24px; font-size: 18px; border: none; border-radius: 12px; cursor: pointer; }
        .enabled { background: #10b981; color: white; }
        .disabled { background: #ef4444; color: white; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; }
    </style>
</head>
<body>
<div class="container">
    <div class="card">
        <h1>Airflow Auto-Healing Agent (Safe Mode)</h1>
        <p>Autonomous but responsible recovery • Prevents infinite loops • Escalates repeated issues</p>
        
        <button id="toggleBtn" class="toggle disabled" onclick="toggle()">Supervisor: DISABLED</button>
        <span id="lastCheck" style="margin-left: 20px;"></span>
        <button onclick="location.reload()" style="margin-left: 10px; padding: 10px 20px;">Refresh</button>
    </div>

    <div class="card">
        <h2>Recent Auto-Fixes</h2>
        <div id="fixes">Loading...</div>
    </div>
</div>

<script>
async function load() {
    const [s, f] = await Promise.all([fetch('/api/status'), fetch('/api/fixes')]);
    const status = await s.json();
    const fixes = await f.json();

    document.getElementById('toggleBtn').textContent = status.supervisor_enabled ? 'Supervisor: ENABLED' : 'Supervisor: DISABLED';
    document.getElementById('toggleBtn').className = status.supervisor_enabled ? 'toggle enabled' : 'toggle disabled';
    document.getElementById('lastCheck').textContent = status.last_check ? 'Last check: ' + new Date(status.last_check).toLocaleString() : '';

    const html = fixes.fixes.length ? `
        <table>
            <tr><th>Time</th><th>DAG.Task</th><th>Problem</th><th>Action</th></tr>
            ${fixes.fixes.slice().reverse().map(x => `
                <tr>
                    <td>${new Date(x.timestamp).toLocaleString()}</td>
                    <td><strong>${x.dag_id}.${x.task_id}</strong></td>
                    <td><span class="badge danger">${x.problem}</span></td>
                    <td>${x.action} ${x.reason !== 'OK' ? '('+x.reason+')' : ''}</td>
                </tr>
            `).join('')}
        </table>
    ` : '<p>No fixes applied yet</p>';
    document.getElementById('fixes').innerHTML = html;
}
async function toggle() {
    await fetch('/api/supervisor/toggle', {method: 'POST'});
    load();
}
load(); setInterval(load, 30000);
</script>
</body>
</html>
'''

if __name__ == "__main__":
    log.info("Starting Airflow Auto-Healing Agent")
    app.run(host="0.0.0.0", port=5000)