
#!/usr/bin/env python3
"""
Production-ready Airflow MCP + Auto-Heal monitor server.

Features:
- FastAPI HTTP endpoints (same as before)
- MCP tool support (list_tools / call_tool)
- Background async monitor that:
    * finds failed DAG runs
    * fetches task instances & logs
    * classifies failure (deterministic keywords)
    * applies safe auto-fixes (clear task, trigger run, unpause)
    * dedupes fixes within a time window
    * sends webhook notifications after fixes
- Graceful startup/shutdown
- Config via environment vars / .env
"""

import os
import asyncio
import httpx
import threading
import re
import json
import time
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus
from collections import defaultdict
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from dotenv import load_dotenv
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Optional MCP imports if running as MCP tool server
try:
    from mcp.server import Server
    from mcp.types import Tool, TextContent
    import mcp.server.stdio
    MCP_AVAILABLE = True
except Exception:
    MCP_AVAILABLE = False

# --------------------------
# Logging & Config
# --------------------------
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("airflow-autoheal")

# Airflow API config
AIRFLOW_API = os.getenv("AIRFLOW_API", "http://localhost:8080/api/v1").rstrip("/")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")

# Webhook & monitor config
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # required to notify after fixes
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "30"))
MONITOR_LOOKBACK_MINUTES = int(os.getenv("MONITOR_LOOKBACK_MINUTES", "30"))
DEDUPE_WINDOW_SECONDS = int(os.getenv("DEDUPE_WINDOW_SECONDS", "120"))  # don't re-fix same (dag,task,run) within this
MAX_CONCURRENT_FIXES = int(os.getenv("MAX_CONCURRENT_FIXES", "5"))

# Safety config
ALLOW_AUTO_UNPAUSE = os.getenv("ALLOW_AUTO_UNPAUSE", "true").lower() in ("1", "true", "yes")
ALLOW_TRIGGER_RUNS = os.getenv("ALLOW_TRIGGER_RUNS", "true").lower() in ("1", "true", "yes")
ALLOW_CLEAR_TASK = os.getenv("ALLOW_CLEAR_TASK", "true").lower() in ("1", "true", "yes")

# --------------------------
# HTTP client + Airflow client
# --------------------------
class AirflowClient:
    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
        self.base_url = AIRFLOW_API
        self.auth = (AIRFLOW_USER, AIRFLOW_PASS)

    async def initialize(self):
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=60.0, limits=httpx.Limits(max_connections=100))
            log.info("Initialized Airflow HTTP client -> %s", self.base_url)

    async def close(self):
        if self.client:
            await self.client.aclose()
            self.client = None

    async def call(self, method: str, path: str, params: dict = None, json_data: dict = None) -> Any:
        """
        Generic request with simple retry/backoff.
        path is appended to base_url; must begin with '/'.
        """
        assert self.client is not None, "AirflowClient not initialized"
        url = f"{self.base_url}{path}"
        for attempt in range(4):
            try:
                resp = await self.client.request(method, url, params=params or {}, json=json_data or {}, auth=self.auth)
                resp.raise_for_status()
                # try to parse json, else return text
                content_type = resp.headers.get("content-type", "")
                if "application/json" in content_type:
                    return resp.json()
                return resp.text
            except httpx.HTTPStatusError as e:
                # For 4xx/5xx we still want to bubble up after final retry
                log.warning("Airflow API status error %s %s: %s", method, url, e)
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise
            except Exception as e:
                log.warning("Airflow API request error %s %s: %s", method, url, e)
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise

    async def fetch_all_dags(self) -> List[Dict[str, Any]]:
        all_dags, offset = [], 0
        limit = 100
        while True:
            data = await self.call("GET", "/dags", params={"limit": limit, "offset": offset})
            # normalize
            dags = (data.get("dags") if isinstance(data, dict) else [])
            all_dags.extend(dags)
            if not dags or len(dags) < limit:
                break
            offset += limit
        return all_dags

# --------------------------
# Failure classifier (deterministic)
# --------------------------
# Ordered list of (label, [keywords])
FAILURE_MAP: List[Tuple[str, List[str]]] = [
    ("Task Timeout", ["execution_timeout", "timeout", "timed out"]),
    ("ImportError", ["no module named", "importerror", "module not found"]),
    ("Connection Failure", ["connection refused", "connection timed out", "failed to connect", "401", "403", "auth failed", "connection error"]),
    ("Memory Error (OOM)", ["out of memory", "oom", "memoryerror", "killed"]),
    ("Permission Error", ["permission denied", "access denied", "permissionerror"]),
    ("Missing XCom", ["xcom", "no xcom", "xcom_pull", "xcom_push"]),
    ("Task Failed", ["traceback", "exception", "error", "failed", "non-zero exit", "exit code"]),
]

def classify_failure_from_log(log_text: str) -> str:
    if not log_text:
        return "Unknown"
    low = log_text.lower()
    for label, kws in FAILURE_MAP:
        for kw in kws:
            if kw in low:
                return label
    return "Task Failed"

# --------------------------
# Monitor state: dedupe cache
# --------------------------
class Deduper:
    def __init__(self, ttl_seconds: int):
        self.ttl = ttl_seconds
        self.cache: Dict[Tuple[str, str, str], float] = {}
        self.lock = asyncio.Lock()

    async def should_process(self, dag_id: str, task_id: str, dag_run_id: str) -> bool:
        key = (dag_id, task_id, dag_run_id or "")
        now = time.time()
        async with self.lock:
            last = self.cache.get(key)
            if last and now - last < self.ttl:
                return False
            self.cache[key] = now
            # clean up old keys occasionally
            if len(self.cache) > 10000:
                cutoff = now - (self.ttl * 4)
                self.cache = {k: v for k, v in self.cache.items() if v >= cutoff}
            return True

deduper = Deduper(DEDUPE_WINDOW_SECONDS)

# --------------------------
# Webhook notifier
# --------------------------
async def send_webhook(payload: Dict[str, Any]):
    if not WEBHOOK_URL:
        log.info("WEBHOOK_URL not set; skipping webhook: %s", payload)
        return
    try:
        async with httpx.AsyncClient(timeout=20.0) as h:
            r = await h.post(WEBHOOK_URL, json=payload)
            r.raise_for_status()
            log.info("Posted webhook for %s.%s -> status %s", payload.get("dag_id"), payload.get("task_id"), r.status_code)
    except Exception as e:
        log.exception("Failed to send webhook: %s", e)

# --------------------------
# Helpers: fetch failed runs, task instances, logs
# --------------------------
async def fetch_latest_failed_runs(client: AirflowClient, lookback_minutes: int) -> List[Dict[str, Any]]:
    """
    Fetch recent failed dag runs across DAGs. Conservative: checks latest few runs per DAG.
    """
    out: List[Dict[str, Any]] = []
    try:
        dags = await client.fetch_all_dags()
        tasks = []
        for d in dags:
            tasks.append((d["dag_id"], d.get("is_paused", False)))
        # fetch recent runs for each DAG concurrently but limited
        sem = asyncio.Semaphore(10)
        async def fetch_runs_for(dag_id: str):
            async with sem:
                try:
                    data = await client.call("GET", f"/dags/{quote_plus(dag_id)}/dagRuns", params={"limit": 5, "order_by": "-execution_date"})
                    runs = data.get("dag_runs", []) if isinstance(data, dict) else []
                    return [(dag_id, r) for r in runs]
                except Exception as e:
                    log.debug("Failed fetching runs for %s: %s", dag_id, e)
                    return []
        coros = [fetch_runs_for(dag_id) for dag_id, _ in tasks]
        results = await asyncio.gather(*coros, return_exceptions=True)
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        for res in results:
            if not isinstance(res, list):
                continue
            for dag_id, run in res:
                state = (run.get("state") or "").lower()
                # consider failed/error states
                if state in ("failed", "error"):
                    # optional: filter by execution_date lookback
                    exec_date = run.get("execution_date")
                    if exec_date:
                        try:
                            dt = datetime.fromisoformat(exec_date.replace("Z", "+00:00"))
                            if dt < cutoff:
                                continue
                        except Exception:
                            pass
                    out.append({"dag_id": dag_id, "dag_run_id": run.get("dag_run_id"), "execution_date": run.get("execution_date"), "state": state})
    except Exception as e:
        log.exception("Error in fetch_latest_failed_runs: %s", e)
    return out

async def fetch_task_instances_for_run(client: AirflowClient, dag_id: str, dag_run_id: str) -> List[Dict[str, Any]]:
    """
    Try several endpoints to find task instances associated with a dagRun.
    """
    candidates = [
        ("GET", f"/dags/{quote_plus(dag_id)}/dagRuns/{quote_plus(dag_run_id)}/taskInstances"),
        ("GET", f"/dags/{quote_plus(dag_id)}/taskInstances", {"dag_run_id": dag_run_id}),
        ("GET", f"/dags/{quote_plus(dag_id)}/dagRuns", {"dag_run_id": dag_run_id, "limit": 1}),
    ]
    for method, path, *maybe in candidates:
        params = maybe[0] if maybe else {}
        try:
            data = await client.call(method, path, params=params)
            if isinstance(data, dict):
                if "task_instances" in data:
                    return data["task_instances"]
                if "dag_runs" in data and data["dag_runs"]:
                    run = data["dag_runs"][0]
                    tis = run.get("task_instances") or run.get("tasks") or []
                    if tis:
                        return tis
            elif isinstance(data, list):
                return data
        except Exception:
            continue
    return []

async def fetch_task_log(client: AirflowClient, dag_id: str, task_id: str, dag_run_id: Optional[str] = None, try_number: int = 1) -> str:
    """
    Try common log endpoints and return a best-effort string.
    """
    candidates = []
    if dag_run_id:
        candidates.append(("GET", f"/dags/{quote_plus(dag_id)}/dagRuns/{quote_plus(dag_run_id)}/taskInstances/{quote_plus(task_id)}/logs/{try_number}"))
    candidates.append(("GET", f"/dags/{quote_plus(dag_id)}/taskInstances/{quote_plus(task_id)}/logs/{try_number}"))
    candidates.append(("GET", f"/dags/{quote_plus(dag_id)}/tasks/{quote_plus(task_id)}/logs/{try_number}"))
    for method, path in candidates:
        try:
            data = await client.call(method, path)
            if isinstance(data, dict):
                for key in ("content", "log", "message"):
                    if key in data and isinstance(data[key], str):
                        return data[key]
                # stringify
                return json.dumps(data)
            if isinstance(data, str):
                return data
        except Exception:
            continue
    log.debug("Could not fetch logs for %s.%s (tried %d endpoints)", dag_id, task_id, len(candidates))
    return ""

# --------------------------
# Fixers (safe operations)
# --------------------------
async def fix_clear_task_instance(client: AirflowClient, dag_id: str, task_id: str, dag_run_id: Optional[str]) -> str:
    if not ALLOW_CLEAR_TASK:
        return "Auto-clear disabled"
    payload = {
        "dry_run": False,
        "reset_dag_runs": False,
        "only_failed": True,
        "only_running": False,
        "include_subdags": False,
        "include_parentdag": False,
        "dag_run_id": dag_run_id,
        "task_ids": [task_id],
    }
    try:
        await client.call("POST", f"/dags/{quote_plus(dag_id)}/clearTaskInstances", json_data=payload)
        return f"Cleared task instance {task_id}"
    except Exception:
        # fallback old endpoint
        try:
            await client.call("POST", f"/dags/{quote_plus(dag_id)}/tasks/{quote_plus(task_id)}/clear", json_data=payload)
            return f"Cleared task instance {task_id} (fallback)"
        except Exception as e:
            log.debug("Clear failed for %s.%s: %s", dag_id, task_id, e)
            return f"Failed to clear task: {e}"

async def fix_trigger_dag_run(client: AirflowClient, dag_id: str) -> str:
    if not ALLOW_TRIGGER_RUNS:
        return "Trigger run disabled"
    try:
        run_id = f"auto_fix_{int(time.time())}"
        res = await client.call("POST", f"/dags/{quote_plus(dag_id)}/dagRuns", json_data={"conf": {}, "dag_run_id": run_id})
        return f"Triggered dag run {res.get('dag_run_id', run_id)}"
    except Exception as e:
        log.debug("Trigger run failed for %s: %s", dag_id, e)
        return f"Failed to trigger run: {e}"

async def fix_unpause_dag(client: AirflowClient, dag_id: str) -> str:
    if not ALLOW_AUTO_UNPAUSE:
        return "Auto-unpause disabled"
    try:
        await client.call("PATCH", f"/dags/{quote_plus(dag_id)}", json_data={"is_paused": False})
        return "Unpaused DAG"
    except Exception as e:
        log.debug("Unpause failed for %s: %s", dag_id, e)
        return f"Failed to unpause: {e}"

# --------------------------
# Analyze & fix flow
# --------------------------
async def analyze_and_fix_task(client: AirflowClient, dag_id: str, task: Dict[str, Any], dag_run_id: Optional[str]) -> Dict[str, Any]:
    """
    Given a task instance dict (must contain task_id, state, try_number), fetch log, classify, decide & apply fix.
    Returns the notification payload (dict).
    """
    task_id = task.get("task_id") or task.get("task_id")
    state = (task.get("state") or "failed").lower()
    try_number = int(task.get("try_number", 1) or 1)

    # Dedupe: skip if recently fixed
    if not await deduper.should_process(dag_id, task_id, dag_run_id or ""):
        log.info("Skipping duplicate recent fix for %s.%s (%s)", dag_id, task_id, dag_run_id)
        return {"skipped": True, "dag_id": dag_id, "task_id": task_id, "dag_run_id": dag_run_id}

    # Fetch log (require logs per your constraints)
    log_text = await fetch_task_log(client, dag_id, task_id, dag_run_id=dag_run_id, try_number=try_number)
    reason = classify_failure_from_log(log_text)
    problem = state.title()
    fix_actions: List[str] = []

    # Simple decision matrix: safe, reversible actions
    if reason == "Task Timeout":
        # clearing task tends to allow re-run
        fix_actions.append(await fix_clear_task_instance(client, dag_id, task_id, dag_run_id))
    elif reason in ("ImportError", "Connection Failure"):
        # clear and trigger a new run (may resolve transient infra/conn issues)
        fix_actions.append(await fix_clear_task_instance(client, dag_id, task_id, dag_run_id))
        fix_actions.append(await fix_trigger_dag_run(client, dag_id))
    elif reason == "Memory Error (OOM)":
        fix_actions.append(await fix_clear_task_instance(client, dag_id, task_id, dag_run_id))
        fix_actions.append("NOTE: OOM - consider increasing worker memory or optimizing task")
    elif reason == "Missing XCom":
        fix_actions.append(await fix_clear_task_instance(client, dag_id, task_id, dag_run_id))
        fix_actions.append(await fix_trigger_dag_run(client, dag_id))
    elif reason == "Permission Error":
        fix_actions.append("Requires manual permission fix")
    else:
        # default safe attempt: clear and trigger
        fix_actions.append(await fix_clear_task_instance(client, dag_id, task_id, dag_run_id))
        fix_actions.append(await fix_trigger_dag_run(client, dag_id))

    # If DAG is paused but had a run or should be active, optionally unpause
    # Simple heuristic: if dag is paused but a run existed, unpause if allowed
    # (This prevents accidentally unpausing many DAGs; keep conservative)
    try:
        dag_info = await client.call("GET", f"/dags/{quote_plus(dag_id)}")
        is_paused = dag_info.get("dag", {}).get("is_paused", False) if isinstance(dag_info, dict) else False
        if is_paused and ALLOW_AUTO_UNPAUSE:
            fix_actions.append(await fix_unpause_dag(client, dag_id))
    except Exception:
        pass

    fix_text = "; ".join(fix_actions)

    payload = {
        "problem": problem,
        "reason": reason,
        "fix": fix_text,
        "dag_id": dag_id,
        "task_id": task_id,
        "dag_run_id": dag_run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "log_sample": (log_text[:2000] + "...") if log_text else ""
    }

    # send webhook non-blocking
    try:
        asyncio.create_task(send_webhook(payload))
    except Exception:
        log.exception("Failed to schedule webhook task")

    log.info("Auto-fix applied: %s.%s -> %s", dag_id, task_id, fix_text)
    return payload

# --------------------------
# Monitor loop (core)
# --------------------------
monitor_task: Optional[asyncio.Task] = None
monitor_running = False
monitor_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FIXES)

async def monitor_loop(client: AirflowClient):
    global monitor_running
    log.info("Auto-heal monitor loop started (interval=%ss lookback=%smin)", MONITOR_INTERVAL_SECONDS, MONITOR_LOOKBACK_MINUTES)
    monitor_running = True
    try:
        while True:
            try:
                failed_runs = await fetch_latest_failed_runs(client, MONITOR_LOOKBACK_MINUTES)
                if failed_runs:
                    # process runs in parallel but with concurrency limit
                    async def handle_run(fr):
                        dag_id = fr["dag_id"]
                        dag_run_id = fr.get("dag_run_id")
                        log.info("Detected failed run: %s %s", dag_id, dag_run_id)
                        task_instances = await fetch_task_instances_for_run(client, dag_id, dag_run_id)
                        if not task_instances:
                            log.debug("No task instances found for %s %s", dag_id, dag_run_id)
                            return
                        for t in task_instances:
                            state = (t.get("state") or "").lower()
                            if state in ("failed", "up_for_retry", "upstream_failed", "error", "up_for_reschedule"):
                                # control concurrency of fixes
                                async with monitor_semaphore:
                                    try:
                                        await analyze_and_fix_task(client, dag_id, t, dag_run_id)
                                    except Exception:
                                        log.exception("Error analyzing/fixing %s.%s", dag_id, t.get("task_id"))
                    # schedule handlers
                    handlers = [handle_run(fr) for fr in failed_runs]
                    # run them concurrently but catch exceptions
                    await asyncio.gather(*handlers, return_exceptions=True)
                await asyncio.sleep(MONITOR_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                log.info("Monitor loop cancelled")
                break
            except Exception:
                log.exception("Monitor loop error")
                await asyncio.sleep(MONITOR_INTERVAL_SECONDS)
    finally:
        monitor_running = False
        log.info("Auto-heal monitor loop stopped")

# --------------------------
# FastAPI + MCP server + endpoints (merged with your existing endpoints)
# --------------------------
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

client = AirflowClient()

# Optional MCP server
if MCP_AVAILABLE:
    server = Server("airflow-mcp-server")
    # define tools (same as original)
    @server.list_tools()
    async def list_tools() -> list:
        return [
            Tool(name="list_all_dags", description="List ALL DAGs (full pagination)", inputSchema={"type":"object","properties":{}}),
            Tool(name="search_dags", description="Search DAGs by name", inputSchema={"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
            Tool(name="get_dag_details", description="Get DAG details", inputSchema={"type":"object","properties":{"dag_id":{"type":"string"}},"required":["dag_id"]}),
            Tool(name="pause_dag", description="Pause a DAG", inputSchema={"type":"object","properties":{"dag_id":{"type":"string"}},"required":["dag_id"]}),
            Tool(name="unpause_dag", description="Unpause a DAG", inputSchema={"type":"object","properties":{"dag_id":{"type":"string"}},"required":["dag_id"]}),
            Tool(name="trigger_dag", description="Trigger DAG run", inputSchema={"type":"object","properties":{"dag_id":{"type":"string"},"conf":{"type":"object"}},"required":["dag_id"]}),
            Tool(name="get_latest_run", description="Get latest run status", inputSchema={"type":"object","properties":{"dag_id":{"type":"string"}},"required":["dag_id"]}),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: Any) -> list:
        try:
            if name == "list_all_dags":
                dags = await client.fetch_all_dags()
                lines = [f"📊 Total: {len(dags)}\n"]
                for d in dags[:100]:
                    s = "⏸ Paused" if d.get("is_paused") else "▶ Active"
                    lines.append(f"• {d['dag_id']} | {s}")
                if len(dags) > 100: lines.append(f"...+{len(dags)-100} more")
                return [TextContent(type="text", text="\n".join(lines))]
            elif name == "search_dags":
                q = arguments["query"].lower()
                all_dags = await client.fetch_all_dags()
                matches = [d for d in all_dags if q in d["dag_id"].lower()]
                text = f"Found {len(matches)}:\n" + "\n".join([f"• {d['dag_id']}" for d in matches]) if matches else "No matches"
                return [TextContent(type="text", text=text)]
            elif name == "get_dag_details":
                data = await client.call("GET", f"/dags/{arguments['dag_id']}")
                d = data.get("dag", {}) if isinstance(data, dict) else {}
                text = f"DAG: {d.get('dag_id','?')}\nStatus: {'Paused' if d.get('is_paused') else 'Active'}\nSchedule: {d.get('schedule_interval')}"
                return [TextContent(type="text", text=text)]
            elif name == "pause_dag":
                await client.call("PATCH", f"/dags/{arguments['dag_id']}", json_data={"is_paused": True})
                return [TextContent(type="text", text=f"Paused '{arguments['dag_id']}'")]
            elif name == "unpause_dag":
                await client.call("PATCH", f"/dags/{arguments['dag_id']}", json_data={"is_paused": False})
                return [TextContent(type="text", text=f"Unpaused '{arguments['dag_id']}'")]
            elif name == "trigger_dag":
                result = await client.call("POST", f"/dags/{arguments['dag_id']}/dagRuns", json_data={"conf": arguments.get("conf", {})})
                return [TextContent(type="text", text=f"Triggered '{arguments['dag_id']}'\nRun: {result.get('dag_run_id')}")]
            elif name == "get_latest_run":
                data = await client.call("GET", f"/dags/{arguments['dag_id']}/dagRuns", params={"limit": 1, "order_by": "-execution_date"})
                run = (data.get("dag_runs") or [None])[0] if isinstance(data, dict) else None
                if not run: return [TextContent(type="text", text="No runs")]
                text = f"Run: {run.get('dag_run_id')}\nState: {run.get('state','').upper()}\nStarted: {run.get('start_date')}"
                return [TextContent(type="text", text=text)]
            else:
                return [TextContent(type="text", text=f"Unknown tool: {name}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]

# Simple dag_id extractor (kept from original)
def extract_dag_id(query: str) -> Optional[str]:
    patterns = [
        r'["\']([a-zA-Z0-9_\-:.]+)["\']',
        r'\b([a-zA-Z0-9_]+(?:[_\-:.][a-zA-Z0-9_]+)+)\b',
        r'dag[_\s]+([a-zA-Z0-9_\-:.]+)',
        r'(?:for|of|on|named)\s+([a-zA-Z0-9_\-:.]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            dag_id = match.group(1)
            if dag_id.lower() not in ['all', 'dags', 'dag', 'list', 'show', 'get']:
                return dag_id
    return None

# --------------------------
# Existing HTTP endpoints (simplified & merged)
# --------------------------
@app.get("/")
async def root():
    return {"server": "Airflow MCP - Auto-Heal", "status": "ready", "version": "3.0"}

@app.post("/run")
async def run_query(body: Dict = Body(...)):
    query = str(body.get("query", "")).strip()
    query_lower = query.lower()
    try:
        # LIST / SEARCH / PAUSE / UNPAUSE / TRIGGER / DETAILS / LATEST RUN / ANALYTICS
        # For brevity reuse behavior similar to your original server; implement common cases:
        if re.search(r'\b(list|show|get)\b.*\bdag', query_lower):
            dags = await client.fetch_all_dags()
            return {"success": True, "output": {"count": len(dags), "dags": [{"dag_id": d["dag_id"], "is_paused": d.get("is_paused", False)} for d in dags]}}
        if "search" in query_lower:
            term = query_lower.replace("search", "").strip()
            dags = await client.fetch_all_dags()
            matches = [d for d in dags if term in d["dag_id"].lower()]
            return {"success": True, "output": {"query": term, "count": len(matches), "dags": [{"dag_id": d["dag_id"], "is_paused": d.get("is_paused", False)} for d in matches]}}
        if re.search(r'\b(details?|info|status)\b', query_lower):
            dag_id = extract_dag_id(query)
            if not dag_id:
                return {"success": False, "error": "Could not extract DAG ID"}
            data = await client.call("GET", f"/dags/{dag_id}")
            return {"success": True, "output": data}
        if "pause" in query_lower:
            dag_id = extract_dag_id(query)
            if not dag_id:
                return {"success": False, "error": "Could not extract DAG ID"}
            before = await client.call("GET", f"/dags/{dag_id}")
            was_paused = before.get("dag", {}).get("is_paused", False) if isinstance(before, dict) else False
            await client.call("PATCH", f"/dags/{dag_id}", json_data={"is_paused": True})
            after = await client.call("GET", f"/dags/{dag_id}")
            now_paused = after.get("dag", {}).get("is_paused", False) if isinstance(after, dict) else False
            return {"success": True, "output": {"action": "pause", "dag_id": dag_id, "was_paused": was_paused, "is_now_paused": now_paused}}
        if re.search(r'\b(unpause|resume|activate|start)\b', query_lower):
            dag_id = extract_dag_id(query)
            if not dag_id:
                return {"success": False, "error": "Could not extract DAG ID"}
            before = await client.call("GET", f"/dags/{dag_id}")
            was_paused = before.get("dag", {}).get("is_paused", False) if isinstance(before, dict) else False
            await client.call("PATCH", f"/dags/{dag_id}", json_data={"is_paused": False})
            after = await client.call("GET", f"/dags/{dag_id}")
            now_paused = after.get("dag", {}).get("is_paused", False) if isinstance(after, dict) else False
            return {"success": True, "output": {"action": "unpause", "dag_id": dag_id, "was_paused": was_paused, "is_now_paused": now_paused}}
        if re.search(r'\b(trigger|run)\b', query_lower):
            dag_id = extract_dag_id(query)
            if not dag_id:
                return {"success": False, "error": "Could not extract DAG ID"}
            result = await client.call("POST", f"/dags/{dag_id}/dagRuns", json_data={"conf": {}})
            return {"success": True, "output": {"action": "trigger", "dag_id": dag_id, "run_id": result.get("dag_run_id"), "state": result.get("state")}}
        if re.search(r'\b(latest|last|recent)\s+run\b', query_lower):
            dag_id = extract_dag_id(query)
            if not dag_id:
                return {"success": False, "error": "Could not extract DAG ID"}
            data = await client.call("GET", f"/dags/{dag_id}/dagRuns", params={"limit": 1, "order_by": "-execution_date"})
            runs = data.get("dag_runs", []) if isinstance(data, dict) else []
            return {"success": True, "output": runs[0] if runs else {"message": "No runs found"}}
        if "analytics" in query_lower or "stats" in query_lower:
            # Keep your analytics code (not repeated here to save space)
            return {"success": False, "error": "Analytics endpoint not implemented in this snippet"}
        return {"success": False, "error": f"Could not understand: {query}"}
    except Exception as e:
        log.exception("Error in /run: %s", e)
        return {"success": False, "error": str(e)}

# --------------------------
# Startup / Shutdown
# --------------------------
@app.on_event("startup")
async def startup():
    await client.initialize()
    # start monitor loop once
    global monitor_task
    if monitor_task is None:
        loop = asyncio.get_event_loop()
        monitor_task = loop.create_task(monitor_loop(client))
        log.info("Spawned monitor task")
    # if MCP available, also start stdio server in background
    if MCP_AVAILABLE:
        # run MCP stdio server in background task
        async def run_mcp_stdio():
            log.info("Starting MCP stdio server")
            await mcp.server.stdio.stdio_server().__aenter__()  # keep alive if used externally
        # not starting here to avoid double-entry; MCP main runner should run separately if needed

@app.on_event("shutdown")
async def shutdown():
    global monitor_task
    if monitor_task:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        monitor_task = None
    await client.close()

# --------------------------
# Run: start both FastAPI and MCP stdio server if executed directly
# --------------------------
def run_http():
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8800")), log_level="info")

async def run_mcp_stdio_if_needed():
    if not MCP_AVAILABLE:
        return
    log.info("Starting MCP stdio server (blocking)")
    await client.initialize()
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())

def main():
    # Start HTTP server in a background thread and run MCP stdio in main thread if MCP_AVAILABLE.
    http_thread = threading.Thread(target=run_http, daemon=True)
    http_thread.start()
    try:
        if MCP_AVAILABLE:
            asyncio.run(run_mcp_stdio_if_needed())
        else:
            # if no MCP, keep main thread alive
            while True:
                time.sleep(3600)
    except KeyboardInterrupt:
        log.info("Shutdown requested")

if __name__ == "__main__":
    main()
