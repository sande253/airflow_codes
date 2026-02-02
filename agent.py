# agent.py
import asyncio
import httpx
import time
from datetime import datetime

LOG_BUFFER = []   # shared memory logs shown in UI
MAX_LOGS = 500    # prevent memory blowup

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{ts}] {msg}"
    LOG_BUFFER.append(entry)
    if len(LOG_BUFFER) > MAX_LOGS:
        LOG_BUFFER.pop(0)
    print(entry)

class AutoHealAgent:
    def __init__(self, airflow_url, user, password):
        self.airflow_url = airflow_url.rstrip("/")
        self.auth = (user, password)
        self.client = httpx.AsyncClient(timeout=30)

    async def fetch_failed_runs(self):
        try:
            res = await self.client.get(
                f"{self.airflow_url}/dags",
                auth=self.auth
            )
            dags = res.json().get("dags", [])
            failed = []
            for d in dags:
                dag_id = d["dag_id"]
                runs = await self.client.get(
                    f"{self.airflow_url}/dags/{dag_id}/dagRuns",
                    params={"limit": 5, "order_by": "-execution_date"},
                    auth=self.auth
                )
                for r in runs.json().get("dag_runs", []):
                    if r["state"] in ("failed", "error"):
                        failed.append((dag_id, r["dag_run_id"]))
            return failed
        except Exception as e:
            log(f"Error fetching runs: {e}")
            return []

    async def fix_task(self, dag_id, run_id):
        try:
            log(f"Fixing {dag_id} | Run {run_id}")

            await self.client.post(
                f"{self.airflow_url}/dags/{dag_id}/dagRuns",
                json={"conf": {}},
                auth=self.auth
            )

            log(f"Triggered new run for {dag_id}")

        except Exception as e:
            log(f"Fix error: {e}")

    async def loop(self):
        log("Auto-Heal Agent Started")

        while True:
            failed = await self.fetch_failed_runs()
            if failed:
                for dag_id, run_id in failed:
                    log(f"Detected failure: {dag_id} ({run_id})")
                    await self.fix_task(dag_id, run_id)
            else:
                log("No failures detected")

            await asyncio.sleep(10)
