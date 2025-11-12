from __future__ import annotations

from typing import Any, Dict, List

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from supabase import create_client
from tenacity import retry, stop_after_attempt, wait_exponential


TARGET_DRIVER_NUMBERS = [63, 12]
OPENF1_BASE_URL = "https://api.openf1.org/v1"


@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(3))
def fetch_openf1(endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fetch data from OpenF1 API with retry logic."""
    url = f"{OPENF1_BASE_URL}/{endpoint}"
    headers = {
        "User-Agent": "Airflow-OpenF1-Pipeline/1.0",
        "Accept": "application/json",
    }
    response = requests.get(url, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        return []
    return data


@dag(
    dag_id="openf1_overtakes_supabase",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: sync overtakes (beta) for latest race to Supabase",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 1,
    },
    tags=["openf1", "supabase"],
)
def openf1_overtakes_supabase():
    @task
    def sync_overtakes() -> None:
        """Sync overtake data from OpenF1 API to Supabase."""
        # Get Supabase connection
        conn = BaseHook.get_connection("davies_supabase_connection_http")

        # Ensure proper base URL (remove /rest/v1 suffix if present)
        supabase_url = (
            conn.host if conn.host.startswith("http") else f"https://{conn.host}"
        )
        supabase_url = supabase_url.rstrip("/").replace("/rest/v1", "")

        sb = create_client(supabase_url, conn.password)

        sessions = fetch_openf1(
            "sessions", {"meeting_key": "latest", "session_name": "Race"}
        )
        if not sessions:
            return
        latest_race = sessions[-1]
        session_key = latest_race.get("session_key")

        data = fetch_openf1("overtakes", {"session_key": session_key})
        if not data:
            return

        rows: List[Dict[str, Any]] = []
        for o in data:
            # Keep only events involving target drivers
            if (
                int(o.get("overtaker", -1)) not in TARGET_DRIVER_NUMBERS
                and int(o.get("defender", -1)) not in TARGET_DRIVER_NUMBERS
            ):
                continue
            rows.append(
                {
                    "session_key": o.get("session_key"),
                    "meeting_key": o.get("meeting_key"),
                    "lap_number": o.get("lap_number"),
                    "overtaker": o.get("overtaker"),
                    "defender": o.get("defender"),
                    "corner": o.get("corner"),
                    "date": o.get("date"),
                    "successful": o.get("successful"),
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                }
            )

        if rows:
            sb.table("overtakes").upsert(
                rows,
                on_conflict=["session_key", "lap_number", "overtaker", "defender"],
            ).execute()

    sync_overtakes()


openf1_overtakes_supabase()
