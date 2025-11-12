from __future__ import annotations

from typing import Any, Dict, List

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from supabase import create_client
from tenacity import retry, stop_after_attempt, wait_exponential


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
    dag_id="openf1_race_control_supabase",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: sync race control messages for latest race to Supabase",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 1,
    },
    tags=["openf1", "supabase"],
)
def openf1_race_control_supabase():
    @task
    def sync_race_control() -> None:
        """Sync race control messages from OpenF1 API to Supabase."""
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

        data = fetch_openf1("race_control", {"session_key": session_key})
        if not data:
            return

        rows: List[Dict[str, Any]] = []
        for e in data:
            rows.append(
                {
                    "session_key": e.get("session_key"),
                    "meeting_key": e.get("meeting_key"),
                    "category": e.get("category"),
                    "flag": e.get("flag"),
                    "lap_number": e.get("lap_number"),
                    "message": e.get("message"),
                    "scope": e.get("scope"),
                    "sector": e.get("sector"),
                    "date": e.get("date"),
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                }
            )

        sb.table("race_control").upsert(
            rows,
            on_conflict=["session_key", "date", "category", "message"],
        ).execute()

    sync_race_control()


openf1_race_control_supabase()
