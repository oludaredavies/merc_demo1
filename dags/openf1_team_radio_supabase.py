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
    dag_id="openf1_team_radio_supabase",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: sync team radio metadata for latest race to Supabase",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 1,
    },
    tags=["openf1", "supabase"],
)
def openf1_team_radio_supabase():
    @task
    def sync_team_radio() -> None:
        """Sync team radio data from OpenF1 API to Supabase."""
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

        rows: List[Dict[str, Any]] = []
        for driver_number in TARGET_DRIVER_NUMBERS:
            data = fetch_openf1(
                "team_radio",
                {"session_key": session_key, "driver_number": driver_number},
            )
            for r in data:
                rows.append(
                    {
                        "session_key": r.get("session_key"),
                        "meeting_key": r.get("meeting_key"),
                        "driver_number": r.get("driver_number"),
                        "date": r.get("date"),
                        "recording_url": r.get("recording_url"),
                        "ingested_at": datetime.utcnow().isoformat() + "Z",
                    }
                )

        if rows:
            sb.table("team_radio").upsert(
                rows,
                on_conflict=["session_key", "driver_number", "date"],
            ).execute()

    sync_team_radio()


openf1_team_radio_supabase()
