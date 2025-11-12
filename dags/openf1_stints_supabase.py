from __future__ import annotations

import logging
from typing import Any, Dict, List
from datetime import datetime

import psycopg2
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from tenacity import retry, stop_after_attempt, wait_exponential


logger = logging.getLogger(__name__)
TARGET_DRIVER_NUMBERS = [63, 12]
OPENF1_BASE_URL = "https://api.openf1.org/v1"
SUPABASE_CONN_ID = "davies_supabase_connection"


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
    dag_id="openf1_stints_supabase",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: sync stints for latest race to Supabase",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 1,
    },
    tags=["openf1", "supabase"],
)
def openf1_stints_supabase():
    @task
    def sync_stints() -> None:
        """Sync stint data from OpenF1 API to Supabase using PostgreSQL connection."""
        # Get PostgreSQL connection to Supabase
        postgres_hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
        airflow_conn = postgres_hook.get_connection(SUPABASE_CONN_ID)
        password = airflow_conn.password

        # Get latest completed race session
        sessions = fetch_openf1(
            "sessions", {"meeting_key": "latest", "session_name": "Race"}
        )
        if not sessions:
            logger.info("No race sessions found")
            return
        latest_race = sessions[-1]
        session_key = latest_race.get("session_key")

        # Fetch stint data for target drivers
        rows: List[Dict[str, Any]] = []
        for driver_number in TARGET_DRIVER_NUMBERS:
            data = fetch_openf1(
                "stints", {"session_key": session_key, "driver_number": driver_number}
            )
            for s in data:
                rows.append(
                    {
                        "session_key": s.get("session_key"),
                        "meeting_key": s.get("meeting_key"),
                        "driver_number": s.get("driver_number"),
                        "stint_number": s.get("stint_number"),
                        "compound": s.get("compound"),
                        "total_laps": s.get("total_laps"),
                        "start_lap": s.get("start_lap"),
                        "end_lap": s.get("end_lap"),
                        "ingested_at": datetime.utcnow().isoformat() + "Z",
                    }
                )

        if not rows:
            logger.info("No stint data to upsert")
            return

        logger.info(f"Upserting {len(rows)} stint records for session {session_key}")

        # Connect to Supabase PostgreSQL and upsert data
        try:
            conn_kwargs = {
                "host": "aws-1-us-east-1.pooler.supabase.com",
                "port": 5432,
                "dbname": "postgres",
                "user": "postgres.ouzwgivgsluiwzgclmpo",
                "password": password,
                "sslmode": "require",
                "connect_timeout": 10,
            }

            with psycopg2.connect(**conn_kwargs) as conn:
                with conn.cursor() as cursor:
                    # Use INSERT ... ON CONFLICT for upsert
                    insert_sql = """
                        INSERT INTO stints (
                            session_key, meeting_key, driver_number, stint_number,
                            compound, total_laps, start_lap, end_lap, ingested_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (session_key, driver_number, stint_number)
                        DO UPDATE SET
                            meeting_key = EXCLUDED.meeting_key,
                            compound = EXCLUDED.compound,
                            total_laps = EXCLUDED.total_laps,
                            start_lap = EXCLUDED.start_lap,
                            end_lap = EXCLUDED.end_lap,
                            ingested_at = EXCLUDED.ingested_at
                    """

                    # Execute batch insert
                    for row in rows:
                        cursor.execute(
                            insert_sql,
                            (
                                row["session_key"],
                                row["meeting_key"],
                                row["driver_number"],
                                row["stint_number"],
                                row["compound"],
                                row["total_laps"],
                                row["start_lap"],
                                row["end_lap"],
                                row["ingested_at"],
                            ),
                        )

                    conn.commit()
                    logger.info(f"Successfully upserted {len(rows)} stint records")

        except Exception as e:
            logger.error(f"Error upserting stint data: {str(e)}")
            raise

    sync_stints()


openf1_stints_supabase()
