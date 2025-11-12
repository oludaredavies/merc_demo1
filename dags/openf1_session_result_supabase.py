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
    dag_id="openf1_session_result_supabase",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: sync session results for latest race to Supabase",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 1,
    },
    tags=["openf1", "supabase"],
)
def openf1_session_result_supabase():
    @task
    def sync_session_result() -> None:
        """Sync session result data from OpenF1 API to Supabase using PostgreSQL connection."""
        # Get PostgreSQL connection to Supabase
        postgres_hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
        airflow_conn = postgres_hook.get_connection(SUPABASE_CONN_ID)
        password = airflow_conn.password

        # Fetch latest race session
        sessions = fetch_openf1(
            "sessions", {"meeting_key": "latest", "session_name": "Race"}
        )
        if not sessions:
            logger.info("No race sessions found")
            return
        latest_race = sessions[-1]
        session_key = latest_race.get("session_key")

        # Fetch session results
        results = fetch_openf1("session_result", {"session_key": session_key})
        if not results:
            logger.info("No session results found")
            return

        # Filter for target drivers
        rows: List[Dict[str, Any]] = []
        for r in results:
            if int(r.get("driver_number", -1)) not in TARGET_DRIVER_NUMBERS:
                continue
            rows.append(
                {
                    "session_key": r.get("session_key"),
                    "meeting_key": r.get("meeting_key"),
                    "driver_number": r.get("driver_number"),
                    "position": r.get("position"),
                    "points": r.get("points"),
                    "status": r.get("status"),
                    "laps_completed": r.get("laps_completed"),
                    "grid_position": r.get("grid_position"),
                    "best_lap_time": r.get("best_lap_time"),
                    "best_lap_number": r.get("best_lap_number"),
                    "avg_lap_time": r.get("avg_lap_time"),
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                }
            )

        if not rows:
            logger.info("No session results to upsert for target drivers")
            return

        logger.info(
            f"Upserting {len(rows)} session result records for session {session_key}"
        )

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
                        INSERT INTO session_results (
                            session_key, meeting_key, driver_number, position, points,
                            status, laps_completed, grid_position, best_lap_time,
                            best_lap_number, avg_lap_time, ingested_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (session_key, driver_number)
                        DO UPDATE SET
                            meeting_key = EXCLUDED.meeting_key,
                            position = EXCLUDED.position,
                            points = EXCLUDED.points,
                            status = EXCLUDED.status,
                            laps_completed = EXCLUDED.laps_completed,
                            grid_position = EXCLUDED.grid_position,
                            best_lap_time = EXCLUDED.best_lap_time,
                            best_lap_number = EXCLUDED.best_lap_number,
                            avg_lap_time = EXCLUDED.avg_lap_time,
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
                                row["position"],
                                row["points"],
                                row["status"],
                                row["laps_completed"],
                                row["grid_position"],
                                row["best_lap_time"],
                                row["best_lap_number"],
                                row["avg_lap_time"],
                                row["ingested_at"],
                            ),
                        )

                    conn.commit()
                    logger.info(
                        f"Successfully upserted {len(rows)} session result records"
                    )

        except Exception as e:
            logger.error(f"Error upserting session result data: {str(e)}")
            raise

    sync_session_result()


openf1_session_result_supabase()
