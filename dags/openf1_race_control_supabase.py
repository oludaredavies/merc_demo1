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
        """Sync race control messages from OpenF1 API to Supabase using PostgreSQL connection."""
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

        # Fetch race control data
        data = fetch_openf1("race_control", {"session_key": session_key})
        if not data:
            logger.info("No race control data found")
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

        logger.info(
            f"Upserting {len(rows)} race control records for session {session_key}"
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
                        INSERT INTO race_control (
                            session_key, meeting_key, category, flag, lap_number,
                            message, scope, sector, date, ingested_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (session_key, date, category, message)
                        DO UPDATE SET
                            meeting_key = EXCLUDED.meeting_key,
                            flag = EXCLUDED.flag,
                            lap_number = EXCLUDED.lap_number,
                            scope = EXCLUDED.scope,
                            sector = EXCLUDED.sector,
                            ingested_at = EXCLUDED.ingested_at
                    """

                    # Execute batch insert
                    for row in rows:
                        cursor.execute(
                            insert_sql,
                            (
                                row["session_key"],
                                row["meeting_key"],
                                row["category"],
                                row["flag"],
                                row["lap_number"],
                                row["message"],
                                row["scope"],
                                row["sector"],
                                row["date"],
                                row["ingested_at"],
                            ),
                        )

                    conn.commit()
                    logger.info(
                        f"Successfully upserted {len(rows)} race control records"
                    )

        except Exception as e:
            logger.error(f"Error upserting race control data: {str(e)}")
            raise

    sync_race_control()


openf1_race_control_supabase()
