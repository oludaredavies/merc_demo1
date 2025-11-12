from __future__ import annotations

import logging
from typing import Any, Dict, List
from datetime import datetime

import psycopg2
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)
TARGET_DRIVER_NUMBERS = [63, 12]
SUPABASE_CONN_ID = "davies_supabase_connection"


@dag(
    dag_id="openf1_postrace_kpis_supabase",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: compute per-driver KPIs and store in Supabase",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["openf1", "supabase"],
)§
def openf1_postrace_kpis_supabase():
    @task
    def compute_and_store_kpis() -> None:
        """Compute KPIs from existing Supabase data and store results using PostgreSQL connection."""
        # Get PostgreSQL connection to Supabase
        postgres_hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
        airflow_conn = postgres_hook.get_connection(SUPABASE_CONN_ID)
        password = airflow_conn.password

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
                # Identify latest race session from laps table
                cursor.execute(
                    "SELECT session_key FROM laps ORDER BY session_key DESC LIMIT 1"
                )
                result = cursor.fetchone()
                if not result:
                    logger.info("No laps data found")
                    return
                session_key = result[0]
                logger.info(f"Computing KPIs for session {session_key}")

                # Pull laps for target drivers
                cursor.execute(
                    """
                    SELECT driver_number, lap_number, lap_duration, sector_1, sector_2, sector_3, is_pit_out_lap
                    FROM laps
                    WHERE session_key = %s AND driver_number = ANY(%s)
                    """,
                    (session_key, TARGET_DRIVER_NUMBERS),
                )
                laps = cursor.fetchall()

                # Pull stints for target drivers
                cursor.execute(
                    """
                    SELECT driver_number, stint_number, compound, total_laps, start_lap, end_lap
                    FROM stints
                    WHERE session_key = %s AND driver_number = ANY(%s)
                    """,
                    (session_key, TARGET_DRIVER_NUMBERS),
                )
                stints = cursor.fetchall()

                # Pull pit stops for target drivers
                cursor.execute(
                    """
                    SELECT driver_number, lap_number, pit_duration
                    FROM pit_stops
                    WHERE session_key = %s AND driver_number = ANY(%s)
                    """,
                    (session_key, TARGET_DRIVER_NUMBERS),
                )
                pits = cursor.fetchall()

                # Pull session results for target drivers
                cursor.execute(
                    """
                    SELECT driver_number, position, points, laps_completed, best_lap_time, avg_lap_time
                    FROM session_results
                    WHERE session_key = %s AND driver_number = ANY(%s)
                    """,
                    (session_key, TARGET_DRIVER_NUMBERS),
                )
                results = cursor.fetchall()

                # Group data by driver
                laps_by_driver: Dict[int, List[tuple]] = {}
                for lap in laps:
                    driver_num = lap[0]
                    laps_by_driver.setdefault(driver_num, []).append(lap)

                pits_by_driver: Dict[int, List[tuple]] = {}
                for pit in pits:
                    driver_num = pit[0]
                    pits_by_driver.setdefault(driver_num, []).append(pit)

                stints_by_driver: Dict[int, List[tuple]] = {}
                for stint in stints:
                    driver_num = stint[0]
                    stints_by_driver.setdefault(driver_num, []).append(stint)

                results_by_driver: Dict[int, tuple] = {}
                for res in results:
                    driver_num = res[0]
                    results_by_driver[driver_num] = res

                # Calculate KPIs for each driver
                summaries: List[Dict[str, Any]] = []
                for driver_number in TARGET_DRIVER_NUMBERS:
                    driver_laps = laps_by_driver.get(driver_number, [])
                    if not driver_laps:
                        continue

                    total_laps = len(driver_laps)
                    # lap format: (driver_number, lap_number, lap_duration, sector_1, sector_2, sector_3, is_pit_out_lap)
                    valid_laps = [l for l in driver_laps if l[2] is not None]
                    avg_lap = (
                        sum(float(l[2]) for l in valid_laps) / len(valid_laps)
                        if valid_laps
                        else None
                    )
                    best_lap = (
                        min(float(l[2]) for l in valid_laps) if valid_laps else None
                    )

                    # Calculate consistency (exclude pit-out laps)
                    clean_laps = [l for l in valid_laps if not l[6]]
                    if clean_laps:
                        mean = sum(float(l[2]) for l in clean_laps) / len(clean_laps)
                        variance = sum(
                            (float(l[2]) - mean) ** 2 for l in clean_laps
                        ) / len(clean_laps)
                        consistency = variance**0.5
                    else:
                        consistency = None

                    pit_list = pits_by_driver.get(driver_number, [])
                    num_pit_stops = len(pit_list)
                    # pit format: (driver_number, lap_number, pit_duration)
                    avg_pit = (
                        sum(float(p[2] or 0) for p in pit_list) / num_pit_stops
                        if pit_list
                        else None
                    )

                    stint_list = stints_by_driver.get(driver_number, [])
                    # stint format: (driver_number, stint_number, compound, total_laps, start_lap, end_lap)
                    tyre_usage = (
                        ",".join(
                            sorted({(s[2] or "").upper() for s in stint_list if s[2]})
                        )
                        or None
                    )

                    # result format: (driver_number, position, points, laps_completed, best_lap_time, avg_lap_time)
                    res = results_by_driver.get(driver_number)
                    summaries.append(
                        {
                            "session_key": session_key,
                            "driver_number": driver_number,
                            "total_laps": total_laps,
                            "avg_lap_time": avg_lap,
                            "best_lap_time": best_lap,
                            "lap_time_stddev": consistency,
                            "num_pit_stops": num_pit_stops,
                            "avg_pit_duration": avg_pit,
                            "tyre_compounds_used": tyre_usage,
                            "finishing_position": res[1] if res else None,
                            "points": res[2] if res else None,
                            "ingested_at": datetime.utcnow().isoformat() + "Z",
                        }
                    )

                if summaries:
                    logger.info(
                        f"Upserting {len(summaries)} KPI records for session {session_key}"
                    )
                    insert_sql = """
                        INSERT INTO postrace_driver_kpis (
                            session_key, driver_number, total_laps, avg_lap_time, best_lap_time,
                            lap_time_stddev, num_pit_stops, avg_pit_duration, tyre_compounds_used,
                            finishing_position, points, ingested_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (session_key, driver_number)
                        DO UPDATE SET
                            total_laps = EXCLUDED.total_laps,
                            avg_lap_time = EXCLUDED.avg_lap_time,
                            best_lap_time = EXCLUDED.best_lap_time,
                            lap_time_stddev = EXCLUDED.lap_time_stddev,
                            num_pit_stops = EXCLUDED.num_pit_stops,
                            avg_pit_duration = EXCLUDED.avg_pit_duration,
                            tyre_compounds_used = EXCLUDED.tyre_compounds_used,
                            finishing_position = EXCLUDED.finishing_position,
                            points = EXCLUDED.points,
                            ingested_at = EXCLUDED.ingested_at
                    """

                    for row in summaries:
                        cursor.execute(
                            insert_sql,
                            (
                                row["session_key"],
                                row["driver_number"],
                                row["total_laps"],
                                row["avg_lap_time"],
                                row["best_lap_time"],
                                row["lap_time_stddev"],
                                row["num_pit_stops"],
                                row["avg_pit_duration"],
                                row["tyre_compounds_used"],
                                row["finishing_position"],
                                row["points"],
                                row["ingested_at"],
                            ),
                        )

                    conn.commit()
                    logger.info(f"Successfully upserted {len(summaries)} KPI records")
                else:
                    logger.info("No KPI data to upsert")

    compute_and_store_kpis()


openf1_postrace_kpis_supabase()
