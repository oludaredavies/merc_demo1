"""
Post-race KPI computation using PostgreSQL instead of REST API.

This is an alternative version of openf1_postrace_kpis_supabase.py that uses
direct PostgreSQL queries instead of the Supabase REST API for better performance
with complex aggregations.
"""

from __future__ import annotations

import logging
from datetime import datetime

import psycopg2
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)
TARGET_DRIVER_NUMBERS = [63, 12]
SUPABASE_CONN_ID = "davies_supabase_connection"


@dag(
    dag_id="openf1_postrace_kpis_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: compute per-driver KPIs using PostgreSQL queries",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["openf1", "supabase", "postgres"],
)
def openf1_postrace_kpis_postgres():
    @task
    def compute_kpis_with_sql() -> None:
        """Compute KPIs using a single SQL query and PostgreSQL connection."""
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
                # Get latest session key
                cursor.execute("SELECT MAX(session_key) FROM laps")
                result = cursor.fetchone()

                if not result or result[0] is None:
                    logger.info("No session data found")
                    return

                session_key = result[0]
                logger.info(f"Computing KPIs for session {session_key}")

                # Build driver filter for SQL
                driver_filter = ",".join(str(d) for d in TARGET_DRIVER_NUMBERS)

                # Single comprehensive SQL query to compute all KPIs
                kpi_query = f"""
                    WITH lap_stats AS (
                        SELECT
                            l.driver_number,
                            COUNT(*) as total_laps,
                            AVG(l.lap_duration) as avg_lap_time,
                            MIN(l.lap_duration) as best_lap_time,
                            STDDEV_POP(
                                CASE WHEN l.is_pit_out_lap = false
                                THEN l.lap_duration
                                ELSE NULL END
                            ) as lap_time_stddev
                        FROM laps l
                        WHERE l.session_key = {session_key}
                          AND l.driver_number IN ({driver_filter})
                          AND l.lap_duration IS NOT NULL
                        GROUP BY l.driver_number
                    ),
                    pit_stats AS (
                        SELECT
                            p.driver_number,
                            COUNT(*) as num_pit_stops,
                            AVG(p.pit_duration) as avg_pit_duration
                        FROM pit_stops p
                        WHERE p.session_key = {session_key}
                          AND p.driver_number IN ({driver_filter})
                          AND p.pit_duration IS NOT NULL
                        GROUP BY p.driver_number
                    ),
                    stint_stats AS (
                        SELECT
                            s.driver_number,
                            STRING_AGG(DISTINCT UPPER(s.compound), ',' ORDER BY UPPER(s.compound))
                                as tyre_compounds_used
                        FROM stints s
                        WHERE s.session_key = {session_key}
                          AND s.driver_number IN ({driver_filter})
                          AND s.compound IS NOT NULL
                        GROUP BY s.driver_number
                    ),
                    result_stats AS (
                        SELECT
                            r.driver_number,
                            r.position as finishing_position,
                            r.points
                        FROM session_results r
                        WHERE r.session_key = {session_key}
                          AND r.driver_number IN ({driver_filter})
                    )
                    SELECT
                        l.driver_number,
                        l.total_laps,
                        l.avg_lap_time,
                        l.best_lap_time,
                        l.lap_time_stddev,
                        COALESCE(p.num_pit_stops, 0) as num_pit_stops,
                        p.avg_pit_duration,
                        s.tyre_compounds_used,
                        r.finishing_position,
                        r.points
                    FROM lap_stats l
                    LEFT JOIN pit_stats p ON l.driver_number = p.driver_number
                    LEFT JOIN stint_stats s ON l.driver_number = s.driver_number
                    LEFT JOIN result_stats r ON l.driver_number = r.driver_number
                    ORDER BY l.driver_number
                """

                # Execute the query
                cursor.execute(kpi_query)
                kpi_records = cursor.fetchall()

                if not kpi_records:
                    logger.info("No KPI data computed")
                    return

                logger.info(f"Computed KPIs for {len(kpi_records)} drivers")

                # Display computed KPIs
                for record in kpi_records:
                    logger.info(
                        f"Driver {record[0]}: {record[1]} laps, avg: {record[2]:.3f}s, best: {record[3]:.3f}s"
                    )

                # Upsert KPI records to postrace_driver_kpis table
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

                ingested_at = datetime.utcnow().isoformat() + "Z"

                for record in kpi_records:
                    # record format: (driver_number, total_laps, avg_lap_time, best_lap_time,
                    #                 lap_time_stddev, num_pit_stops, avg_pit_duration,
                    #                 tyre_compounds_used, finishing_position, points)
                    cursor.execute(
                        insert_sql,
                        (
                            session_key,
                            record[0],  # driver_number
                            record[1],  # total_laps
                            record[2],  # avg_lap_time
                            record[3],  # best_lap_time
                            record[4],  # lap_time_stddev
                            record[5],  # num_pit_stops
                            record[6],  # avg_pit_duration
                            record[7],  # tyre_compounds_used
                            record[8],  # finishing_position
                            record[9],  # points
                            ingested_at,
                        ),
                    )

                conn.commit()
                logger.info(f"Successfully upserted {len(kpi_records)} KPI records")

    compute_kpis_with_sql()


openf1_postrace_kpis_postgres()
