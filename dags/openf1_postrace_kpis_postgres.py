"""
Post-race KPI computation using PostgreSQL instead of REST API.

This is an alternative version of openf1_postrace_kpis_supabase.py that uses
direct PostgreSQL queries instead of the Supabase REST API for better performance
with complex aggregations.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


TARGET_DRIVER_NUMBERS = [63, 12]


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
        """Compute KPIs using a single SQL query instead of multiple API calls."""
        from include.supabase_utils import fetch_supabase_dataframe

        # Get latest session key
        latest_session_df = fetch_supabase_dataframe("""
            SELECT MAX(session_key) as session_key
            FROM laps
        """)

        if latest_session_df.empty or latest_session_df.iloc[0]["session_key"] is None:
            print("No session data found")
            return

        session_key = int(latest_session_df.iloc[0]["session_key"])
        print(f"Computing KPIs for session {session_key}")

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
                {session_key} as session_key,
                l.driver_number,
                l.total_laps,
                l.avg_lap_time,
                l.best_lap_time,
                l.lap_time_stddev,
                COALESCE(p.num_pit_stops, 0) as num_pit_stops,
                p.avg_pit_duration,
                s.tyre_compounds_used,
                r.finishing_position,
                r.points,
                NOW() as ingested_at
            FROM lap_stats l
            LEFT JOIN pit_stats p ON l.driver_number = p.driver_number
            LEFT JOIN stint_stats s ON l.driver_number = s.driver_number
            LEFT JOIN result_stats r ON l.driver_number = r.driver_number
            ORDER BY l.driver_number
        """

        # Execute the query
        kpis_df = fetch_supabase_dataframe(kpi_query)

        if kpis_df.empty:
            print("No KPI data computed")
            return

        print(f"\nComputed KPIs for {len(kpis_df)} drivers:")
        print(kpis_df.to_string(index=False))

        # Convert to records for upsert
        kpi_records = kpis_df.to_dict(orient="records")

        # Upsert to postrace_driver_kpis table using PostgreSQL
        from include.supabase_utils import get_supabase_client

        sb = get_supabase_client("davies_supabase_connection_http")

        result = (
            sb.table("postrace_driver_kpis")
            .upsert(kpi_records, on_conflict="session_key,driver_number")
            .execute()
        )

        print(f"\nSuccessfully upserted {len(result.data)} KPI records")

    compute_kpis_with_sql()


openf1_postrace_kpis_postgres()
