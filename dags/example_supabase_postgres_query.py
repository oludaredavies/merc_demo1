"""
Example DAG demonstrating direct PostgreSQL connection to Supabase.

This DAG shows how to use the fetch_supabase_dataframe utility to run
SQL queries against Supabase using the PostgreSQL Session Pooler.

SETUP:
Uses the existing Airflow connection 'davies_supabase_connection'.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="example_supabase_postgres_query",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Example: Query Supabase using direct PostgreSQL connection",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 1,
    },
    tags=["openf1", "supabase", "example"],
)
def example_supabase_postgres_query():
    @task
    def query_latest_session() -> dict:
        """Query the latest race session using PostgreSQL."""
        from include.supabase_utils import fetch_supabase_dataframe

        # Example 1: Get the latest session
        sql = """
            SELECT
                session_key,
                meeting_key,
                COUNT(*) as lap_count,
                AVG(lap_duration) as avg_lap_time,
                MIN(lap_duration) as fastest_lap
            FROM laps
            WHERE session_key = (
                SELECT MAX(session_key) FROM laps
            )
            GROUP BY session_key, meeting_key
            LIMIT 1
        """

        df = fetch_supabase_dataframe(sql)

        if df.empty:
            return {"status": "no_data"}

        # Convert DataFrame row to dict for XCom
        return df.iloc[0].to_dict()

    @task
    def query_driver_performance(session_data: dict) -> list:
        """Query driver performance for the latest session."""
        from include.supabase_utils import fetch_supabase_dataframe

        if session_data.get("status") == "no_data":
            return []

        session_key = session_data["session_key"]

        # Example 2: Get driver performance metrics
        sql = f"""
            SELECT
                l.driver_number,
                COUNT(l.lap_number) as total_laps,
                AVG(l.lap_duration) as avg_lap_time,
                MIN(l.lap_duration) as best_lap_time,
                STDDEV(l.lap_duration) as lap_consistency,
                COUNT(p.lap_number) as pit_stops,
                AVG(p.pit_duration) as avg_pit_duration
            FROM laps l
            LEFT JOIN pit_stops p
                ON l.session_key = p.session_key
                AND l.driver_number = p.driver_number
            WHERE l.session_key = {session_key}
            GROUP BY l.driver_number
            ORDER BY avg_lap_time ASC
        """

        df = fetch_supabase_dataframe(sql)

        # Convert DataFrame to list of dicts
        return df.to_dict(orient="records")

    @task
    def analyze_results(drivers: list) -> None:
        """Print analysis of driver performance."""
        if not drivers:
            print("No driver data available")
            return

        print(f"\n{'=' * 60}")
        print("DRIVER PERFORMANCE ANALYSIS")
        print(f"{'=' * 60}")

        for i, driver in enumerate(drivers[:5], 1):  # Top 5 drivers
            print(f"\n#{i} - Driver {driver['driver_number']}")
            print(f"   Total Laps: {driver['total_laps']}")
            print(f"   Avg Lap Time: {driver['avg_lap_time']:.3f}s")
            print(f"   Best Lap: {driver['best_lap_time']:.3f}s")
            print(f"   Consistency (σ): {driver['lap_consistency']:.3f}s")
            print(f"   Pit Stops: {driver['pit_stops']}")
            if driver["avg_pit_duration"]:
                print(f"   Avg Pit Time: {driver['avg_pit_duration']:.3f}s")

    # Task flow
    session = query_latest_session()
    perf = query_driver_performance(session)
    analyze_results(perf)


example_supabase_postgres_query()
