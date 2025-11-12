"""
Test DAG for Supabase PostgreSQL operations.

This DAG demonstrates:
1. Inserting sample data into the starting_grid table
2. Reading data back from the table

Uses connection: davies_supabase_connection
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
SUPABASE_CONN_ID = "davies_supabase_connection"


def fetch_supabase_dataframe(sql: str) -> pd.DataFrame:
    """
    Execute SQL against Supabase using direct psycopg2 connection with SSL settings.
    Uses the same working logic from test_supabase_connection.py.
    """
    import psycopg2

    postgres_hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
    airflow_conn = postgres_hook.get_connection(SUPABASE_CONN_ID)

    # Get password from Airflow connection
    password = airflow_conn.password

    try:
        # Build connection parameters with SSL - same as test_session_pooler
        conn_kwargs = {
            "host": "aws-1-us-east-1.pooler.supabase.com",
            "port": 5432,
            "dbname": "postgres",
            "user": "postgres.ouzwgivgsluiwzgclmpo",
            "password": password,
            "sslmode": "require",
            "connect_timeout": 10,
        }

        # Execute query and return DataFrame
        with psycopg2.connect(**conn_kwargs) as connection:
            df = pd.read_sql_query(sql, connection)
            return df

    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise


@dag(
    dag_id="test_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Test Supabase PostgreSQL insert and read operations",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    tags=["test", "postgres", "supabase"],
)
def test_postgres():
    @task
    def insert_data() -> dict:
        """Insert sample data into starting_grid table."""
        import psycopg2

        postgres_hook = PostgresHook(postgres_conn_id=SUPABASE_CONN_ID)
        airflow_conn = postgres_hook.get_connection(SUPABASE_CONN_ID)

        # Get password from Airflow connection
        password = airflow_conn.password

        # Sample starting grid data (fictional F1 session)
        insert_sql = """
        INSERT INTO public.starting_grid (session_key, meeting_key, driver_number, grid_position, penalties)
        VALUES
            (9999, 1234, 1, 1, NULL),
            (9999, 1234, 11, 2, NULL),
            (9999, 1234, 44, 3, NULL),
            (9999, 1234, 16, 4, NULL),
            (9999, 1234, 55, 5, NULL),
            (9999, 1234, 4, 6, '3 place grid penalty'),
            (9999, 1234, 81, 7, NULL),
            (9999, 1234, 63, 8, NULL),
            (9999, 1234, 10, 9, NULL),
            (9999, 1234, 22, 10, NULL)
        ON CONFLICT (session_key, driver_number)
        DO UPDATE SET
            grid_position = EXCLUDED.grid_position,
            penalties = EXCLUDED.penalties,
            ingested_at = now();
        """

        try:
            # Build connection parameters with SSL
            conn_kwargs = {
                "host": "aws-1-us-east-1.pooler.supabase.com",
                "port": 5432,
                "dbname": "postgres",
                "user": "postgres.ouzwgivgsluiwzgclmpo",
                "password": password,
                "sslmode": "require",
                "connect_timeout": 10,
            }

            # Execute insert
            with psycopg2.connect(**conn_kwargs) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(insert_sql)
                    connection.commit()

            print("✅ Successfully inserted 10 drivers into starting_grid table")
            print("   Session Key: 9999")
            print("   Meeting Key: 1234")

        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise

        return {"session_key": 9999, "rows_inserted": 10}

    @task
    def read_data(insert_info: dict) -> list:
        """Read data from starting_grid table."""
        session_key = insert_info["session_key"]

        # Query the data we just inserted
        sql = f"""
        SELECT
            session_key,
            meeting_key,
            driver_number,
            grid_position,
            penalties,
            ingested_at
        FROM public.starting_grid
        WHERE session_key = {session_key}
        ORDER BY grid_position ASC
        """

        df = fetch_supabase_dataframe(sql)

        print(f"\n{'=' * 70}")
        print(f"STARTING GRID DATA - Session {session_key}")
        print(f"{'=' * 70}")
        print(f"\nTotal rows retrieved: {len(df)}\n")

        # Convert to list of dicts for display
        results = df.to_dict(orient="records")

        for row in results:
            penalty_str = f" ({row['penalties']})" if row["penalties"] else ""
            print(
                f"P{row['grid_position']:2d} - Driver #{row['driver_number']:2d}{penalty_str}"
            )

        return results

    # Task flow
    insert_info = insert_data()
    read_data(insert_info)


test_postgres()
