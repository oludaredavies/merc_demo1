"""Shared utilities for Supabase integration."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests
from airflow.hooks.base import BaseHook
from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


def get_supabase_client(conn_id: str = "davies_supabase_connection") -> Client:
    """
    Get Supabase client from Airflow connection.

    Args:
        conn_id: Airflow connection ID for Supabase credentials

    Returns:
        Authenticated Supabase client

    Raises:
        RuntimeError: If connection is invalid or credentials are missing
    """
    conn = BaseHook.get_connection(conn_id)

    # Ensure proper base URL (remove /rest/v1 suffix if present)
    url = conn.host if conn.host.startswith("http") else f"https://{conn.host}"
    url = url.rstrip("/").replace("/rest/v1", "")

    key = conn.password

    if not url or not key:
        raise RuntimeError(f"Invalid Supabase connection: {conn_id}")

    return create_client(url, key)


@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(3))
def fetch_openf1(endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch data from OpenF1 API with retry logic.

    Args:
        endpoint: API endpoint (e.g., 'sessions', 'laps')
        params: Query parameters

    Returns:
        List of results from API
    """
    url = f"https://api.openf1.org/v1/{endpoint}"
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        return []
    return data


def fetch_supabase_dataframe(
    sql: str, postgres_conn_id: str = "davies_supabase_connection"
) -> "pd.DataFrame":
    """
    Execute SQL against Supabase using direct psycopg2 connection with SSL.

    This function uses the Supabase Session Pooler for direct PostgreSQL access,
    which is more efficient for analytical queries than the REST API.

    Args:
        sql: SQL query to execute
        postgres_conn_id: Airflow connection ID for Supabase PostgreSQL credentials

    Returns:
        pandas DataFrame with query results

    Raises:
        Exception: If connection fails or query execution fails

    Example:
        >>> df = fetch_supabase_dataframe(
        ...     "SELECT * FROM laps WHERE session_key = 9158 LIMIT 10"
        ... )
    """
    import pandas as pd
    import psycopg2
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    airflow_conn = postgres_hook.get_connection(postgres_conn_id)

    # Get credentials from Airflow connection - all required, no defaults
    host = airflow_conn.host
    port = airflow_conn.port or 5432
    dbname = airflow_conn.schema
    user = airflow_conn.login
    password = airflow_conn.password

    # Validate required connection parameters
    if not host:
        raise ValueError(
            f"Connection '{postgres_conn_id}' missing 'host'. "
            f"Expected format: 'aws-0-[region].pooler.supabase.com'"
        )
    if not user:
        raise ValueError(
            f"Connection '{postgres_conn_id}' missing 'login'. "
            f"Expected format: 'postgres.[project-ref]'"
        )
    if not password:
        raise ValueError(
            f"Connection '{postgres_conn_id}' missing 'password'. "
            f"Get this from your Supabase dashboard -> Settings -> Database"
        )
    if not dbname:
        dbname = "postgres"  # Default for Supabase

    try:
        # Build connection parameters with SSL (required for Supabase)
        conn_kwargs = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "sslmode": "require",
            "connect_timeout": 10,
        }

        logger.info(
            f"Connecting to Supabase PostgreSQL at {user}@{host}:{port}/{dbname}"
        )

        # Execute query and return DataFrame
        with psycopg2.connect(**conn_kwargs) as connection:
            df = pd.read_sql_query(sql, connection)
            logger.info(f"Query returned {len(df)} rows")
            return df

    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "Tenant or user not found" in error_msg:
            logger.error(
                f"Authentication failed: The username '{user}' or host '{host}' is incorrect.\n"
                f"Check your Supabase dashboard -> Settings -> Database:\n"
                f"  - Host should be: aws-0-[region].pooler.supabase.com\n"
                f"  - User should be: postgres.[your-project-ref]\n"
                f"  - Ensure you're using Session Pooler (port 5432), not Transaction Pooler (6543)"
            )
        elif "authentication failed" in error_msg.lower():
            logger.error(
                f"Authentication failed: Password is incorrect for user '{user}'\n"
                f"Get the correct password from Supabase dashboard -> Settings -> Database"
            )
        else:
            logger.error(f"Connection error: {error_msg}")
        raise
    except Exception as e:
        logger.error(f"Error executing Supabase query: {str(e)}")
        raise
