"""
Test and validate Supabase PostgreSQL connection.

This DAG helps diagnose connection issues by checking the configuration
and attempting a simple connection test.

SETUP (Optional):
Set these environment variables to auto-create the connection:
- SUPABASE_HOST: aws-0-[region].pooler.supabase.com
- SUPABASE_USER: postgres.[project-ref]
- SUPABASE_PASSWORD: your database password

Or create connection 'davies_supabase_connection' manually via Airflow UI.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="test_supabase_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Validate Supabase PostgreSQL connection configuration",
    tags=["test", "supabase", "postgres"],
)
def test_supabase_connection():
    @task
    def check_connection_config() -> dict:
        """Check the Airflow connection configuration."""
        from airflow.hooks.base import BaseHook

        conn_id = "davies_supabase_connection"

        try:
            conn = BaseHook.get_connection(conn_id)

            print(f"\n{'=' * 60}")
            print(f"CONNECTION CONFIGURATION: {conn_id}")
            print(f"{'=' * 60}")
            print(f"Connection Type: {conn.conn_type}")
            print(f"Host: {conn.host}")
            print(f"Port: {conn.port or 5432}")
            print(f"Schema/Database: {conn.schema or 'postgres'}")
            print(f"Login/User: {conn.login}")
            print(
                f"Password: {'*' * len(conn.password) if conn.password else 'NOT SET'}"
            )
            print(f"Extra: {conn.extra}")
            print(f"{'=' * 60}\n")

            # Validate configuration
            issues = []

            if not conn.host:
                issues.append("❌ Host is not set")
            elif not conn.host.endswith(".pooler.supabase.com"):
                issues.append(
                    f"⚠️  Host '{conn.host}' doesn't look like a Supabase pooler URL.\n"
                    f"   Expected format: aws-0-[region].pooler.supabase.com"
                )

            if not conn.login:
                issues.append("❌ Login/User is not set")
            elif not conn.login.startswith("postgres."):
                issues.append(
                    f"⚠️  User '{conn.login}' doesn't look like a Supabase user.\n"
                    f"   Expected format: postgres.[project-ref]"
                )

            if not conn.password:
                issues.append("❌ Password is not set")

            if conn.port and conn.port not in [5432, 6543]:
                issues.append(
                    f"⚠️  Port {conn.port} is unusual. Supabase uses:\n"
                    f"   - 5432 for Session Pooler (recommended)\n"
                    f"   - 6543 for Transaction Pooler"
                )

            if issues:
                print("CONFIGURATION ISSUES:")
                for issue in issues:
                    print(f"  {issue}")
                print()
                return {
                    "status": "invalid",
                    "issues": issues,
                    "conn_type": conn.conn_type,
                    "host": conn.host,
                    "user": conn.login,
                }
            else:
                print("✅ Configuration looks good!\n")
                return {
                    "status": "valid",
                    "conn_type": conn.conn_type,
                    "host": conn.host,
                    "user": conn.login,
                }

        except Exception as e:
            print(f"❌ Error reading connection: {str(e)}\n")
            return {
                "status": "error",
                "error": str(e),
            }

    @task
    def test_connection(config: dict) -> dict:
        """Test the actual PostgreSQL connection."""
        import psycopg2

        if config.get("status") != "valid":
            print("⏭️  Skipping connection test due to configuration issues")
            return {"status": "skipped"}

        print(f"\n{'=' * 60}")
        print("TESTING CONNECTION")
        print(f"{'=' * 60}\n")

        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            postgres_hook = PostgresHook(postgres_conn_id="davies_supabase_connection")
            conn_info = postgres_hook.get_connection("davies_supabase_connection")

            conn_kwargs = {
                "host": conn_info.host,
                "port": conn_info.port or 5432,
                "dbname": conn_info.schema or "postgres",
                "user": conn_info.login,
                "password": conn_info.password,
                "sslmode": "require",
                "connect_timeout": 10,
            }

            print(
                f"Connecting to {conn_info.login}@{conn_info.host}:{conn_kwargs['port']}/{conn_kwargs['dbname']}..."
            )

            with psycopg2.connect(**conn_kwargs) as connection:
                with connection.cursor() as cursor:
                    # Test query
                    cursor.execute("SELECT version();")
                    version = cursor.fetchone()[0]
                    print("✅ Connection successful!")
                    print(f"PostgreSQL version: {version}\n")

                    # Check if we can see tables
                    cursor.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        ORDER BY table_name
                        LIMIT 10;
                    """)
                    tables = cursor.fetchall()

                    if tables:
                        print(f"Found {len(tables)} tables (showing first 10):")
                        for table in tables:
                            print(f"  - {table[0]}")
                    else:
                        print("⚠️  No tables found in public schema")

            return {
                "status": "success",
                "version": version,
                "tables_found": len(tables),
            }

        except psycopg2.OperationalError as e:
            error_msg = str(e)
            print(f"❌ Connection failed: {error_msg}\n")

            # Provide helpful diagnostics
            if "Tenant or user not found" in error_msg:
                print("DIAGNOSIS:")
                print("  This error means either:")
                print(f"  1. The host '{config['host']}' is incorrect (wrong region?)")
                print(f"  2. The user '{config['user']}' is incorrect (wrong project?)")
                print("\nTO FIX:")
                print("  1. Go to Supabase Dashboard -> Settings -> Database")
                print("  2. Under 'Connection string', select 'Session pooler'")
                print("  3. Copy the connection details:")
                print("     - Host: aws-0-[region].pooler.supabase.com")
                print("     - User: postgres.[your-project-ref]")
                print("  4. Update your Airflow connection with these values")

            elif "authentication failed" in error_msg.lower():
                print("DIAGNOSIS:")
                print("  The password is incorrect")
                print("\nTO FIX:")
                print("  1. Go to Supabase Dashboard -> Settings -> Database")
                print("  2. Copy the 'Database password'")
                print("  3. Update your Airflow connection password")

            return {
                "status": "failed",
                "error": error_msg,
            }

        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}\n")
            return {
                "status": "error",
                "error": str(e),
            }

    @task
    def test_query(conn_result: dict) -> None:
        """Test a simple query using the utility function."""
        if conn_result.get("status") != "success":
            print("⏭️  Skipping query test due to connection issues")
            return

        print(f"\n{'=' * 60}")
        print("TESTING QUERY")
        print(f"{'=' * 60}\n")

        try:
            from include.supabase_utils import fetch_supabase_dataframe

            # Simple test query
            df = fetch_supabase_dataframe("""
                SELECT
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                LIMIT 5;
            """)

            print(f"✅ Query successful! Retrieved {len(df)} rows\n")
            print("Top 5 largest tables:")
            print(df.to_string(index=False))
            print()

        except Exception as e:
            print(f"❌ Query failed: {str(e)}\n")

    # Task flow
    config = check_connection_config()
    conn_result = test_connection(config)
    test_query(conn_result)


test_supabase_connection()
