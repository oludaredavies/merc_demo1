# Supabase PostgreSQL Connection Setup

This guide explains how to set up and use direct PostgreSQL connections to Supabase from your Airflow DAGs.

## Why Use PostgreSQL Instead of REST API?

- **Better for analytical queries**: Complex JOINs, aggregations, and window functions
- **Direct SQL access**: Full PostgreSQL features and syntax
- **Efficient for large datasets**: No pagination limits
- **Transaction support**: ACID guarantees for data consistency

## Connection Setup

### 1. Get Your Supabase PostgreSQL Credentials

From your Supabase dashboard:
1. Go to **Settings** → **Database**
2. Scroll down to **Connection string** section
3. Select **Session pooler** mode (NOT Transaction pooler or Direct connection)
4. Click **URI** dropdown and select **PSQL**
5. You'll see a connection string like:
   ```
   psql 'postgresql://postgres.PROJECT_REF:[YOUR-PASSWORD]@aws-0-REGION.pooler.supabase.com:5432/postgres'
   ```
6. Extract these values:
   - **Host**: `aws-0-[region].pooler.supabase.com` (e.g., `aws-0-us-east-1.pooler.supabase.com`)
   - **Port**: `5432` (Session Pooler)
   - **Database**: `postgres`
   - **User**: `postgres.[project-ref]` (e.g., `postgres.wmwrkfhkcthknlphsyvp`)
   - **Password**: Your database password (shown in connection string)

⚠️ **Important**: Make sure you're using the **Session Pooler** (port 5432), NOT:
   - Transaction Pooler (port 6543) - for high-throughput transactional queries
   - Direct Connection (port 5432 with different host) - for serverless functions

### 2. Create Airflow Connection

#### Option A: Via Airflow UI
1. Go to **Admin** → **Connections**
2. Click **+** to add a new connection
3. Fill in the details:
   - **Connection Id**: `davies_supabase_connection`
   - **Connection Type**: `Postgres`
   - **Host**: `aws-0-[region].pooler.supabase.com` (e.g., `aws-0-us-east-1.pooler.supabase.com`)
   - **Schema**: `postgres`
   - **Login**: `postgres.[your-project-ref]` (e.g., `postgres.wmwrkfhkcthknlphsyvp`)
   - **Password**: Your Supabase database password
   - **Port**: `5432`
   - **Extra**: `{"sslmode": "require"}` (optional, set automatically)

⚠️ **Critical**: Use the EXACT values from your Supabase dashboard. Copy-paste to avoid typos!

#### Option B: Via Astro CLI (for local development)
```bash
astro connection create \
  --connection-id davies_supabase_connection \
  --connection-type postgres \
  --host aws-1-eu-north-1.pooler.supabase.com \
  --port 5432 \
  --schema postgres \
  --login postgres.wmwrkfhkcthknlphsyvp \
  --password YOUR_PASSWORD
```

#### Option C: Via Environment Variable
```bash
# Format: postgresql://user:password@host:port/database?sslmode=require
export AIRFLOW_CONN_DAVIES_SUPABASE_CONNECTION='postgresql://postgres.wmwrkfhkcthknlphsyvp:YOUR_PASSWORD@aws-1-eu-north-1.pooler.supabase.com:5432/postgres?sslmode=require'
```

## Usage in DAGs

### Basic Query Example

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="my_supabase_query",
    start_date=datetime(2024, 1, 1),
    schedule=None,
)
def my_supabase_query():
    @task
    def run_query():
        from include.supabase_utils import fetch_supabase_dataframe

        # Execute SQL query
        df = fetch_supabase_dataframe("""
            SELECT *
            FROM laps
            WHERE session_key = 9158
            LIMIT 10
        """)

        print(f"Retrieved {len(df)} rows")
        print(df.head())

    run_query()

my_supabase_query()
```

### Advanced Query with JOINs

```python
@task
def analyze_race_data():
    from include.supabase_utils import fetch_supabase_dataframe

    sql = """
        SELECT
            l.driver_number,
            l.lap_number,
            l.lap_duration,
            p.position,
            i.gap_to_leader,
            ps.pit_duration
        FROM laps l
        INNER JOIN positions p
            ON l.session_key = p.session_key
            AND l.driver_number = p.driver_number
            AND l.date_start <= p.date
            AND l.date_end >= p.date
        LEFT JOIN intervals i
            ON l.session_key = i.session_key
            AND l.driver_number = i.driver_number
            AND l.date_end = i.date
        LEFT JOIN pit_stops ps
            ON l.session_key = ps.session_key
            AND l.driver_number = ps.driver_number
            AND l.lap_number = ps.lap_number
        WHERE l.session_key = 9158
        ORDER BY l.lap_number, p.position
    """

    df = fetch_supabase_dataframe(sql)
    return df.to_dict(orient="records")
```

### Using Parameters Safely

```python
@task
def query_session(session_key: int):
    from include.supabase_utils import fetch_supabase_dataframe
    import psycopg2.sql as sql

    # Use parameterized queries to prevent SQL injection
    query = """
        SELECT driver_number, AVG(lap_duration) as avg_lap
        FROM laps
        WHERE session_key = %s
        GROUP BY driver_number
    """

    # Note: pandas read_sql_query supports params
    df = fetch_supabase_dataframe(query, params=(session_key,))
    return df
```

## Function Reference

### `fetch_supabase_dataframe(sql, postgres_conn_id="davies_supabase_connection")`

Execute SQL against Supabase using direct PostgreSQL connection.

**Parameters:**
- `sql` (str): SQL query to execute
- `postgres_conn_id` (str, optional): Airflow connection ID. Default: `"davies_supabase_connection"`

**Returns:**
- `pandas.DataFrame`: Query results as a DataFrame

**Raises:**
- `Exception`: If connection fails or query execution fails

**Example:**
```python
df = fetch_supabase_dataframe(
    "SELECT * FROM laps WHERE session_key = 9158",
    postgres_conn_id="my_custom_connection"
)
```

## Quick Diagnosis

Run the test DAG first to diagnose connection issues:

```bash
# Trigger the test DAG
astro dev run dags test test_supabase_connection
```

This will:
- ✅ Validate your connection configuration
- ✅ Test the actual connection
- ✅ Run a sample query
- 📋 Provide specific fixes for any issues found

## Troubleshooting

### "Tenant or user not found"
```
psycopg2.OperationalError: connection to server at "..." failed: FATAL: Tenant or user not found
```

**Cause**: The host or username is incorrect.

**Solution**:
1. Go to Supabase Dashboard → **Settings** → **Database**
2. Select **Session pooler** mode
3. Verify these EXACT values in your Airflow connection:
   - Host format: `aws-0-[region].pooler.supabase.com` (note: `aws-0`, not `aws-1`)
   - User format: `postgres.[your-exact-project-ref]`
4. Copy-paste these values (don't type manually to avoid typos)

### Connection Timeout
```
psycopg2.OperationalError: timeout expired
```
**Solution**: Increase the connection timeout in your query or connection settings.

### SSL Required
```
psycopg2.OperationalError: SSL connection required
```
**Solution**: Ensure `sslmode=require` is set in your connection. The utility function sets this by default.

### Authentication Failed
```
psycopg2.OperationalError: authentication failed
```
**Solution**:
- Verify your password is correct (copy from Supabase dashboard)
- Ensure you're using the Session Pooler credentials (not Transaction Pooler or Direct Connection)
- Check that your IP is allowed in Supabase database settings

### Import Errors
```
ModuleNotFoundError: No module named 'pandas' / 'psycopg2'
```
**Solution**: Ensure `requirements.txt` includes:
```
psycopg2-binary>=2.9.9
pandas>=2.0.0
apache-airflow-providers-postgres>=5.10.2
```

## Performance Tips

1. **Use LIMIT**: Always limit result sets for exploratory queries
2. **Index your queries**: Ensure columns in WHERE/JOIN clauses are indexed
3. **Batch operations**: For large inserts, use batch operations instead of row-by-row
4. **Connection pooling**: The Session Pooler handles connection pooling automatically
5. **Avoid SELECT ***: Only select columns you need

## Security Best Practices

1. **Never hardcode credentials**: Always use Airflow connections
2. **Use parameterized queries**: Prevent SQL injection attacks
3. **Limit permissions**: Create read-only database users for query-only DAGs
4. **Rotate passwords**: Regularly update your Supabase database password
5. **Monitor access**: Review Supabase logs for unauthorized access attempts

## Example DAGs

See these example DAGs for more usage patterns:
- `example_supabase_postgres_query.py` - Basic query examples
- `openf1_postrace_kpis_supabase.py` - Complex aggregations

## Additional Resources

- [Supabase Database Documentation](https://supabase.com/docs/guides/database)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Airflow Postgres Provider](https://airflow.apache.org/docs/apache-airflow-providers-postgres/)
- [pandas read_sql_query](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html)
