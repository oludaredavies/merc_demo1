# Setup Guide: Supabase PostgreSQL Connection

This guide will help you set up the Supabase PostgreSQL connection for your OpenF1 data pipeline.

## Quick Start (2 Methods)

### Method 1: Using Environment Variables (Recommended)

The easiest way - the DAGs will auto-create the connection for you!

1. **Copy the example env file:**
   ```bash
   cp .env.example .env
   ```

2. **Get your Supabase credentials:**
   - Go to [Supabase Dashboard](https://supabase.com/dashboard)
   - Select your project
   - Navigate to **Settings** → **Database**
   - Scroll to **Connection string** section
   - Click **Session pooler** tab
   - Change dropdown to **PSQL**
   - You'll see: `psql 'postgresql://USER:PASSWORD@HOST:5432/postgres'`

3. **Update your `.env` file:**
   ```bash
   # Extract from the connection string above
   SUPABASE_HOST=aws-0-us-east-1.pooler.supabase.com
   SUPABASE_USER=postgres.wmwrkfhkcthknlphsyvp
   SUPABASE_PASSWORD=your_actual_password
   ```

4. **Load the env file:**
   ```bash
   # If using Astro CLI
   source .env

   # Or add to your .bashrc/.zshrc
   echo "source $(pwd)/.env" >> ~/.bashrc
   ```

5. **Run the test DAG:**
   ```bash
   astro dev run dags test test_supabase_connection
   ```

   You should see:
   ```
   ✅ Created connection 'davies_supabase_connection' from environment variables
   ✅ Connection successful!
   ✅ Query successful!
   ```

That's it! The connection is now available for all DAGs.

### Method 2: Manual Setup via Airflow UI

If you prefer not to use environment variables:

1. **Get your Supabase credentials** (same as Method 1, step 2)

2. **Create connection in Airflow UI:**
   - Open Airflow UI: http://localhost:8080 (or your Airflow URL)
   - Go to **Admin** → **Connections**
   - Click **+** (Add a new record)
   - Fill in:
     - **Connection Id**: `davies_supabase_connection`
     - **Connection Type**: `Postgres`
     - **Host**: `aws-0-[region].pooler.supabase.com` (from your Supabase dashboard)
     - **Schema**: `postgres`
     - **Login**: `postgres.[project-ref]` (from your Supabase dashboard)
     - **Password**: Your database password
     - **Port**: `5432`
   - Click **Save**

3. **Test the connection:**
   - Run the `test_supabase_connection` DAG from the UI
   - Check logs for ✅ success messages

## Verify Setup

After setup (either method), verify everything works:

1. **Run test DAG:**
   ```bash
   astro dev run dags test test_supabase_connection
   ```

2. **Run example DAG:**
   ```bash
   astro dev run dags test example_supabase_postgres_query
   ```

3. **Check the logs** - you should see:
   ```
   ✅ Connection 'davies_supabase_connection' already exists
   ✅ Query successful! Retrieved X rows
   ```

## Troubleshooting

### "Tenant or user not found"

**Cause**: Wrong host or username.

**Fix**:
1. Double-check you copied the EXACT values from Supabase dashboard
2. Ensure host format is `aws-0-[region].pooler.supabase.com` (note: `aws-0`, NOT `aws-1`)
3. Ensure user format is `postgres.[project-ref]`

See: `docs/QUICK_FIX_POSTGRES_CONNECTION.md`

### "Connection not found and cannot create it"

**Cause**: Environment variables not set or not loaded.

**Fix**:
```bash
# Check if env vars are set
echo $SUPABASE_HOST
echo $SUPABASE_USER
# (password should not be echoed for security)

# If empty, load the .env file
source .env

# Or set them manually
export SUPABASE_HOST=aws-0-us-east-1.pooler.supabase.com
export SUPABASE_USER=postgres.wmwrkfhkcthknlphsyvp
export SUPABASE_PASSWORD=your_password
```

### "Authentication failed"

**Cause**: Wrong password.

**Fix**:
1. Go to Supabase Dashboard → Settings → Database
2. Find your database password (may need to reset it)
3. Update `.env` file or Airflow connection with correct password

## Security Notes

⚠️ **Important**:
- Never commit `.env` file to git (it's in `.gitignore`)
- Never hardcode passwords in DAG files
- Rotate your database password regularly
- Use environment-specific passwords (dev, staging, prod)

## Next Steps

Once setup is complete, you can:
1. ✅ Run all OpenF1 DAGs
2. ✅ Use `fetch_supabase_dataframe()` in your own DAGs
3. ✅ Query any table in your Supabase database
4. ✅ Build analytics and reporting on top of OpenF1 data

## Additional Resources

- **Full documentation**: `docs/SUPABASE_POSTGRES_SETUP.md`
- **Quick fix guide**: `docs/QUICK_FIX_POSTGRES_CONNECTION.md`
- **Example DAGs**:
  - `dags/example_supabase_postgres_query.py`
  - `dags/openf1_postrace_kpis_postgres.py`
  - `dags/test_supabase_connection.py`

## Need Help?

1. Run `test_supabase_connection` DAG - it provides detailed diagnostics
2. Check the troubleshooting section above
3. See `docs/QUICK_FIX_POSTGRES_CONNECTION.md` for common issues
