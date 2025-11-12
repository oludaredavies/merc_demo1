# Quick Fix: "Tenant or user not found" Error

## The Error
```
psycopg2.OperationalError: connection to server at "aws-1-us-east-1.pooler.supabase.com"
failed: FATAL: Tenant or user not found
```

## The Fix (3 Steps)

### Step 1: Get Correct Credentials from Supabase

1. Open your Supabase project dashboard
2. Go to **Settings** → **Database** (in left sidebar)
3. Scroll to **Connection string** section
4. Click **Session pooler** tab (NOT Transaction pooler or Direct connection)
5. Change dropdown from **URI** to **PSQL**
6. You'll see something like:

```bash
psql 'postgresql://postgres.abcdefg12345:[YOUR-PASSWORD]@aws-0-us-east-1.pooler.supabase.com:5432/postgres'
```

**Extract these values:**
- **Host**: `aws-0-us-east-1.pooler.supabase.com` (note: `aws-0`, NOT `aws-1`)
- **User**: `postgres.abcdefg12345` (everything before `:` and after `//`)
- **Password**: The part between `:` and `@`
- **Port**: `5432`
- **Database**: `postgres`

### Step 2: Update Your Airflow Connection

#### Option A: Via Airflow UI
1. Go to **Admin** → **Connections**
2. Find connection `davies_supabase_connection`
3. Click **Edit** (pencil icon)
4. **Copy-paste** (don't type!) these values:
   - **Host**: (from Step 1)
   - **Login**: (from Step 1)
   - **Password**: (from Step 1)
   - **Port**: `5432`
   - **Schema**: `postgres`
5. Click **Save**

#### Option B: Via Astro CLI
```bash
# Delete old connection if it exists
astro connection delete davies_supabase_connection

# Create new connection with correct values
astro connection create \
  --connection-id davies_supabase_connection \
  --connection-type postgres \
  --host aws-0-us-east-1.pooler.supabase.com \
  --port 5432 \
  --schema postgres \
  --login postgres.abcdefg12345 \
  --password "YOUR_PASSWORD"
```

**Replace**:
- `aws-0-us-east-1.pooler.supabase.com` with YOUR host
- `postgres.abcdefg12345` with YOUR user
- `YOUR_PASSWORD` with YOUR password

### Step 3: Test the Connection

Run the test DAG:

```bash
astro dev run dags test test_supabase_connection
```

Or manually trigger it from Airflow UI.

You should see:
```
✅ Configuration looks good!
✅ Connection successful!
Found 14 tables
✅ Query successful! Retrieved 5 rows
```

## Common Mistakes

❌ **Wrong host format**
- Bad: `aws-1-eu-north-1.pooler.supabase.com` (should be `aws-0`)
- Bad: `supabase.co` (missing region)
- Good: `aws-0-us-east-1.pooler.supabase.com`

❌ **Wrong user format**
- Bad: `postgres` (missing project ref)
- Bad: `wmwrkfhkcthknlphsyvp` (missing `postgres.` prefix)
- Good: `postgres.wmwrkfhkcthknlphsyvp`

❌ **Wrong pooler mode**
- Bad: Transaction pooler (port 6543)
- Bad: Direct connection
- Good: Session pooler (port 5432)

## Still Not Working?

Run the diagnostic DAG for detailed error messages:
```bash
astro dev run dags test test_supabase_connection
```

The DAG will tell you exactly what's wrong and how to fix it.

## Need More Help?

See the full documentation: `docs/SUPABASE_POSTGRES_SETUP.md`
