Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.

Post-Race Analysis Use Case
===========================

This project includes Airflow DAGs that perform a post-race analysis workflow for Mercedes-AMG PETRONAS drivers George Russell (63) and Kimi Antonelli (12) using the OpenF1 API ([OpenF1 API endpoints](https://openf1.org/#api-endpoints)). The pipelines focus on ingesting race-complete data and computing driver performance KPIs that race engineers care about after the checkered flag.

What gets ingested post-race
----------------------------

- Drivers (latest session context)
- Laps (full race lap chart, including sectors and pit-out flags)
- Car telemetry snapshots (for deeper correlation, limited windowed pulls are available)
- Weather (context for track evolution and conditions)
- Stints (per-tyre compound usage with start/end laps)
- Pit stops (per-lap pit timings)
- Session results (finishing position, points, best/avg lap metrics)

Computed KPIs (stored in Supabase)
----------------------------------

- Total laps completed
- Average lap time and best lap time
- Lap time consistency (standard deviation excluding pit-out laps)
- Number of pit stops and average pit duration
- Tyre compounds used across stints
- Finishing position and points (from session results)

Setup Instructions
------------------

**Quick Start** (recommended):

1. **Set up Supabase connection** (choose one method):

   **Method A: Environment Variables** (auto-creates connection)
   ```bash
   # Copy example env file
   cp .env.example .env

   # Edit .env with your Supabase credentials
   # Get these from: Supabase Dashboard → Settings → Database → Connection string (Session pooler)
   SUPABASE_HOST=aws-0-[region].pooler.supabase.com
   SUPABASE_USER=postgres.[project-ref]
   SUPABASE_PASSWORD=your_password

   # Load environment variables
   source .env
   ```

   **Method B: Airflow UI**
   - Go to Admin → Connections → Create
   - Connection Id: `davies_supabase_connection`
   - Connection Type: `Postgres`
   - Fill in details from Supabase Dashboard

2. **Verify setup**:
   ```bash
   # Test the connection
   astro dev run dags test test_supabase_connection

   # Run example DAG
   astro dev run dags test example_supabase_postgres_query
   ```

3. **See detailed setup guide**: `docs/SETUP_GUIDE.md`

How to run the post-race workflow
---------------------------------

1. **Ensure Supabase connection is configured** (see Setup Instructions above)

2. **Ensure the Supabase schema is created**. Example tables used by these DAGs: `drivers`, `laps`, `car_data`, `weather`, `stints`, `pit_stops`, `session_results`, `postrace_driver_kpis`.

3. **Trigger the following DAGs** after a race completes (they default to manual/None schedule for post-race runs):
   - `openf1_stints_supabase`
   - `openf1_pit_supabase`
   - `openf1_session_result_supabase`
   - `openf1_postrace_kpis_supabase` (computes and stores KPIs)
   - `openf1_postrace_kpis_postgres` (alternative using SQL for better performance)

4. **Optional supporting DAGs** (scheduled) that also enrich context:
   - `openf1_drivers_supabase`
   - `openf1_laps_supabase`
   - `openf1_car_data_supabase`
   - `openf1_weather_supabase`

Documentation
-------------

- **Setup Guide**: `docs/SETUP_GUIDE.md` - Complete setup instructions with troubleshooting
- **Quick Fix Guide**: `docs/QUICK_FIX_POSTGRES_CONNECTION.md` - Fix common connection errors
- **Full Documentation**: `docs/SUPABASE_POSTGRES_SETUP.md` - Comprehensive PostgreSQL setup guide
- **Example DAGs**:
  - `dags/test_supabase_connection.py` - Diagnostic and testing DAG
  - `dags/example_supabase_postgres_query.py` - Query examples with auto-connection setup
  - `dags/openf1_postrace_kpis_postgres.py` - Advanced SQL aggregation example

Notes
-----

- All data is sourced from OpenF1 using supported filters (e.g., `session_key=latest`, time-based filtering). See: [OpenF1 Data filtering](https://openf1.org/#api-endpoints).
- Upserts and composite primary keys are used in Supabase to keep the DAGs idempotent for repeated post-race runs.
- DAGs support both REST API (via `supabase-py`) and direct PostgreSQL connections (via `psycopg2`) - use PostgreSQL for complex analytics queries.
