from __future__ import annotations

from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from supabase import create_client


TARGET_DRIVER_NUMBERS = [63, 12]


@dag(
    dag_id="openf1_postrace_kpis_supabase",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Post-race: compute per-driver KPIs and store in Supabase",
    default_args={
        "owner": "openf1",
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["openf1", "supabase"],
)
def openf1_postrace_kpis_supabase():
    @task
    def compute_and_store_kpis() -> None:
        """Compute KPIs from existing Supabase data and store results."""
        # Get Supabase connection
        conn = BaseHook.get_connection("davies_supabase_connection_http")

        # Ensure proper base URL (remove /rest/v1 suffix if present)
        supabase_url = (
            conn.host if conn.host.startswith("http") else f"https://{conn.host}"
        )
        supabase_url = supabase_url.rstrip("/").replace("/rest/v1", "")

        sb = create_client(supabase_url, conn.password)

        # Identify latest race session from laps table
        latest_resp = (
            sb.table("laps")
            .select("session_key")
            .order("session_key", desc=True)
            .limit(1)
            .execute()
        )
        if not latest_resp.data:
            return
        session_key = latest_resp.data[0]["session_key"]

        # Pull laps, stints, pit, session_results for target drivers
        laps = (
            sb.table("laps")
            .select(
                "driver_number,lap_number,lap_duration,sector_1,sector_2,sector_3,is_pit_out_lap"
            )
            .eq("session_key", session_key)
            .in_("driver_number", TARGET_DRIVER_NUMBERS)
            .execute()
        ).data

        stints = (
            sb.table("stints")
            .select("driver_number,stint_number,compound,total_laps,start_lap,end_lap")
            .eq("session_key", session_key)
            .in_("driver_number", TARGET_DRIVER_NUMBERS)
            .execute()
        ).data

        pits = (
            sb.table("pit_stops")
            .select("driver_number,lap_number,pit_duration")
            .eq("session_key", session_key)
            .in_("driver_number", TARGET_DRIVER_NUMBERS)
            .execute()
        ).data

        results = (
            sb.table("session_results")
            .select(
                "driver_number,position,points,laps_completed,best_lap_time,avg_lap_time"
            )
            .eq("session_key", session_key)
            .in_("driver_number", TARGET_DRIVER_NUMBERS)
            .execute()
        ).data

        # Group data by driver
        laps_by_driver: Dict[int, List[Dict[str, Any]]] = {}
        for row in laps:
            laps_by_driver.setdefault(int(row["driver_number"]), []).append(row)

        pits_by_driver: Dict[int, List[Dict[str, Any]]] = {}
        for row in pits:
            pits_by_driver.setdefault(int(row["driver_number"]), []).append(row)

        stints_by_driver: Dict[int, List[Dict[str, Any]]] = {}
        for row in stints:
            stints_by_driver.setdefault(int(row["driver_number"]), []).append(row)

        results_by_driver: Dict[int, Dict[str, Any]] = {
            int(r["driver_number"]): r for r in results
        }

        # Calculate KPIs for each driver
        summaries: List[Dict[str, Any]] = []
        for driver_number in TARGET_DRIVER_NUMBERS:
            driver_laps = laps_by_driver.get(driver_number, [])
            if not driver_laps:
                continue
            total_laps = len(driver_laps)
            valid_laps = [l for l in driver_laps if l.get("lap_duration")]
            avg_lap = (
                sum(float(l.get("lap_duration")) for l in valid_laps) / len(valid_laps)
                if valid_laps
                else None
            )
            best_lap = (
                min(float(l.get("lap_duration")) for l in valid_laps)
                if valid_laps
                else None
            )

            # Calculate consistency (exclude pit-out laps)
            clean_laps = [l for l in valid_laps if not l.get("is_pit_out_lap")]
            if clean_laps:
                mean = sum(float(l.get("lap_duration")) for l in clean_laps) / len(
                    clean_laps
                )
                variance = sum(
                    (float(l.get("lap_duration")) - mean) ** 2 for l in clean_laps
                ) / len(clean_laps)
                consistency = variance**0.5
            else:
                consistency = None

            pit_list = pits_by_driver.get(driver_number, [])
            num_pit_stops = len(pit_list)
            avg_pit = (
                sum(float(p.get("pit_duration") or 0) for p in pit_list) / num_pit_stops
                if pit_list
                else None
            )

            stint_list = stints_by_driver.get(driver_number, [])
            tyre_usage = (
                ",".join(
                    sorted(
                        {
                            (s.get("compound") or "").upper()
                            for s in stint_list
                            if s.get("compound")
                        }
                    )
                )
                or None
            )

            res = results_by_driver.get(driver_number, {})
            summaries.append(
                {
                    "session_key": session_key,
                    "driver_number": driver_number,
                    "total_laps": total_laps,
                    "avg_lap_time": avg_lap,
                    "best_lap_time": best_lap,
                    "lap_time_stddev": consistency,
                    "num_pit_stops": num_pit_stops,
                    "avg_pit_duration": avg_pit,
                    "tyre_compounds_used": tyre_usage,
                    "finishing_position": res.get("position"),
                    "points": res.get("points"),
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                }
            )

        if summaries:
            print(f"Upserting {len(summaries)} KPI records for session {session_key}")
            result = (
                sb.table("postrace_driver_kpis")
                .upsert(
                    summaries,
                    on_conflict="session_key,driver_number",
                )
                .execute()
            )
            print(f"Successfully upserted {len(result.data)} KPI records")
        else:
            print("No KPI data to upsert")

    compute_and_store_kpis()


openf1_postrace_kpis_supabase()
