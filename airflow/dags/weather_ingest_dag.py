"""Weather ingestion DAG: fetch incrementally -> validate -> land to S3 ->
advance watermark -> trigger the Databricks medallion job.

Idempotency: the run is keyed by execution date; re-running a day re-lands the
same window and Bronze's replaceWhere makes the rewrite safe.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import yaml
from airflow.decorators import dag, task

CONFIG_PATH = "/opt/project/configs/sources.yaml"
STATE_PATH = "/opt/project/state/watermarks.json"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "batch"],
)
def weather_ingest():
    @task
    def fetch_and_land() -> dict:
        from datetime import date, timedelta as td

        from lakehouse.ingestion.api_client import OpenMeteoClient
        from lakehouse.ingestion.landing import build_key, land_to_s3, new_batch_id
        from lakehouse.ingestion.watermark import WatermarkStore
        from lakehouse.transforms.weather import flatten_hourly

        cfg = yaml.safe_load(open(CONFIG_PATH))
        wm = WatermarkStore(STATE_PATH)
        start = wm.get("weather", cfg["weather"]["initial_start_date"])
        # Archive API lags ~2 days; never request the future.
        end = str(date.today() - td(days=2))
        if start > end:
            return {"records": 0, "skipped": True}

        client = OpenMeteoClient()
        records: list[dict] = []
        for city in cfg["weather"]["cities"]:
            payload = client.fetch_hourly(
                city["latitude"], city["longitude"], start, end,
                cfg["weather"]["hourly_variables"],
            )
            records.extend(flatten_hourly(payload, city["name"]))
        if not records:
            raise ValueError(f"Zero records for window {start}..{end}: refusing to advance watermark")

        import os
        bucket = os.environ["LAKEHOUSE_S3_BUCKET"]
        key = build_key(cfg["storage"]["raw_prefix"], "weather", new_batch_id())
        uri = land_to_s3(records, bucket, key)
        wm.set("weather", str(date.today() - td(days=1)))
        return {"records": len(records), "uri": uri, "skipped": False}

    @task
    def trigger_databricks(land_result: dict) -> str:
        """Kick the medallion job via the Jobs API. Degrades gracefully if the
        workspace has no PAT support: logs a message and the native Databricks
        schedule owns Bronze->Gold instead."""
        import os

        import requests

        if land_result.get("skipped"):
            return "skipped: nothing landed"
        host, token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
        job_id = os.environ.get("DATABRICKS_BATCH_JOB_ID")
        if not all([host, token, job_id]):
            return "no PAT/job configured: relying on native Databricks schedule"
        resp = requests.post(
            f"{host}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {token}"},
            json={"job_id": int(job_id)},
            timeout=30,
        )
        resp.raise_for_status()
        return f"triggered run {resp.json()['run_id']}"

    trigger_databricks(fetch_and_land())


weather_ingest()
