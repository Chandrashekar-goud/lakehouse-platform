"""Chicago crimes ingestion: incremental on updated_on, paged, landed to S3."""
from __future__ import annotations

from datetime import datetime, timedelta

import yaml
from airflow.decorators import dag, task

CONFIG_PATH = "/opt/project/configs/sources.yaml"
STATE_PATH = "/opt/project/state/watermarks.json"

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)}


@dag(
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "batch"],
)
def crimes_ingest():
    @task
    def fetch_and_land() -> dict:
        import os

        from lakehouse.ingestion.api_client import ChicagoCrimesClient
        from lakehouse.ingestion.landing import build_key, land_to_s3, new_batch_id
        from lakehouse.ingestion.watermark import WatermarkStore

        cfg = yaml.safe_load(open(CONFIG_PATH))
        wm = WatermarkStore(STATE_PATH)
        watermark = wm.get("crimes", cfg["crimes"]["initial_watermark"])

        client = ChicagoCrimesClient(cfg["crimes"]["api_url"], cfg["crimes"]["page_size"])
        rows = client.fetch_updated_since(watermark)
        if not rows:
            return {"records": 0}

        bucket = os.environ["LAKEHOUSE_S3_BUCKET"]
        key = build_key(cfg["storage"]["raw_prefix"], "crimes", new_batch_id())
        uri = land_to_s3(rows, bucket, key)
        # Advance to the max updated_on actually received: no data is skipped
        # even if the run happens mid-update on the portal side.
        wm.set("crimes", max(r["updated_on"] for r in rows if r.get("updated_on")))
        return {"records": len(rows), "uri": uri}

    fetch_and_land()


crimes_ingest()
