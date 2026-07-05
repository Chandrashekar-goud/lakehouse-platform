"""Land raw payloads to the landing zone: S3 primary, UC volume fallback.

Layout: {prefix}/{source}/ingest_date=YYYY-MM-DD/{batch_id}.json
Hive-style ingest_date partitioning lets Bronze jobs prune by arrival date and
makes reruns idempotent at the batch level: re-landing the same batch_id
overwrites rather than duplicates.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.logging_conf import get_logger

log = get_logger(__name__)


def new_batch_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


def build_key(prefix: str, source: str, batch_id: str,
              ingest_date: str | None = None) -> str:
    ingest_date = ingest_date or f"{datetime.now(timezone.utc):%Y-%m-%d}"
    return f"{prefix}/{source}/ingest_date={ingest_date}/{batch_id}.json"


def land_to_s3(payload: list | dict, bucket: str, key: str) -> str:
    import boto3  # local import: not needed on the UC-volume fallback path

    body = json.dumps(payload)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode())
    uri = f"s3://{bucket}/{key}"
    log.info("Landed %d bytes to %s", len(body), uri)
    return uri


def land_to_volume(payload: list | dict, volume_root: str, key: str) -> str:
    """Fallback when serverless egress to S3 is unavailable."""
    path = Path(volume_root) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    log.info("Landed payload to %s", path)
    return str(path)
