"""Streaming event producer: writes JSON micro-batches of synthetic Divvy-style
bike trip events. Point --out at a local dir for testing or at
/Volumes/lakehouse/landing/events when run inside Databricks.

Usage: python scripts/produce_events.py --out ./data/events --batches 10 --interval 5
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

STATIONS = [
    ("13022", "Streeter Dr & Grand Ave"),
    ("13300", "DuSable Lake Shore Dr & Monroe St"),
    ("13042", "Michigan Ave & Oak St"),
    ("TA1308000050", "Wells St & Concord Ln"),
    ("KA1503000043", "Kingsbury St & Kinzie St"),
]
RIDER_TYPES = ["member", "casual"]
EVENT_TYPES = ["trip_start", "trip_end"]


def make_event(now: datetime) -> dict:
    station_id, station_name = random.choice(STATIONS)
    # ~5% late events to exercise the watermark path
    lateness = timedelta(minutes=random.randint(20, 40)) if random.random() < 0.05 else timedelta()
    return {
        "trip_id": uuid.uuid4().hex,
        "station_id": station_id,
        "station_name": station_name,
        "rider_type": random.choice(RIDER_TYPES),
        "event_type": random.choice(EVENT_TYPES),
        "event_ts": (now - lateness).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--events-per-batch", type=int, default=50)
    parser.add_argument("--interval", type=float, default=5.0)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for i in range(args.batches):
        now = datetime.now(timezone.utc)
        events = [make_event(now) for _ in range(args.events_per_batch)]
        path = out / f"events-{now:%Y%m%dT%H%M%S}-{i:03d}.json"
        path.write_text("\n".join(json.dumps(e) for e in events))
        print(f"[{i + 1}/{args.batches}] wrote {len(events)} events -> {path}")
        if i < args.batches - 1:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
