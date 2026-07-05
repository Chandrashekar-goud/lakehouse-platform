"""File-backed watermark store for incremental loads.

A JSON file keyed by source name. Writes are atomic (write temp file, then
rename) so a crashed run never leaves a corrupt store. In production this
would be a Delta table or DynamoDB; a file keeps the free tier honest while
the interface stays swappable.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from lakehouse.logging_conf import get_logger

log = get_logger(__name__)


class WatermarkStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict:
        if not self.path.exists():
            return {}
        with open(self.path) as fh:
            return json.load(fh)

    def get(self, source: str, default: str) -> str:
        value = self._load().get(source, default)
        log.info("Watermark for %s: %s", source, value)
        return value

    def set(self, source: str, value: str) -> None:
        data = self._load()
        data[source] = value
        fd, tmp = tempfile.mkstemp(dir=self.path.parent)
        with os.fdopen(fd, "w") as fh:
            json.dump(data, fh, indent=2)
        os.replace(tmp, self.path)
        log.info("Watermark for %s advanced to %s", source, value)
