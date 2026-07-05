"""HTTP clients for source APIs with retry, backoff, and pagination.

Design notes (interview-relevant):
- Transient failures (429, 5xx, timeouts, connection errors) are retried with
  exponential backoff. Client errors (4xx other than 429) fail fast: retrying
  a bad request wastes quota and hides bugs.
- Pagination for the Socrata (Chicago crimes) API uses offset paging with a
  stable order clause; without ordering, offset paging can skip/duplicate rows.
"""
from __future__ import annotations

import time
from typing import Any

import requests

from lakehouse.logging_conf import get_logger

log = get_logger(__name__)


class TransientAPIError(Exception):
    """Retryable failure: rate limit, server error, or network issue."""


class _RetryingClient:
    def __init__(self, max_retries: int = 4, backoff_base: float = 2.0, timeout: int = 60):
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.timeout = timeout

    def _get(self, url: str, params: dict[str, Any]) -> Any:
        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise TransientAPIError(f"HTTP {resp.status_code} from {url}")
                resp.raise_for_status()
                return resp.json()
            except (requests.ConnectionError, requests.Timeout, TransientAPIError) as err:
                last_err = err
                if attempt == self.max_retries:
                    break
                sleep_s = self.backoff_base**attempt
                log.warning("Attempt %d/%d failed (%s); retrying in %.1fs",
                            attempt, self.max_retries, err, sleep_s)
                time.sleep(sleep_s)
        log.error("Giving up on %s after %d attempts", url, self.max_retries)
        raise TransientAPIError(str(last_err))


class OpenMeteoClient(_RetryingClient):
    """Historical weather. No API key required."""

    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def fetch_hourly(self, latitude: float, longitude: float, start_date: str,
                     end_date: str, variables: list[str]) -> dict:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(variables),
            "timezone": "UTC",
        }
        log.info("Fetching Open-Meteo hourly %s -> %s for (%.4f, %.4f)",
                 start_date, end_date, latitude, longitude)
        return self._get(self.BASE_URL, params)


class ChicagoCrimesClient(_RetryingClient):
    """Socrata SODA API for Chicago crimes, incremental on `updated_on`."""

    def __init__(self, api_url: str, page_size: int = 50000, **kwargs):
        super().__init__(**kwargs)
        self.api_url = api_url
        self.page_size = page_size

    def fetch_updated_since(self, watermark_iso: str) -> list[dict]:
        """Fetch all rows updated after the watermark, paging until exhausted."""
        rows: list[dict] = []
        offset = 0
        while True:
            params = {
                "$where": f"updated_on > '{watermark_iso}'",
                "$order": "updated_on, id",
                "$limit": self.page_size,
                "$offset": offset,
            }
            page = self._get(self.api_url, params)
            rows.extend(page)
            log.info("Fetched page at offset %d: %d rows", offset, len(page))
            if len(page) < self.page_size:
                return rows
            offset += self.page_size
