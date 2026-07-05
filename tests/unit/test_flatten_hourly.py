"""Pure-Python tests: no Spark needed, run in milliseconds."""
from lakehouse.transforms.weather import flatten_hourly


def test_flatten_pivots_columnar_payload():
    payload = {
        "hourly": {
            "time": ["2026-06-01T00:00", "2026-06-01T01:00"],
            "temperature_2m": [21.5, 20.9],
            "precipitation": [0.0, 0.2],
        }
    }
    records = flatten_hourly(payload, "chicago")
    assert len(records) == 2
    assert records[0] == {
        "city": "chicago",
        "observed_at": "2026-06-01T00:00",
        "temperature_2m": 21.5,
        "precipitation": 0.0,
    }


def test_flatten_handles_ragged_and_empty_payloads():
    assert flatten_hourly({}, "chicago") == []
    ragged = {"hourly": {"time": ["2026-06-01T00:00"], "temperature_2m": []}}
    assert flatten_hourly(ragged, "chicago")[0]["temperature_2m"] is None
