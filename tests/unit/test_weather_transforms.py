from datetime import datetime

from pyspark.sql import functions as F

from lakehouse.transforms.weather import gold_daily_weather, silver_weather


def _bronze(spark, rows):
    df = spark.createDataFrame(
        rows,
        "city string, observed_at string, temperature_2m string, "
        "relative_humidity_2m string, precipitation string, wind_speed_10m string, "
        "_ingested_at timestamp",
    )
    return df


def test_silver_dedupes_keeping_latest_ingestion(spark):
    rows = [
        ("chicago", "2026-06-01T00:00", "20.0", "50", "0", "10", datetime(2026, 6, 2)),
        ("chicago", "2026-06-01T00:00", "21.0", "50", "0", "10", datetime(2026, 6, 3)),
    ]
    out = silver_weather(_bronze(spark, rows)).collect()
    assert len(out) == 1
    assert out[0]["temperature_2m"] == 21.0  # later ingestion wins


def test_silver_nulls_out_of_range_values(spark):
    rows = [("chicago", "2026-06-01T00:00", "999.0", "150", "0", "10", datetime(2026, 6, 2))]
    out = silver_weather(_bronze(spark, rows)).collect()[0]
    assert out["temperature_2m"] is None
    assert out["relative_humidity_2m"] is None


def test_gold_daily_aggregates(spark):
    rows = [
        ("chicago", "2026-06-01T00:00", "10.0", "40", "1.0", "5", datetime(2026, 6, 2)),
        ("chicago", "2026-06-01T12:00", "30.0", "60", "2.0", "15", datetime(2026, 6, 2)),
    ]
    gold = gold_daily_weather(silver_weather(_bronze(spark, rows)))
    row = gold.filter(F.col("city") == "chicago").collect()[0]
    assert row["temp_min_c"] == 10.0
    assert row["temp_max_c"] == 30.0
    assert row["precip_total_mm"] == 3.0
    assert row["hours_observed"] == 2
