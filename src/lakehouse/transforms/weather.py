"""Weather pipeline transforms: Open-Meteo payload -> Bronze -> Silver -> Gold."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.transforms.common import dedupe_latest

# Physical sanity bounds for the Silver quality gate (Celsius, %, mm, km/h).
TEMP_RANGE = (-60.0, 60.0)
HUMIDITY_RANGE = (0.0, 100.0)


def flatten_hourly(payload: dict, city: str) -> list[dict]:
    """Open-Meteo returns columnar arrays; pivot to one record per hour.

    Pure Python on purpose: testable without Spark, reusable in Airflow.
    """
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    variables = [k for k in hourly if k != "time"]
    records = []
    for i, ts in enumerate(times):
        rec = {"city": city, "observed_at": ts}
        for var in variables:
            values = hourly.get(var) or []
            rec[var] = values[i] if i < len(values) else None
        records.append(rec)
    return records


def silver_weather(bronze: DataFrame) -> DataFrame:
    """Cast, bound-check, and dedupe. Forecast revisions mean the same
    (city, hour) can arrive in multiple batches; latest ingestion wins."""
    typed = (
        bronze.withColumn("observed_at", F.to_timestamp("observed_at"))
        .withColumn("temperature_2m", F.col("temperature_2m").cast("double"))
        .withColumn("relative_humidity_2m", F.col("relative_humidity_2m").cast("double"))
        .withColumn("precipitation", F.col("precipitation").cast("double"))
        .withColumn("wind_speed_10m", F.col("wind_speed_10m").cast("double"))
        .filter(F.col("observed_at").isNotNull() & F.col("city").isNotNull())
    )
    bounded = typed.withColumn(
        "temperature_2m",
        F.when(F.col("temperature_2m").between(*TEMP_RANGE), F.col("temperature_2m")),
    ).withColumn(
        "relative_humidity_2m",
        F.when(
            F.col("relative_humidity_2m").between(*HUMIDITY_RANGE),
            F.col("relative_humidity_2m"),
        ),
    )
    deduped = dedupe_latest(bounded, keys=["city", "observed_at"], order_col="_ingested_at")
    return deduped.withColumn("observed_date", F.to_date("observed_at"))


def gold_daily_weather(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("city", "observed_date")
        .agg(
            F.round(F.min("temperature_2m"), 2).alias("temp_min_c"),
            F.round(F.max("temperature_2m"), 2).alias("temp_max_c"),
            F.round(F.avg("temperature_2m"), 2).alias("temp_avg_c"),
            F.round(F.sum("precipitation"), 2).alias("precip_total_mm"),
            F.round(F.avg("relative_humidity_2m"), 2).alias("humidity_avg_pct"),
            F.round(F.max("wind_speed_10m"), 2).alias("wind_max_kmh"),
            F.count("*").alias("hours_observed"),
        )
    )
