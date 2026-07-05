"""Streaming pipeline: Auto Loader over a UC volume drop zone.

Why not Kafka: Databricks Free Edition is serverless-only, so a Kafka broker
on a laptop is unreachable from the workspace. Auto Loader gives the same
streaming semantics that matter in interviews (incremental discovery,
checkpointing, exactly-once sinks, schema evolution) against the real
lakehouse instead of a disconnected demo.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

EVENT_SCHEMA_HINTS = (
    "trip_id STRING, station_id STRING, station_name STRING, "
    "rider_type STRING, event_type STRING, event_ts TIMESTAMP"
)


def read_events_stream(spark: SparkSession, source_path: str, schema_location: str) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaHints", EVENT_SCHEMA_HINTS)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def gold_station_windows(silver_stream: DataFrame) -> DataFrame:
    """10-minute tumbling windows per station; 15-minute watermark bounds
    state and defines how late an event may arrive and still count."""
    return (
        silver_stream.withWatermark("event_ts", "15 minutes")
        .groupBy(F.window("event_ts", "10 minutes"), "station_id", "event_type")
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "station_id",
            "event_type",
            "event_count",
        )
    )
