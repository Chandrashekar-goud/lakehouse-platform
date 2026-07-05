"""Shared transform helpers. Pure functions over DataFrames: no I/O, no spark
session creation, so every function is unit-testable with a local Spark."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_bronze_metadata(df: DataFrame, source_system: str, batch_id: str,
                        source_file: str | None = None) -> DataFrame:
    """Bronze contract: raw columns untouched, lineage columns added."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(source_file))
    )


def dedupe_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep the most recent row per key: standard late-arriving-data pattern."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
