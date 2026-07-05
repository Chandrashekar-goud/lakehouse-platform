"""Chicago crimes transforms: cleansing, dedup, Gold aggregates, and an SCD2
dimension for IUCR crime codes (descriptions change over time)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.transforms.common import dedupe_latest

# Rough bounding box for Chicago: coordinates outside it are data errors.
LAT_RANGE = (41.6, 42.1)
LON_RANGE = (-87.95, -87.5)


def silver_crimes(bronze: DataFrame) -> DataFrame:
    cleaned = (
        bronze.filter(F.col("case_number").isNotNull() & F.col("date").isNotNull())
        .withColumn("case_number", F.upper(F.trim("case_number")))
        .withColumn("occurred_at", F.to_timestamp("date"))
        .withColumn("updated_on", F.to_timestamp("updated_on"))
        .withColumn("primary_type", F.upper(F.trim("primary_type")))
        .withColumn("arrest", F.col("arrest").cast("boolean"))
        .withColumn("domestic", F.col("domestic").cast("boolean"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
    )
    # Null out impossible coordinates instead of dropping the row: the crime
    # happened even if the geocoding failed.
    geo_fixed = cleaned.withColumn(
        "latitude", F.when(F.col("latitude").between(*LAT_RANGE), F.col("latitude"))
    ).withColumn(
        "longitude", F.when(F.col("longitude").between(*LON_RANGE), F.col("longitude"))
    )
    deduped = dedupe_latest(geo_fixed, keys=["case_number"], order_col="updated_on")
    return deduped.withColumn("occurred_date", F.to_date("occurred_at"))


def gold_crimes_daily(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("occurred_date", "primary_type")
        .agg(
            F.count("*").alias("incident_count"),
            F.sum(F.col("arrest").cast("int")).alias("arrest_count"),
            F.sum(F.col("domestic").cast("int")).alias("domestic_count"),
        )
        .withColumn(
            "arrest_rate",
            F.round(F.col("arrest_count") / F.col("incident_count"), 4),
        )
    )


def scd2_upsert_iucr(spark: SparkSession, updates: DataFrame, target_table: str) -> None:
    """SCD Type 2 merge for the IUCR code dimension.

    updates schema: iucr, primary_type, description, effective_from (timestamp)
    Target gains: effective_to (timestamp, null = open), is_current (boolean).

    Standard two-pass staging: changed rows are staged twice, once matching the
    existing row (to close it) and once with a null merge key (to insert the
    new version). One MERGE, atomic under Delta.
    """
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(target_table):
        (
            updates.withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta")
            .saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(spark, target_table)
    current = target.toDF().filter("is_current = true").alias("t")
    changed = (
        updates.alias("s")
        .join(current, on="iucr", how="inner")
        .where(
            "NOT (s.primary_type <=> t.primary_type) OR NOT (s.description <=> t.description)"
        )
        .select("s.*")
    )
    staged = (
        updates.withColumn("_merge_key", F.col("iucr"))
        .unionByName(changed.withColumn("_merge_key", F.lit(None).cast("string")))
    )
    (
        target.alias("t")
        .merge(staged.alias("s"), "t.iucr = s._merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition=(
                "NOT (s.primary_type <=> t.primary_type) "
                "OR NOT (s.description <=> t.description)"
            ),
            set={"is_current": "false", "effective_to": "s.effective_from"},
        )
        .whenNotMatchedInsert(
            values={
                "iucr": "s.iucr",
                "primary_type": "s.primary_type",
                "description": "s.description",
                "effective_from": "s.effective_from",
                "effective_to": "null",
                "is_current": "true",
            }
        )
        .execute()
    )
