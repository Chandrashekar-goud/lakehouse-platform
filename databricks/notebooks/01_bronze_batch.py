# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: land raw JSON batches into Delta
# MAGIC Reads from S3 (primary) or the UC volume fallback. Raw columns untouched;
# MAGIC lineage metadata added. Idempotent per batch: reruns replace the batch.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F

from lakehouse.transforms.common import add_bronze_metadata

dbutils.widgets.text("catalog", "lakehouse")
dbutils.widgets.dropdown("source", "weather", ["weather", "crimes"])
dbutils.widgets.text("landing_root", "/Volumes/lakehouse/landing/raw")
dbutils.widgets.text("ingest_date", "")  # empty = today

catalog = dbutils.widgets.get("catalog")
source = dbutils.widgets.get("source")
landing_root = dbutils.widgets.get("landing_root")
ingest_date = dbutils.widgets.get("ingest_date") or None

# COMMAND ----------
# If landing_root is an s3:// path, wire spark to the secret-scoped keys.
if landing_root.startswith("s3"):
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", dbutils.secrets.get("lakehouse-aws", "access_key_id")
    )
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", dbutils.secrets.get("lakehouse-aws", "secret_access_key")
    )

from datetime import datetime, timezone

ingest_date = ingest_date or f"{datetime.now(timezone.utc):%Y-%m-%d}"
path = f"{landing_root}/{source}/ingest_date={ingest_date}"

raw = (
    spark.read.option("multiLine", "true").json(path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
)
print(f"Read {raw.count()} raw rows from {path}")

# COMMAND ----------
batch_id = f"{source}-{ingest_date}"
bronze = add_bronze_metadata(
    raw.drop("_source_file"),
    source_system=source,
    batch_id=batch_id,
).withColumn("_source_file", F.lit(path)).withColumn("_ingest_date", F.lit(ingest_date))

target = f"{catalog}.bronze.{source}"
(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ingest_date = '{ingest_date}'")
    .option("mergeSchema", "true")  # schema evolution: new API fields append columns
    .saveAsTable(target)
)
print(f"Wrote batch {batch_id} to {target}")
