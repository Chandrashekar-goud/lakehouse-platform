# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: crimes — cleanse, dedupe on case_number, SCD2 IUCR dimension

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F

from lakehouse.quality.expectations import assert_unique, expect_not_null, run_suite
from lakehouse.transforms.crimes import scd2_upsert_iucr, silver_crimes

dbutils.widgets.text("catalog", "lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
bronze = spark.table(f"{catalog}.bronze.crimes")
silver = silver_crimes(bronze)

good, quarantine, result = run_suite(silver, [expect_not_null("case_number", "occurred_at")])
assert_unique(good, ["case_number"])
print(f"DQ pass rate {result.pass_rate:.2%}")

good.write.format("delta").mode("overwrite").partitionBy("occurred_date").saveAsTable(
    f"{catalog}.silver.crimes"
)
if result.quarantined_rows:
    quarantine.write.format("delta").mode("append").saveAsTable(
        f"{catalog}.ops.crimes_quarantine"
    )

# COMMAND ----------
# SCD2 dimension: IUCR code -> primary_type/description versions over time
iucr_updates = (
    good.select("iucr", "primary_type", "description")
    .filter(F.col("iucr").isNotNull())
    .dropDuplicates(["iucr"])
    .withColumn("effective_from", F.current_timestamp())
)
scd2_upsert_iucr(spark, iucr_updates, f"{catalog}.silver.dim_iucr")
print("SCD2 dim_iucr upserted")
