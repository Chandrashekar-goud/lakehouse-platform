# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: weather — cast, bound-check, dedupe, quality gate

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../../src"))

from lakehouse.quality.expectations import (
    assert_unique,
    expect_between,
    expect_not_null,
    run_suite,
)
from lakehouse.transforms.weather import silver_weather

dbutils.widgets.text("catalog", "lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
bronze = spark.table(f"{catalog}.bronze.weather")
silver = silver_weather(bronze)

checks = [
    expect_not_null("city", "observed_at"),
    expect_between("temperature_2m", -60, 60),
    expect_between("relative_humidity_2m", 0, 100),
]
good, quarantine, result = run_suite(silver, checks)
print(f"DQ pass rate {result.pass_rate:.2%} | quarantined {result.quarantined_rows}")
print(result.failures_by_check)

# COMMAND ----------
assert_unique(good, ["city", "observed_at"])

good.write.format("delta").mode("overwrite").partitionBy("observed_date").saveAsTable(
    f"{catalog}.silver.weather"
)
if result.quarantined_rows:
    quarantine.write.format("delta").mode("append").saveAsTable(
        f"{catalog}.ops.weather_quarantine"
    )
if result.pass_rate < 0.95:
    raise ValueError(f"DQ pass rate {result.pass_rate:.2%} below 95% threshold; failing run")
