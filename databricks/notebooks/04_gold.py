# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: business-ready aggregates for dashboards

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../../src"))

from lakehouse.transforms.crimes import gold_crimes_daily
from lakehouse.transforms.weather import gold_daily_weather

dbutils.widgets.text("catalog", "lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
gold_daily_weather(spark.table(f"{catalog}.silver.weather")).write.format("delta").mode(
    "overwrite"
).saveAsTable(f"{catalog}.gold.weather_daily")

gold_crimes_daily(spark.table(f"{catalog}.silver.crimes")).write.format("delta").mode(
    "overwrite"
).saveAsTable(f"{catalog}.gold.crimes_daily")

# COMMAND ----------
# Cross-domain mart: do weather conditions correlate with incident volume?
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.crimes_weather_daily AS
SELECT
  c.occurred_date,
  SUM(c.incident_count) AS incidents,
  ANY_VALUE(w.temp_avg_c) AS temp_avg_c,
  ANY_VALUE(w.precip_total_mm) AS precip_total_mm
FROM {catalog}.gold.crimes_daily c
LEFT JOIN {catalog}.gold.weather_daily w
  ON w.city = 'chicago' AND w.observed_date = c.occurred_date
GROUP BY c.occurred_date
""")
print("Gold tables written")
