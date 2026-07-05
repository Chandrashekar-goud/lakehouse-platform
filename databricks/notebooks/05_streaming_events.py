# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming: Auto Loader -> Bronze -> windowed Gold
# MAGIC Run scripts/produce_events.py (or the cell at the bottom) to drop
# MAGIC micro-batches into the events volume, then watch tables update.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../../src"))

from lakehouse.transforms.streaming import gold_station_windows, read_events_stream

dbutils.widgets.text("catalog", "lakehouse")
catalog = dbutils.widgets.get("catalog")

events_path = f"/Volumes/{catalog}/landing/events"
ckpt_root = f"/Volumes/{catalog}/ops/checkpoints"

# COMMAND ----------
bronze_stream = read_events_stream(spark, events_path, f"{ckpt_root}/events_schema")
(
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", f"{ckpt_root}/events_bronze")
    .trigger(availableNow=True)  # serverless-friendly: drain and stop
    .toTable(f"{catalog}.bronze.events")
).awaitTermination()

# COMMAND ----------
silver_stream = spark.readStream.table(f"{catalog}.bronze.events").filter(
    "trip_id IS NOT NULL AND event_ts IS NOT NULL"
)
(
    gold_station_windows(silver_stream)
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{ckpt_root}/station_windows")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.gold.station_windows")
).awaitTermination()
print("Streaming pass complete")

# COMMAND ----------
# Optional: generate sample events directly from the notebook
# from scripts_produce import ...  # or run scripts/produce_events.py locally
