# Databricks notebook source
# MAGIC %md
# MAGIC # One-time setup: catalog, schemas, volumes
# MAGIC Run once per workspace. Idempotent.

# COMMAND ----------
dbutils.widgets.text("catalog", "lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
for schema in ["bronze", "silver", "gold", "landing", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# UC volumes: raw file fallback zone + streaming drop zone + checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.events")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints")
print("Catalog ready:", catalog)

# COMMAND ----------
# MAGIC %md
# MAGIC ## S3 credentials (only if S3 landing is used)
# MAGIC Create a secret scope once from your laptop with the Databricks CLI:
# MAGIC ```
# MAGIC databricks secrets create-scope lakehouse-aws
# MAGIC databricks secrets put-secret lakehouse-aws access_key_id
# MAGIC databricks secrets put-secret lakehouse-aws secret_access_key
# MAGIC ```
# MAGIC Notebooks read them via dbutils.secrets.get — keys never appear in code or git.
