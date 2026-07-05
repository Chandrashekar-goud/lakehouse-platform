# Architecture

Sources → Airflow (Docker, local) → S3 raw zone (or UC volume fallback) →
Databricks Free Edition: Bronze → Silver → Gold Delta tables → Databricks SQL.
Streaming: producer → UC volume → Auto Loader → Bronze → windowed Gold.
CI/CD: GitHub Actions → ruff + pytest → Databricks Asset Bundle deploy.

## Decisions and tradeoffs
- **No Kafka.** Free Edition is serverless-only; a laptop broker is unreachable
  from the workspace. Auto Loader delivers the streaming semantics that matter
  (incremental discovery, checkpoints, exactly-once, schema evolution) against
  the real lakehouse. `trigger(availableNow=True)` drains-and-stops, which is
  both serverless-friendly and cost-bounded.
- **No Kubernetes.** Single-machine project; K8s would be resume decoration.
- **Notebooks are thin; logic lives in src/.** Transformations are pure
  functions over DataFrames, unit-tested with local Spark in CI. Notebooks
  only wire widgets, tables, and I/O.
- **Orchestration split.** Airflow owns source→landing (retries, watermarks,
  pagination). Databricks Lakeflow Jobs own Bronze→Gold (task graph mirrors
  the medallion dependency). Airflow triggers the Databricks job via the Jobs
  API when a PAT exists; otherwise the native schedule owns it. Boundaries
  follow ownership: ingestion failures and lakehouse failures page different
  concerns.
- **Idempotency.** Landing keys are batch-scoped; Bronze uses
  `replaceWhere` on `_ingest_date` so reruns replace, never duplicate. Silver
  dedupes with window functions (late-arriving data). Watermarks only advance
  after a successful landing and never on empty fetches.
- **Quality gates quarantine, not drop.** Row-level failures are preserved in
  ops.*_quarantine with failure reasons; runs fail hard only below a 95% pass
  rate or on key-uniqueness violations.
- **Secrets.** AWS keys live in Databricks secret scopes and a git-ignored
  .env for Airflow. Nothing sensitive in code or git history.

## Data model
- bronze.weather / bronze.crimes / bronze.events: raw + lineage columns
  (_ingested_at, _source_system, _batch_id, _source_file, _ingest_date)
- silver.weather (partitioned by observed_date), silver.crimes (by
  occurred_date), silver.dim_iucr (SCD2: effective_from/to, is_current)
- gold.weather_daily, gold.crimes_daily, gold.crimes_weather_daily,
  gold.station_windows
