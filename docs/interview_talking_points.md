# Interview talking points

Each of these is a 60-second story: situation → decision → tradeoff.

1. **"Why no Kafka?"** Serverless workspace can't reach a laptop broker. I
   chose Auto Loader to keep streaming semantics real (checkpoints,
   exactly-once, watermarks, schema evolution) instead of shipping a
   disconnected Kafka demo. I can whiteboard where Kafka slots in on a paid
   tier: broker → Structured Streaming source, same downstream code.
2. **"How is this idempotent?"** Batch-scoped landing keys, replaceWhere on
   ingest date in Bronze, window-function dedup in Silver, watermarks that
   advance only after success. Any day can be re-run safely end to end.
3. **"How do you handle bad data?"** Quarantine with reasons, not silent
   drops; hard failure only on pass-rate or uniqueness violations. Bad
   geocodes are nulled, not dropped — the incident still happened.
4. **"How do you test Spark code?"** Logic is pure functions in src/;
   notebooks are wiring. pytest runs local Spark + Delta in CI on every PR.
   The SCD2 merge has a dedicated test proving old versions close correctly.
5. **"Walk me through late-arriving data."** Batch: forecast revisions
   re-deliver (city, hour) rows; latest ingestion wins via window dedup.
   Streaming: 15-minute watermark bounds state; the producer deliberately
   emits ~5% late events so I can show what gets included vs dropped.
6. **"How does deployment work?"** PR → ruff + pytest → merge → Asset Bundle
   deploy from GitHub Actions with workspace host/token as repo secrets.
   Bundles version the job definitions too, so orchestration is code-reviewed.
7. **"What would change at production scale?"** Watermark file → Delta/DynamoDB;
   IAM user keys → instance profiles/Unity Catalog external locations; Airflow
   on a laptop → MWAA or Astronomer; add table constraints + expectations in
   Lakeflow Declarative Pipelines; add CloudWatch/observability on ingestion.
