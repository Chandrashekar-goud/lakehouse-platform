# Lakehouse platform (free-tier medallion architecture)

Production-style data engineering portfolio project: two batch pipelines
(Open-Meteo weather API, Chicago crimes) and one streaming pipeline
(Auto Loader) flowing through Bronze → Silver → Gold Delta tables on
Databricks Free Edition, with Airflow ingestion, S3 landing, a custom
data-quality framework with quarantine, SCD2 dimensions, pytest + local
Spark in CI, and Databricks Asset Bundle deployment from GitHub Actions.

## Architecture
See [docs/architecture.md](docs/architecture.md) for the diagram narrative and
the decision log (why no Kafka/K8s, orchestration split, idempotency design).

## Layout
```
airflow/            docker-compose Airflow + ingestion DAGs
src/lakehouse/      all logic: ingestion clients, transforms, quality framework
databricks/         thin notebooks + Lakeflow job definitions (bundle resources)
databricks.yml      Asset Bundle config
tests/              pytest: pure-python + local Spark/Delta unit tests
.github/workflows/  CI (lint+test) and CD (bundle deploy)
terraform/          optional S3 + IAM provisioning
docs/               setup guide, architecture, interview talking points
scripts/            streaming event producer
```

## Quickstart
```bash
pip install -e ".[dev]" && pytest -v          # local
# then follow docs/setup.md for AWS, Databricks, Airflow, GitHub
```

## Status
- [x] Batch weather pipeline (incremental, retry, quality gate)
- [x] Batch crimes pipeline (dedup, SCD2 dim, quarantine)
- [x] Streaming events (Auto Loader, watermark, windowed gold)
- [x] CI/CD via Asset Bundles
- [ ] Databricks SQL dashboard (build in-workspace, screenshot into docs/)
