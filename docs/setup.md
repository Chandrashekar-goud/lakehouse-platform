# Setup guide

The repo deploys to three runtimes. Do them in this order.

## 0. Prerequisites
- Python 3.11, Docker Desktop, git
- Databricks Free Edition workspace (https://www.databricks.com/learn/free-edition)
- AWS free-tier account
- Databricks CLI: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`

## 1. Verify the two Free Edition constraints (do this first)
1. **PAT support**: workspace → Settings → Developer → Access tokens. If you can
   generate a token, the Airflow→Databricks trigger and CI/CD deploy both work.
   If not, skip `deploy.yml` secrets and rely on native job schedules + Git folders.
2. **Outbound S3**: in a notebook run
   `import boto3; boto3.client("s3", aws_access_key_id=..., aws_secret_access_key=...).list_buckets()`
   If egress is blocked, set `landing_root` to the UC volume fallback
   (`/Volumes/lakehouse/landing/raw`) and use `land_to_volume` in the DAGs.

## 2. Local
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -v        # requires Java 17 for local Spark
ruff check src tests
```

## 3. AWS
Either `cd terraform && terraform init && terraform apply -var bucket_name=<unique>`,
or by hand: create a private S3 bucket, an IAM user with the policy in
terraform/main.tf, and an access key. Record bucket + keys.

## 4. Databricks
```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks secrets create-scope lakehouse-aws
databricks secrets put-secret lakehouse-aws access_key_id
databricks secrets put-secret lakehouse-aws secret_access_key
databricks bundle deploy -t dev
```
Then in the workspace: run `00_setup_catalog`, run the `lakehouse-batch-medallion`
job once manually, unpause its schedule. Alternative to bundles: add this GitHub
repo as a Git folder (Workspace → Create → Git folder) and run notebooks from it.

## 5. Airflow (local)
```bash
cd airflow
cp .env.example .env   # fill in AWS + Databricks values
docker compose up airflow-init && docker compose up -d
```
UI: http://localhost:8080 (admin/admin). Unpause `weather_ingest` and `crimes_ingest`.

## 6. GitHub
Push to GitHub, add repo secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`,
protect `main`, and work through PRs so CI history exists to show.

## 7. Streaming demo
Run `05_streaming_events` job once, then from a Databricks notebook or web
terminal: `python scripts/produce_events.py --out /Volumes/lakehouse/landing/events --batches 10`
and re-run the streaming job (or unpause its 30-min schedule).
