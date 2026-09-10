# Tech Stack

Local medallion ETL: Airbyte extracts to MinIO, Spark transforms, Airflow orchestrates, FastAPI serves catalog for backend Bridge discovery. See [project-structure.mdc](project-structure.md) for layout.

## Components

| Component | Version / Choice | Role |
|-----------|------------------|------|
| Python | **3.11** | Local dev + Airflow container |
| Java | **17** | Required by Spark |
| Extract | **Airbyte** via `abctl` | External to compose; docs in `airbyte/README.md` |
| Storage | **MinIO** | Buckets: `raw`, `bronze`, `silver`, `gold` |
| Transform | **PySpark 3.5+** / **Delta Lake 3.x** | S3A config pointing at MinIO |
| Orchestration | **Airflow 2.10.x** LocalExecutor | One DAG per platform, `@daily` schedule |
| Catalog API | **FastAPI** + uvicorn | Read-only catalog from silver; async lifespan inits Spark |
| Backend Postgres | **PostgreSQL 15** | Bridge metadata (separate stack; backend owns) |
| Dev infra | **Docker Compose** | MinIO, Postgres (Airflow meta), Airflow, Catalog API |
| Tests | **pytest** | Smoke tests for Spark I/O and pipeline CLI |

## Two Postgres Instances

| Postgres | Compose file | Purpose |
|----------|--------------|---------|
| ETL `postgres` | `binder_etl` docker-compose | Airflow metadata only |
| Backend `postgres_local` | `binder_app_backend` compose | Bridge (backend SSOT) |

ETL does **not** write to backend Postgres for catalog — backend pulls catalog over HTTP.

## Docker Compose Services (binder_etl)

| Service | Default port | Role |
|---------|--------------|------|
| `minio` | 9000 / 9001 | Medallion object storage |
| `minio-init` | — | Creates raw/bronze/silver/gold buckets |
| `postgres` | internal | Airflow metadata DB |
| `airflow-init` | — | DB migrate + admin user |
| `airflow-webserver` | 8081 | Airflow UI |
| `airflow-scheduler` | — | DAG execution |
| `catalog-api` | 8002 | FastAPI lake identities (Spark lifespan; reads silver) |

**Not in compose:** Airbyte (`abctl` on host), backend Postgres (separate stack). Optional `spark-dev` is profile-gated (`--profile dev`).

## Environment Variables

Configure via `.env` (copy from `.env.example`):

| Variable | Purpose |
|----------|---------|
| `MINIO_ENDPOINT` | `http://localhost:9000` (host) or `http://minio:9000` (containers) |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | MinIO credentials |
| `AIRFLOW_FERNET_KEY` | Encrypt Airflow connections |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Airflow UI login |
| `ETL_STRICT` | `true` = fail on missing/empty sources; default warns and skips |
| `CATALOG_API_PORT` | Host port for catalog FastAPI (default `8002`; avoid `8000` — Airbyte abctl) |

## Practices

### Extract (Airbyte)

- Keep Airbyte **outside** docker-compose — run `abctl` on the host.
- Land data to `raw/airbyte/{platform}/{stream}/` as Parquet.
- MVP: manual sync; DAG `sync_raw` is a placeholder until `AirbyteTriggerSyncOperator` is wired.

### Transform (Spark)

- Pipeline CLI: `python -m src.pipelines.run {bronze|silver|gold} {platform}` (or `medallion`).
- Preserve Airbyte lineage: `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`.
- Dedupe in bronze by natural keys from each platform's `tables.py`.

### Catalog API (FastAPI)

- Compose service `catalog-api` (default stack) serves on port `8002`.
- App under `src/api/`; async lifespan initializes `SparkSession` before accepting traffic.
- Reads silver Delta from MinIO; returns hierarchy rows for Bridge discovery.
- Spark/MinIO work is sync — run off the event loop.
- Latency OK (config / new campaigns, not dashboard hot path).
- Optional host debug: `uvicorn src.api.main:app --reload --port ${CATALOG_API_PORT:-8002}`.

### Orchestration (Airflow)

- Invoke Spark via `BashOperator` calling the pipeline CLI.
- DAG flow: `sync_raw → bronze_{platform} → silver_{platform} → gold_{platform}`.
- DAGs paused at creation; schedule `@daily`.

### Dependencies

- Split requirements: `requirements/spark.txt`, `requirements/airflow.txt`.
- Local Spark runs need Java 17: `pip install -r requirements/spark.txt`.
- Catalog API image reuses `infra/spark-dev/Dockerfile` (Java 17 + spark.txt including FastAPI/uvicorn).

## Quick Reference

```bash
cp .env.example .env
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements/spark.txt

# Manual pipeline runs (host)
MINIO_ENDPOINT=http://localhost:9000 python -m src.pipelines.run bronze tiktok
MINIO_ENDPOINT=http://localhost:9000 python -m src.pipelines.run silver tiktok
MINIO_ENDPOINT=http://localhost:9000 python -m src.pipelines.run gold tiktok

# Catalog API (Compose service on :8002; host reload optional for debug)
# uvicorn src.api.main:app --reload --port ${CATALOG_API_PORT:-8002}

pytest
```

See also: [domain-architecture.mdc](architecture.md).
