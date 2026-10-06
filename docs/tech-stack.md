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
| `AIRBYTE_PORT` | Host port for the Airbyte ingress (default `8080`) |
| `AIRBYTE_INSECURE_COOKIES` | Non-empty ⇒ `abctl local install --insecure-cookies`. Required: Airbyte is served over plain HTTP, and a `Secure` session cookie is dropped by the browser, breaking login with no error. Empty only if TLS terminates in front |
| `AIRBYTE_HOST` | Ingress hostname; empty ⇒ wildcard ingress, needed to reach Airbyte by IP over the VPN. Setting it makes access by IP return 404 |
| `AIRBYTE_API_URL` | Public API base URL the DAGs use to trigger syncs and follow jobs. **Not** `localhost`: inside the Airflow container that is the container itself, and `host.docker.internal` does not resolve (no `extra_hosts`, and Airbyte lives in abctl's kind). Use the host IP |
| `AIRBYTE_CLIENT_ID` / `AIRBYTE_CLIENT_SECRET` | Public API credentials (Airbyte application). Passed to the Airflow containers through compose — `settings.py` loads `.env` from the repo root, which is not mounted there |
| `ETL_STRICT` | `true` = fail on missing/empty sources; default warns and skips. Applies to the CLI, the tests and the catalog API |
| `ETL_STRICT_AIRFLOW` | Same switch for the Airflow containers, defaulting to `true`: orchestration must not degrade silently. Separate variable so raising it in the DAGs does not also change CLI behaviour |
| `SPARK_DRIVER_MEMORY` | Driver heap (default `4g`). Spark's own default is 1 GiB, and the medallion runs entirely in the driver (`local[*]`) |
| `CATALOG_API_PORT` | Host port for catalog FastAPI (default `8002`; avoid `AIRBYTE_PORT` — `8080` here — and abctl's default `8000`) |

## Practices

### Extract (Airbyte)

- Keep Airbyte **outside** docker-compose — run `abctl` on the host.
- Always install with the flags built from `.env` (`--insecure-cookies`, `--host`) — see [airbyte/README.md](../airbyte/README.md#access-over-vpn---insecure-cookies). Re-running the install is a `helm upgrade` and keeps connections and sync state; `--persisted` on `uninstall` destroys them.
- Land data to `raw/airbyte/{platform}/{stream}/` as Parquet.
- The organic DAGs trigger their own sync through the public API (`src/airbyte/client.py`) and
  wait for the job — no provider, stdlib only. The paid platforms still rely on the Airbyte
  cron, with `sync_raw` as a placeholder.
- `GET /jobs` sorts **ascending**: without `orderBy=createdAt|DESC`, `limit=1` returns the
  connection's oldest job.

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
- Organic DAG flow: `trigger_sync → wait_for_sync → bronze_{platform} → silver_{platform} →
  gold_{platform}`, one DAG per extraction connection. Shared tasks in `dags/organic_tasks.py`.
- The wait is a sensor in `reschedule` mode, not a blocking poll: the Instagram sync takes
  over twenty minutes and would hold a LocalExecutor slot the whole time.
- Pools: `airbyte_sync` (2 slots) and `spark_medallion` (1 slot), created by `airflow-init`.
- DAGs paused at creation; organic schedules are cron in `America/Sao_Paulo`, from a tz-aware
  `start_date`.

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
