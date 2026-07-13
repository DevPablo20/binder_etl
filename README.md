# binder_etl

Medallion ETL for Binder: Airbyte extracts marketing platform data into MinIO, Spark transforms through bronze/silver/gold layers, and a FastAPI catalog serves lake identities for backend Bridge discovery.

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker + Compose | recent | MinIO, Airflow, local infra |
| Python | 3.11 | Local Spark pipeline runs and tests |
| Java | 17 | Required by PySpark |

Airbyte runs **outside** docker-compose via `abctl` on the host. See [airbyte/README.md](airbyte/README.md).

## Quick start

### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your MinIO credentials, Airflow admin password, and Fernet key.

Generate an Airflow Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Set `AIRFLOW_UID` to your host user id so Airflow volumes are writable:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Start Docker services

From the repo root:

```bash
docker compose up --build -d
```

This starts:

| Service | URL / port | Role |
|---------|------------|------|
| MinIO API | http://localhost:9000 | Object storage (raw → gold buckets) |
| MinIO Console | http://localhost:9001 | Web UI |
| Airflow | http://localhost:8081 | Orchestration (DAGs paused by default) |
| Catalog API | http://localhost:8002 | FastAPI lake identities for Bridge discovery |

Check status:

```bash
docker compose ps
```

### Spark Dev (optional)

For exploring raw MinIO data and prototyping transformers before promoting them into `src/transformers/`, start the profile-gated Spark container:

```bash
docker compose --profile dev up -d --build spark-dev
```

Write scratch scripts under `dev/sandbox/` (gitignored) and run them inside the container:

```bash
docker compose exec spark-dev python dev/examples/inspect_raw_stream.py airbyte/tiktok/ads
docker compose exec spark-dev python dev/sandbox/checkForGoogle.py
```

See [dev/README.md](dev/README.md) for the full inspect → promote workflow.

### 3. Python environment (host pipeline runs)

Spark jobs can run on the host against MinIO at `http://localhost:9000`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements/spark.txt
```

Ensure Java 17 is on your `PATH` (`java -version`).

### 4. Install Airbyte (extract)

Follow [airbyte/README.md](airbyte/README.md) to install `abctl`, render secrets from `.env`, and configure the TikTok → MinIO connection.

MVP: trigger syncs manually in the Airbyte UI. The Airflow `sync_raw` task is a placeholder.

### 5. Run the pipeline

From the repo root with `.venv` active:

```bash
export MINIO_ENDPOINT=http://localhost:9000

# All lake layers in one command (bronze → silver → gold)
python -m src.pipelines.run medallion tiktok
```

Or run layers individually:

```bash
python -m src.pipelines.run bronze tiktok
python -m src.pipelines.run silver tiktok
python -m src.pipelines.run gold tiktok
```

The Airflow `tiktok_daily` DAG runs the same medallion chain (`sync_raw → bronze → silver → gold`).

Enable the `tiktok_daily` DAG in the Airflow UI to orchestrate the medallion chain on a schedule.

### 6. Catalog API

The `catalog-api` service starts with `docker compose up` and stays available for backend Bridge discovery on port **8002** (avoids Airbyte `abctl` on 8000). After silver tables exist in MinIO:

```bash
curl http://localhost:8002/catalog/tiktok
curl 'http://localhost:8002/catalog/tiktok?object_type=campaign'
```

Override the host port with `CATALOG_API_PORT` in `.env` if needed.

For local API debugging with reload (optional; Compose service is the default):

```bash
export MINIO_ENDPOINT=http://localhost:9000
uvicorn src.api.main:app --reload --port ${CATALOG_API_PORT:-8002}
```

### 7. Tests

```bash
pytest
```

## Environment variables

Key variables (full list in `.env.example`):

| Variable | Purpose |
|----------|---------|
| `MINIO_ENDPOINT` | `http://localhost:9000` (host) or `http://minio:9000` (containers) |
| `ETL_STRICT` | `true` = fail on missing sources; default warns and skips |
| `CATALOG_API_PORT` | Host port for catalog FastAPI (default `8002`) |

## Project layout

```
binder_etl/
├── dags/                  # Airflow DAGs (e.g. tiktok_daily)
├── src/
│   ├── api/               # FastAPI catalog (backend pulls identities)
│   ├── pipelines/run.py   # CLI: run {bronze|silver|gold} {platform}
│   ├── transformers/      # Per-platform medallion + catalog transforms
│   └── io/                # MinIO read/write
├── dev/                   # Spark sandbox for transformer exploration (profile: dev)
├── airbyte/               # abctl install docs + secrets template
├── infra/                 # Airflow + spark-dev Dockerfiles + local volume mounts
└── tests/
```

Platform metadata (streams, dedupe keys, join contract) lives in `src/transformers/{platform}/tables.py`.

## Two Postgres instances

| Instance | Where | Purpose |
|----------|-------|---------|
| ETL `postgres` | `docker-compose.yaml` | Airflow metadata only |
| Backend Postgres | `binder_app_backend` compose | Bridge metadata (backend SSOT) |

Catalog is served by ETL FastAPI; backend pulls it over HTTP (no ETL JDBC write to backend for catalog).

## Useful commands

```bash
# Rebuild Airflow image after dependency changes
docker compose up --build -d airflow-webserver airflow-scheduler

# Spark Dev sandbox (transformer exploration)
docker compose --profile dev up -d --build spark-dev
docker compose exec spark-dev python dev/examples/inspect_raw_stream.py

# View logs
docker compose logs -f airflow-scheduler

# Stop everything (includes profile services if they were started)
docker compose --profile dev down
```

Local data volumes (`infra/minio/minio_data/`, `infra/airflow/postgres_data/`) are gitignored and persist across restarts.
