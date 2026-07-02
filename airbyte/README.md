# Airbyte (abctl) — manual install

Airbyte runs **outside** docker-compose via `abctl` on the host. It extracts platform data into MinIO `raw/airbyte/{platform}/{stream}/`.

## Prerequisites

- Docker (for `abctl` local Kubernetes)
- MinIO running (`docker compose up -d minio minio-init`)
- `.env` copied from `.env.example` at the repo root

Generate a Fernet key for Airflow if needed:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# paste result into AIRFLOW_FERNET_KEY in .env
```

## 1. Install abctl

On Linux:

```bash
curl -LsfS https://get.airbyte.com | bash -
```

Confirm:

```bash
abctl version
```

## 2. Generate secret from .env

From the **repo root** (`binder_etl`), load `.env` and render `airbyte/secrets.yaml`:

```bash
set -a && . ./.env && set +a \
  && envsubst '$AIRBYTE_ADMIN_PASSWORD,$AIRBYTE_CLIENT_ID,$AIRBYTE_CLIENT_SECRET' \
    < airbyte/secrets.yaml > /tmp/airbyte-secrets.yaml
```

The rendered file stays in `/tmp` and is not committed.

## 3. Install Airbyte

With `.env` loaded (or in the same shell session as step 2):

```bash
abctl local install --secret /tmp/airbyte-secrets.yaml --port ${AIRBYTE_PORT:-8080}
```

Installation may take several minutes. Airbyte UI: **http://localhost:** + `AIRBYTE_PORT` (default `8080`). Use the email from first-time setup and `AIRBYTE_ADMIN_PASSWORD` from `.env`.

### Quick command summary

```bash
curl -LsfS https://get.airbyte.com | bash -
set -a && . ./.env && set +a \
  && envsubst '$AIRBYTE_ADMIN_PASSWORD,$AIRBYTE_CLIENT_ID,$AIRBYTE_CLIENT_SECRET' \
    < airbyte/secrets.yaml > /tmp/airbyte-secrets.yaml
abctl local install --secret /tmp/airbyte-secrets.yaml --port ${AIRBYTE_PORT:-8080}
```

## Useful abctl commands

| Command | Description |
|---------|-------------|
| `abctl local status` | Cluster and Airbyte status |
| `abctl local credentials` | Current client_id, client_secret, password |
| `abctl local uninstall` | Stop and remove (data retained) |

## TikTok → MinIO connection (MVP)

Configure manually in the Airbyte UI after install.

### Source

- Connector: **TikTok Marketing**
- Credentials: TikTok app access token / advertiser access per your TikTok developer setup

### Destination

- Connector: **S3** (S3-compatible / MinIO)
- **Endpoint:** `http://host.docker.internal:9000` (or host LAN IP if `host.docker.internal` is unavailable)
- **Bucket:** `raw`
- **Path format:** `airbyte/tiktok/{stream}`
- **Format:** Parquet

### Streams to sync

| Stream | Landing path |
|--------|----------------|
| `advertisers` | `raw/airbyte/tiktok/advertisers/` |
| `campaigns` | `raw/airbyte/tiktok/campaigns/` |
| `ad_groups` | `raw/airbyte/tiktok/ad_groups/` |
| `ads` | `raw/airbyte/tiktok/ads/` |
| `ads_reports_daily` | `raw/airbyte/tiktok/ads_reports_daily/` |

### MinIO credentials

Use values from `.env`:

- Access key: `MINIO_ROOT_USER` (default `minioadmin`)
- Secret key: `MINIO_ROOT_PASSWORD` (default `minioadmin`)

MVP: run sync manually in Airbyte. The Airflow DAG `sync_raw` task is a placeholder until `AirbyteTriggerSyncOperator` is wired.
