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
- **Endpoint:** `http://172.17.0.1:9000` (Docker bridge gateway — works reliably from abctl/kind on Linux). Fallbacks: `http://host.docker.internal:9000` or your host LAN IP if the gateway IP differs (`ip route | grep docker0`).
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

## Facebook Pages → MinIO connection (`facebook_organic`)

One connection **per page**, all landing under the same prefix. The official connector is
used as-is (no fork): its metric list is fixed in the connector manifest.

### Source

- Connector: **Facebook Pages** (Graph API v24.0)
- `page_id`: one page per connection; long-lived Page access token

### Destination

- Same S3/MinIO destination as above, bucket `raw`
- **Path format:** `airbyte/facebook_organic/{page_id}/{stream}` — the pipeline reads the
  `page_id` from this path

### Streams to sync

All four streams are **Full Refresh + Append** — the connector offers no incremental mode.
Each sync is a complete snapshot; the bronze keeps one snapshot per day.

| Stream | Fields to select | Never select |
|--------|------------------|--------------|
| `page` | `id`, `name`, `username`, `link`, `category`, `fan_count`, `followers_count` | `page_token` (access token); any edge (`feed`, `posts`, `photos`, …) |
| `post` | `id`, `from`, `created_time`, `message`, `permalink_url`, `status_type`, `is_published`, `is_hidden`, `is_expired`, `shares`, `full_picture` | `attachments` (always lands as `{}`), `insights` (duplicates `post_insights`), obsolete `type`/`name`/`description`/`caption`/`link`/`picture` |
| `post_insights` | `id`, `name`, `period`, `values` | `title`, `description` |
| `page_insights` | `id`, `name`, `period`, `values` | `title`, `description` |

### Schedule

Cron **01:00 America/Sao_Paulo**. The DAG `facebook_organic_daily` runs afterwards, before
06:00 São Paulo, so the snapshot closes the previous day.

## Instagram → MinIO connections (`instagram_organic`)

One access token sees **every** Instagram professional account linked to it — no per-account
connection. Every record carries `business_account_id` (and the linked Facebook `page_id`), so
the path has no account folder. The official connector is used as-is: the metrics requested per
media type are fixed in its manifest.

Two connections write to the same destination, bucket `raw`, **path format
`airbyte/instagram_organic/{stream}`**:

| Connection | Streams | Schedule (Quartz cron) |
|------------|---------|------------------------|
| main | `users`, `user_insights`, `user_lifetime_insights`, `media`, `media_insights` | `0 0 1 * * ?` — 01:00 America/Sao_Paulo |
| stories | `stories`, `story_insights` | `0 0 * * * ?` — every hour |

Stories get their own hourly connection because story metrics only exist while the story is
live (24h): the last hourly reading is the final number.

**Every stream is Append — never Overwrite.** Overwrite deletes the previous files of the
stream on each sync; the stories connection would then keep only the last hour, and a story that
expired between two DAG runs would vanish before the bronze reads it. `user_insights` is
**Incremental + Append** (cursor `date`, 1-day lookback); all others are **Full Refresh +
Append**. Leave the `Api` stream unselected — the connector still uses it internally.

| Stream | Fields to select | Never select |
|--------|------------------|--------------|
| `users` | `id`, `username`, `name`, `page_id`, `followers_count`, `follows_count`, `media_count` | `profile_picture_url` (expires), `biography`, `website`, `ig_id` |
| `user_insights` | all | — |
| `user_lifetime_insights` | all | — |
| `media` | `id`, `ig_id`, `caption`, `permalink`, `timestamp`, `media_type`, `media_product_type`, **`like_count`**, **`comments_count`**, `is_comment_enabled`, `thumbnail_url`, `username`, `business_account_id`, `page_id` | `children` (one extra API call per carousel item), `owner`, `media_url` (expires; null for licensed audio) |
| `media_insights` | all but `total_interactions` | `total_interactions` (never requested — always null) |
| `stories` | `id`, `caption`, `permalink`, `shortcode`, `timestamp`, `media_type`, `media_product_type`, `thumbnail_url`, `business_account_id`, `page_id` | `like_count`, `media_url`, `owner`, `username`, `ig_id` |
| `story_insights` | all | — |

`like_count` and `comments_count` matter: `media_insights` does not request likes and comments
for carousels, and the `media` stream returns them for every media type (organic only).
