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
abctl local install \
  --secret /tmp/airbyte-secrets.yaml \
  --port "${AIRBYTE_PORT:-8080}" \
  ${AIRBYTE_INSECURE_COOKIES:+--insecure-cookies} \
  ${AIRBYTE_HOST:+--host "$AIRBYTE_HOST"}
```

Do not drop the last two lines. They are driven by `.env` and both matter for remote access —
see [Access over VPN](#access-over-vpn---insecure-cookies) before changing either.

Installation may take several minutes. Airbyte UI: `http://localhost:${AIRBYTE_PORT}` (default
**http://localhost:8080**). Use the email from first-time setup and `AIRBYTE_ADMIN_PASSWORD`
from `.env`.

### Quick command summary

```bash
curl -LsfS https://get.airbyte.com | bash -
set -a && . ./.env && set +a \
  && envsubst '$AIRBYTE_ADMIN_PASSWORD,$AIRBYTE_CLIENT_ID,$AIRBYTE_CLIENT_SECRET' \
    < airbyte/secrets.yaml > /tmp/airbyte-secrets.yaml
abctl local install \
  --secret /tmp/airbyte-secrets.yaml \
  --port "${AIRBYTE_PORT:-8080}" \
  ${AIRBYTE_INSECURE_COOKIES:+--insecure-cookies} \
  ${AIRBYTE_HOST:+--host "$AIRBYTE_HOST"}
```

## Access over VPN (`--insecure-cookies`)

Airbyte here is served over **plain HTTP** — there is no TLS in front of it. Airbyte marks its
session cookie `Secure` by default (`AB_COOKIE_SECURE=true`), and a `Secure` cookie is dropped
by the browser on an HTTP origin. The result is a login that **fails with the correct
credentials and shows no error** — you just land back on the login screen.

`--insecure-cookies` renders `global.auth.security.cookieSecureSetting: false`, which becomes
`AB_COOKIE_SECURE: "false"` in the `airbyte-abctl-airbyte-env` ConfigMap. Only the
`airbyte-abctl-server` deployment consumes it.

Driven by `AIRBYTE_INSECURE_COOKIES` in `.env` (presence-based: any non-empty value passes the
flag). Drop it **only** if you terminate TLS in front of Airbyte.

### `AIRBYTE_HOST`

Leave it **empty**. An empty value means a wildcard ingress that answers on any hostname, which
is what lets you reach Airbyte by IP over the VPN. Setting `--host` restricts the ingress to
that single name and access by IP starts returning **404**.

### Checking the live value

```bash
docker exec airbyte-abctl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf exec -n airbyte-abctl deploy/airbyte-abctl-server -- printenv AB_COOKIE_SECURE
```

Expected: `false`.

## Useful abctl commands

| Command | Description |
|---------|-------------|
| `abctl local status` | Cluster and Airbyte status |
| `abctl local credentials` | Current client_id, client_secret, password |
| `abctl local uninstall` | Remove the Helm releases; **persisted data retained** |
| `abctl local uninstall --persisted` | ⚠️ Also **deletes all persisted data** — connections, sources, destinations, sync state |

## Re-installing / changing flags without losing data

`abctl local install` is a `helm upgrade` against the existing kind cluster — it does **not**
recreate the cluster and does **not** touch the volumes. You do not need to uninstall first;
just re-run the install command with the flags you want.

Data lives in two PVs, both `reclaimPolicy: Retain`, hostPath inside the kind container
`airbyte-abctl-control-plane`:

| PV | Contents |
|----|----------|
| `airbyte-volume-db` | Postgres `db-airbyte` (user `airbyte`) — connections, sources, destinations, sync state, job history |
| `airbyte-local-pv` | workload/log storage |

Back up before any re-install:

```bash
docker exec airbyte-abctl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf exec -n airbyte-abctl airbyte-db-0 -- pg_dump -U airbyte -d db-airbyte > ~/airbyte-backup-$(date +%F).sql
```

A re-install restarts every pod, so do it with no sync running:

```bash
docker exec airbyte-abctl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf exec -n airbyte-abctl airbyte-db-0 -- psql -U airbyte -d db-airbyte -t -c "select id, scope, status from jobs where status in ('running','pending');"
```

**Never** `docker rm airbyte-abctl-control-plane` or `docker volume prune` to "reset" Airbyte —
the PVs are hostPaths inside that container and die with it.

### Known blocker: `abctl local install` fails on `pgdata` permissions

On this host every `abctl local install` (abctl v0.30.4) aborts **before** the Helm upgrade with:

```
ERROR  failed to determine if any previous psql version exists:
       error reading pgdata version file: .../airbyte-volume-db/pgdata/PG_VERSION: permission denied
```

`pgdata` is `drwx------` uid/gid `70` (the `postgres` user inside the container) while `abctl`
runs as the host user. The check is pointless here — `PG_VERSION` is `17` and the running
Postgres is 17.5, so no migration is pending — but it is fatal.

Two things to know:

- **The failure is safe.** It aborts before touching anything; pods keep their previous uptime
  and no data is modified.
- **The exit code lies if you wrap it.** `abctl` exits `1`, but `abctl ... ; echo $?` reports the
  exit code of `echo`, which makes the run look successful. Check the log for `ERROR`, and
  confirm the pods actually restarted.

Workaround — run the install as root while keeping the same `HOME` so `abctl` finds its
kubeconfig and data dir. Back up first (above) and check the output for `ERROR`: the run is
only successful if the pods actually restarted.

```bash
sudo -E env HOME="$HOME" "$(command -v abctl)" local install \
  --secret /tmp/airbyte-secrets.yaml \
  --port "${AIRBYTE_PORT:-8080}" \
  ${AIRBYTE_INSECURE_COOKIES:+--insecure-cookies} \
  ${AIRBYTE_HOST:+--host "$AIRBYTE_HOST"}
```

Afterwards, give back ownership of the files root created — **only these paths**:

```bash
sudo chown -R "$USER:$USER" ~/.airbyte/abctl/.helmcache ~/.airbyte/abctl/.helmrepo ~/.airbyte/abctl/abctl.kubeconfig
```

⚠️ Do **not** run `sudo chown -R` on `~/.airbyte` as a whole. That would also chown `pgdata` to
your user, and Postgres (running as uid 70) can then no longer open its own data directory — the
database stops starting.

### Changing one setting without a full re-install

For a single config change, patching the ConfigMap and restarting the affected deployment is
equivalent to the Helm-rendered value and never touches the database. Example for the cookie
setting:

```bash
docker exec airbyte-abctl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf patch cm airbyte-abctl-airbyte-env -n airbyte-abctl --type merge -p '{"data":{"AB_COOKIE_SECURE":"false"}}'
```

```bash
docker exec airbyte-abctl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf rollout restart deploy/airbyte-abctl-server -n airbyte-abctl
```

The patch survives pod restarts, host reboots and Docker restarts (it is in the kind cluster's
etcd), but a **successful** `abctl local install` re-renders the ConfigMap from the chart and
reverts it. That is why the flags belong in `.env` and in the documented install command above.

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
| main | `users`, `user_insights`, `user_lifetime_insights`, `media`, `media_insights` | `0 0 2 * * ?` — 02:00 America/Sao_Paulo |
| stories | `stories`, `story_insights` | `0 30 9 * * ?` — 09:30 America/Sao_Paulo |

Stories get their own connection because story metrics only exist while the story is live
(24h): once the story expires, its numbers are gone for good, so whatever the last reading
captured is all the lake will ever have.

The schedule is therefore a trade: an hourly connection read every story at ~23h of life (the
final number) and appended a full copy of the stream 24 times a day; a daily connection reads
each story once, at whatever age it happens to be when the sync fires. Hence 09:30: the
accounts publish around 11:00 and 17:30 São Paulo, so a read just before the first window
catches the previous morning's stories at ~23h of life. The evening ones are still read at
~16h — closing that too takes a second daily read in the late afternoon, not an earlier hour.
Three rules follow:

- **Never let the gap between two reads reach 24h.** A story published right after a read
  expires before the next one and is lost. Changing the schedule leaves exactly such a gap —
  the switch from hourly to daily left ~33h without a read, and whatever was published in it
  is gone.
- **Compare stories by `hours_live_at_last_read`, not by raw numbers.** The column is in
  `silver/instagram_organic/stories` and in `gold/organic/stories` for this reason: a story
  read at 9h of life has not finished accumulating.
- **Moving this cron across 06:00 São Paulo changes the `snapshot_date` label.** A reading
  before 06:00 closes the previous day (`SNAPSHOT_CUTOFF_HOURS` in
  `src/transformers/snapshots.py`); 09:30 closes the same day. Business dates in the gold come
  from `published_date`, so consumers are unaffected — but a photo-by-photo audit of stories
  spans the change.

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
