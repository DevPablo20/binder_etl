# Spark Dev Sandbox

Development-only helpers for exploring MinIO lake data and prototyping transformers before promoting them into `src/transformers/`.

## Prerequisites

- MinIO running (`docker compose up -d minio minio-init`)
- Raw data landed by Airbyte under `raw/airbyte/{platform}/{stream}/`

## Start the service

From `binder_etl/`:

```bash
docker compose --profile dev up -d --build spark-dev
```

The container stays up so you can `exec` scripts repeatedly. Ivy/Maven JARs are cached in the `spark-ivy-cache` volume (first Spark session downloads packages).

## Explore raw data

Copy the example or write a scratch script under `sandbox/`:

```bash
cp dev/examples/inspect_raw_stream.py dev/sandbox/checkForGoogle.py
# edit the path / inspection logic
docker compose exec spark-dev python dev/sandbox/checkForGoogle.py
```

Or run the checked-in example:

```bash
docker compose exec spark-dev python dev/examples/inspect_raw_stream.py airbyte/tiktok/ads
```

Typical imports:

```python
from src.spark_session import get_spark_session
from src.io.reader import read_raw_parquet, read_delta

spark = get_spark_session("dev-inspect")
df = read_raw_parquet(spark, "airbyte/tiktok/ads")
df.printSchema()
df.dtypes
df.show(5, truncate=False)
spark.stop()
```

You can also read Delta layers while iterating:

```python
bronze = read_delta(spark, "bronze", "tiktok/ads")
silver = read_delta(spark, "silver", "tiktok/ads")
gold = read_delta(spark, "gold", "tiktok/ads_daily_metrics")
```

## Promote to a transformer

Once schema and transforms look right:

1. Create `src/transformers/{platform}/` following the TikTok layout (`tables.py`, `bronze.py`, `silver.py`, `gold.py`, `transforms/`).
2. Register the platform in `src/transformers/__init__.py` and the pipeline `PLATFORMS` list.
3. Validate with `python -m src.pipelines.run bronze|silver|gold {platform}` (host or Airflow).

Scratch files under `sandbox/` are gitignored — only promote finished code into `src/transformers/`.

## Stop

```bash
docker compose --profile dev stop spark-dev
```
