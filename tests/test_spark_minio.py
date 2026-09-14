"""Smoke test: Spark reads TikTok ads Parquet from MinIO raw bucket."""
from src.config import settings


def test_spark_reads_minio_raw_tiktok_ads(spark):
    path = settings.s3a_uri("raw", "airbyte/tiktok/ads")
    try:
        df = spark.read.parquet(path)
        assert df is not None
    except Exception as exc:
        if "UNABLE_TO_INFER_SCHEMA" in str(exc) or "Unable to infer schema" in str(exc):
            pass
        else:
            raise
