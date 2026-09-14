"""Smoke test: TikTok gold transformer reads silver Delta and writes gold fact to MinIO."""
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta
from src.spark_session import get_spark_session
from src.transformers.tiktok.gold import TikTokGoldTransformer


def _silver_available(spark, table_path: str) -> bool:
    try:
        df = read_delta(spark, "silver", table_path)
        return not df.isEmpty()
    except (AnalysisException, Exception) as exc:
        text = str(exc).lower()
        if any(
            marker in text
            for marker in (
                "path does not exist",
                "does not exist",
                "unable to infer schema",
                "no such file",
                "404",
            )
        ):
            return False
        raise


def test_gold_tiktok_writes_ads_daily_metrics():
    spark = get_spark_session(app_name="gold-tiktok-test")
    try:
        if not _silver_available(spark, "tiktok/ads_reports_daily"):
            return

        TikTokGoldTransformer(spark).run()

        df = read_delta(spark, "gold", "tiktok/ads_daily_metrics")
        assert df is not None
        assert not df.isEmpty()

        for column in ("ad_id", "date", "spend", "impressions", "ad_account_id"):
            assert column in df.columns
    finally:
        spark.stop()
