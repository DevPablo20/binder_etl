"""Smoke test: TikTok silver transformer reads bronze Delta and writes silver Delta to MinIO."""
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta
from src.transformers.tiktok.silver import TikTokSilverTransformer
from src.transformers.tiktok.tables import TIKTOK_STREAMS


def _bronze_available(spark, table_path: str) -> bool:
    try:
        df = read_delta(spark, "bronze", table_path)
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


def test_silver_tiktok_writes_delta_tables(spark):
    if not any(
        _bronze_available(spark, stream.bronze_table_name) for stream in TIKTOK_STREAMS
    ):
        return

    TikTokSilverTransformer(spark).run()

    for stream in TIKTOK_STREAMS:
        if not _bronze_available(spark, stream.bronze_table_name):
            continue
        df = read_delta(spark, "silver", stream.silver_table_name)
        assert df is not None
        assert not df.isEmpty()

    advertisers = read_delta(spark, "silver", "tiktok/advertisers")
    if not advertisers.isEmpty():
        assert "ad_account_id" in advertisers.columns

    reports = read_delta(spark, "silver", "tiktok/ads_reports_daily")
    if not reports.isEmpty():
        assert "spend" in reports.columns
