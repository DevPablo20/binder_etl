"""Smoke test: TikTok bronze transformer reads raw Parquet and writes Delta to MinIO."""
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta, read_raw_parquet
from src.transformers.tiktok.bronze import TikTokBronzeTransformer
from src.transformers.tiktok.tables import TIKTOK_STREAMS


def _raw_available(spark, raw_path: str) -> bool:
    try:
        df = read_raw_parquet(spark, raw_path)
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


def test_bronze_tiktok_writes_delta_tables(spark):
    if not any(_raw_available(spark, stream.raw_path) for stream in TIKTOK_STREAMS):
        return

    TikTokBronzeTransformer(spark).run()

    for stream in TIKTOK_STREAMS:
        if not _raw_available(spark, stream.raw_path):
            continue
        df = read_delta(spark, "bronze", stream.bronze_table_name)
        assert df is not None
        assert not df.isEmpty()
