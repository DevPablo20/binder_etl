"""Smoke test: bronze do instagram_organic lê o raw com schema explícito e escreve Delta."""
from pyspark.sql.functions import count
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta, read_raw_parquet
from src.transformers.instagram_organic.bronze import InstagramOrganicBronzeTransformer
from src.transformers.instagram_organic.tables import INSTAGRAM_STREAMS


def _raw_available(spark, stream) -> bool:
    try:
        df = read_raw_parquet(spark, stream.raw_path, schema=stream.schema)
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


def test_bronze_instagram_organic_respects_stream_keys(spark):
    available = [stream for stream in INSTAGRAM_STREAMS if _raw_available(spark, stream)]
    if not available:
        return

    InstagramOrganicBronzeTransformer(spark).run()

    for stream in available:
        df = read_delta(spark, "bronze", stream.bronze_table_name)
        assert not df.isEmpty()
        for column in ("business_account_id", "snapshot_date", "_airbyte_raw_id"):
            assert column in df.columns
        duplicated = (
            df.groupBy(*stream.dedupe_columns).agg(count("*").alias("n")).filter("n > 1")
        )
        assert duplicated.isEmpty(), f"{stream.name}: chave de dedupe repetida"
