"""Smoke test: bronze do facebook_organic lê o raw com schema explícito e escreve Delta."""
from pyspark.sql.functions import count
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta, read_raw_parquet
from src.transformers.facebook_organic.bronze import FacebookOrganicBronzeTransformer
from src.transformers.facebook_organic.tables import FACEBOOK_STREAMS


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


def test_bronze_facebook_organic_writes_one_snapshot_per_day(spark):
    available = [stream for stream in FACEBOOK_STREAMS if _raw_available(spark, stream)]
    if not available:
        return

    FacebookOrganicBronzeTransformer(spark).run()

    for stream in available:
        df = read_delta(spark, "bronze", stream.bronze_table_name)
        assert not df.isEmpty()
        for column in ("page_id", "snapshot_date", "_airbyte_raw_id", "_airbyte_extracted_at"):
            assert column in df.columns

        duplicated = (
            df.groupBy(*stream.dedupe_columns).agg(count("*").alias("n")).filter("n > 1")
        )
        assert duplicated.isEmpty(), f"{stream.name}: chave de dedupe repetida"
        assert df.filter("id IS NULL OR page_id = ''").isEmpty()
