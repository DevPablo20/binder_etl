"""Smoke test: silver do facebook_organic lê o bronze Delta e escreve as tabelas silver."""
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta
from src.transformers.facebook_organic.silver import FacebookOrganicSilverTransformer
from src.transformers.facebook_organic.tables import PLATFORM, SILVER_TABLES

EXPECTED_COLUMNS = {
    "pages": ("page_id", "page_name"),
    "page_followers_snapshot": ("page_id", "snapshot_date", "followers_count"),
    "posts": ("post_id", "page_id", "created_at", "media_type", "last_seen_date"),
    "post_insights_snapshot": ("post_id", "snapshot_date", "media_views_lifetime"),
    "page_insights_daily": ("page_id", "metric", "period", "metric_date", "value"),
}


def _bronze_available(spark, source: str) -> bool:
    try:
        df = read_delta(spark, "bronze", f"{PLATFORM}/{source}")
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


def test_silver_facebook_organic_writes_delta_tables(spark):
    tables = [
        table
        for table in SILVER_TABLES
        if all(_bronze_available(spark, source) for source in table.bronze_sources)
    ]
    if not tables:
        return

    FacebookOrganicSilverTransformer(spark).run()

    for table in tables:
        df = read_delta(spark, "silver", table.silver_table_name)
        assert not df.isEmpty()
        for column in EXPECTED_COLUMNS[table.name]:
            assert column in df.columns
        if "page_id" in df.columns:
            assert dict(df.dtypes)["page_id"] == "string"
