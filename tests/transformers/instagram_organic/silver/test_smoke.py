"""Smoke test: silver do instagram_organic lê o bronze Delta e escreve as tabelas silver."""
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta
from src.transformers.instagram_organic.silver import InstagramOrganicSilverTransformer
from src.transformers.instagram_organic.tables import PLATFORM, SILVER_TABLES

EXPECTED_COLUMNS = {
    "accounts": ("business_account_id", "username", "page_id"),
    "account_followers_snapshot": ("business_account_id", "snapshot_date", "followers_count"),
    "account_insights_daily": ("business_account_id", "metric_date", "is_partial", "reach"),
    "follower_demographics_snapshot": ("business_account_id", "breakdown", "followers"),
    "media": ("media_id", "business_account_id", "format", "created_at"),
    "media_metrics_daily": ("media_id", "snapshot_date", "likes_lifetime", "likes_delta"),
    "stories": ("story_id", "business_account_id", "reach", "last_read_at"),
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


def test_silver_instagram_organic_writes_delta_tables(spark):
    tables = [
        table
        for table in SILVER_TABLES
        if all(_bronze_available(spark, source) for source in table.bronze_sources)
    ]
    if not tables:
        return

    InstagramOrganicSilverTransformer(spark).run()

    for table in tables:
        df = read_delta(spark, "silver", table.silver_table_name)
        assert not df.isEmpty()
        for column in EXPECTED_COLUMNS[table.name]:
            assert column in df.columns
        assert dict(df.dtypes)["business_account_id"] == "string"
