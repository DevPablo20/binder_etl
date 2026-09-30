"""Smoke test: gold do instagram_organic lê o silver Delta e escreve os três fatos."""
from pyspark.sql.functions import count
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta
from src.transformers.instagram_organic.gold import InstagramOrganicGoldTransformer
from src.transformers.instagram_organic.tables import GOLD_FACTS

GRAIN = {
    "media_daily_metrics": ("media_id", "snapshot_date"),
    "account_daily_metrics": ("business_account_id", "metric_date"),
    "story_metrics": ("story_id",),
}


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


def test_gold_instagram_organic_writes_facts_at_their_grain(spark):
    facts = [
        fact
        for fact in GOLD_FACTS
        if all(_silver_available(spark, path) for path in fact.silver_source_paths)
    ]
    if not facts:
        return

    InstagramOrganicGoldTransformer(spark).run()

    for fact in facts:
        df = read_delta(spark, "gold", fact.gold_table_name)
        assert not df.isEmpty()
        grain = GRAIN[fact.name]
        duplicated = df.groupBy(*grain).agg(count("*").alias("n")).filter("n > 1")
        assert duplicated.isEmpty(), f"{fact.name} fora do grão {grain}"
