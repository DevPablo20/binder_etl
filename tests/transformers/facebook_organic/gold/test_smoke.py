"""Smoke test: gold do facebook_organic lê o silver Delta e escreve a sua fatia da gold orgânica."""
from pyspark.sql.functions import count
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta
from src.transformers.facebook_organic.gold import FacebookOrganicGoldTransformer
from src.transformers.facebook_organic.tables import GOLD_FACTS, GOLD_PLATFORM
from src.transformers.organic_gold import GRAIN_BY_TABLE


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


def test_gold_facebook_organic_writes_facts_at_their_grain(spark):
    facts = [
        fact
        for fact in GOLD_FACTS
        if all(_silver_available(spark, path) for path in fact.silver_source_paths)
    ]
    if not facts:
        return

    FacebookOrganicGoldTransformer(spark).run()

    for fact in facts:
        df = read_delta(spark, "gold", fact.gold_table_name).filter(
            f"platform = '{GOLD_PLATFORM}'"
        )
        assert not df.isEmpty()
        grain = GRAIN_BY_TABLE[fact.name]
        duplicated = df.groupBy(*grain).agg(count("*").alias("n")).filter("n > 1")
        assert duplicated.isEmpty(), f"{fact.name} fora do grão {grain}"
