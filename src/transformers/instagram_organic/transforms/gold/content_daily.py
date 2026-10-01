from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.transformers.instagram_organic.tables import GOLD_PLATFORM, GoldFactConfig
from src.transformers.organic_gold import ORGANIC, conform, content_daily_pairs, series_flags

# Métrica da gold → métrica do silver `media_metrics_daily`. O Instagram não entrega cliques
# por mídia: fica nulo. As métricas que o conector não pede para o tipo da mídia (carrossel sem
# `views`, reel sem `follows`) chegam nulas e continuam nulas.
METRIC_SOURCES: dict[str, str] = {
    "views": "views",
    "reach": "reach",
    "likes": "likes",
    "comments": "comments",
    "shares": "shares",
    "saves": "saved",
    "follows": "follows",
    "profile_visits": "profile_visits",
}


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """`content_daily` do Instagram: uma mídia num dia. Função pura.

    Cada métrica sai em par: o que aconteceu no dia (`likes`) e o total até o dia
    (`likes_total`). No primeiro dia fotografado de uma mídia antiga, o do dia é nulo. Orgânico.
    """
    series = sources["media_metrics_daily"]
    df = series.select(
        col("snapshot_date").alias("date"),
        col("business_account_id").alias("account_id"),
        col("media_id").alias("content_id"),
        *series_flags(),
        *content_daily_pairs(METRIC_SOURCES),
    )
    return conform(df, "content_daily", GOLD_PLATFORM, metrics_scope=lit(ORGANIC))
