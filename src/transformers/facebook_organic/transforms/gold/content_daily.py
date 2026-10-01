from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.transformers.facebook_organic.tables import GOLD_PLATFORM, GoldFactConfig
from src.transformers.organic_gold import TOTAL, conform, content_daily_pairs, series_flags

# Métrica da gold → métrica do silver `post_metrics_daily`. `likes` é a soma de todas as
# reações (a curtida é uma delas). O Facebook não entrega comentários, salvamentos, follows
# nem visitas ao perfil por post: ficam nulos.
METRIC_SOURCES: dict[str, str] = {
    "views": "media_views",
    "reach": "reach",
    "likes": "reactions_total",
    "shares": "shares",
    "clicks": "clicks",
}


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """`content_daily` do Facebook: um post num dia. Função pura.

    Cada métrica sai em par: o que aconteceu no dia (`views`) e o total até o dia
    (`views_total`). No primeiro dia fotografado de um post antigo, o do dia é nulo — o total é
    conhecido, o que aconteceu naquele dia não. Totais, não orgânico.
    """
    series = sources["post_metrics_daily"]
    df = series.select(
        col("snapshot_date").alias("date"),
        col("page_id").alias("account_id"),
        col("post_id").alias("content_id"),
        *series_flags(),
        *content_daily_pairs(METRIC_SOURCES),
    )
    return conform(df, "content_daily", GOLD_PLATFORM, metrics_scope=lit(TOTAL))
