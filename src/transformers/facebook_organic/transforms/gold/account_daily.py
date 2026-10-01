from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first, lit, max as max_, when

from src.transformers.facebook_organic.tables import (
    GOLD_PLATFORM,
    PAGE_FAN_ADDS_METRIC,
    GoldFactConfig,
)
from src.transformers.organic_gold import TOTAL, conform

# Métrica diária da página (período `day`) → coluna da gold.
PAGE_METRICS: dict[str, str] = {
    "page_media_view": "views",
    "page_total_media_view_unique": "reach",
    "page_post_engagements": "post_engagements",
}


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """`account_daily` do Facebook: a página num dia. Função pura.

    **Total da página, dominado por anúncios** — inclui conteúdo pago e conteúdo fora do feed.
    `new_followers_organic` (curtidas novas não pagas) é a única separação orgânico × pago que
    o conector entrega. Os dados chegam com uns 2 dias de atraso. `followers_total` vem da foto
    do mesmo dia.
    """
    daily = sources["page_insights_daily"].filter(col("period") == "day")
    followers = sources["page_followers_snapshot"].select(
        col("page_id"),
        col("snapshot_date").alias("metric_date"),
        col("followers_count").alias("followers_total"),
    )
    pages = sources["pages"].select(col("page_id"), col("page_name"))

    metrics = daily.groupBy("page_id", "metric_date").agg(
        *(
            max_(when(col("metric") == metric, col("value"))).alias(column)
            for metric, column in PAGE_METRICS.items()
        ),
        first(
            when(col("metric") == PAGE_FAN_ADDS_METRIC, col("value_breakdown")),
            ignorenulls=True,
        ).alias("_fan_adds"),
    )

    df = (
        metrics.join(followers, ["page_id", "metric_date"], "left")
        .join(pages, "page_id", "left")
        .select(
            col("metric_date").alias("date"),
            col("page_id").alias("account_id"),
            col("page_name").alias("account_name"),
            lit(False).alias("is_partial"),
            *(col(column) for column in PAGE_METRICS.values()),
            col("_fan_adds")["total"].alias("new_followers"),
            col("_fan_adds")["unpaid"].alias("new_followers_organic"),
            col("followers_total"),
        )
    )
    return conform(df, "account_daily", GOLD_PLATFORM, metrics_scope=lit(TOTAL))
