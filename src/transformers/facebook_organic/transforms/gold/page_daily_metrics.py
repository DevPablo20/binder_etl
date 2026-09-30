from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first, max as max_, when

from src.transformers.facebook_organic.tables import (
    PAGE_DAY_METRICS,
    PAGE_FAN_ADDS_METRIC,
    GoldFactConfig,
)


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """Métricas diárias da página: página × `metric_date`, só o período `day`. Função pura.

    `media_views`, `viewers`, `post_engagements` e `total_actions` são o **total da página,
    dominado por anúncios** — conteúdo pago e conteúdo fora do feed de posts entram aqui.
    Nunca some nem cruze com `post_daily_metrics`.

    `fan_adds_unpaid` é a única separação orgânico × pago que o conector entrega.

    `fan_count` e `followers_count` vêm da foto do dia (`snapshot_date = metric_date`: a foto
    que fecha o dia D traz os seguidores ao fim de D). Como a API da página chega com uns
    2 dias de atraso, os seguidores dos dias mais recentes só aparecem aqui quando as métricas
    daquele dia chegam.
    """
    daily = sources["page_insights_daily"].filter(col("period") == "day")
    followers = sources["page_followers_snapshot"].select(
        col("page_id"),
        col("snapshot_date").alias("metric_date"),
        col("fan_count"),
        col("followers_count"),
    )
    pages = sources["pages"].select(col("page_id"), col("page_name"))

    metrics = daily.groupBy("page_id", "metric_date").agg(
        *(
            max_(when(col("metric") == metric, col("value"))).alias(column)
            for metric, column in PAGE_DAY_METRICS.items()
        ),
        first(
            when(col("metric") == PAGE_FAN_ADDS_METRIC, col("value_breakdown")),
            ignorenulls=True,
        ).alias("_fan_adds"),
    )

    return (
        metrics.join(followers, ["page_id", "metric_date"], "left")
        .join(pages, "page_id", "left")
        .select(
            col("page_id"),
            col("page_name"),
            col("metric_date"),
            *(col(column) for column in PAGE_DAY_METRICS.values()),
            col("_fan_adds")["total"].alias("fan_adds_total"),
            col("_fan_adds")["paid"].alias("fan_adds_paid"),
            col("_fan_adds")["unpaid"].alias("fan_adds_unpaid"),
            col("fan_count"),
            col("followers_count"),
        )
    )
