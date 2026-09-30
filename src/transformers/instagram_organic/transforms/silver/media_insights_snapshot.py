from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col

from src.transformers.instagram_organic.tables import MEDIA_INSIGHT_METRICS


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Foto diária das métricas de mídia: mídia × `snapshot_date`. Todas orgânicas.

    As métricas do `/insights` são as que o conector pede para o tipo da mídia; a que não foi
    pedida vem nula e continua nula (carrossel não tem `views`, reel não tem `follows`).

    `likes` e `comments` vêm de `like_count` e `comments_count` do stream `media`, que os traz
    para todos os tipos — o `/insights` não os pede para carrossel. Nas fotos de antes da
    seleção desses campos, cai no valor do `/insights` (iguais em 1.881 de 1.882 mídias).

    `reels_avg_watch_time` é média, não soma: fica como veio, sem virar série de delta.
    """
    insights = sources["media_insights"]
    counts = sources["media"].select(
        col("id"),
        col("snapshot_date"),
        col("like_count"),
        col("comments_count"),
    )

    joined = insights.join(counts, ["id", "snapshot_date"], "left")

    return joined.select(
        col("id").cast("string").alias("media_id"),
        col("business_account_id").cast("string").alias("business_account_id"),
        col("snapshot_date"),
        col("_airbyte_extracted_at").alias("snapshot_at"),
        coalesce(col("like_count"), col("likes")).alias("likes_lifetime"),
        coalesce(col("comments_count"), col("comments")).alias("comments_lifetime"),
        *(
            col(source).alias(f"{column}_lifetime")
            for source, column in MEDIA_INSIGHT_METRICS.items()
        ),
        col("ig_reels_avg_watch_time").alias("reels_avg_watch_time"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
