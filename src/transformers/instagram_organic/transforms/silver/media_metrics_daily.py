from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col

from src.transformers.instagram_organic.tables import MEDIA_INSIGHT_METRICS
from src.transformers.snapshots import add_lifetime_deltas, latest_per, local_date

# Métricas aditivas: cada uma sai com `{m}_lifetime` (o total acumulado na foto) e `{m}_delta`
# (a variação desde a foto anterior da mesma mídia).
ADDITIVE_METRICS: tuple[str, ...] = ("likes", "comments", *MEDIA_INSIGHT_METRICS.values())


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Série diária das métricas de mídia: mídia × `snapshot_date`. Todas orgânicas.

    Cada foto diária vira uma linha com o total acumulado (`{m}_lifetime`) e a variação desde a
    foto anterior (`{m}_delta`), com as regras de `src/transformers/snapshots.py`. As colunas de
    controle (`baseline_kind`, `gap_days`, `hours_since_prev`) ficam aqui, para auditoria.

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

    # Data de publicação para o `baseline_kind`: a da última foto da mídia.
    published = latest_per(sources["media"], "id").select(
        col("id"),
        col("timestamp").alias("created_at"),
        local_date(col("timestamp")).alias("created_date"),
    )

    joined = insights.join(counts, ["id", "snapshot_date"], "left").join(published, "id", "left")

    series = joined.select(
        col("id").cast("string").alias("media_id"),
        col("business_account_id").cast("string").alias("business_account_id"),
        col("snapshot_date"),
        col("_airbyte_extracted_at").alias("snapshot_at"),
        col("created_at"),
        col("created_date"),
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

    return add_lifetime_deltas(series, "media_id", ADDITIVE_METRICS)
