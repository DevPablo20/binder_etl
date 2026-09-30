from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.transformers.instagram_organic.tables import MEDIA_INSIGHT_METRICS, GoldFactConfig
from src.transformers.snapshots import add_lifetime_deltas

# Métricas aditivas: cada uma sai com `{m}_lifetime` (o total acumulado na foto) e
# `{m}_delta` (a variação desde a foto anterior da mesma mídia).
ADDITIVE_METRICS: tuple[str, ...] = ("likes", "comments", *MEDIA_INSIGHT_METRICS.values())


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """Desempenho diário das mídias: mídia × `snapshot_date`. Função pura, sem I/O.

    **Desempenho orgânico.** A Meta exclui interações em anúncios de `likes`, `comments` e
    `views`, e o `reach` bateu com total − anúncios no Business Suite.

    A base é a foto diária do silver; `media` e `accounts` entram por `LEFT JOIN`. O delta segue
    as regras de `src/transformers/snapshots.py` (primeira foto, buraco não interpolado, delta
    negativo mantido). No Instagram o delta negativo é frequente e real: comentário apagado,
    descurtida, salvamento desfeito, `reach` estimado revisado para baixo.

    Métrica que o conector não pede para o tipo da mídia tem `_lifetime` e `_delta` nulos.
    """
    snapshot = sources["media_insights_snapshot"]
    media = sources["media"].select(
        col("media_id"),
        col("created_at"),
        col("created_date"),
        col("format"),
        col("media_product_type"),
        col("permalink"),
    )
    accounts = sources["accounts"].select(col("business_account_id"), col("username"))

    df = (
        snapshot.select(
            col("media_id"),
            col("business_account_id"),
            col("snapshot_date"),
            col("snapshot_at"),
            *(col(f"{metric}_lifetime") for metric in ADDITIVE_METRICS),
            col("reels_avg_watch_time"),
        )
        .join(media, "media_id", "left")
        .join(accounts, "business_account_id", "left")
    )

    df = add_lifetime_deltas(df, "media_id", ADDITIVE_METRICS)

    return df.select(
        col("business_account_id"),
        col("username"),
        col("media_id"),
        col("snapshot_date"),
        col("snapshot_at"),
        col("prev_snapshot_date"),
        col("gap_days"),
        col("hours_since_prev"),
        col("baseline_kind"),
        col("created_at"),
        col("days_since_publish"),
        col("format"),
        col("media_product_type"),
        col("permalink"),
        *(
            col(f"{metric}_{kind}")
            for metric in ADDITIVE_METRICS
            for kind in ("lifetime", "delta")
        ),
        col("reels_avg_watch_time"),
    )
