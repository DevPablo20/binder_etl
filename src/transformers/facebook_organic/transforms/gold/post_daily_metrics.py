from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    datediff,
    expr,
    lag,
    lit,
    round as round_,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window

from src.transformers.facebook_organic.tables import REACTION_TYPES, GoldFactConfig

# Métricas aditivas: cada uma sai com `{m}_lifetime` (o total acumulado na foto) e
# `{m}_delta` (a variação desde a foto anterior do mesmo post).
ADDITIVE_METRICS: tuple[str, ...] = (
    "media_views",
    "reach",
    "clicks",
    "reactions_total",
    *(f"reaction_{reaction}" for reaction in REACTION_TYPES),
    "shares",
)


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """Desempenho diário dos posts: post × `snapshot_date`. Função pura, sem I/O.

    Desempenho **total** do post no Facebook: num post impulsionado, as métricas incluem a
    distribuição paga (o conector não separa orgânico de anúncio).

    A base é a foto diária do silver; `posts` e `pages` entram por `LEFT JOIN` — uma foto
    de post sem dimensão sobrevive, com os atributos nulos.

    A Meta só entrega totais acumulados (`lifetime`); a evolução diária é a diferença entre
    duas fotos consecutivas:

    - buraco na série não é interpolado — o delta cobre o intervalo inteiro, e `gap_days` e
      `hours_since_prev` dizem quanto;
    - delta negativo fica como veio — cortar em zero quebraria a conservação;
    - na primeira foto de um post, `baseline_kind` diz o que fazer: `new_post` (publicado nas
      24h anteriores à foto) tem a vida inteira como delta; `pre_existing` tem histórico
      anterior desconhecido e delta nulo.

    `reach_delta` é **alcance novo** — pessoas alcançadas pela primeira vez —, não o alcance
    do dia. E `reach` (`post_total_media_view_unique`) subestima muito os posts antigos.
    """
    snapshot = sources["post_insights_snapshot"]
    posts = sources["posts"].select(
        col("post_id"),
        col("created_at"),
        col("created_date"),
        col("status_type"),
        col("media_type"),
    )
    pages = sources["pages"].select(col("page_id"), col("page_name"))

    df = (
        snapshot.select(
            col("post_id"),
            col("page_id"),
            col("snapshot_date"),
            col("snapshot_at"),
            col("media_views_lifetime"),
            col("reach_lifetime"),
            col("clicks_lifetime"),
            col("reactions_total_lifetime"),
            *(
                reaction_count(col("reactions_by_type_lifetime"), reaction).alias(
                    f"reaction_{reaction}_lifetime"
                )
                for reaction in REACTION_TYPES
            ),
            col("shares_lifetime"),
        )
        .join(posts, "post_id", "left")
        .join(pages, "page_id", "left")
    )

    window = Window.partitionBy("post_id").orderBy("snapshot_date")
    prev_snapshot_at = lag("snapshot_at").over(window)

    df = (
        df.withColumn("prev_snapshot_date", lag("snapshot_date").over(window))
        .withColumn("gap_days", datediff(col("snapshot_date"), col("prev_snapshot_date")))
        .withColumn(
            "hours_since_prev",
            round_(
                (unix_timestamp(col("snapshot_at")) - unix_timestamp(prev_snapshot_at))
                / 3600,
                2,
            ),
        )
        .withColumn(
            "baseline_kind",
            when(col("prev_snapshot_date").isNotNull(), lit(None).cast("string"))
            .when(
                col("created_at") >= col("snapshot_at") - expr("INTERVAL 1 DAY"),
                lit("new_post"),
            )
            .otherwise(lit("pre_existing")),
        )
        .withColumn("days_since_publish", datediff(col("snapshot_date"), col("created_date")))
    )

    for metric in ADDITIVE_METRICS:
        lifetime = col(f"{metric}_lifetime")
        df = df.withColumn(
            f"{metric}_delta",
            when(col("baseline_kind") == "new_post", lifetime)
            .when(col("baseline_kind") == "pre_existing", lit(None).cast("long"))
            .otherwise(lifetime - lag(lifetime).over(window)),
        )

    return df.select(
        col("page_id"),
        col("page_name"),
        col("post_id"),
        col("snapshot_date"),
        col("snapshot_at"),
        col("prev_snapshot_date"),
        col("gap_days"),
        col("hours_since_prev"),
        col("baseline_kind"),
        col("created_at"),
        col("days_since_publish"),
        col("status_type"),
        col("media_type"),
        *(
            col(f"{metric}_{kind}")
            for metric in ADDITIVE_METRICS
            for kind in ("lifetime", "delta")
        ),
    )


def reaction_count(reactions, reaction: str):
    """Chave ausente num map presente vale 0 (a Meta só devolve tipos com valor); map nulo
    — métrica que não veio — continua nulo."""
    return when(reactions.isNotNull(), coalesce(reactions[reaction], lit(0).cast("long")))
