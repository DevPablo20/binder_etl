from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, when

from src.transformers.facebook_organic.tables import REACTION_TYPES, GoldFactConfig
from src.transformers.snapshots import add_lifetime_deltas

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

    df = add_lifetime_deltas(df, "post_id", ADDITIVE_METRICS)

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
