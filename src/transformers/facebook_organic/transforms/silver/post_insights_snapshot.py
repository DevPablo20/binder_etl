from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    aggregate,
    coalesce,
    col,
    collect_set,
    first,
    from_json,
    get_json_object,
    lit,
    map_values,
    max as max_,
    split,
    when,
)
from pyspark.sql.types import LongType, MapType, StringType

from src.transformers.facebook_organic.tables import POST_MAP_METRICS, POST_SCALAR_METRICS

COUNT_MAP = MapType(StringType(), LongType())


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Foto diária das métricas de post: post × `snapshot_date`, uma coluna por métrica.

    As métricas chegam em formato longo (uma linha por métrica), todas com período
    `lifetime` — o total acumulado do post até o momento da foto. O período `day` vem sempre
    zerado e com `end_time` congelado, e é descartado.

    Um único `groupBy` sobre uma lista fixa de métricas (`tables.py`): métrica nova na API não
    muda o schema. As que chegam como JSON viram `MAP<STRING, BIGINT>` — map vazio `{}` quer
    dizer que a API respondeu e não houve nada (a Meta só devolve chaves com valor diferente
    de zero); map nulo quer dizer que a métrica não veio.
    """
    insights = sources["post_insights"]
    post = sources["post"]

    base = insights.filter(col("period") == "lifetime").select(
        split(col("id"), "/")[0].alias("post_id"),
        col("page_id"),
        col("snapshot_date"),
        col("name").alias("metric"),
        col("values")[0]["value"].alias("v"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
    )

    scalar_aggs = [
        max_(when(col("metric") == metric, col("v.integer"))).alias(column)
        for metric, column in POST_SCALAR_METRICS.items()
    ]
    map_aggs = [
        first(
            when(col("metric") == metric, from_json(col("v.object"), COUNT_MAP)),
            ignorenulls=True,
        ).alias(column)
        for metric, column in POST_MAP_METRICS.items()
    ]

    snapshot = base.groupBy("post_id", "page_id", "snapshot_date").agg(
        max_("_airbyte_extracted_at").alias("snapshot_at"),
        *scalar_aggs,
        *map_aggs,
        collect_set("_airbyte_raw_id").alias("_airbyte_raw_ids"),
    )

    # `post_clicks` falta em ~1/3 dos posts e, onde existe, é igual à soma do breakdown.
    snapshot = snapshot.withColumn(
        "clicks_lifetime", map_total(col("clicks_by_type_lifetime"))
    ).withColumn("reactions_total_lifetime", map_total(col("reactions_by_type_lifetime")))

    shares = post.select(
        col("id").alias("post_id"),
        col("snapshot_date"),
        shares_lifetime(col("shares"), col("is_published")).alias("shares_lifetime"),
    )

    return snapshot.join(shares, ["post_id", "snapshot_date"], "left").select(
        col("post_id").cast("string").alias("post_id"),
        col("page_id").cast("string").alias("page_id"),
        col("snapshot_date"),
        col("snapshot_at"),
        col("media_views_lifetime"),
        col("reach_lifetime"),
        col("clicks_lifetime"),
        col("clicks_by_type_lifetime"),
        col("reactions_total_lifetime"),
        col("reactions_by_type_lifetime"),
        col("shares_lifetime"),
        col("_airbyte_raw_ids"),
        col("snapshot_at").alias("_airbyte_extracted_at"),
    )


def map_total(counts):
    """Soma dos valores de um map de contagem. Map nulo continua nulo."""
    return when(
        counts.isNotNull(),
        aggregate(map_values(counts), lit(0).cast("long"), lambda acc, x: acc + x),
    )


def shares_lifetime(shares, is_published):
    """`{"count": N}`. A Meta omite o campo em post sem compartilhamento, então ausente vale 0.

    Exceção: nas fotos de antes da seleção de campos a coluna nem existia, e o valor é
    desconhecido. `is_published` nulo só acontece nesses arquivos e serve de sinal.
    """
    count = get_json_object(shares, "$.count").cast("long")
    return when(is_published.isNull(), lit(None).cast("long")).otherwise(
        coalesce(count, lit(0).cast("long"))
    )
