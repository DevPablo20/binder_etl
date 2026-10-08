from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    get_json_object,
    lit,
    max as max_,
    when,
)

from src.transformers.snapshots import latest_per, local_date


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Dimensão de post: a última foto de cada post da própria página.

    O `/feed` também traz posts de visitantes e posts em que a página foi marcada — esses não
    são conteúdo da página, e o `from.id` os separa.

    **`is_published` é atributo, não filtro.** Antes ele entrava no mesmo `AND` do `from.id`,
    e isso juntava duas perguntas diferentes: *é nosso?* e *está visível?*. A segunda não
    justifica apagar o post — `is_hidden` e `is_expired`, que são da mesma natureza, sempre
    foram colunas. E filtrar fazia o fato e a dimensão discordarem da mesma linha de origem:
    o `post_metrics_daily` sai de `post_insights` e nunca aplicou filtro nenhum, então a
    métrica de um post despublicado sobrevivia e o atributo dele não (D7 em
    `docs/plans/organic.md`). Despublicado não quer dizer que nunca foi publicado: os dois
    casos medidos tinham milhares de reações.

    `is_published` nulo só acontece em fotos de antes da seleção do campo, onde não há como
    saber — fica nulo, sem virar `True`.

    `last_seen_date` é o último dia em que o post apareceu. Como o stream é full refresh,
    sumir da lista significa sair da página.
    """
    post = sources["post"]
    last_seen = post.groupBy("id").agg(max_("snapshot_date").alias("last_seen_date"))

    latest = latest_per(post, "id").join(last_seen, "id", "left")
    own = get_json_object(col("from"), "$.id") == col("page_id")

    return latest.filter(own).select(
        col("id").cast("string").alias("post_id"),
        col("page_id").cast("string").alias("page_id"),
        col("created_time").alias("created_at"),
        local_date(col("created_time")).alias("created_date"),
        col("message"),
        col("permalink_url"),
        col("status_type"),
        media_type(col("permalink_url"), col("status_type")).alias("media_type"),
        col("is_published"),
        col("is_hidden"),
        col("is_expired"),
        col("full_picture"),
        col("last_seen_date"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )


def media_type(permalink_url, status_type):
    """Formato do post. O `attachments` do conector chega vazio, então o formato sai do link
    (reels têm `/reel/` no permalink) e do `status_type`."""
    return (
        when(permalink_url.contains("/reel/"), lit("reel"))
        .when(status_type == "added_video", lit("video"))
        .when(status_type == "added_photos", lit("photo"))
        .when(status_type == "mobile_status_update", lit("status"))
        .otherwise(status_type)
    )
