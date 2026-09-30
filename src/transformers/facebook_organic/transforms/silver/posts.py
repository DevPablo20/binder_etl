from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    from_utc_timestamp,
    get_json_object,
    lit,
    max as max_,
    to_date,
    when,
)

from src.transformers.facebook_organic.tables import SNAPSHOT_TIMEZONE

from ._common import latest_per


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Dimensão de post: a última foto de cada post publicado pela própria página.

    O `/feed` também traz posts de visitantes, posts em que a página foi marcada e posts não
    publicados. `is_published` nulo só acontece em fotos de antes da seleção do campo — ali
    não há como saber, e o post fica.

    `last_seen_date` é o último dia em que o post apareceu. Como o stream é full refresh,
    sumir da lista significa sair da página.
    """
    post = sources["post"]
    last_seen = post.groupBy("id").agg(max_("snapshot_date").alias("last_seen_date"))

    latest = latest_per(post, "id").join(last_seen, "id", "left")
    own_published = (get_json_object(col("from"), "$.id") == col("page_id")) & coalesce(
        col("is_published"), lit(True)
    )

    return latest.filter(own_published).select(
        col("id").cast("string").alias("post_id"),
        col("page_id").cast("string").alias("page_id"),
        col("created_time").alias("created_at"),
        to_date(from_utc_timestamp(col("created_time"), SNAPSHOT_TIMEZONE)).alias(
            "created_date"
        ),
        col("message"),
        col("permalink_url"),
        col("status_type"),
        media_type(col("permalink_url"), col("status_type")).alias("media_type"),
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
