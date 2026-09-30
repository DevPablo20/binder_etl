from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, lower, max as max_, when

from src.transformers.snapshots import latest_per, local_date


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Dimensão de mídia: a última foto de cada post do feed ou reel.

    `last_seen_date` é o último dia em que a mídia apareceu. Como o stream é full refresh,
    sumir da lista significa sair da conta (ou passar das 10 mil mais recentes que a API
    devolve).
    """
    media = sources["media"]
    last_seen = media.groupBy("id").agg(max_("snapshot_date").alias("last_seen_date"))

    return latest_per(media, "id").join(last_seen, "id", "left").select(
        col("id").cast("string").alias("media_id"),
        col("business_account_id").cast("string").alias("business_account_id"),
        col("username"),
        col("timestamp").alias("created_at"),
        local_date(col("timestamp")).alias("created_date"),
        col("media_type"),
        col("media_product_type"),
        media_format(col("media_type"), col("media_product_type")).alias("format"),
        col("caption"),
        col("permalink"),
        col("is_comment_enabled"),
        col("thumbnail_url"),
        col("last_seen_date"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )


def media_format(media_type, media_product_type):
    """`reel`, `carousel`, `image` ou `video` (vídeo de feed antigo, anterior aos reels)."""
    return (
        when(media_product_type == "REELS", lit("reel"))
        .when(media_type == "CAROUSEL_ALBUM", lit("carousel"))
        .when(media_type == "IMAGE", lit("image"))
        .when(media_type == "VIDEO", lit("video"))
        .otherwise(lower(media_product_type))
    )
