from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

from src.transformers.facebook_organic.tables import GOLD_PLATFORM, GoldFactConfig
from src.transformers.organic_gold import TOTAL, conform


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """`content` do Facebook: um post por linha, só atributos. Função pura.

    As métricas do Facebook são **totais** — num post impulsionado, incluem a distribuição
    paga; o conector não separa.
    """
    posts = sources["posts"]
    pages = sources["pages"].select(col("page_id"), col("page_name"))

    df = posts.join(pages, "page_id", "left").select(
        col("post_id").alias("content_id"),
        col("page_id").alias("account_id"),
        col("page_name").alias("account_name"),
        when(col("media_type") == "photo", lit("image")).otherwise(col("media_type")).alias(
            "format"
        ),
        col("created_at").alias("published_at"),
        col("created_date").alias("published_date"),
        col("message").alias("caption"),
        col("permalink_url").alias("permalink"),
        col("last_seen_date"),
    )
    return conform(df, "content", GOLD_PLATFORM, metrics_scope=lit(TOTAL))
