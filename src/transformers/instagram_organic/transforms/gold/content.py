from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.transformers.instagram_organic.tables import GOLD_PLATFORM, GoldFactConfig
from src.transformers.organic_gold import ORGANIC, conform


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """`content` do Instagram: uma mídia do feed ou reel por linha, só atributos. Função pura.

    As métricas de mídia do Instagram são **orgânicas**: a Meta exclui interações em anúncios,
    e o alcance bateu com total − anúncios no Business Suite.
    """
    media = sources["media"]
    accounts = sources["accounts"].select(col("business_account_id"), col("username"))

    df = media.drop("username").join(accounts, "business_account_id", "left").select(
        col("media_id").alias("content_id"),
        col("business_account_id").alias("account_id"),
        col("username").alias("account_name"),
        col("format"),
        col("created_at").alias("published_at"),
        col("created_date").alias("published_date"),
        col("caption"),
        col("permalink"),
        col("last_seen_date"),
    )
    return conform(df, "content", GOLD_PLATFORM, metrics_scope=lit(ORGANIC))
