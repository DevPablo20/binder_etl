from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, lower

from src.transformers.instagram_organic.tables import GOLD_PLATFORM, GoldFactConfig
from src.transformers.organic_gold import ORGANIC, conform


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """`stories`: um story por linha, com os números da última leitura. Função pura.

    Os números são os da última leitura antes de o story expirar (`hours_live_at_last_read`
    diz a idade do story nela). Story com menos de 5 visualizações não tem métrica.
    """
    stories = sources["stories"]
    accounts = sources["accounts"].select(col("business_account_id"), col("username"))

    df = stories.join(accounts, "business_account_id", "left").select(
        col("story_id"),
        col("business_account_id").alias("account_id"),
        col("username").alias("account_name"),
        lower(col("media_type")).alias("format"),
        col("created_at").alias("published_at"),
        col("created_date").alias("published_date"),
        col("permalink"),
        col("reach"),
        col("views"),
        col("shares"),
        col("follows"),
        col("replies"),
        col("profile_visits"),
        col("total_interactions").alias("interactions"),
        col("last_read_at"),
        col("hours_live_at_last_read"),
    )
    return conform(df, "stories", GOLD_PLATFORM, metrics_scope=lit(ORGANIC))
