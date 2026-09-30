from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.transformers.instagram_organic.tables import GoldFactConfig


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """Um story por linha, com os números finais. Função pura, sem I/O.

    Os números são os da última leitura antes de o story expirar (ver
    `hours_live_at_last_read`); story lido cedo demais — conexão parada, por exemplo — fica com
    os números daquela idade.
    """
    stories = sources["stories"]
    accounts = sources["accounts"].select(col("business_account_id"), col("username"))

    return stories.join(accounts, "business_account_id", "left").select(
        col("business_account_id"),
        col("username"),
        col("story_id"),
        col("created_at"),
        col("created_date"),
        col("media_type"),
        col("permalink"),
        col("reach"),
        col("views"),
        col("shares"),
        col("follows"),
        col("replies"),
        col("profile_visits"),
        col("total_interactions"),
        col("last_read_at"),
        col("hours_live_at_last_read"),
    )
