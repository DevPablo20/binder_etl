from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Série de seguidores: conta × `snapshot_date`.

    `followers_count`, `follows_count` e `media_count` são o valor atual no momento da foto; só
    viram série porque o bronze guarda uma foto por dia.
    """
    return sources["users"].select(
        col("id").cast("string").alias("business_account_id"),
        col("snapshot_date"),
        col("_airbyte_extracted_at").alias("snapshot_at"),
        col("followers_count"),
        col("follows_count"),
        col("media_count"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
