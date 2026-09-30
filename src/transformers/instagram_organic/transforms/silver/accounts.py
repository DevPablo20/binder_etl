from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.transformers.snapshots import latest_per


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Dimensão de conta: a última foto de cada conta.

    `page_id` é a página do Facebook ligada à conta — a ponte com o `facebook_organic`.
    """
    return latest_per(sources["users"], "id").select(
        col("id").cast("string").alias("business_account_id"),
        col("username"),
        col("name").alias("account_name"),
        col("page_id").cast("string").alias("page_id"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
