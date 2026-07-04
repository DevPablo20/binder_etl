from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform(df: DataFrame) -> DataFrame:
    return df.select(
        col("advertiser_id").cast("string").alias("ad_account_id"),
        col("name").alias("ad_account_name"),
        col("country"),
        col("currency"),
        col("role").alias("permissions"),
        col("create_time").cast("timestamp").alias("created_at"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
