from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from src.io.reader import read_delta
from src.transformers.tiktok.tables import PLATFORM


def transform(spark: SparkSession) -> DataFrame:
    advertisers = read_delta(spark, "silver", f"{PLATFORM}/advertisers")

    return advertisers.select(
        lit(PLATFORM).alias("platform"),
        lit("account").alias("object_type"),
        col("ad_account_id").alias("account_id"),
        col("ad_account_name").alias("account_name"),
        lit(None).cast("string").alias("campaign_id"),
        lit(None).cast("string").alias("campaign_name"),
        lit(None).cast("string").alias("ad_group_id"),
        lit(None).cast("string").alias("ad_group_name"),
        lit(None).cast("string").alias("ad_id"),
        lit(None).cast("string").alias("ad_name"),
    )
