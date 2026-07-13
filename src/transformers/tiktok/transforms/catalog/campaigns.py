from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from src.io.reader import read_delta
from src.transformers.tiktok.tables import PLATFORM


def transform(spark: SparkSession) -> DataFrame:
    campaigns = read_delta(spark, "silver", f"{PLATFORM}/campaigns")
    advertisers = read_delta(spark, "silver", f"{PLATFORM}/advertisers")

    selected_campaigns = campaigns.select(
        col("ad_account_id"),
        col("campaign_id"),
        col("campaign_name"),
    )
    selected_advertisers = advertisers.select(
        col("ad_account_id"),
        col("ad_account_name"),
    )

    joined = selected_campaigns.alias("c").join(
        selected_advertisers.alias("a"),
        on=col("c.ad_account_id") == col("a.ad_account_id"),
        how="inner",
    )

    return joined.select(
        lit(PLATFORM).alias("platform"),
        lit("campaign").alias("object_type"),
        col("c.ad_account_id").alias("account_id"),
        col("a.ad_account_name").alias("account_name"),
        col("c.campaign_id").alias("campaign_id"),
        col("c.campaign_name").alias("campaign_name"),
        lit(None).cast("string").alias("ad_group_id"),
        lit(None).cast("string").alias("ad_group_name"),
        lit(None).cast("string").alias("ad_id"),
        lit(None).cast("string").alias("ad_name"),
    )
