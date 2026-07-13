from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from src.io.reader import read_delta
from src.transformers.tiktok.tables import PLATFORM


def transform(spark: SparkSession) -> DataFrame:
    ads = read_delta(spark, "silver", f"{PLATFORM}/ads")
    advertisers = read_delta(spark, "silver", f"{PLATFORM}/advertisers")

    selected_ads = ads.select(
        col("ad_account_id"),
        col("campaign_id"),
        col("campaign_name"),
        col("ad_group_id"),
        col("ad_group_name"),
        col("ad_id"),
        col("ad_name"),
    )
    selected_advertisers = advertisers.select(
        col("ad_account_id"),
        col("ad_account_name"),
    )

    joined = selected_ads.alias("ad").join(
        selected_advertisers.alias("a"),
        on=col("ad.ad_account_id") == col("a.ad_account_id"),
        how="inner",
    )

    return joined.select(
        lit(PLATFORM).alias("platform"),
        lit("ad").alias("object_type"),
        col("ad.ad_account_id").alias("account_id"),
        col("a.ad_account_name").alias("account_name"),
        col("ad.campaign_id").alias("campaign_id"),
        col("ad.campaign_name").alias("campaign_name"),
        col("ad.ad_group_id").alias("ad_group_id"),
        col("ad.ad_group_name").alias("ad_group_name"),
        col("ad.ad_id").alias("ad_id"),
        col("ad.ad_name").alias("ad_name"),
    )
