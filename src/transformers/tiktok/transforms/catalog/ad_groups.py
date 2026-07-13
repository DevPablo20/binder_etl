from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from src.io.reader import read_delta
from src.transformers.tiktok.tables import PLATFORM


def transform(spark: SparkSession) -> DataFrame:
    ad_groups = read_delta(spark, "silver", f"{PLATFORM}/ad_groups")
    campaigns = read_delta(spark, "silver", f"{PLATFORM}/campaigns")
    advertisers = read_delta(spark, "silver", f"{PLATFORM}/advertisers")

    selected_ad_groups = ad_groups.select(
        col("ad_account_id"),
        col("campaign_id"),
        col("ad_group_id"),
        col("ad_group_name"),
    )
    selected_campaigns = campaigns.select(
        col("ad_account_id"),
        col("campaign_id"),
        col("campaign_name"),
    )
    selected_advertisers = advertisers.select(
        col("ad_account_id"),
        col("ad_account_name"),
    )

    joined = (
        selected_ad_groups.alias("ag")
        .join(
            selected_campaigns.alias("c"),
            on=(
                (col("ag.campaign_id") == col("c.campaign_id"))
                & (col("ag.ad_account_id") == col("c.ad_account_id"))
            ),
            how="inner",
        )
        .join(
            selected_advertisers.alias("a"),
            on=col("ag.ad_account_id") == col("a.ad_account_id"),
            how="inner",
        )
    )

    return joined.select(
        lit(PLATFORM).alias("platform"),
        lit("ad_group").alias("object_type"),
        col("ag.ad_account_id").alias("account_id"),
        col("a.ad_account_name").alias("account_name"),
        col("ag.campaign_id").alias("campaign_id"),
        col("c.campaign_name").alias("campaign_name"),
        col("ag.ad_group_id").alias("ad_group_id"),
        col("ag.ad_group_name").alias("ad_group_name"),
        lit(None).cast("string").alias("ad_id"),
        lit(None).cast("string").alias("ad_name"),
    )
