from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.io.reader import read_delta
from src.transformers.tiktok.tables import GoldFactConfig, PLATFORM


def transform(spark: SparkSession, _fact: GoldFactConfig) -> DataFrame:
    silver_advertisers = read_delta(spark, "silver", f"{PLATFORM}/advertisers")
    silver_campaigns = read_delta(spark, "silver", f"{PLATFORM}/campaigns")
    silver_ad_groups = read_delta(spark, "silver", f"{PLATFORM}/ad_groups")
    silver_ads = read_delta(spark, "silver", f"{PLATFORM}/ads")
    silver_ads_reports_daily = read_delta(
        spark, "silver", f"{PLATFORM}/ads_reports_daily"
    )

    selected_advertisers = silver_advertisers.select(
        col("ad_account_id"),
        col("ad_account_name"),
    )

    selected_campaigns = silver_campaigns.select(
        col("ad_account_id"),
        col("campaign_id"),
        col("campaign_name"),
        col("campaign_status"),
        col("campaign_operation_status"),
        col("objective_type"),
        col("budget"),
    )

    selected_ad_groups = silver_ad_groups.select(
        col("ad_account_id"),
        col("campaign_id"),
        col("ad_group_id"),
        col("optimization_goal"),
        col("billing_event"),
    )

    selected_ads = silver_ads.select(
        col("ad_account_id"),
        col("campaign_id"),
        col("ad_group_id"),
        col("ad_id"),
        col("ad_name"),
        col("ad_text"),
        col("display_name"),
    )

    selected_ads_reports_daily = silver_ads_reports_daily.select(
        col("campaign_id"),
        col("ad_group_id"),
        col("ad_id"),
        col("stat_time_day").alias("date"),
        col("conversion").alias("conversions"),
        col("clicks"),
        col("result").alias("engagement"),
        col("impressions"),
        col("spend"),
        col("purchase"),
        col("shares"),
        col("comments"),
        col("complete_payment"),
        col("clicks_on_music_disc"),
        col("profile_visits"),
        col("total_app_event_add_to_cart"),
        col("registration"),
        col("sales_lead"),
        col("onsite_shopping"),
        col("video_watched_2s").alias("video_views_2s"),
        col("video_watched_6s").alias("video_views_6s"),
        col("video_views_p25").alias("video_views_25p"),
        col("video_views_p50").alias("video_views_50p"),
        col("video_views_p75").alias("video_views_75p"),
        col("video_views_p100").alias("video_views_100p"),
        col("video_play_actions").alias("video_views"),
        col("reach"),
    )

    joined = (
        selected_ads_reports_daily.alias("tard")
        .join(
            selected_ads.alias("tad"),
            on=(
                (col("tard.ad_id") == col("tad.ad_id"))
                & (col("tard.ad_group_id") == col("tad.ad_group_id"))
                & (col("tard.campaign_id") == col("tad.campaign_id"))
            ),
            how="inner",
        )
        .join(
            selected_ad_groups.alias("tag"),
            on=(
                (col("tad.ad_group_id") == col("tag.ad_group_id"))
                & (col("tad.campaign_id") == col("tag.campaign_id"))
            ),
            how="inner",
        )
        .join(
            selected_campaigns.alias("tc"),
            on=(
                (col("tag.campaign_id") == col("tc.campaign_id"))
                & (col("tag.ad_account_id") == col("tc.ad_account_id"))
            ),
            how="inner",
        )
        .join(
            selected_advertisers.alias("taa"),
            on=(col("tc.ad_account_id") == col("taa.ad_account_id")),
            how="inner",
        )
    )

    return joined.select(
        col("taa.ad_account_id"),
        col("taa.ad_account_name"),
        col("tc.campaign_id"),
        col("tc.campaign_name"),
        col("tc.campaign_status"),
        col("tc.campaign_operation_status"),
        col("tc.objective_type"),
        col("tc.budget"),
        col("tag.ad_group_id"),
        col("tag.optimization_goal"),
        col("tag.billing_event"),
        col("tad.ad_id"),
        col("tad.ad_name"),
        col("tad.ad_text"),
        col("tad.display_name"),
        col("tard.date"),
        col("tard.conversions"),
        col("tard.clicks"),
        col("tard.engagement"),
        col("tard.impressions"),
        col("tard.spend"),
        col("tard.purchase"),
        col("tard.shares"),
        col("tard.comments"),
        col("tard.complete_payment"),
        col("tard.clicks_on_music_disc"),
        col("tard.profile_visits"),
        col("tard.total_app_event_add_to_cart"),
        col("tard.registration"),
        col("tard.sales_lead"),
        col("tard.onsite_shopping"),
        col("tard.video_views_2s"),
        col("tard.video_views_6s"),
        col("tard.video_views_25p"),
        col("tard.video_views_50p"),
        col("tard.video_views_75p"),
        col("tard.video_views_100p"),
        col("tard.video_views"),
        col("tard.reach"),
    )
