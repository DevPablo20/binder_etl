from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform(df: DataFrame) -> DataFrame:
    return df.select(
        col("campaign_id").cast("string").alias("campaign_id"),
        col("campaign_name"),
        col("campaign_type"),
        col("advertiser_id").cast("string").alias("ad_account_id"),
        col("budget").cast("decimal(10,3)").alias("budget"),
        col("budget_mode"),
        col("operation_status").alias("campaign_status"),
        col("secondary_status").alias("campaign_secondary_status"),
        col("operation_status").alias("campaign_operation_status"),
        col("objective_type"),
        col("budget_optimize_on"),
        col("is_new_structure"),
        col("create_time").cast("timestamp").alias("created_at"),
        col("modify_time").cast("timestamp").alias("updated_at"),
        col("roas_bid").cast("decimal(10,3)"),
        col("is_smart_performance_campaign"),
        col("is_search_campaign"),
        col("app_promotion_type"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
