"""Land gold ads_daily_metrics into backend Postgres analytics schema."""

from __future__ import annotations

import logging

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from src.config import settings
from src.io.reader import read_delta

logger = logging.getLogger(__name__)

LAND_COLUMNS = (
    "platform",
    "ad_account_id",
    "ad_account_name",
    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_operation_status",
    "objective_type",
    "budget",
    "ad_group_id",
    "optimization_goal",
    "billing_event",
    "ad_id",
    "ad_name",
    "ad_text",
    "display_name",
    "date",
    "conversions",
    "clicks",
    "engagement",
    "impressions",
    "spend",
    "purchase",
    "shares",
    "comments",
    "complete_payment",
    "clicks_on_music_disc",
    "profile_visits",
    "total_app_event_add_to_cart",
    "registration",
    "sales_lead",
    "onsite_shopping",
    "video_views_2s",
    "video_views_6s",
    "video_views_25p",
    "video_views_50p",
    "video_views_75p",
    "video_views_100p",
    "video_views",
    "reach",
)

JDBC_TABLE = "analytics.ads_daily_metrics"


def _prepare_frame(spark: SparkSession, platform: str) -> DataFrame:
    gold = read_delta(spark, "gold", f"{platform}/ads_daily_metrics")
    return gold.withColumn("platform", lit(platform)).select(
        *[col(name) for name in LAND_COLUMNS]
    )


def _jdbc_props() -> dict[str, str]:
    return {
        "user": settings.backend_db_user,
        "password": settings.backend_db_password,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }


def _backend_conn():
    return psycopg2.connect(
        host=settings.backend_db_host,
        port=settings.backend_db_port,
        user=settings.backend_db_user,
        password=settings.backend_db_password,
        dbname=settings.backend_db_database,
    )


def _ensure_target_table() -> None:
    """Create the analytics serving table if backend migrations have not run yet."""
    conn = _backend_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS "analytics"')
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics.ads_daily_metrics (
                    platform character varying(64) NOT NULL,
                    ad_id character varying(255) NOT NULL,
                    date date NOT NULL,
                    ad_account_id character varying(255) NOT NULL,
                    ad_account_name character varying(512),
                    campaign_id character varying(255) NOT NULL,
                    campaign_name character varying(512),
                    campaign_status character varying(255),
                    campaign_operation_status character varying(255),
                    objective_type character varying(255),
                    budget numeric(18,4),
                    ad_group_id character varying(255) NOT NULL,
                    optimization_goal character varying(255),
                    billing_event character varying(255),
                    ad_name character varying(512),
                    ad_text text,
                    display_name character varying(512),
                    conversions integer,
                    clicks integer,
                    engagement integer,
                    impressions bigint,
                    spend numeric(18,4),
                    purchase integer,
                    shares integer,
                    comments integer,
                    complete_payment integer,
                    clicks_on_music_disc integer,
                    profile_visits integer,
                    total_app_event_add_to_cart integer,
                    registration integer,
                    sales_lead integer,
                    onsite_shopping integer,
                    video_views_2s integer,
                    video_views_6s integer,
                    video_views_25p integer,
                    video_views_50p integer,
                    video_views_75p integer,
                    video_views_100p integer,
                    video_views integer,
                    reach integer,
                    CONSTRAINT pk_ads_daily_metrics PRIMARY KEY (platform, ad_id, date)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ads_daily_metrics_platform_account_date
                ON analytics.ads_daily_metrics (platform, ad_account_id, date)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ads_daily_metrics_platform_campaign_date
                ON analytics.ads_daily_metrics (platform, campaign_id, date)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ads_daily_metrics_platform_ad_group_date
                ON analytics.ads_daily_metrics (platform, ad_group_id, date)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ads_daily_metrics_date
                ON analytics.ads_daily_metrics (date)
                """
            )
        conn.commit()
    finally:
        conn.close()


def _delete_platform_rows(platform: str) -> None:
    """Delete existing rows for this platform (overwrite-per-platform)."""
    conn = _backend_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {JDBC_TABLE} WHERE platform = %s",
                (platform,),
            )
            deleted = cur.rowcount
        conn.commit()
        logger.info("Deleted %s existing rows for platform=%s", deleted, platform)
    finally:
        conn.close()


def land_ads_daily_metrics(spark: SparkSession, platform: str) -> int:
    if not settings.backend_db_host:
        raise RuntimeError(
            "Backend Postgres is not configured. Set BACKEND_DB_HOST "
            "(and related BACKEND_DB_* vars) to land analytics facts."
        )

    df = _prepare_frame(spark, platform)
    if df.isEmpty():
        logger.warning("Gold ads_daily_metrics empty for %s — skipping land", platform)
        return 0

    row_count = df.count()
    _ensure_target_table()
    _delete_platform_rows(platform)

    (
        df.write.format("jdbc")
        .option("url", settings.backend_jdbc_url)
        .option("dbtable", JDBC_TABLE)
        .options(**_jdbc_props())
        .mode("append")
        .save()
    )

    logger.info(
        "Landed %s rows into %s for platform=%s",
        row_count,
        JDBC_TABLE,
        platform,
    )
    return row_count
