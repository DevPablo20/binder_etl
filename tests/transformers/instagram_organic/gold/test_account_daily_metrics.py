"""Unitário e isolado (sem MinIO): métricas diárias da conta no gold."""
from datetime import date

from src.transformers.instagram_organic.tables import GOLD_FACTS
from src.transformers.instagram_organic.transforms.gold.account_daily_metrics import transform

FACT = next(fact for fact in GOLD_FACTS if fact.name == "account_daily_metrics")

DAILY_SCHEMA = (
    "business_account_id string, metric_date date, is_partial boolean, reach bigint, "
    "reach_week bigint, reach_days_28 bigint, new_followers bigint"
)
FOLLOWERS_SCHEMA = (
    "business_account_id string, snapshot_date date, followers_count bigint, "
    "follows_count bigint, media_count bigint"
)


def test_followers_join_by_day_without_dropping_metric_days(spark):
    sources = {
        "account_insights_daily": spark.createDataFrame(
            [
                ("acc", date(2026, 9, 28), False, 46087, 1, 1, 148),
                ("acc", date(2026, 9, 29), False, 66930, 1, 1, 0),
            ],
            schema=DAILY_SCHEMA,
        ),
        "account_followers_snapshot": spark.createDataFrame(
            [("acc", date(2026, 9, 29), 140944, 175, 1728), ("acc", date(2026, 10, 1), 1, 1, 1)],
            schema=FOLLOWERS_SCHEMA,
        ),
        "accounts": spark.createDataFrame([("acc", "texaco")], "business_account_id string, username string"),
    }

    rows = {row["metric_date"]: row for row in transform(sources, FACT).collect()}

    assert set(rows) == {date(2026, 9, 28), date(2026, 9, 29)}
    assert rows[date(2026, 9, 28)]["followers_count"] is None
    assert rows[date(2026, 9, 29)]["followers_count"] == 140944
    assert rows[date(2026, 9, 29)]["username"] == "texaco"
