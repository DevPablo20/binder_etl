"""Unitário e isolado (sem MinIO): as métricas diárias da página no gold."""
from datetime import date

from src.transformers.facebook_organic.tables import GOLD_FACTS
from src.transformers.facebook_organic.transforms.gold.page_daily_metrics import transform

FACT = next(fact for fact in GOLD_FACTS if fact.name == "page_daily_metrics")

DAILY_SCHEMA = (
    "page_id string, metric string, period string, metric_date date, value bigint, "
    "value_breakdown map<string, bigint>"
)
FOLLOWERS_SCHEMA = "page_id string, snapshot_date date, fan_count bigint, followers_count bigint"
PAGES_SCHEMA = "page_id string, page_name string"

D26 = date(2026, 9, 26)
D27 = date(2026, 9, 27)


def _run(spark, daily, followers):
    sources = {
        "page_insights_daily": spark.createDataFrame(daily, schema=DAILY_SCHEMA),
        "page_followers_snapshot": spark.createDataFrame(followers, schema=FOLLOWERS_SCHEMA),
        "pages": spark.createDataFrame([("p1", "Page")], schema=PAGES_SCHEMA),
    }
    return {row["metric_date"]: row for row in transform(sources, FACT).collect()}


def test_pivots_day_metrics_and_fan_adds(spark):
    rows = _run(
        spark,
        [
            ("p1", "page_media_view", "day", D26, 842533, None),
            ("p1", "page_media_view", "week", D26, 4199455, None),  # janela móvel: fora
            ("p1", "page_total_media_view_unique", "day", D26, 691404, None),
            ("p1", "page_post_engagements", "day", D26, 1839, None),
            ("p1", "page_total_actions", "day", D26, 0, None),
            (
                "p1",
                "page_fan_adds_by_paid_non_paid_unique",
                "day",
                D26,
                None,
                {"total": 6, "paid": 6, "unpaid": 0},
            ),
        ],
        [],
    )

    row = rows[D26]
    assert row["media_views"] == 842533
    assert row["viewers"] == 691404
    assert row["post_engagements"] == 1839
    assert row["total_actions"] == 0
    assert (row["fan_adds_total"], row["fan_adds_paid"], row["fan_adds_unpaid"]) == (6, 6, 0)
    assert row["page_name"] == "Page"


def test_followers_join_by_day_without_dropping_metric_days(spark):
    """Seguidores entram pela foto do mesmo dia; dia sem foto fica com seguidores nulos."""
    rows = _run(
        spark,
        [
            ("p1", "page_media_view", "day", D26, 1, None),
            ("p1", "page_media_view", "day", D27, 2, None),
        ],
        [("p1", D27, 1783, 1783), ("p1", date(2026, 9, 30), 1790, 1790)],
    )

    assert set(rows) == {D26, D27}  # a foto de 30/09 sem métrica não cria linha
    assert rows[D26]["followers_count"] is None
    assert rows[D27]["followers_count"] == 1783
