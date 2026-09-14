"""Unitário e isolado (sem MinIO): o filtro de linhas sem atividade do silver
ads_reports_daily.

Só descarta ad × dia se **todas** as métricas forem nulas ou zero. Uma linha com spend 0
mas alguma conversão precisa sobreviver, e nenhuma soma pode mudar com o filtro.
"""
import json

import pytest

from src.spark_session import get_spark_session
from src.transformers.tiktok.transforms.silver.ads_reports_daily import transform

# O transform acessa cada campo por nome (col("metrics.X")) — todos precisam existir no
# JSON sintético, senão o Spark falha ao resolver a coluna. Strings ficam vazias; métricas
# numéricas ficam em 0, que o filtro trata como "sem atividade".
_STRING_FIELDS = (
    "campaign_id",
    "adgroup_id",
    "ad_text",
    "mobile_app_id",
    "tt_app_id",
    "placement_type",
    "promotion_type",
    "tt_app_name",
)
_NUMERIC_FIELDS = (
    "app_install",
    "average_video_play",
    "average_video_play_per_user",
    "clicks",
    "clicks_on_music_disc",
    "comments",
    "complete_payment",
    "conversion",
    "conversion_rate",
    "cost_per_1000_reached",
    "cost_per_app_install",
    "cost_per_conversion",
    "cost_per_purchase",
    "cost_per_registration",
    "cost_per_result",
    "cost_per_sales_lead",
    "cost_per_total_app_event_add_to_cart",
    "cost_per_total_sales_lead",
    "cpc",
    "cpm",
    "ctr",
    "frequency",
    "impressions",
    "onsite_shopping",
    "profile_visits",
    "profile_visits_rate",
    "purchase",
    "purchase_rate",
    "reach",
    "real_time_app_install",
    "real_time_app_install_cost",
    "real_time_conversion",
    "real_time_conversion_rate",
    "real_time_cost_per_conversion",
    "real_time_cost_per_result",
    "real_time_result",
    "real_time_result_rate",
    "registration",
    "registration_rate",
    "result",
    "result_rate",
    "sales_lead",
    "sales_lead_rate",
    "shares",
    "spend",
    "total_app_event_add_to_cart",
    "total_complete_payment_rate",
    "total_onsite_shopping_value",
    "total_purchase_value",
    "value_per_complete_payment",
    "video_play_actions",
    "video_views_p100",
    "video_views_p25",
    "video_views_p50",
    "video_views_p75",
    "video_watched_2s",
    "video_watched_6s",
    "vta_purchase",
)
BASE_METRICS = {
    **{name: "" for name in _STRING_FIELDS},
    **{name: 0 for name in _NUMERIC_FIELDS},
    "campaign_id": "111",
    "adgroup_id": "222",
}


def _row(ad_id: str, stat_time_day: str, **metric_overrides) -> str:
    metrics = {**BASE_METRICS, **metric_overrides}
    return json.dumps(
        {
            "ad_id": ad_id,
            "stat_time_day": stat_time_day,
            "metrics": metrics,
            "_airbyte_raw_id": f"raw-{ad_id}-{stat_time_day}",
            "_airbyte_extracted_at": "2026-09-14T00:00:00Z",
            "_airbyte_meta": {"sync_id": "1"},
        }
    )


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session(app_name="silver-ads-reports-filter-test")
    yield session
    session.stop()


def test_drops_row_with_every_metric_zero(spark):
    lines = [_row("1", "2026-09-01")]  # todas as métricas em BASE_METRICS são zero
    df = spark.read.json(spark.sparkContext.parallelize(lines))

    result = transform(df)

    assert result.count() == 0


def test_keeps_row_with_only_conversion_nonzero(spark):
    lines = [_row("2", "2026-09-01", conversion=1)]  # spend e impressions continuam 0
    df = spark.read.json(spark.sparkContext.parallelize(lines))

    result = transform(df)

    assert result.count() == 1
    assert result.collect()[0]["conversion"] == 1
    assert result.collect()[0]["spend"] == 0


def test_keeps_row_with_negative_metric(spark):
    # cost_per_result não está em BASE_METRICS; um valor negativo isolado tem que contar
    # como atividade (abs() antes de comparar com zero).
    lines = [_row("3", "2026-09-01", cost_per_result=-1.5)]
    df = spark.read.json(spark.sparkContext.parallelize(lines))

    result = transform(df)

    assert result.count() == 1


def test_filter_does_not_change_metric_sums(spark):
    lines = [
        _row("4", "2026-09-01"),  # zerada — descartada
        _row("5", "2026-09-01", spend=10.5, impressions=100, clicks=3),  # tem atividade
        _row("6", "2026-09-01"),  # zerada — descartada
    ]
    df = spark.read.json(spark.sparkContext.parallelize(lines))

    result = transform(df)

    assert result.count() == 1
    row = result.collect()[0]
    assert row["spend"] == 10.5
    assert row["impressions"] == 100
    assert row["clicks"] == 3
