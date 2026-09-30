"""Unitário e isolado (sem MinIO): as métricas diárias da conta no silver.

No Instagram o `date` é o **início** do dia no Pacífico (conferido contra o Business Suite:
`date` 2026-09-26T07:00Z = dia 26/09) — sem o −1 das páginas do Facebook. O dia ainda em
andamento na leitura é marcado como parcial.
"""
from datetime import date, datetime, timezone

from src.transformers.instagram_organic.transforms.silver.account_insights_daily import (
    transform,
)

SCHEMA = (
    "business_account_id string, date timestamp, reach bigint, reach_week bigint, "
    "reach_days_28 bigint, follower_count bigint, online_followers string, "
    "_airbyte_raw_id string, _airbyte_extracted_at timestamp, _airbyte_meta struct<sync_id: bigint>"
)


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _row(day_start, reach, extracted_at, followers=0, online=None):
    return ("acc", day_start, reach, 0, 0, followers, online, "raw", extracted_at, (1,))


def _run(spark, rows):
    df = spark.createDataFrame(rows, schema=SCHEMA)
    return {row["metric_date"]: row for row in transform({"user_insights": df}).collect()}


def test_date_is_start_of_pacific_day_in_summer_and_winter(spark):
    rows = _run(
        spark,
        [
            _row(_utc(2026, 9, 26, 7), 37668, _utc(2026, 9, 29, 19, 2)),
            _row(_utc(2026, 12, 2, 8), 1, _utc(2026, 12, 5, 4, 0)),  # horário de inverno
        ],
    )

    assert rows[date(2026, 9, 26)]["reach"] == 37668
    assert rows[date(2026, 12, 2)]["reach"] == 1


def test_day_read_before_it_ends_is_partial(spark):
    rows = _run(
        spark,
        [
            # 29/09 no Pacífico termina em 30/09 07:00Z; lido às 05:01Z → parcial
            _row(_utc(2026, 9, 29, 7), 65857, _utc(2026, 9, 30, 5, 1)),
            # 28/09 termina em 29/09 07:00Z; lido às 05:01Z de 30/09 → fechado
            _row(_utc(2026, 9, 28, 7), 46087, _utc(2026, 9, 30, 5, 1), followers=148),
        ],
    )

    assert rows[date(2026, 9, 29)]["is_partial"] is True
    assert rows[date(2026, 9, 28)]["is_partial"] is False
    assert rows[date(2026, 9, 28)]["new_followers"] == 148


def test_online_followers_becomes_map(spark):
    rows = _run(
        spark,
        [_row(_utc(2026, 9, 28, 7), 1, _utc(2026, 9, 30, 5, 1), online='{"0":9073,"1":18419}')],
    )

    assert rows[date(2026, 9, 28)]["online_followers"] == {"0": 9073, "1": 18419}
