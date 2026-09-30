"""Unitário e isolado (sem MinIO): as métricas diárias da página no silver.

O `end_time` marca o fim da janela, à meia-noite do Pacífico — o dia medido é o anterior
(conferido contra o Business Suite: `end_time` 27/09 07:00Z = dia 26/09). O mesmo dia chega em
mais de um sync, e vence a extração mais recente.
"""
from datetime import date, datetime, timezone

from src.transformers.facebook_organic.transforms.silver.page_insights_daily import (
    missing_metric_dates,
    transform,
)

SCHEMA = (
    "page_id string, name string, period string, "
    "values array<struct<value: struct<type: string, object: string, integer: bigint>, "
    "end_time: timestamp>>, "
    "_airbyte_raw_id string, _airbyte_extracted_at timestamp, "
    "_airbyte_meta struct<sync_id: bigint>"
)


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _row(name, values, extracted_at, period="day", page_id="p1"):
    return (page_id, name, period, values, f"raw-{extracted_at}", extracted_at, (1,))


def _int(value, end_time):
    return (("integer", None, value), end_time)


def test_end_time_maps_to_previous_pacific_day(spark):
    df = spark.createDataFrame(
        [
            _row(
                "page_media_view",
                [_int(842533, _utc(2026, 9, 27, 7)), _int(485620, _utc(2026, 9, 28, 7))],
                _utc(2026, 9, 30, 4, 3),
            ),
            # inverno no Pacífico: meia-noite é 08:00 UTC
            _row("page_media_view", [_int(1, _utc(2026, 12, 2, 8))], _utc(2026, 12, 3, 4, 3)),
        ],
        schema=SCHEMA,
    )

    rows = {row["metric_date"]: row["value"] for row in transform({"page_insights": df}).collect()}

    assert rows == {date(2026, 9, 26): 842533, date(2026, 9, 27): 485620, date(2026, 12, 1): 1}


def test_same_day_from_two_syncs_keeps_latest_extraction(spark):
    df = spark.createDataFrame(
        [
            _row("page_media_view", [_int(100, _utc(2026, 9, 28, 7))], _utc(2026, 9, 30, 4, 3)),
            _row("page_media_view", [_int(120, _utc(2026, 9, 28, 7))], _utc(2026, 10, 1, 4, 3)),
        ],
        schema=SCHEMA,
    )

    rows = transform({"page_insights": df}).collect()

    assert [(row["metric_date"], row["value"]) for row in rows] == [(date(2026, 9, 27), 120)]


def test_breakdown_becomes_map(spark):
    df = spark.createDataFrame(
        [
            _row(
                "page_fan_adds_by_paid_non_paid_unique",
                [(("object", '{"total":8,"paid":7,"unpaid":1}', None), _utc(2026, 9, 29, 7))],
                _utc(2026, 9, 30, 14, 17),
            )
        ],
        schema=SCHEMA,
    )

    row = transform({"page_insights": df}).collect()[0]

    assert row["value"] is None
    assert row["value_breakdown"] == {"total": 8, "paid": 7, "unpaid": 1}


def test_missing_metric_dates_points_out_gaps(spark):
    df = spark.createDataFrame(
        [
            _row(
                "page_media_view",
                [_int(1, _utc(2026, 9, 27, 7)), _int(1, _utc(2026, 9, 30, 7))],
                _utc(2026, 10, 1, 4, 3),
            ),
            _row("page_media_view", [_int(1, _utc(2026, 9, 29, 7))], _utc(2026, 10, 1, 4, 3), period="week"),
        ],
        schema=SCHEMA,
    )

    missing = missing_metric_dates(transform({"page_insights": df}))

    # 27 e 28/09 faltam no período `day`; o `week` não conta
    assert missing == [("p1", date(2026, 9, 27)), ("p1", date(2026, 9, 28))]
