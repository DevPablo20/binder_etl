"""Unitário e isolado (sem MinIO): as chaves do bronze do instagram_organic.

Cada stream tem a sua natureza — foto diária, série nativa ou última leitura — e a chave de
dedupe é o que a garante. Os testes provam as três, e o descarte das linhas vazias do
`user_insights`.
"""
from datetime import date, datetime, timezone

from src.transformers.instagram_organic.bronze import InstagramOrganicBronzeTransformer
from src.transformers.instagram_organic.tables import INSTAGRAM_STREAMS

STREAMS = {stream.name: stream for stream in INSTAGRAM_STREAMS}


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _run(spark, stream_name, rows, schema):
    stream = STREAMS[stream_name]
    raw = spark.createDataFrame(rows, schema=schema)
    prepared = InstagramOrganicBronzeTransformer.prepare(raw, stream)
    return InstagramOrganicBronzeTransformer(spark).dedupe(prepared, list(stream.dedupe_columns))


def test_media_insights_keeps_one_snapshot_per_day(spark):
    rows = _run(
        spark,
        "media_insights",
        [
            ("m1", 10, _utc(2026, 9, 29, 19, 4)),  # 16:04 SP → fecha 29/09
            ("m1", 12, _utc(2026, 9, 30, 5, 1)),  # 02:01 SP → também fecha 29/09, vence
            ("m1", 15, _utc(2026, 9, 30, 18, 5)),  # 15:05 SP → fecha 30/09
        ],
        "id string, reach bigint, _airbyte_extracted_at timestamp",
    ).collect()

    assert {row["snapshot_date"]: row["reach"] for row in rows} == {
        date(2026, 9, 29): 12,
        date(2026, 9, 30): 15,
    }


def test_user_insights_keyed_by_day_drops_empty_rows(spark):
    """A releitura do dia em andamento substitui a leitura parcial; linha sem `date` sai."""
    rows = _run(
        spark,
        "user_insights",
        [
            ("acc", _utc(2026, 9, 29, 7), 30684, _utc(2026, 9, 29, 19, 2)),
            ("acc", _utc(2026, 9, 29, 7), 65857, _utc(2026, 9, 30, 5, 1)),
            ("acc", None, None, _utc(2026, 9, 29, 19, 2)),
        ],
        "business_account_id string, date timestamp, reach bigint, _airbyte_extracted_at timestamp",
    ).collect()

    assert [row["reach"] for row in rows] == [65857]


def test_stories_keep_only_last_reading(spark):
    """O story atravessa dois dias de foto; fica uma linha só, a última leitura."""
    rows = _run(
        spark,
        "story_insights",
        [
            ("s1", 100, _utc(2026, 9, 30, 20, 0)),
            ("s1", 900, _utc(2026, 10, 1, 12, 0)),
        ],
        "id string, reach bigint, _airbyte_extracted_at timestamp",
    ).collect()

    assert [row["reach"] for row in rows] == [900]
