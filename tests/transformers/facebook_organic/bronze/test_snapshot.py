"""Unitário e isolado (sem MinIO): a foto diária do bronze.

O bronze do facebook_organic deduplica por `id + snapshot_date`, não só pelo id — é isso que
guarda o histórico de uma fonte que só entrega o total acumulado. Os testes provam o corte
das 06:00, uma linha por dia (a extração mais recente), a sobrevivência de fotos antigas e a
reconstrução do `id` do `page_insights` nas fotos de antes da seleção de campos.
"""
from datetime import date, datetime, timezone

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.transformers.facebook_organic.bronze import (
    FacebookOrganicBronzeTransformer,
    snapshot_date,
)

SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("value", StringType()),
        StructField("_airbyte_extracted_at", TimestampType()),
    ]
)
DEDUPE_COLUMNS = ["id", "snapshot_date"]


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _df(spark, rows):
    df = spark.createDataFrame(rows, schema=SCHEMA)
    return df.withColumn("snapshot_date", snapshot_date(col("_airbyte_extracted_at")))


def _dates(df) -> dict[str, date]:
    return {row["value"]: row["snapshot_date"] for row in df.collect()}


def test_snapshot_date_cutoff_at_six_in_sao_paulo(spark):
    """Sync da madrugada fecha o dia anterior; sync da tarde fecha o próprio dia."""
    df = _df(
        spark,
        [
            ("1", "cron 01:01 SP", _utc(2026, 9, 30, 4, 1)),
            ("1", "manual 17:35 SP", _utc(2026, 9, 29, 20, 35)),
            ("1", "manual 11:16 SP", _utc(2026, 9, 30, 14, 16)),
            ("1", "05:59 SP", _utc(2026, 9, 30, 8, 59)),
            ("1", "06:00 SP", _utc(2026, 9, 30, 9, 0)),
        ],
    )

    assert _dates(df) == {
        "cron 01:01 SP": date(2026, 9, 29),
        "manual 17:35 SP": date(2026, 9, 29),
        "manual 11:16 SP": date(2026, 9, 30),
        "05:59 SP": date(2026, 9, 29),
        "06:00 SP": date(2026, 9, 30),
    }


def test_one_row_per_id_and_day_latest_extraction_wins(spark):
    """Três syncs fecham 29/09 (o último vence); um fecha 30/09 — duas fotos, não uma."""
    df = _df(
        spark,
        [
            ("post_a", "sync 28", _utc(2026, 9, 29, 18, 23)),
            ("post_a", "sync 33", _utc(2026, 9, 29, 20, 35)),
            ("post_a", "sync 34", _utc(2026, 9, 30, 4, 1)),
            ("post_a", "sync 38", _utc(2026, 9, 30, 14, 16)),
        ],
    )

    result = FacebookOrganicBronzeTransformer(spark).dedupe(df, DEDUPE_COLUMNS)

    assert _dates(result) == {
        "sync 34": date(2026, 9, 29),
        "sync 38": date(2026, 9, 30),
    }


def test_snapshot_missing_from_raw_survives_via_bronze(spark):
    """A foto de um dia que não está mais no raw (retenção) continua no bronze."""
    existing = _df(spark, [("post_a", "foto de 29/09", _utc(2026, 9, 30, 4, 1))])
    new = _df(spark, [("post_a", "foto de 30/09", _utc(2026, 10, 1, 4, 1))])

    combined = FacebookOrganicBronzeTransformer._accumulate(existing, new)
    result = FacebookOrganicBronzeTransformer(spark).dedupe(combined, DEDUPE_COLUMNS)

    assert set(_dates(result)) == {"foto de 29/09", "foto de 30/09"}


def test_page_insights_id_rebuilt_when_missing(spark):
    """Fotos antigas do `page_insights` vêm sem `id`; sem reconstruí-lo, as 15 métricas de uma
    foto teriam a mesma chave nula e o dedupe as colapsaria numa linha só."""
    raw = spark.createDataFrame(
        [
            (None, "page_media_view", "day", _utc(2026, 9, 30, 4, 3)),
            (None, "page_media_view", "week", _utc(2026, 9, 30, 4, 3)),
            ("p1/insights/page_total_actions/day", "page_total_actions", "day", _utc(2026, 9, 30, 14, 17)),
        ],
        schema=StructType(
            [
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("period", StringType()),
                StructField("_airbyte_extracted_at", TimestampType()),
            ]
        ),
    )

    prepared = FacebookOrganicBronzeTransformer.prepare(raw, "page_insights", page_id=lit("p1"))

    assert {row["id"] for row in prepared.collect()} == {
        "p1/insights/page_media_view/day",
        "p1/insights/page_media_view/week",
        "p1/insights/page_total_actions/day",
    }
    assert {row["page_id"] for row in prepared.collect()} == {"p1"}
