"""Unitário e isolado (sem MinIO): acumulação do bronze.

`_accumulate` é pura — só decide o que unir, não faz I/O. O teste que importa é o
terceiro: uma linha presente no bronze acumulado mas ausente do raw desta rodada (o
cenário de retenção no raw, ou de conector só-Overwrite) tem que sobreviver depois de
`_accumulate` + `dedupe`. Sem acumulação, ela simplesmente sumiria.
"""
import pytest

from src.spark_session import get_spark_session
from src.transformers.tiktok.bronze import TikTokBronzeTransformer

DEDUPE_COLUMNS = ["object_id"]


def _row(object_id: str, value: str, extracted_at: str):
    return (object_id, value, extracted_at)


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session(app_name="bronze-accumulate-test", master="local[2]")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()


def _df(spark, rows):
    return spark.createDataFrame(
        list(rows), schema=["object_id", "value", "_airbyte_extracted_at"]
    )


def test_accumulate_returns_new_when_no_existing_bronze(spark):
    """Primeira execução — bronze ainda não existe."""
    new = _df(spark, [_row("1", "v1", "2026-09-01")])

    combined = TikTokBronzeTransformer._accumulate(None, new)

    assert combined.count() == 1


def test_accumulate_unions_existing_and_new(spark):
    existing = _df(spark, [_row("1", "v1", "2026-09-01")])
    new = _df(spark, [_row("2", "v2", "2026-09-02")])

    combined = TikTokBronzeTransformer._accumulate(existing, new)

    assert combined.count() == 2
    ids = {row["object_id"] for row in combined.collect()}
    assert ids == {"1", "2"}


def test_object_missing_from_raw_survives_via_bronze(spark):
    """O cenário real: um objeto que não veio no raw desta rodada (retenção, ou raw
    Overwrite de um conector sem Incremental) continua no bronze porque já estava lá."""
    existing = _df(
        spark,
        [
            _row("1", "existia-ontem", "2026-09-01"),  # sumiu do raw hoje
            _row("2", "v2-antigo", "2026-09-01"),
        ],
    )
    new = _df(spark, [_row("2", "v2-atualizado", "2026-09-02")])  # só o 2 veio hoje

    combined = TikTokBronzeTransformer._accumulate(existing, new)
    result = TikTokBronzeTransformer(spark).dedupe(combined, DEDUPE_COLUMNS)

    rows = {row["object_id"]: row["value"] for row in result.collect()}
    assert rows == {"1": "existia-ontem", "2": "v2-atualizado"}


def test_dedupe_keeps_latest_version_across_rounds(spark):
    """SCD tipo 1 continua valendo entre rodadas: a versão mais recente vence, sem
    duplicar a linha do objeto."""
    existing = _df(spark, [_row("1", "versao-velha", "2026-09-01")])
    new = _df(spark, [_row("1", "versao-nova", "2026-09-02")])

    combined = TikTokBronzeTransformer._accumulate(existing, new)
    result = TikTokBronzeTransformer(spark).dedupe(combined, DEDUPE_COLUMNS)

    assert result.count() == 1
    assert result.collect()[0]["value"] == "versao-nova"
