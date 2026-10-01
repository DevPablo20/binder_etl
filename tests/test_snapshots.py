"""Unitário e isolado (sem MinIO): as regras do delta entre fotos diárias.

`src/transformers/snapshots.py` é compartilhado pelo Facebook e pelo Instagram — a Meta só
entrega o total acumulado dos posts, e a atividade do dia é a diferença entre duas fotos. Os
testes provam o que dá significado a essa diferença: a primeira foto (`new_post` ×
`pre_existing`), o buraco na série, o delta negativo e a conservação.
"""
from datetime import date, datetime, time, timedelta, timezone

from src.transformers.snapshots import add_lifetime_deltas

SCHEMA = (
    "post_id string, snapshot_date date, snapshot_at timestamp, created_at timestamp, "
    "created_date date, views_lifetime bigint, likes_lifetime bigint"
)
METRICS = ("views", "likes")


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _snap(post_id, day, views, likes=0, *, created=None, at=None):
    at = at or datetime.combine(day + timedelta(days=1), time(4, 1), tzinfo=timezone.utc)
    created = created or _utc(2026, 1, 1, 12)
    return (post_id, day, at, created, created.date(), views, likes)


def _run(spark, rows):
    df = add_lifetime_deltas(spark.createDataFrame(rows, schema=SCHEMA), "post_id", METRICS)
    return {(row["post_id"], row["snapshot_date"]): row for row in df.collect()}


def test_delta_is_difference_between_consecutive_snapshots(spark):
    rows = _run(
        spark,
        [
            _snap("a", date(2026, 9, 28), 100),
            _snap("a", date(2026, 9, 29), 160),
            _snap("a", date(2026, 9, 30), 190),
        ],
    )

    first = rows[("a", date(2026, 9, 28))]
    assert first["baseline_kind"] == "pre_existing"
    assert first["views_delta"] is None  # histórico anterior desconhecido
    assert rows[("a", date(2026, 9, 29))]["views_delta"] == 60
    assert rows[("a", date(2026, 9, 30))]["views_delta"] == 30
    assert rows[("a", date(2026, 9, 30))]["gap_days"] == 1


def test_new_post_counts_whole_life_on_first_snapshot(spark):
    """Publicado 13h antes da foto: a vida inteira do post aconteceu dentro da janela."""
    created = _utc(2026, 9, 29, 14, 50)
    rows = _run(
        spark,
        [
            _snap("new", date(2026, 9, 29), 40, created=created, at=_utc(2026, 9, 30, 4, 1)),
            _snap("new", date(2026, 9, 30), 51, created=created, at=_utc(2026, 9, 30, 14, 16)),
        ],
    )

    first, second = rows[("new", date(2026, 9, 29))], rows[("new", date(2026, 9, 30))]
    assert first["baseline_kind"] == "new_post"
    assert first["views_delta"] == 40
    assert second["baseline_kind"] is None
    assert second["views_delta"] == 11
    assert second["hours_since_prev"] == 10.25


def test_gap_is_not_interpolated_and_negative_delta_is_kept(spark):
    rows = _run(
        spark,
        [
            _snap("a", date(2026, 9, 28), 100),
            _snap("a", date(2026, 9, 30), 130),  # 29/09 faltou
            _snap("a", date(2026, 10, 1), 125),  # a Meta revisou para baixo
        ],
    )

    assert len(rows) == 3  # nenhuma linha inventada para 29/09
    assert rows[("a", date(2026, 9, 30))]["gap_days"] == 2
    assert rows[("a", date(2026, 9, 30))]["views_delta"] == 30
    assert rows[("a", date(2026, 10, 1))]["views_delta"] == -5


def test_null_metric_gives_null_delta(spark):
    """Métrica que não existia numa foto (campo ainda não selecionado) não vira 0."""
    rows = _run(
        spark,
        [
            _snap("a", date(2026, 9, 29), 10, likes=None),
            _snap("a", date(2026, 9, 30), 12, likes=200),
        ],
    )

    assert rows[("a", date(2026, 9, 30))]["likes_delta"] is None
    assert rows[("a", date(2026, 9, 30))]["views_delta"] == 2


def test_conservation_per_post(spark):
    """Foto inicial (se `pre_existing`) + Σ delta = total da última foto, métrica a métrica."""
    rows = _run(
        spark,
        [
            _snap("old", date(2026, 9, 28), 100, 1),
            _snap("old", date(2026, 9, 29), 110, 1),
            _snap("old", date(2026, 10, 2), 108, 4),
            _snap("new", date(2026, 9, 29), 40, 0, created=_utc(2026, 9, 29, 14), at=_utc(2026, 9, 30, 4)),
            _snap("new", date(2026, 9, 30), 51, 2, created=_utc(2026, 9, 29, 14)),
        ],
    )

    for post_id in ("old", "new"):
        series = sorted(
            (row for key, row in rows.items() if key[0] == post_id),
            key=lambda row: row["snapshot_date"],
        )
        for metric in METRICS:
            first = series[0]
            baseline = first[f"{metric}_lifetime"] if first["baseline_kind"] == "pre_existing" else 0
            deltas = sum(row[f"{metric}_delta"] or 0 for row in series)
            assert baseline + deltas == series[-1][f"{metric}_lifetime"], (post_id, metric)
