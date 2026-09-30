"""Unitário e isolado (sem MinIO): a evolução diária dos posts no gold.

A Meta só entrega o total acumulado; o delta é a diferença entre duas fotos consecutivas. Os
testes provam as regras que dão significado a essa diferença: a primeira foto (`new_post` ×
`pre_existing`), o buraco na série, o delta negativo, as reações por tipo, o fato órfão — e a
conservação por post, a invariante desta plataforma.
"""
from datetime import date, datetime, time, timedelta, timezone

from src.transformers.facebook_organic.tables import GOLD_FACTS
from src.transformers.facebook_organic.transforms.gold.post_daily_metrics import (
    ADDITIVE_METRICS,
    transform,
)

FACT = next(fact for fact in GOLD_FACTS if fact.name == "post_daily_metrics")

SNAPSHOT_SCHEMA = (
    "post_id string, page_id string, snapshot_date date, snapshot_at timestamp, "
    "media_views_lifetime bigint, reach_lifetime bigint, clicks_lifetime bigint, "
    "reactions_total_lifetime bigint, reactions_by_type_lifetime map<string, bigint>, "
    "shares_lifetime bigint"
)
POSTS_SCHEMA = (
    "post_id string, created_at timestamp, created_date date, status_type string, "
    "media_type string"
)
PAGES_SCHEMA = "page_id string, page_name string"


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _snap(post_id, day, views, *, at=None, reactions=None, shares=None):
    at = at or datetime.combine(day + timedelta(days=1), time(4, 1), tzinfo=timezone.utc)
    total = sum(reactions.values()) if reactions is not None else None
    return (post_id, "p1", day, at, views, views, 0, total, reactions, shares)


def _post(post_id, created_at):
    return (post_id, created_at, created_at.date(), "added_photos", "photo")


def _run(spark, snapshots, posts):
    sources = {
        "post_insights_snapshot": spark.createDataFrame(snapshots, schema=SNAPSHOT_SCHEMA),
        "posts": spark.createDataFrame(posts, schema=POSTS_SCHEMA),
        "pages": spark.createDataFrame([("p1", "Page")], schema=PAGES_SCHEMA),
    }
    rows = transform(sources, FACT).collect()
    return {(row["post_id"], row["snapshot_date"]): row for row in rows}


def test_delta_is_difference_between_consecutive_snapshots(spark):
    old = _utc(2026, 1, 1, 12)
    rows = _run(
        spark,
        [
            _snap("p1_a", date(2026, 9, 28), 100),
            _snap("p1_a", date(2026, 9, 29), 160),
            _snap("p1_a", date(2026, 9, 30), 190),
        ],
        [_post("p1_a", old)],
    )

    first = rows[("p1_a", date(2026, 9, 28))]
    assert first["baseline_kind"] == "pre_existing"
    assert first["media_views_delta"] is None  # histórico anterior desconhecido
    assert rows[("p1_a", date(2026, 9, 29))]["media_views_delta"] == 60
    assert rows[("p1_a", date(2026, 9, 30))]["media_views_delta"] == 30
    assert rows[("p1_a", date(2026, 9, 30))]["gap_days"] == 1


def test_new_post_counts_whole_life_on_first_snapshot(spark):
    """Publicado 13h antes da foto: a vida inteira do post aconteceu dentro da janela."""
    rows = _run(
        spark,
        [
            _snap("p1_new", date(2026, 9, 29), 40, at=_utc(2026, 9, 30, 4, 1)),
            _snap("p1_new", date(2026, 9, 30), 51, at=_utc(2026, 9, 30, 14, 16)),
        ],
        [_post("p1_new", _utc(2026, 9, 29, 14, 50))],
    )

    first = rows[("p1_new", date(2026, 9, 29))]
    second = rows[("p1_new", date(2026, 9, 30))]
    assert first["baseline_kind"] == "new_post"
    assert first["media_views_delta"] == 40
    assert second["baseline_kind"] is None
    assert second["media_views_delta"] == 11
    assert second["hours_since_prev"] == 10.25


def test_gap_is_not_interpolated_and_negative_delta_is_kept(spark):
    rows = _run(
        spark,
        [
            _snap("p1_a", date(2026, 9, 28), 100),
            _snap("p1_a", date(2026, 9, 30), 130),  # 29/09 faltou
            _snap("p1_a", date(2026, 10, 1), 125),  # a Meta revisou para baixo
        ],
        [_post("p1_a", _utc(2026, 1, 1, 12))],
    )

    assert len(rows) == 3  # nenhuma linha inventada para 29/09
    gap = rows[("p1_a", date(2026, 9, 30))]
    assert gap["gap_days"] == 2
    assert gap["media_views_delta"] == 30
    assert rows[("p1_a", date(2026, 10, 1))]["media_views_delta"] == -5


def test_reaction_columns_zero_for_absent_key_null_for_absent_map(spark):
    rows = _run(
        spark,
        [
            _snap("p1_a", date(2026, 9, 29), 1, reactions={"like": 3}),
            _snap("p1_a", date(2026, 9, 30), 1, reactions={"like": 4, "love": 1}),
            _snap("p1_b", date(2026, 9, 30), 1, reactions=None),
        ],
        [_post("p1_a", _utc(2026, 1, 1)), _post("p1_b", _utc(2026, 1, 1))],
    )

    later = rows[("p1_a", date(2026, 9, 30))]
    assert later["reaction_like_delta"] == 1
    assert later["reaction_love_delta"] == 1  # 0 → 1: `love` ausente no dia anterior vale 0
    assert later["reaction_haha_lifetime"] == 0
    assert rows[("p1_b", date(2026, 9, 30))]["reaction_like_lifetime"] is None


def test_orphan_snapshot_survives_left_join(spark):
    """Foto de post sem linha em `posts`: sobrevive, com os atributos nulos."""
    rows = _run(spark, [_snap("p1_orphan", date(2026, 9, 30), 5)], [])

    row = rows[("p1_orphan", date(2026, 9, 30))]
    assert row["media_views_lifetime"] == 5
    assert row["created_at"] is None
    assert row["page_name"] == "Page"


def test_conservation_per_post(spark):
    """Foto inicial (se `pre_existing`) + Σ delta = total da última foto, métrica a métrica."""
    rows = _run(
        spark,
        [
            _snap("p1_old", date(2026, 9, 28), 100, reactions={"like": 1}, shares=2),
            _snap("p1_old", date(2026, 9, 29), 110, reactions={"like": 1}, shares=2),
            _snap("p1_old", date(2026, 10, 2), 108, reactions={"like": 3, "wow": 1}, shares=5),
            _snap("p1_new", date(2026, 9, 29), 40, at=_utc(2026, 9, 30, 4, 1), reactions={}, shares=0),
            _snap("p1_new", date(2026, 9, 30), 51, reactions={"love": 2}, shares=1),
        ],
        [_post("p1_old", _utc(2025, 6, 2)), _post("p1_new", _utc(2026, 9, 29, 14, 50))],
    )

    for post_id in ("p1_old", "p1_new"):
        series = sorted(
            (row for key, row in rows.items() if key[0] == post_id),
            key=lambda row: row["snapshot_date"],
        )
        for metric in ADDITIVE_METRICS:
            baseline = series[0][f"{metric}_lifetime"] if series[0]["baseline_kind"] == "pre_existing" else 0
            deltas = sum(row[f"{metric}_delta"] or 0 for row in series)
            assert baseline + deltas == series[-1][f"{metric}_lifetime"], (post_id, metric)
