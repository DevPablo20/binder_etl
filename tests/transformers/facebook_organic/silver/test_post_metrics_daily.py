"""Unitário e isolado (sem MinIO): a série diária das métricas de post no silver.

Prova as regras de leitura das métricas — só `lifetime`, map vazio × map ausente, cliques pela
soma do breakdown, `shares` ausente vale 0 (exceto antes da seleção de campos) — e que a série
sai com o delta por post. As regras do delta em si estão provadas em `tests/test_snapshots.py`.
"""
from datetime import date, datetime, timezone

from src.transformers.facebook_organic.transforms.silver.post_metrics_daily import (
    transform,
)

INSIGHTS_SCHEMA = (
    "id string, page_id string, snapshot_date date, name string, period string, "
    "values array<struct<value: struct<type: string, object: string, string: string, "
    "integer: bigint>, end_time: timestamp>>, "
    "_airbyte_raw_id string, _airbyte_extracted_at timestamp"
)
POST_SCHEMA = (
    "id string, snapshot_date date, shares string, is_published boolean, "
    "created_time timestamp, _airbyte_extracted_at timestamp"
)
OLD_POST = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)

DAY = date(2026, 9, 30)
AT = datetime(2026, 9, 30, 4, 1, tzinfo=timezone.utc)


def _metric(post_id, name, period="lifetime", integer=None, obj=None, day=DAY):
    value_type = "object" if obj is not None else "integer"
    return (
        f"{post_id}/insights/{name}/{period}",
        "p1",
        day,
        name,
        period,
        [((value_type, obj, None, integer), None)],
        f"raw-{post_id}-{name}-{period}",
        AT,
    )


def _run(spark, metrics, posts):
    sources = {
        "post_insights": spark.createDataFrame(metrics, schema=INSIGHTS_SCHEMA),
        "post": spark.createDataFrame(posts, schema=POST_SCHEMA),
    }
    return {
        (row["post_id"], row["snapshot_date"]): row for row in transform(sources).collect()
    }


def test_pivots_lifetime_metrics_and_drops_day_period(spark):
    rows = _run(
        spark,
        [
            _metric("p1_a", "post_media_view", integer=40),
            _metric("p1_a", "post_total_media_view_unique", integer=47),
            _metric("p1_a", "post_total_media_view_unique", period="day", integer=0),
            _metric("p1_a", "post_clicks_by_type", obj='{"photo view":1,"other clicks":2}'),
            _metric("p1_a", "post_reactions_by_type_total", obj='{"like":9,"love":1}'),
        ],
        [("p1_a", DAY, None, True, OLD_POST, AT)],
    )

    row = rows[("p1_a", DAY)]
    assert row["media_views_lifetime"] == 40
    assert row["reach_lifetime"] == 47  # o período `day` (0) não sobrescreveu o lifetime
    assert row["clicks_lifetime"] == 3
    assert row["reactions_total_lifetime"] == 10
    assert row["reactions_by_type_lifetime"] == {"like": 9, "love": 1}


def test_empty_map_is_zero_missing_map_is_null(spark):
    """`{}` = a API respondeu e não houve nada; métrica ausente = desconhecido."""
    rows = _run(
        spark,
        [
            _metric("p1_empty", "post_media_view", integer=10),
            _metric("p1_empty", "post_reactions_by_type_total", obj="{}"),
            _metric("p1_empty", "post_clicks_by_type", obj="{}"),
            _metric("p1_missing", "post_media_view", integer=10),
        ],
        [
            ("p1_empty", DAY, None, True, OLD_POST, AT),
            ("p1_missing", DAY, None, True, OLD_POST, AT),
        ],
    )

    assert rows[("p1_empty", DAY)]["reactions_total_lifetime"] == 0
    assert rows[("p1_empty", DAY)]["clicks_lifetime"] == 0
    assert rows[("p1_missing", DAY)]["reactions_total_lifetime"] is None
    assert rows[("p1_missing", DAY)]["clicks_lifetime"] is None


def test_shares_absent_is_zero_but_unknown_before_field_selection(spark):
    rows = _run(
        spark,
        [
            _metric("p1_shared", "post_media_view", integer=1),
            _metric("p1_unshared", "post_media_view", integer=1),
            _metric("p1_old_file", "post_media_view", integer=1),
        ],
        [
            ("p1_shared", DAY, '{"count":7}', True, OLD_POST, AT),
            ("p1_unshared", DAY, None, True, OLD_POST, AT),
            ("p1_old_file", DAY, None, None, OLD_POST, AT),  # arquivo sem as colunas novas
        ],
    )

    assert rows[("p1_shared", DAY)]["shares_lifetime"] == 7
    assert rows[("p1_unshared", DAY)]["shares_lifetime"] == 0
    assert rows[("p1_old_file", DAY)]["shares_lifetime"] is None


def test_post_without_post_row_keeps_metrics(spark):
    """O `shares` entra por LEFT JOIN: foto de métrica sem linha no `post` não some."""
    rows = _run(spark, [_metric("p1_orphan", "post_media_view", integer=5)], [])

    assert rows[("p1_orphan", DAY)]["media_views_lifetime"] == 5
    assert rows[("p1_orphan", DAY)]["shares_lifetime"] is None


def test_series_has_delta_and_reaction_columns_per_post(spark):
    """Duas fotos do mesmo post: o delta sai por post, inclusive por tipo de reação."""
    day2 = date(2026, 10, 1)
    rows = _run(
        spark,
        [
            _metric("p1_a", "post_media_view", integer=40),
            _metric("p1_a", "post_reactions_by_type_total", obj='{"like":3}'),
            _metric("p1_a", "post_media_view", integer=51, day=day2),
            _metric("p1_a", "post_reactions_by_type_total", obj='{"like":4,"love":1}', day=day2),
        ],
        [("p1_a", DAY, None, True, OLD_POST, AT), ("p1_a", day2, None, True, OLD_POST, AT)],
    )

    first, second = rows[("p1_a", DAY)], rows[("p1_a", day2)]
    assert first["baseline_kind"] == "pre_existing"
    assert first["media_views_delta"] is None
    assert second["media_views_delta"] == 11
    assert second["reaction_like_delta"] == 1
    assert second["reaction_love_delta"] == 1  # `love` ausente na foto anterior vale 0
    assert second["reaction_haha_lifetime"] == 0
