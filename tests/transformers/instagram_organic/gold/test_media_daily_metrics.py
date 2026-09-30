"""Unitário e isolado (sem MinIO): a evolução diária das mídias no gold.

As regras do delta são as de `src/transformers/snapshots.py`, provadas também no Facebook.
Aqui o que importa é o que é do Instagram: métrica não pedida para o tipo continua nula (não
vira 0 nem gera delta), delta negativo real, e a conservação por mídia.
"""
from datetime import date, datetime, time, timedelta, timezone

from src.transformers.instagram_organic.tables import GOLD_FACTS
from src.transformers.instagram_organic.transforms.gold.media_daily_metrics import (
    ADDITIVE_METRICS,
    transform,
)

FACT = next(fact for fact in GOLD_FACTS if fact.name == "media_daily_metrics")

LIFETIME_COLUMNS = ", ".join(f"{metric}_lifetime bigint" for metric in ADDITIVE_METRICS if metric != "reels_view_total_time")
SNAPSHOT_SCHEMA = (
    "media_id string, business_account_id string, snapshot_date date, snapshot_at timestamp, "
    f"{LIFETIME_COLUMNS}, reels_view_total_time_lifetime double, reels_avg_watch_time double"
)
MEDIA_SCHEMA = (
    "media_id string, created_at timestamp, created_date date, format string, "
    "media_product_type string, permalink string"
)
ACCOUNTS_SCHEMA = "business_account_id string, username string"


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _snap(media_id, day, likes, comments, reach, views=None, follows=None, at=None):
    at = at or datetime.combine(day + timedelta(days=1), time(4, 1), tzinfo=timezone.utc)
    values = {
        "likes": likes, "comments": comments, "reach": reach, "views": views, "saved": 1,
        "shares": 2, "follows": follows, "profile_visits": follows,
    }
    lifetimes = [values[m] for m in ADDITIVE_METRICS if m != "reels_view_total_time"]
    total_time = 10.0 * views if views is not None else None
    return (media_id, "acc", day, at, *lifetimes, total_time, None)


def _media(media_id, created_at, fmt):
    return (media_id, created_at, created_at.date(), fmt, "FEED", "https://instagram.com/p/x")


def _run(spark, snapshots, media):
    sources = {
        "media_insights_snapshot": spark.createDataFrame(snapshots, schema=SNAPSHOT_SCHEMA),
        "media": spark.createDataFrame(media, schema=MEDIA_SCHEMA),
        "accounts": spark.createDataFrame([("acc", "user")], schema=ACCOUNTS_SCHEMA),
    }
    rows = transform(sources, FACT).collect()
    return {(row["media_id"], row["snapshot_date"]): row for row in rows}


def test_metric_not_requested_for_type_has_no_delta(spark):
    rows = _run(
        spark,
        [
            _snap("carousel", date(2026, 9, 29), 190, 5, 3100, follows=3),
            _snap("carousel", date(2026, 9, 30), 200, 5, 3159, follows=3),
        ],
        [_media("carousel", _utc(2026, 9, 11, 21), "carousel")],
    )

    later = rows[("carousel", date(2026, 9, 30))]
    assert later["likes_delta"] == 10
    assert later["reach_delta"] == 59
    assert later["views_lifetime"] is None
    assert later["views_delta"] is None  # carrossel não tem views: nulo, não 0


def test_negative_delta_is_kept(spark):
    """Comentário apagado é variação real, não erro."""
    rows = _run(
        spark,
        [
            _snap("reel", date(2026, 9, 29), 227, 18, 5458, views=6777),
            _snap("reel", date(2026, 9, 30), 226, 16, 5633, views=6778),
        ],
        [_media("reel", _utc(2026, 9, 4, 17), "reel")],
    )

    later = rows[("reel", date(2026, 9, 30))]
    assert later["comments_delta"] == -2
    assert later["likes_delta"] == -1
    assert later["views_delta"] == 1


def test_orphan_snapshot_survives_left_join(spark):
    rows = _run(spark, [_snap("orphan", date(2026, 9, 30), 1, 0, 10)], [])

    row = rows[("orphan", date(2026, 9, 30))]
    assert row["reach_lifetime"] == 10
    assert row["format"] is None
    assert row["username"] == "user"


def test_conservation_per_media(spark):
    """Foto inicial (se `pre_existing`) + Σ delta = total da última foto, métrica a métrica."""
    rows = _run(
        spark,
        [
            _snap("old", date(2026, 9, 28), 100, 5, 1000, views=2000),
            _snap("old", date(2026, 9, 29), 110, 4, 1100, views=2100),
            _snap("old", date(2026, 10, 2), 115, 6, 1090, views=2400),
            _snap("new", date(2026, 9, 29), 10, 0, 50, views=60, at=_utc(2026, 9, 30, 4, 1)),
            _snap("new", date(2026, 9, 30), 40, 2, 300, views=400),
        ],
        [_media("old", _utc(2025, 6, 2), "reel"), _media("new", _utc(2026, 9, 29, 14), "reel")],
    )

    for media_id in ("old", "new"):
        series = sorted(
            (row for key, row in rows.items() if key[0] == media_id),
            key=lambda row: row["snapshot_date"],
        )
        for metric in ADDITIVE_METRICS:
            first = series[0]
            if first[f"{metric}_lifetime"] is None:
                continue
            baseline = first[f"{metric}_lifetime"] if first["baseline_kind"] == "pre_existing" else 0
            deltas = sum(row[f"{metric}_delta"] or 0 for row in series)
            assert baseline + deltas == series[-1][f"{metric}_lifetime"], (media_id, metric)
