"""Unitário e isolado (sem MinIO): a série diária das métricas de mídia no silver.

O conector pede métricas diferentes por tipo de mídia: a não pedida vem nula e continua nula.
Curtidas e comentários vêm do `like_count`/`comments_count` do stream `media`, que os traz para
todos os tipos — o `/insights` não os pede para carrossel.
"""
from datetime import date, datetime, timezone

from src.transformers.instagram_organic.transforms.silver.media_metrics_daily import (
    transform,
)

INSIGHTS_SCHEMA = (
    "id string, business_account_id string, snapshot_date date, likes bigint, comments bigint, "
    "reach bigint, views bigint, saved bigint, shares bigint, follows bigint, "
    "profile_visits bigint, ig_reels_avg_watch_time double, "
    "ig_reels_video_view_total_time double, _airbyte_raw_id string, "
    "_airbyte_extracted_at timestamp, _airbyte_meta struct<sync_id: bigint>"
)
MEDIA_SCHEMA = (
    "id string, snapshot_date date, like_count bigint, comments_count bigint, "
    "`timestamp` timestamp, _airbyte_extracted_at timestamp"
)
PUBLISHED = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)

DAY = date(2026, 9, 30)
AT = datetime(2026, 9, 30, 18, 5, tzinfo=timezone.utc)


def _insight(media_id, likes=None, comments=None, reach=100, views=None, follows=None, day=DAY):
    return (
        media_id, "acc", day, likes, comments, reach, views, 1, 2, follows, None,
        None, None, f"raw-{media_id}", AT, (1,),
    )


def _run(spark, insights, media):
    sources = {
        "media_insights": spark.createDataFrame(insights, schema=INSIGHTS_SCHEMA),
        "media": spark.createDataFrame(media, schema=MEDIA_SCHEMA),
    }
    return {row["media_id"]: row for row in transform(sources).collect()}


def test_carousel_likes_come_from_media_stream(spark):
    """Carrossel: o `/insights` não traz curtidas; o `like_count` traz — e é o orgânico."""
    rows = _run(spark, [_insight("carousel")], [("carousel", DAY, 200, 5, PUBLISHED, AT)])

    assert rows["carousel"]["likes_lifetime"] == 200
    assert rows["carousel"]["comments_lifetime"] == 5


def test_falls_back_to_insights_before_field_selection(spark):
    """Fotos de antes da seleção de `like_count`: usa o `likes` do `/insights`."""
    rows = _run(
        spark,
        [_insight("reel", likes=227, comments=18)],
        [("reel", DAY, None, None, PUBLISHED, AT)],
    )

    assert rows["reel"]["likes_lifetime"] == 227
    assert rows["reel"]["comments_lifetime"] == 18


def test_metric_not_requested_for_type_stays_null(spark):
    rows = _run(
        spark,
        [_insight("image", views=None, follows=3), _insight("reel", views=6778, follows=None)],
        [("image", DAY, 1, 0, PUBLISHED, AT), ("reel", DAY, 1, 0, PUBLISHED, AT)],
    )

    assert rows["image"]["views_lifetime"] is None
    assert rows["image"]["follows_lifetime"] == 3
    assert rows["reel"]["views_lifetime"] == 6778
    assert rows["reel"]["follows_lifetime"] is None


def test_insight_without_media_row_survives(spark):
    rows = _run(spark, [_insight("orphan", likes=4)], [])

    assert rows["orphan"]["reach_lifetime"] == 100
    assert rows["orphan"]["likes_lifetime"] == 4


def test_series_delta_stays_null_for_metric_not_requested(spark):
    """Carrossel em dois dias: curtidas têm delta; `views` não é pedido e fica nulo nas duas
    colunas — nunca vira 0."""
    day2 = date(2026, 10, 1)
    sources = {
        "media_insights": spark.createDataFrame(
            [_insight("carousel", reach=3100), _insight("carousel", reach=3159, day=day2)],
            schema=INSIGHTS_SCHEMA,
        ),
        "media": spark.createDataFrame(
            [
                ("carousel", DAY, 190, 5, PUBLISHED, AT),
                ("carousel", day2, 200, 5, PUBLISHED, AT),
            ],
            schema=MEDIA_SCHEMA,
        ),
    }

    rows = {row["snapshot_date"]: row for row in transform(sources).collect()}

    later = rows[day2]
    assert later["likes_delta"] == 10
    assert later["reach_delta"] == 59
    assert later["views_lifetime"] is None
    assert later["views_delta"] is None
