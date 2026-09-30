"""Unitário e isolado (sem MinIO): a dimensão de posts no silver.

Prova a última foto como dado atual, o filtro de posts da própria página publicados, o
`last_seen_date` e o formato derivado do link e do `status_type` (o `attachments` do conector
chega vazio).
"""
from datetime import date, datetime, timezone

from src.transformers.facebook_organic.transforms.silver.posts import transform

SCHEMA = (
    "id string, page_id string, snapshot_date date, `from` string, created_time timestamp, "
    "message string, permalink_url string, status_type string, is_published boolean, "
    "is_hidden boolean, is_expired boolean, full_picture string, "
    "_airbyte_raw_id string, _airbyte_extracted_at timestamp, _airbyte_meta struct<sync_id: bigint>"
)
PAGE = '{"name":"Page","id":"p1"}'
VISITOR = '{"name":"Visitor","id":"u9"}'


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _row(post_id, day, message, extracted_at, *, author=PAGE, published=True,
         permalink="https://www.facebook.com/p1/posts/1", status="added_photos"):
    return (
        post_id, "p1", day, author, _utc(2026, 9, 29, 14, 50), message, permalink, status,
        published, False, False, None, f"raw-{post_id}-{day}", extracted_at, (1,),
    )


def _run(spark, rows):
    df = spark.createDataFrame(rows, schema=SCHEMA)
    return {row["post_id"]: row for row in transform({"post": df}).collect()}


def test_latest_snapshot_wins_and_last_seen_is_tracked(spark):
    rows = _run(
        spark,
        [
            _row("p1_a", date(2026, 9, 29), "antes da edição", _utc(2026, 9, 30, 4, 1)),
            _row("p1_a", date(2026, 9, 30), "depois da edição", _utc(2026, 10, 1, 4, 1)),
            _row("p1_gone", date(2026, 9, 29), "sumiu depois", _utc(2026, 9, 30, 4, 1)),
        ],
    )

    assert rows["p1_a"]["message"] == "depois da edição"
    assert rows["p1_a"]["last_seen_date"] == date(2026, 9, 30)
    assert rows["p1_gone"]["last_seen_date"] == date(2026, 9, 29)
    assert rows["p1_a"]["created_date"] == date(2026, 9, 29)  # 14:50 UTC = 11:50 em SP


def test_keeps_only_published_posts_by_the_page(spark):
    at = _utc(2026, 9, 30, 4, 1)
    day = date(2026, 9, 29)
    rows = _run(
        spark,
        [
            _row("p1_own", day, "", at),
            _row("p1_visitor", day, "", at, author=VISITOR),
            _row("p1_draft", day, "", at, published=False),
            _row("p1_old_file", day, "", at, published=None),  # campo ainda não selecionado
        ],
    )

    assert set(rows) == {"p1_own", "p1_old_file"}


def test_media_type_from_permalink_and_status(spark):
    at = _utc(2026, 9, 30, 4, 1)
    day = date(2026, 9, 29)
    rows = _run(
        spark,
        [
            _row("p1_reel", day, "", at, permalink="https://www.facebook.com/reel/123/", status="added_video"),
            _row("p1_video", day, "", at, status="added_video"),
            _row("p1_photo", day, "", at, status="added_photos"),
            _row("p1_status", day, "", at, status="mobile_status_update"),
        ],
    )

    assert {post_id: row["media_type"] for post_id, row in rows.items()} == {
        "p1_reel": "reel",
        "p1_video": "video",
        "p1_photo": "photo",
        "p1_status": "status",
    }
