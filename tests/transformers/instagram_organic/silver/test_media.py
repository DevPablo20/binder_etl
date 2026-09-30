"""Unitário e isolado (sem MinIO): a dimensão de mídia no silver."""
from datetime import date, datetime, timezone

from src.transformers.instagram_organic.transforms.silver.media import transform

SCHEMA = (
    "id string, business_account_id string, username string, snapshot_date date, "
    "`timestamp` timestamp, media_type string, media_product_type string, caption string, "
    "permalink string, is_comment_enabled boolean, thumbnail_url string, "
    "_airbyte_raw_id string, _airbyte_extracted_at timestamp, _airbyte_meta struct<sync_id: bigint>"
)


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _row(media_id, media_type, product, day, caption, extracted_at):
    return (
        media_id, "acc", "user", day, _utc(2026, 9, 29, 2, 30), media_type, product, caption,
        "https://instagram.com/p/x", True, None, "raw", extracted_at, (1,),
    )


def test_format_latest_snapshot_and_last_seen(spark):
    df = spark.createDataFrame(
        [
            _row("reel", "VIDEO", "REELS", date(2026, 9, 29), "antes", _utc(2026, 9, 30, 5)),
            _row("reel", "VIDEO", "REELS", date(2026, 9, 30), "depois", _utc(2026, 10, 1, 4)),
            _row("carousel", "CAROUSEL_ALBUM", "FEED", date(2026, 9, 29), "", _utc(2026, 9, 30, 5)),
            _row("image", "IMAGE", "FEED", date(2026, 9, 29), "", _utc(2026, 9, 30, 5)),
            _row("video", "VIDEO", "FEED", date(2026, 9, 29), "", _utc(2026, 9, 30, 5)),
        ],
        schema=SCHEMA,
    )

    rows = {row["media_id"]: row for row in transform({"media": df}).collect()}

    assert {media_id: row["format"] for media_id, row in rows.items()} == {
        "reel": "reel",
        "carousel": "carousel",
        "image": "image",
        "video": "video",
    }
    assert rows["reel"]["caption"] == "depois"
    assert rows["reel"]["last_seen_date"] == date(2026, 9, 30)
    assert rows["reel"]["created_date"] == date(2026, 9, 28)  # 02:30 UTC = 23:30 de 28/09 em SP
