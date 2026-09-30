"""Unitário e isolado (sem MinIO): stories no silver — uma linha por story, última leitura."""
from datetime import datetime, timezone

from src.transformers.instagram_organic.transforms.silver.stories import transform

STORIES_SCHEMA = (
    "id string, business_account_id string, `timestamp` timestamp, media_type string, "
    "caption string, permalink string, shortcode string, _airbyte_raw_id string, "
    "_airbyte_extracted_at timestamp, _airbyte_meta struct<sync_id: bigint>"
)
INSIGHTS_SCHEMA = (
    "id string, reach bigint, views bigint, shares bigint, follows bigint, replies bigint, "
    "profile_visits bigint, total_interactions bigint, _airbyte_extracted_at timestamp"
)


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_story_without_insights_survives_and_age_at_last_read(spark):
    stories = spark.createDataFrame(
        [
            ("s1", "acc", _utc(2026, 9, 30, 1, 0), "VIDEO", None, "p", "c", "r", _utc(2026, 10, 1, 0, 0), (1,)),
            ("s2", "acc", _utc(2026, 9, 30, 18, 40), "IMAGE", None, "p", "c", "r", _utc(2026, 9, 30, 19, 0), (1,)),
        ],
        schema=STORIES_SCHEMA,
    )
    insights = spark.createDataFrame(
        [("s1", 5358, 6069, 15, 0, 1, 32, 48, _utc(2026, 10, 1, 0, 0))], schema=INSIGHTS_SCHEMA
    )

    rows = {row["story_id"]: row for row in transform({"stories": stories, "story_insights": insights}).collect()}

    assert rows["s1"]["reach"] == 5358
    assert rows["s1"]["hours_live_at_last_read"] == 23.0
    assert rows["s2"]["reach"] is None  # menos de 5 visualizações: a Meta devolve erro
