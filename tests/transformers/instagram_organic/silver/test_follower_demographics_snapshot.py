"""Unitário e isolado (sem MinIO): a demografia dos seguidores vira linhas no silver."""
from datetime import date

from src.transformers.instagram_organic.transforms.silver.follower_demographics_snapshot import (
    transform,
)

SCHEMA = (
    "business_account_id string, snapshot_date date, metric string, breakdown string, "
    "value string, _airbyte_raw_id string, _airbyte_extracted_at timestamp, "
    "_airbyte_meta struct<sync_id: bigint>"
)


def test_breakdown_map_is_exploded(spark):
    df = spark.createDataFrame(
        [
            (
                "acc", date(2026, 9, 30), "follower_demographics", "age,gender",
                '{"18-24":5435,"25-34":18284}', "r", None, (1,),
            )
        ],
        schema=SCHEMA,
    )

    rows = transform({"user_lifetime_insights": df}).collect()

    assert {row["breakdown_value"]: row["followers"] for row in rows} == {
        "18-24": 5435,
        "25-34": 18284,
    }
