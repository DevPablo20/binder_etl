"""Smoke test: TikTok catalog transforms read silver Delta and shape hierarchy rows."""
from pyspark.sql.utils import AnalysisException

from src.io.reader import read_delta
from src.transformers.catalog import get_catalog_transforms
from src.transformers.tiktok.tables import CATALOG_ENTITIES

CATALOG_COLUMNS = {
    "platform",
    "object_type",
    "account_id",
    "account_name",
    "campaign_id",
    "campaign_name",
    "ad_group_id",
    "ad_group_name",
    "ad_id",
    "ad_name",
}


def _silver_available(spark, table_path: str) -> bool:
    try:
        df = read_delta(spark, "silver", table_path)
        return not df.isEmpty()
    except (AnalysisException, Exception) as exc:
        text = str(exc).lower()
        if any(
            marker in text
            for marker in (
                "path does not exist",
                "does not exist",
                "unable to infer schema",
                "no such file",
                "404",
            )
        ):
            return False
        raise


def test_catalog_tiktok_shapes_hierarchy_rows(spark):
    if not any(
        _silver_available(spark, entity.silver_table_path) for entity in CATALOG_ENTITIES
    ):
        return

    transforms = get_catalog_transforms("tiktok")
    assert set(transforms) == {"account", "campaign", "ad_group", "ad"}

    for object_type, transform in transforms.items():
        entity = next(e for e in CATALOG_ENTITIES if e.object_type == object_type)
        if not _silver_available(spark, entity.silver_table_path):
            continue
        for source_path in entity.silver_source_paths:
            if not _silver_available(spark, source_path):
                break
        else:
            df = transform(spark)
            assert set(df.columns) >= CATALOG_COLUMNS
            if df.isEmpty():
                continue

            row = df.first().asDict()
            assert row["platform"] == "tiktok"
            assert row["object_type"] == object_type
            assert row["account_id"] is not None
            assert row["account_name"] is not None

            if object_type == "account":
                assert row["campaign_id"] is None
                assert row["ad_group_id"] is None
                assert row["ad_id"] is None
            elif object_type == "campaign":
                assert row["campaign_id"] is not None
                assert row["ad_group_id"] is None
                assert row["ad_id"] is None
            elif object_type == "ad_group":
                assert row["campaign_id"] is not None
                assert row["ad_group_id"] is not None
                assert row["ad_id"] is None
            elif object_type == "ad":
                assert row["campaign_id"] is not None
                assert row["ad_group_id"] is not None
                assert row["ad_id"] is not None


def test_catalog_platform_registry_has_tiktok():
    transforms = get_catalog_transforms("tiktok")
    assert "account" in transforms
    assert "ad" in transforms
