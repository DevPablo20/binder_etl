"""Platform → catalog transform registry for the FastAPI catalog API."""
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from src.transformers.tiktok.transforms.catalog import CATALOG_TRANSFORMS as TIKTOK_CATALOG

CatalogTransform = Callable[[SparkSession], DataFrame]

CATALOG_BY_PLATFORM: dict[str, dict[str, CatalogTransform]] = {
    "tiktok": TIKTOK_CATALOG,
}


def get_catalog_transforms(platform: str) -> dict[str, CatalogTransform]:
    try:
        return CATALOG_BY_PLATFORM[platform]
    except KeyError as exc:
        raise KeyError(f"No catalog transforms registered for platform: {platform}") from exc
