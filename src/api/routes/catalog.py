"""Catalog API: lake hierarchy identities for backend Bridge discovery."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Query, Request
from pyspark.sql import DataFrame, SparkSession

from src.api.schemas import CatalogItem, ObjectType
from src.transformers import PLATFORMS, get_catalog_transforms

router = APIRouter(prefix="/catalog", tags=["catalog"])

CATALOG_COLUMNS = (
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
)


def _dataframe_to_items(df: DataFrame) -> list[CatalogItem]:
    rows = df.select(*CATALOG_COLUMNS).collect()
    return [CatalogItem.model_validate(row.asDict(recursive=True)) for row in rows]


def _build_catalog(
    spark: SparkSession,
    platform: str,
    object_type: ObjectType | None,
) -> list[CatalogItem]:
    transforms = get_catalog_transforms(platform)
    if object_type is not None:
        if object_type not in transforms:
            raise KeyError(f"Unknown object_type for {platform}: {object_type}")
        return _dataframe_to_items(transforms[object_type](spark))

    items: list[CatalogItem] = []
    for transform in transforms.values():
        items.extend(_dataframe_to_items(transform(spark)))
    return items


@router.get("/{platform}", response_model=list[CatalogItem])
async def get_catalog(
    platform: str,
    request: Request,
    object_type: ObjectType | None = Query(
        default=None,
        description="Optional filter: account | campaign | ad_group | ad",
    ),
) -> list[CatalogItem]:
    if platform not in PLATFORMS:
        raise HTTPException(status_code=404, detail=f"Unknown platform: {platform}")

    spark: SparkSession = request.app.state.spark

    try:
        return await asyncio.to_thread(_build_catalog, spark, platform, object_type)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        # Missing silver tables / MinIO errors surface as 503 for MVP.
        text = str(exc).lower()
        if any(
            marker in text
            for marker in (
                "path does not exist",
                "does not exist",
                "no such file",
                "404",
                "unable to infer schema",
            )
        ):
            raise HTTPException(
                status_code=503,
                detail=f"Catalog data unavailable for platform: {platform}",
            ) from exc
        raise
