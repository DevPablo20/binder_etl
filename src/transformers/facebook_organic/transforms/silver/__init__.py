from collections.abc import Callable

from pyspark.sql import DataFrame

from . import (
    page_followers_snapshot,
    page_insights_daily,
    pages,
    post_insights_snapshot,
    posts,
)

# `sources` mapeia nome do stream bronze (SilverTableConfig.bronze_sources) -> DataFrame já
# carregado. A função é pura: não lê nem escreve no MinIO. Quem faz I/O é `silver.py`.
SilverTransform = Callable[[dict[str, DataFrame]], DataFrame]

SILVER_TRANSFORMS: dict[str, SilverTransform] = {
    "pages": pages.transform,
    "page_followers_snapshot": page_followers_snapshot.transform,
    "posts": posts.transform,
    "post_insights_snapshot": post_insights_snapshot.transform,
    "page_insights_daily": page_insights_daily.transform,
}
