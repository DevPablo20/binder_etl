from collections.abc import Callable

from pyspark.sql import DataFrame

from . import (
    account_followers_snapshot,
    account_insights_daily,
    accounts,
    follower_demographics_snapshot,
    media,
    media_insights_snapshot,
    stories,
)

# `sources` mapeia nome do stream bronze (SilverTableConfig.bronze_sources) -> DataFrame já
# carregado. A função é pura: não lê nem escreve no MinIO. Quem faz I/O é `silver.py`.
SilverTransform = Callable[[dict[str, DataFrame]], DataFrame]

SILVER_TRANSFORMS: dict[str, SilverTransform] = {
    "accounts": accounts.transform,
    "account_followers_snapshot": account_followers_snapshot.transform,
    "account_insights_daily": account_insights_daily.transform,
    "follower_demographics_snapshot": follower_demographics_snapshot.transform,
    "media": media.transform,
    "media_insights_snapshot": media_insights_snapshot.transform,
    "stories": stories.transform,
}
