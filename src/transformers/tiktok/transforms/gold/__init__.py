from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from src.transformers.tiktok.tables import GoldFactConfig

from . import ads_daily_metrics

GoldTransform = Callable[[SparkSession, GoldFactConfig], DataFrame]

GOLD_TRANSFORMS: dict[str, GoldTransform] = {
    "ads_daily_metrics": ads_daily_metrics.transform,
}
