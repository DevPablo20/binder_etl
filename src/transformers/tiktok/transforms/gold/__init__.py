from collections.abc import Callable

from pyspark.sql import DataFrame

from src.transformers.tiktok.tables import GoldFactConfig

from . import ads_daily_metrics

# `sources` mapeia nome do stream silver (GoldFactConfig.silver_sources) -> DataFrame já
# carregado. A função é pura: não lê nem escreve no MinIO. Quem faz I/O é `gold.py`.
GoldTransform = Callable[[dict[str, DataFrame], GoldFactConfig], DataFrame]

GOLD_TRANSFORMS: dict[str, GoldTransform] = {
    "ads_daily_metrics": ads_daily_metrics.transform,
}
