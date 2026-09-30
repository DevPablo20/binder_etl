from collections.abc import Callable

from pyspark.sql import DataFrame

from src.transformers.facebook_organic.tables import GoldFactConfig

from . import page_daily_metrics, post_daily_metrics

# `sources` mapeia nome da tabela silver (GoldFactConfig.silver_sources) -> DataFrame já
# carregado. A função é pura: não lê nem escreve no MinIO. Quem faz I/O é `gold.py`.
GoldTransform = Callable[[dict[str, DataFrame], GoldFactConfig], DataFrame]

GOLD_TRANSFORMS: dict[str, GoldTransform] = {
    "post_daily_metrics": post_daily_metrics.transform,
    "page_daily_metrics": page_daily_metrics.transform,
}
