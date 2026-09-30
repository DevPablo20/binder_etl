from collections.abc import Callable

from pyspark.sql import DataFrame

from src.transformers.instagram_organic.tables import GoldFactConfig

from . import account_daily_metrics, media_daily_metrics, story_metrics

# `sources` mapeia nome da tabela silver (GoldFactConfig.silver_sources) -> DataFrame já
# carregado. A função é pura: não lê nem escreve no MinIO. Quem faz I/O é `gold.py`.
GoldTransform = Callable[[dict[str, DataFrame], GoldFactConfig], DataFrame]

GOLD_TRANSFORMS: dict[str, GoldTransform] = {
    "media_daily_metrics": media_daily_metrics.transform,
    "account_daily_metrics": account_daily_metrics.transform,
    "story_metrics": story_metrics.transform,
}
