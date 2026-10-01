from collections.abc import Callable

from pyspark.sql import DataFrame

from src.transformers.instagram_organic.tables import GoldFactConfig

from . import account_daily, content, content_daily, stories

# `sources` mapeia nome da tabela silver (GoldFactConfig.silver_sources) -> DataFrame já
# carregado. A função é pura: não lê nem escreve no MinIO. Quem faz I/O é `gold.py`.
GoldTransform = Callable[[dict[str, DataFrame], GoldFactConfig], DataFrame]

GOLD_TRANSFORMS: dict[str, GoldTransform] = {
    "content": content.transform,
    "content_daily": content_daily.transform,
    "account_daily": account_daily.transform,
    "stories": stories.transform,
}
