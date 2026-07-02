from pyspark.sql import SparkSession

from src.transformers.base import BaseTransformer


class TikTokSilverTransformer(BaseTransformer):
    @property
    def platform_name(self) -> str:
        return "tiktok"

    def run(self) -> None:
        raise NotImplementedError("TikTok silver transformer is not implemented yet")
