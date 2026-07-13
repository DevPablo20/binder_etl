from src.transformers.catalog import CATALOG_BY_PLATFORM, get_catalog_transforms
from src.transformers.tiktok.bronze import TikTokBronzeTransformer
from src.transformers.tiktok.gold import TikTokGoldTransformer
from src.transformers.tiktok.silver import TikTokSilverTransformer

TRANSFORMERS = {
    "bronze": {"tiktok": TikTokBronzeTransformer},
    "silver": {"tiktok": TikTokSilverTransformer},
    "gold": {"tiktok": TikTokGoldTransformer},
}

PLATFORMS = ["tiktok"]

__all__ = [
    "TRANSFORMERS",
    "PLATFORMS",
    "CATALOG_BY_PLATFORM",
    "get_catalog_transforms",
    "TikTokBronzeTransformer",
    "TikTokSilverTransformer",
    "TikTokGoldTransformer",
]
