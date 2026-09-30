from src.transformers.catalog import CATALOG_BY_PLATFORM, get_catalog_transforms
from src.transformers.facebook_organic.bronze import FacebookOrganicBronzeTransformer
from src.transformers.facebook_organic.gold import FacebookOrganicGoldTransformer
from src.transformers.facebook_organic.silver import FacebookOrganicSilverTransformer
from src.transformers.instagram_organic.bronze import InstagramOrganicBronzeTransformer
from src.transformers.instagram_organic.gold import InstagramOrganicGoldTransformer
from src.transformers.instagram_organic.silver import InstagramOrganicSilverTransformer
from src.transformers.tiktok.bronze import TikTokBronzeTransformer
from src.transformers.tiktok.gold import TikTokGoldTransformer
from src.transformers.tiktok.silver import TikTokSilverTransformer

TRANSFORMERS = {
    "bronze": {
        "tiktok": TikTokBronzeTransformer,
        "facebook_organic": FacebookOrganicBronzeTransformer,
        "instagram_organic": InstagramOrganicBronzeTransformer,
    },
    "silver": {
        "tiktok": TikTokSilverTransformer,
        "facebook_organic": FacebookOrganicSilverTransformer,
        "instagram_organic": InstagramOrganicSilverTransformer,
    },
    "gold": {
        "tiktok": TikTokGoldTransformer,
        "facebook_organic": FacebookOrganicGoldTransformer,
        "instagram_organic": InstagramOrganicGoldTransformer,
    },
}

# Plataformas do pipeline. Nem toda tem catálogo: as orgânicas (`facebook_organic`,
# `instagram_organic`) não têm hierarquia campanha → anúncio e ficam fora de
# CATALOG_BY_PLATFORM — a Catalog API responde 404.
PLATFORMS = ["tiktok", "facebook_organic", "instagram_organic"]

__all__ = [
    "TRANSFORMERS",
    "PLATFORMS",
    "CATALOG_BY_PLATFORM",
    "get_catalog_transforms",
    "FacebookOrganicBronzeTransformer",
    "FacebookOrganicSilverTransformer",
    "FacebookOrganicGoldTransformer",
    "InstagramOrganicBronzeTransformer",
    "InstagramOrganicSilverTransformer",
    "InstagramOrganicGoldTransformer",
    "TikTokBronzeTransformer",
    "TikTokSilverTransformer",
    "TikTokGoldTransformer",
]
