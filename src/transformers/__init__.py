from src.transformers.catalog import CATALOG_BY_PLATFORM, get_catalog_transforms
from src.transformers.facebook_organic.bronze import FacebookOrganicBronzeTransformer
from src.transformers.facebook_organic.gold import FacebookOrganicGoldTransformer
from src.transformers.facebook_organic.silver import FacebookOrganicSilverTransformer
from src.transformers.tiktok.bronze import TikTokBronzeTransformer
from src.transformers.tiktok.gold import TikTokGoldTransformer
from src.transformers.tiktok.silver import TikTokSilverTransformer

TRANSFORMERS = {
    "bronze": {
        "tiktok": TikTokBronzeTransformer,
        "facebook_organic": FacebookOrganicBronzeTransformer,
    },
    "silver": {
        "tiktok": TikTokSilverTransformer,
        "facebook_organic": FacebookOrganicSilverTransformer,
    },
    "gold": {
        "tiktok": TikTokGoldTransformer,
        "facebook_organic": FacebookOrganicGoldTransformer,
    },
}

# Plataformas do pipeline. Nem toda tem catálogo: `facebook_organic` não tem hierarquia
# campanha → anúncio e fica fora de CATALOG_BY_PLATFORM — a Catalog API responde 404.
PLATFORMS = ["tiktok", "facebook_organic"]

__all__ = [
    "TRANSFORMERS",
    "PLATFORMS",
    "CATALOG_BY_PLATFORM",
    "get_catalog_transforms",
    "FacebookOrganicBronzeTransformer",
    "FacebookOrganicSilverTransformer",
    "FacebookOrganicGoldTransformer",
    "TikTokBronzeTransformer",
    "TikTokSilverTransformer",
    "TikTokGoldTransformer",
]
