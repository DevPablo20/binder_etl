from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from . import accounts, ad_groups, ads, campaigns

CatalogTransform = Callable[[SparkSession], DataFrame]

CATALOG_TRANSFORMS: dict[str, CatalogTransform] = {
    "account": accounts.transform,
    "campaign": campaigns.transform,
    "ad_group": ad_groups.transform,
    "ad": ads.transform,
}
