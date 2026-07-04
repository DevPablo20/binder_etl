from collections.abc import Callable

from pyspark.sql import DataFrame

from . import ad_groups, ads, ads_reports_daily, advertisers, campaigns

SilverTransform = Callable[[DataFrame], DataFrame]

SILVER_TRANSFORMS: dict[str, SilverTransform] = {
    "advertisers": advertisers.transform,
    "campaigns": campaigns.transform,
    "ad_groups": ad_groups.transform,
    "ads": ads.transform,
    "ads_reports_daily": ads_reports_daily.transform,
}
