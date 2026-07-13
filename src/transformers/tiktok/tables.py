"""Single source of truth for TikTok table metadata."""
from dataclasses import dataclass

RAW_PREFIX = "airbyte/tiktok"
PLATFORM = "tiktok"


@dataclass(frozen=True)
class BridgeMapping:
    column: str
    target: str


@dataclass(frozen=True)
class TikTokStream:
    name: str
    raw_path: str
    dedupe_columns: tuple[str, ...]
    bridge_mappings: tuple[BridgeMapping, ...] = ()

    @property
    def bronze_table_name(self) -> str:
        return f"{PLATFORM}/{self.name}"

    @property
    def silver_table_name(self) -> str:
        return f"{PLATFORM}/{self.name}"


@dataclass(frozen=True)
class GoldFactConfig:
    name: str
    silver_sources: tuple[str, ...]
    bridge_mappings: tuple[BridgeMapping, ...] = ()

    @property
    def gold_table_name(self) -> str:
        return f"{PLATFORM}/{self.name}"

    @property
    def silver_source_paths(self) -> list[str]:
        return [f"{PLATFORM}/{source}" for source in self.silver_sources]


TIKTOK_STREAMS: tuple[TikTokStream, ...] = (
    TikTokStream(
        name="advertisers",
        raw_path=f"{RAW_PREFIX}/advertisers",
        dedupe_columns=("advertiser_id",),
        bridge_mappings=(
            BridgeMapping("ad_account_id", "platform_account.external_account_id"),
        ),
    ),
    TikTokStream(
        name="campaigns",
        raw_path=f"{RAW_PREFIX}/campaigns",
        dedupe_columns=("advertiser_id", "campaign_id"),
        bridge_mappings=(
            BridgeMapping("campaign_id", "platform_object_map.external_id (campaign)"),
        ),
    ),
    TikTokStream(
        name="ad_groups",
        raw_path=f"{RAW_PREFIX}/ad_groups",
        dedupe_columns=("advertiser_id", "campaign_id", "adgroup_id"),
        bridge_mappings=(
            BridgeMapping("ad_group_id", "platform_object_map.external_id (ad_group)"),
        ),
    ),
    TikTokStream(
        name="ads",
        raw_path=f"{RAW_PREFIX}/ads",
        dedupe_columns=("advertiser_id", "campaign_id", "adgroup_id", "ad_id"),
        bridge_mappings=(
            BridgeMapping("ad_id", "platform_object_map.external_id (ad)"),
        ),
    ),
    TikTokStream(
        name="ads_reports_daily",
        raw_path=f"{RAW_PREFIX}/ads_reports_daily",
        dedupe_columns=(
            "ad_id",
            "stat_time_day",
            "metrics.campaign_id",
            "metrics.adgroup_id",
        ),
    ),
)

GOLD_FACTS: tuple[GoldFactConfig, ...] = (
    GoldFactConfig(
        name="ads_daily_metrics",
        silver_sources=(
            "advertisers",
            "campaigns",
            "ad_groups",
            "ads",
            "ads_reports_daily",
        ),
        bridge_mappings=(
            BridgeMapping("ad_account_id", "platform_account.external_account_id"),
            BridgeMapping("campaign_id", "platform_object_map.external_id (campaign)"),
            BridgeMapping("ad_group_id", "platform_object_map.external_id (ad_group)"),
            BridgeMapping("ad_id", "platform_object_map.external_id (ad)"),
        ),
    ),
)
