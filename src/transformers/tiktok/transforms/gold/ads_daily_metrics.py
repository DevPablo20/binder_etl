from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.transformers.tiktok.tables import GoldFactConfig


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """Monta o gold ads_daily_metrics. Função pura — recebe os DataFrames do silver já
    carregados, não faz I/O. Quem lê do MinIO é `TikTokGoldTransformer` em `gold.py`.

    O fato (`ads_reports_daily`) é a base, e as quatro dimensões entram por `LEFT JOIN`:
    a extração nunca é garantidamente completa, e uma dimensão faltando não pode
    descartar a linha do fato. As chaves de hierarquia (`campaign_id`, `ad_group_id`,
    `ad_id`) vêm do fato, que sempre as carrega — nunca das dimensões, que podem faltar.

    `ad_account_id` não existe no fato (o conector do TikTok o emite nulo no relatório)
    e é derivado da dimensão `ads`. Uma linha cujo ad não está na dimensão fica sem conta,
    mesmo que a campanha esteja vinculada — ver "Decisões em aberto" em docs/architecture.md.
    """
    advertisers = sources["advertisers"]
    campaigns = sources["campaigns"]
    ad_groups = sources["ad_groups"]
    ads = sources["ads"]
    fact = sources["ads_reports_daily"]

    selected_advertisers = advertisers.select(
        col("ad_account_id"),
        col("ad_account_name"),
    )

    selected_campaigns = campaigns.select(
        col("campaign_id"),
        col("campaign_name"),
        col("campaign_status"),
        col("campaign_operation_status"),
        col("objective_type"),
        col("budget"),
    )

    selected_ad_groups = ad_groups.select(
        col("ad_group_id"),
        col("optimization_goal"),
        col("billing_event"),
    )

    selected_ads = ads.select(
        col("ad_id"),
        col("ad_account_id"),
        col("ad_name"),
        col("ad_text"),
        col("display_name"),
    )

    selected_fact = fact.select(
        col("campaign_id"),
        col("ad_group_id"),
        col("ad_id"),
        col("stat_time_day").alias("date"),
        col("conversion").alias("conversions"),
        col("clicks"),
        col("result").alias("engagement"),
        col("impressions"),
        col("spend"),
        col("purchase"),
        col("shares"),
        col("comments"),
        col("complete_payment"),
        col("clicks_on_music_disc"),
        col("profile_visits"),
        col("total_app_event_add_to_cart"),
        col("registration"),
        col("sales_lead"),
        col("onsite_shopping"),
        col("video_watched_2s").alias("video_views_2s"),
        col("video_watched_6s").alias("video_views_6s"),
        col("video_views_p25").alias("video_views_25p"),
        col("video_views_p50").alias("video_views_50p"),
        col("video_views_p75").alias("video_views_75p"),
        col("video_views_p100").alias("video_views_100p"),
        col("video_play_actions").alias("video_views"),
        col("reach"),
    )

    joined = (
        selected_fact.alias("tard")
        .join(
            selected_ads.alias("tad"),
            on=col("tard.ad_id") == col("tad.ad_id"),
            how="left",
        )
        .join(
            selected_ad_groups.alias("tag"),
            on=col("tard.ad_group_id") == col("tag.ad_group_id"),
            how="left",
        )
        .join(
            selected_campaigns.alias("tc"),
            on=col("tard.campaign_id") == col("tc.campaign_id"),
            how="left",
        )
        .join(
            selected_advertisers.alias("taa"),
            on=col("tad.ad_account_id") == col("taa.ad_account_id"),
            how="left",
        )
    )

    return joined.select(
        col("tad.ad_account_id"),
        col("taa.ad_account_name"),
        col("tard.campaign_id"),
        col("tc.campaign_name"),
        col("tc.campaign_status"),
        col("tc.campaign_operation_status"),
        col("tc.objective_type"),
        col("tc.budget"),
        col("tard.ad_group_id"),
        col("tag.optimization_goal"),
        col("tag.billing_event"),
        col("tard.ad_id"),
        col("tad.ad_name"),
        col("tad.ad_text"),
        col("tad.display_name"),
        col("tard.date"),
        col("tard.conversions"),
        col("tard.clicks"),
        col("tard.engagement"),
        col("tard.impressions"),
        col("tard.spend"),
        col("tard.purchase"),
        col("tard.shares"),
        col("tard.comments"),
        col("tard.complete_payment"),
        col("tard.clicks_on_music_disc"),
        col("tard.profile_visits"),
        col("tard.total_app_event_add_to_cart"),
        col("tard.registration"),
        col("tard.sales_lead"),
        col("tard.onsite_shopping"),
        col("tard.video_views_2s"),
        col("tard.video_views_6s"),
        col("tard.video_views_25p"),
        col("tard.video_views_50p"),
        col("tard.video_views_75p"),
        col("tard.video_views_100p"),
        col("tard.video_views"),
        col("tard.reach"),
    )
