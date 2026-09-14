"""Unitário e isolado (sem MinIO): o gold ads_daily_metrics parte do fato com LEFT JOIN.

Prova a regra que o teste de conservação sozinho não consegue mais provar (ver
docs/architecture.md — depois do filtro do silver, não sobra órfão real nos dados de
hoje): uma linha do fato cuja dimensão está faltando não pode ser descartada. Com o
`inner join` antigo, o primeiro destes testes falharia.
"""
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.spark_session import get_spark_session
from src.transformers.tiktok.tables import GOLD_FACTS
from src.transformers.tiktok.transforms.gold.ads_daily_metrics import transform

ADVERTISERS_SCHEMA = StructType(
    [
        StructField("ad_account_id", StringType()),
        StructField("ad_account_name", StringType()),
    ]
)
CAMPAIGNS_SCHEMA = StructType(
    [
        StructField("campaign_id", StringType()),
        StructField("campaign_name", StringType()),
        StructField("campaign_status", StringType()),
        StructField("campaign_operation_status", StringType()),
        StructField("objective_type", StringType()),
        StructField("budget", DecimalType(10, 2)),
    ]
)
AD_GROUPS_SCHEMA = StructType(
    [
        StructField("ad_group_id", StringType()),
        StructField("optimization_goal", StringType()),
        StructField("billing_event", StringType()),
    ]
)
ADS_SCHEMA = StructType(
    [
        StructField("ad_id", StringType()),
        StructField("ad_account_id", StringType()),
        StructField("ad_name", StringType()),
        StructField("ad_text", StringType()),
        StructField("display_name", StringType()),
    ]
)
FACT_SCHEMA = StructType(
    [
        StructField("campaign_id", StringType()),
        StructField("ad_group_id", StringType()),
        StructField("ad_id", StringType()),
        StructField("stat_time_day", StringType()),
        StructField("conversion", IntegerType()),
        StructField("clicks", IntegerType()),
        StructField("result", IntegerType()),
        StructField("impressions", IntegerType()),
        StructField("spend", DecimalType(10, 2)),
        StructField("purchase", IntegerType()),
        StructField("shares", IntegerType()),
        StructField("comments", IntegerType()),
        StructField("complete_payment", IntegerType()),
        StructField("clicks_on_music_disc", IntegerType()),
        StructField("profile_visits", IntegerType()),
        StructField("total_app_event_add_to_cart", IntegerType()),
        StructField("registration", IntegerType()),
        StructField("sales_lead", IntegerType()),
        StructField("onsite_shopping", IntegerType()),
        StructField("video_watched_2s", IntegerType()),
        StructField("video_watched_6s", IntegerType()),
        StructField("video_views_p25", IntegerType()),
        StructField("video_views_p50", IntegerType()),
        StructField("video_views_p75", IntegerType()),
        StructField("video_views_p100", IntegerType()),
        StructField("video_play_actions", IntegerType()),
        StructField("reach", IntegerType()),
    ]
)

FACT_CONFIG = next(f for f in GOLD_FACTS if f.name == "ads_daily_metrics")


def _fact_row(campaign_id, ad_group_id, ad_id, spend=Decimal("0.00"), impressions=0):
    return (
        campaign_id,
        ad_group_id,
        ad_id,
        "2026-09-01",
        0,  # conversion
        0,  # clicks
        0,  # result
        impressions,
        spend,
        0,  # purchase
        0,  # shares
        0,  # comments
        0,  # complete_payment
        0,  # clicks_on_music_disc
        0,  # profile_visits
        0,  # total_app_event_add_to_cart
        0,  # registration
        0,  # sales_lead
        0,  # onsite_shopping
        0,  # video_watched_2s
        0,  # video_watched_6s
        0,  # video_views_p25
        0,  # video_views_p50
        0,  # video_views_p75
        0,  # video_views_p100
        0,  # video_play_actions
        0,  # reach
    )


@pytest.fixture(scope="module")
def spark():
    # local[2] em vez do padrão local[*] (24 cores aqui): para um join de 1-2 linhas,
    # disputar dezenas de workers Python de uma vez travava esta máquina
    # (Python worker exited unexpectedly / EOFException) de forma intermitente.
    session = get_spark_session(app_name="gold-ads-daily-metrics-test", master="local[2]")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()


def _sources(spark, *, fact_rows, advertisers=(), campaigns=(), ad_groups=(), ads=()):
    return {
        "advertisers": spark.createDataFrame(list(advertisers), schema=ADVERTISERS_SCHEMA),
        "campaigns": spark.createDataFrame(list(campaigns), schema=CAMPAIGNS_SCHEMA),
        "ad_groups": spark.createDataFrame(list(ad_groups), schema=AD_GROUPS_SCHEMA),
        "ads": spark.createDataFrame(list(ads), schema=ADS_SCHEMA),
        "ads_reports_daily": spark.createDataFrame(list(fact_rows), schema=FACT_SCHEMA),
    }


def test_orphan_fact_row_survives_left_join(spark):
    """Fato sem nenhuma dimensão correspondente — o cenário que o inner join descartava."""
    sources = _sources(
        spark,
        fact_rows=[_fact_row("camp-orphan", "adgroup-orphan", "ad-orphan", spend=Decimal("42.00"))],
    )

    result = transform(sources, FACT_CONFIG)

    assert result.count() == 1
    row = result.collect()[0]
    assert row["spend"] == 42.00
    assert row["campaign_id"] == "camp-orphan"
    assert row["ad_group_id"] == "adgroup-orphan"
    assert row["ad_id"] == "ad-orphan"
    # dimensão ausente vira NULL no gold base — "Não informado" é decisão do enriquecido
    assert row["campaign_name"] is None
    assert row["ad_account_id"] is None
    assert row["ad_account_name"] is None


def test_hierarchy_keys_come_from_fact_not_dimensions(spark):
    """As chaves de saída são do fato — mesmo com a dimensão presente e casando certo."""
    sources = _sources(
        spark,
        fact_rows=[_fact_row("camp-1", "adgroup-1", "ad-1")],
        campaigns=[("camp-1", "Campanha 1", "ENABLE", "ENABLE", "TRAFFIC", Decimal("1000.00"))],
        ad_groups=[("adgroup-1", "GOAL", "CPM")],
        ads=[("ad-1", "acc-1", "Ad 1", "texto", "Display 1")],
        advertisers=[("acc-1", "Conta 1")],
    )

    result = transform(sources, FACT_CONFIG)

    assert result.count() == 1
    row = result.collect()[0]
    assert row["campaign_id"] == "camp-1"
    assert row["ad_group_id"] == "adgroup-1"
    assert row["ad_id"] == "ad-1"
    assert row["campaign_name"] == "Campanha 1"
    assert row["ad_account_id"] == "acc-1"
    assert row["ad_account_name"] == "Conta 1"


def test_account_id_derives_from_ads_dimension(spark):
    """ad_account_id não existe no fato — vem da dimensão ads. Sem o ad, fica NULL mesmo
    a campanha estando vinculada."""
    sources = _sources(
        spark,
        fact_rows=[_fact_row("camp-1", "adgroup-1", "ad-missing")],
        campaigns=[("camp-1", "Campanha 1", "ENABLE", "ENABLE", "TRAFFIC", Decimal("1000.00"))],
        advertisers=[("acc-1", "Conta 1")],
        # nenhuma linha em `ads` para "ad-missing"
    )

    result = transform(sources, FACT_CONFIG)

    row = result.collect()[0]
    assert row["campaign_name"] == "Campanha 1"  # a campanha resolveu normalmente
    assert row["ad_account_id"] is None  # mas a conta não, por faltar a dimensão ads
    assert row["ad_account_name"] is None


def test_left_join_does_not_duplicate_rows(spark):
    """Dimensões sem id duplicado (pós Etapa 1) não podem gerar fan-out no LEFT JOIN."""
    sources = _sources(
        spark,
        fact_rows=[
            _fact_row("camp-1", "adgroup-1", "ad-1", spend=Decimal("10.00")),
            _fact_row("camp-1", "adgroup-1", "ad-2", spend=Decimal("5.00")),
        ],
        campaigns=[("camp-1", "Campanha 1", "ENABLE", "ENABLE", "TRAFFIC", Decimal("1000.00"))],
        ad_groups=[("adgroup-1", "GOAL", "CPM")],
        ads=[
            ("ad-1", "acc-1", "Ad 1", "texto", "Display 1"),
            ("ad-2", "acc-1", "Ad 2", "texto", "Display 2"),
        ],
        advertisers=[("acc-1", "Conta 1")],
    )

    result = transform(sources, FACT_CONFIG)

    assert result.count() == 2
    total_spend = sum(row["spend"] for row in result.collect())
    assert total_spend == 15.00
