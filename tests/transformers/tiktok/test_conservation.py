"""Invariante de conservação: nenhuma camada pode comer nem duplicar métrica do fato.

A soma de qualquer métrica, sem filtro, tem que ser idêntica em qualquer camada
derivada do fato. Se divergir para menos, algum join está descartando (inner) ou algum
filtro está cortando linha com atividade; para mais, é fan-out. Lê as tabelas já
materializadas — rode o medallion antes.
"""
import pytest
from pyspark.sql.functions import col, count, sum as sum_

from src.io.reader import read_delta

METRICS = ("spend", "impressions", "clicks")


def _totals(df, columns=METRICS):
    aggs = [count("*").alias("rows"), *(sum_(c).alias(c) for c in columns)]
    return df.agg(*aggs).collect()[0].asDict()


def test_gold_conserves_silver_fact(spark):
    try:
        fact = read_delta(spark, "silver", "tiktok/ads_reports_daily")
        gold = read_delta(spark, "gold", "tiktok/ads_daily_metrics")
    except Exception as exc:
        pytest.skip(f"silver/gold não materializados: {exc}")

    expected = _totals(fact)
    actual = _totals(gold)

    assert actual == expected, (
        f"gold diverge do fato silver — silver={expected} gold={actual}"
    )


def test_silver_filter_conserves_bronze_fact(spark):
    """O filtro de linhas sem atividade (silver) não pode remover métrica, só linha vazia.

    Linhas podem cair — 66% do bronze é ad × dia sem entrega — mas a soma de cada métrica
    tem que ser idêntica, porque as linhas descartadas já eram zero.
    """
    try:
        bronze = read_delta(spark, "bronze", "tiktok/ads_reports_daily")
        silver = read_delta(spark, "silver", "tiktok/ads_reports_daily")
    except Exception as exc:
        pytest.skip(f"bronze/silver não materializados: {exc}")

    # Mesmos casts que o transform do silver aplica — sem isso, comparar Decimal(10,2)
    # do silver com o double bruto do bronze diverge por precisão de ponto flutuante,
    # não por linha perdida.
    bronze_metrics = bronze.select(
        col("metrics.spend").cast("Decimal(10,2)").alias("spend"),
        col("metrics.impressions").cast("integer").alias("impressions"),
        col("metrics.clicks").cast("integer").alias("clicks"),
    )
    expected = _totals(bronze_metrics, columns=METRICS)
    actual = _totals(silver, columns=METRICS)

    assert actual["spend"] == expected["spend"]
    assert actual["impressions"] == expected["impressions"]
    assert actual["clicks"] == expected["clicks"]
    assert actual["rows"] <= expected["rows"], (
        "silver tem mais linhas que o bronze — o filtro não deveria criar linha"
    )
