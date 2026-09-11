"""Invariante de conservação: o gold não pode comer nem duplicar linha do fato.

A soma de qualquer métrica no gold, sem filtro, tem que ser idêntica à soma no fato do
silver. Se divergir, algum join está descartando (inner) ou multiplicando (fan-out) dado.
Lê as tabelas já materializadas — rode o medallion antes.
"""
import pytest
from pyspark.sql.functions import count, sum as sum_

from src.io.reader import read_delta
from src.spark_session import get_spark_session

METRICS = ("spend", "impressions", "clicks")


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session(app_name="conservation-tiktok-test")
    yield session
    session.stop()


def _totals(df):
    aggs = [count("*").alias("rows"), *(sum_(m).alias(m) for m in METRICS)]
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
