"""Invariantes de conservação do facebook_organic, sobre as tabelas já materializadas.

Rode o medallion antes. Duas garantias:

1. **O gold não come nem duplica foto.** Cada foto de post do silver vira exatamente uma
   linha do gold, com o mesmo total acumulado.
2. **O delta reconstrói o total.** Para cada post e métrica aditiva: total da foto inicial
   (se `pre_existing`) + Σ delta = total da última foto. Se divergir, o delta está pulando
   ou repetindo variação.

A soma de métrica entre página e posts não é comparada: são fatos que nunca se somam.
"""
import pytest
from pyspark.sql.functions import (
    col,
    count,
    first,
    last,
    sum as sum_,
    when,
)
from pyspark.sql.window import Window

from src.io.reader import read_delta
from src.transformers.facebook_organic.transforms.gold.post_daily_metrics import (
    ADDITIVE_METRICS,
)

SILVER_METRICS = ("media_views", "reach", "clicks", "reactions_total", "shares")


def _read(spark, layer, table):
    try:
        return read_delta(spark, layer, f"facebook_organic/{table}")
    except Exception as exc:
        pytest.skip(f"{layer}/{table} não materializado: {exc}")


def test_gold_keeps_every_silver_snapshot(spark):
    silver = _read(spark, "silver", "post_insights_snapshot")
    gold = _read(spark, "gold", "post_daily_metrics")

    def totals(df):
        return df.agg(
            count("*").alias("rows"),
            *(sum_(f"{metric}_lifetime").alias(metric) for metric in SILVER_METRICS),
        ).collect()[0].asDict()

    assert totals(gold) == totals(silver)


def test_deltas_rebuild_lifetime_per_post(spark):
    gold = _read(spark, "gold", "post_daily_metrics")
    window = Window.partitionBy("post_id").orderBy("snapshot_date").rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    for metric in ADDITIVE_METRICS:
        lifetime = col(f"{metric}_lifetime")
        per_post = (
            gold.withColumn("_first", first(lifetime).over(window))
            .withColumn("_last", last(lifetime).over(window))
            .withColumn("_first_kind", first("baseline_kind").over(window))
            .groupBy("post_id")
            .agg(
                first("_first").alias("first"),
                first("_last").alias("last"),
                first("_first_kind").alias("first_kind"),
                sum_(col(f"{metric}_delta")).alias("deltas"),
                sum_(lifetime.isNull().cast("int")).alias("null_lifetimes"),
            )
            # Métrica que ainda não existia em alguma foto (ex.: `shares` antes da seleção de
            # campos) não tem série completa para conferir.
            .filter(col("null_lifetimes") == 0)
        )
        rebuilt = when(col("first_kind") == "pre_existing", col("first")).otherwise(0) + (
            when(col("deltas").isNull(), 0).otherwise(col("deltas"))
        )
        broken = per_post.filter(rebuilt != col("last"))

        assert broken.isEmpty(), (
            f"{metric}: delta não reconstrói o total em {broken.count()} posts — "
            f"ex.: {broken.limit(3).collect()}"
        )
