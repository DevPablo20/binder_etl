"""Invariantes de conservação da fatia `facebook` da gold orgânica, sobre as tabelas já
materializadas. Rode o medallion antes.

1. **A gold não come nem duplica dia.** Cada linha da série do silver vira exatamente uma linha
   do `content_daily`, com o mesmo total.
2. **O dia reconstrói o total.** Para cada post e métrica: total do primeiro dia fotografado
   (quando o número do dia é desconhecido) + Σ números do dia = total do último dia.
3. **Todo post do `content_daily` está no `content`** — a dimensão cobre o fato.

Alcance da conta e dos posts não se comparam: o da conta é único e inclui anúncios.
"""
import pytest
from pyspark.sql.functions import col, first, last, sum as sum_, when
from pyspark.sql.window import Window

from src.io.reader import read_delta
from src.transformers.organic_gold import CONTENT_METRICS, daily_name, total_name

PLATFORM = "facebook"
SERIES = "facebook_organic/post_metrics_daily"


def _read(spark, layer, table):
    try:
        df = read_delta(spark, layer, table)
    except Exception as exc:
        pytest.skip(f"{layer}/{table} não materializado: {exc}")
    return df


def _gold(spark, table):
    return _read(spark, "gold", f"organic/{table}").filter(col("platform") == PLATFORM)


def test_gold_keeps_every_silver_day(spark):
    silver = _read(spark, "silver", SERIES)
    gold = _gold(spark, "content_daily")

    assert gold.count() == silver.count()


def test_daily_numbers_rebuild_total_per_post(spark):
    gold = _gold(spark, "content_daily")
    window = Window.partitionBy("content_id").orderBy("date").rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    for metric in CONTENT_METRICS:
        daily, total = col(daily_name(metric)), col(total_name(metric))
        per_post = (
            gold.withColumn("_first_total", first(total).over(window))
            .withColumn("_first_daily", first(daily).over(window))
            .withColumn("_last_total", last(total).over(window))
            .groupBy("content_id")
            .agg(
                first("_first_total").alias("first_total"),
                first("_first_daily").alias("first_daily"),
                first("_last_total").alias("last_total"),
                sum_(daily).alias("dailies"),
                sum_(total.isNull().cast("int")).alias("null_totals"),
            )
            # Métrica que a rede não tem, ou que não existia numa foto antiga, não tem série
            # completa para conferir.
            .filter(col("null_totals") == 0)
        )
        baseline = when(col("first_daily").isNull(), col("first_total")).otherwise(0)
        rebuilt = baseline + when(col("dailies").isNull(), 0).otherwise(col("dailies"))
        broken = per_post.filter(rebuilt != col("last_total"))

        assert broken.isEmpty(), (
            f"{metric}: os números do dia não reconstroem o total em {broken.count()} posts — "
            f"ex.: {broken.limit(3).collect()}"
        )


def test_every_daily_post_is_in_content(spark):
    daily = _gold(spark, "content_daily").select("content_id").distinct()
    content = _gold(spark, "content").select("content_id")

    assert daily.join(content, "content_id", "left_anti").isEmpty()
