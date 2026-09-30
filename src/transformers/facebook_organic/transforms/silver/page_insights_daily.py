from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_sub,
    explode,
    from_json,
    from_utc_timestamp,
    to_date,
)
from pyspark.sql.types import LongType, MapType, StringType

from src.transformers.facebook_organic.tables import INSIGHTS_TIMEZONE

from ._common import latest_per

COUNT_MAP = MapType(StringType(), LongType())


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Métricas de página em formato longo: página × métrica × período × `metric_date`.

    Cada foto traz os 2 últimos dias disponíveis num array; aqui o array é aberto. O
    `end_time` marca o fim da janela, à meia-noite do Pacífico — o dia medido é o anterior.
    O mesmo dia chega em mais de um sync; vence a extração mais recente.

    `week` e `days_28` ficam no formato longo: são contagens únicas em janela móvel e não
    somam.
    """
    page_insights = sources["page_insights"]

    exploded = page_insights.select(
        col("page_id"),
        col("name").alias("metric"),
        col("period"),
        explode(col("values")).alias("v"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    ).select(
        col("page_id").cast("string").alias("page_id"),
        col("metric"),
        col("period"),
        metric_date(col("v.end_time")).alias("metric_date"),
        col("v.end_time").alias("end_time"),
        col("v.value.integer").alias("value"),
        from_json(col("v.value.object"), COUNT_MAP).alias("value_breakdown"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )

    return latest_per(exploded, "page_id", "metric", "period", "metric_date")


def metric_date(end_time):
    """Supõe a sessão Spark em UTC (`spark_session.py`)."""
    return date_sub(to_date(from_utc_timestamp(end_time, INSIGHTS_TIMEZONE)), 1)


def missing_metric_dates(df: DataFrame) -> list[tuple[str, date]]:
    """Dias que faltam na sequência de cada página, período `day`.

    A API devolve só os 2 últimos dias disponíveis e o conector não aceita `since`: com o
    cron diário, cada dia passa por 2 syncs, e duas falhas seguidas abrem um buraco que não
    tem como ser recuperado. O dia fica ausente — sem interpolação. Isto só aponta.
    """
    rows = (
        df.filter(col("period") == "day")
        .select("page_id", "metric_date")
        .distinct()
        .collect()
    )
    by_page: dict[str, set[date]] = {}
    for row in rows:
        by_page.setdefault(row["page_id"], set()).add(row["metric_date"])

    missing: list[tuple[str, date]] = []
    for page_id, dates in sorted(by_page.items()):
        day = min(dates)
        while day < max(dates):
            if day not in dates:
                missing.append((page_id, day))
            day += timedelta(days=1)
    return missing
