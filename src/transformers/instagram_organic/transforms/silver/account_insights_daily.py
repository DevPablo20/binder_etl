from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_add,
    from_json,
    from_utc_timestamp,
    to_date,
    to_utc_timestamp,
)
from pyspark.sql.types import LongType, MapType, StringType

from src.transformers.instagram_organic.tables import INSIGHTS_TIMEZONE

COUNT_MAP = MapType(StringType(), LongType())


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Métricas diárias da conta: conta × `metric_date`.

    O `date` do Instagram é a meia-noite do Pacífico que **inicia** o dia medido (o contrário
    do `end_time` das páginas do Facebook): o dia é a própria data, sem −1.

    O dia mais recente de cada sync ainda está em andamento. `is_partial` marca a leitura feita
    antes de o dia terminar; o sync seguinte relê o dia e a marca some. Sem ela, o dia parcial
    parece queda de alcance.

    `reach` inclui anúncios. `follower_count` da API é o número de **novos** seguidores do dia —
    aqui `new_followers`; o total está em `account_followers_snapshot`. O bronze já deixou uma
    linha por conta e dia, a leitura mais recente.
    """
    user_insights = sources["user_insights"]
    day = metric_date(col("date"))

    return user_insights.select(
        col("business_account_id").cast("string").alias("business_account_id"),
        day.alias("metric_date"),
        (col("_airbyte_extracted_at") < day_end(day)).alias("is_partial"),
        col("reach"),
        col("reach_week"),
        col("reach_days_28"),
        col("follower_count").alias("new_followers"),
        from_json(col("online_followers"), COUNT_MAP).alias("online_followers"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )


def metric_date(date):
    """Supõe a sessão Spark em UTC (`spark_session.py`)."""
    return to_date(from_utc_timestamp(date, INSIGHTS_TIMEZONE))


def day_end(day):
    """A meia-noite do Pacífico que encerra o dia, em UTC — 07:00 ou 08:00 conforme o horário
    de verão americano."""
    return to_utc_timestamp(date_add(day, 1).cast("timestamp"), INSIGHTS_TIMEZONE)
