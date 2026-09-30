"""Séries a partir de fotos diárias — compartilhado pelas plataformas orgânicas.

`facebook_organic` e `instagram_organic` recebem da Meta só o total acumulado das métricas de
post (`lifetime`). O bronze guarda uma foto por dia; a evolução diária é a diferença entre
duas fotos consecutivas. As regras dessa diferença moram aqui, uma vez só.

Supõe a sessão Spark em UTC (`spark_session.py`).
"""
from collections.abc import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    datediff,
    expr,
    from_utc_timestamp,
    lag,
    lit,
    round as round_,
    row_number,
    to_date,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window

# Fuso do "dia que a foto fecha". Um sync antes das 06:00 fecha o dia anterior; os crons do
# Airbyte rodam à 01:00.
SNAPSHOT_TIMEZONE = "America/Sao_Paulo"
SNAPSHOT_CUTOFF_HOURS = 6


def snapshot_date(extracted_at: Column) -> Column:
    """O dia que a foto fecha, no horário de São Paulo.

    O sync da 01:00 fecha o dia anterior; um sync manual à tarde cai no próprio dia e é
    substituído pelo da madrugada seguinte.
    """
    local = from_utc_timestamp(extracted_at, SNAPSHOT_TIMEZONE)
    return to_date(local - expr(f"INTERVAL {SNAPSHOT_CUTOFF_HOURS} HOURS"))


def local_date(ts: Column) -> Column:
    """Data de um timestamp UTC no horário de São Paulo."""
    return to_date(from_utc_timestamp(ts, SNAPSHOT_TIMEZONE))


def latest_per(df: DataFrame, *keys: str) -> DataFrame:
    """Última foto de cada chave, pela extração mais recente — o dado atual (SCD tipo 1)."""
    window = Window.partitionBy(*keys).orderBy(col("_airbyte_extracted_at").desc())
    return (
        df.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


def add_lifetime_deltas(
    df: DataFrame, key: str, metrics: Iterable[str]
) -> DataFrame:
    """Acrescenta a variação entre fotos consecutivas de cada objeto.

    Espera `key`, `snapshot_date`, `snapshot_at`, `created_at` e uma coluna `{m}_lifetime` por
    métrica. Acrescenta `prev_snapshot_date`, `gap_days`, `hours_since_prev`, `baseline_kind`,
    `days_since_publish` (se houver `created_date`) e `{m}_delta`:

    - buraco na série não é interpolado — o delta cobre o intervalo inteiro, e `gap_days` e
      `hours_since_prev` dizem quanto;
    - delta negativo fica como veio — cortar em zero quebraria a conservação;
    - na primeira foto de um objeto, `baseline_kind` diz o que fazer: `new_post` (publicado
      nas 24h anteriores à foto) tem a vida inteira como delta; `pre_existing` tem histórico
      anterior desconhecido e delta nulo;
    - métrica nula numa das duas fotos dá delta nulo.
    """
    window = Window.partitionBy(key).orderBy("snapshot_date")
    prev_snapshot_at = lag("snapshot_at").over(window)

    df = (
        df.withColumn("prev_snapshot_date", lag("snapshot_date").over(window))
        .withColumn("gap_days", datediff(col("snapshot_date"), col("prev_snapshot_date")))
        .withColumn(
            "hours_since_prev",
            round_(
                (unix_timestamp(col("snapshot_at")) - unix_timestamp(prev_snapshot_at))
                / 3600,
                2,
            ),
        )
        .withColumn(
            "baseline_kind",
            when(col("prev_snapshot_date").isNotNull(), lit(None).cast("string"))
            .when(
                col("created_at") >= col("snapshot_at") - expr("INTERVAL 1 DAY"),
                lit("new_post"),
            )
            .otherwise(lit("pre_existing")),
        )
    )
    if "created_date" in df.columns:
        df = df.withColumn(
            "days_since_publish", datediff(col("snapshot_date"), col("created_date"))
        )

    for metric in metrics:
        lifetime = col(f"{metric}_lifetime")
        df = df.withColumn(
            f"{metric}_delta",
            when(col("baseline_kind") == "new_post", lifetime)
            .when(col("baseline_kind") == "pre_existing", lit(None).cast(lifetime_type(df, metric)))
            .otherwise(lifetime - lag(lifetime).over(window)),
        )
    return df


def lifetime_type(df: DataFrame, metric: str) -> str:
    return dict(df.dtypes)[f"{metric}_lifetime"]
