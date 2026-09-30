from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def latest_per(df: DataFrame, *keys: str) -> DataFrame:
    """Última foto de cada chave, pela extração mais recente — o dado atual (SCD tipo 1)."""
    window = Window.partitionBy(*keys).orderBy(col("_airbyte_extracted_at").desc())
    return (
        df.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
