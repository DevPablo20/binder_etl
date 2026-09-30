from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import LongType, MapType, StringType

COUNT_MAP = MapType(StringType(), LongType())


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Demografia dos seguidores: conta × `snapshot_date` × `breakdown` × valor.

    O `value` chega como JSON (`{"São Paulo, São Paulo (state)": 790, ...}`), um por
    `breakdown` (`city`, `country`, `age,gender`). A Meta devolve só os 45 maiores e só quem
    tem dado demográfico — a soma fica abaixo do total de seguidores. Fica só no silver.
    """
    lifetime = sources["user_lifetime_insights"]
    return lifetime.select(
        col("business_account_id").cast("string").alias("business_account_id"),
        col("snapshot_date"),
        col("metric"),
        col("breakdown"),
        explode(from_json(col("value"), COUNT_MAP)).alias("breakdown_value", "followers"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
