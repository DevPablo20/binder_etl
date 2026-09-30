from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ._common import latest_per


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Dimensão de página: a última foto de cada `page_id`."""
    return latest_per(sources["page"], "page_id").select(
        col("page_id").cast("string").alias("page_id"),
        col("name").alias("page_name"),
        col("username"),
        col("link"),
        col("category"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
