from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Série de seguidores: página × `snapshot_date`.

    `fan_count` e `followers_count` são o valor atual no momento da foto; só viram série
    porque o bronze guarda uma foto por dia. Fotos de antes da seleção desses campos não têm
    contagem nenhuma e ficam de fora — não há o que registrar nelas.
    """
    page = sources["page"]
    return page.filter(
        col("fan_count").isNotNull() | col("followers_count").isNotNull()
    ).select(
        col("page_id").cast("string").alias("page_id"),
        col("snapshot_date"),
        col("_airbyte_extracted_at").alias("snapshot_at"),
        col("fan_count"),
        col("followers_count"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
