from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.config import settings


def read_raw_parquet(
    spark: SparkSession,
    path: str = "",
    bucket: str | None = None,
    schema: StructType | None = None,
) -> DataFrame:
    """`schema` fixa as colunas lidas: sem ele, o Spark infere de um único arquivo, e um
    arquivo antigo sem as colunas novas as faz sumir sem erro."""
    b = bucket or settings.bucket_raw
    uri = settings.s3a_uri(b, path)
    reader = spark.read.schema(schema) if schema is not None else spark.read
    return reader.parquet(uri)


def read_delta(
    spark: SparkSession,
    layer: str,
    table_path: str,
) -> DataFrame:
    bucket = settings.bucket_for_layer(layer)
    uri = settings.s3a_uri(bucket, table_path)
    return spark.read.format("delta").load(uri)
