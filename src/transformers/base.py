from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


class BaseTransformer(ABC):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @property
    @abstractmethod
    def platform_name(self) -> str:
        ...

    @abstractmethod
    def run(self) -> None:
        ...

    def dedupe(
        self,
        df: DataFrame,
        columns: list[str],
        order_column: str = "_airbyte_extracted_at",
    ) -> DataFrame:
        partition_cols = [col(name) for name in columns]
        window_spec = Window.partitionBy(*partition_cols).orderBy(
            col(order_column).desc()
        )
        return (
            df.withColumn("_row_num", row_number().over(window_spec))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )
