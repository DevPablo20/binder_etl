from pyspark.sql import DataFrame

from src.config import settings


def write_delta(
    df: DataFrame,
    layer: str,
    table_path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    replace_where: str | None = None,
    overwrite_schema: bool = False,
) -> None:
    """`replace_where` sobrescreve só as linhas que casam com o predicado — é como mais de um
    job grava a sua fatia de uma mesma tabela sem apagar a dos outros.

    `overwrite_schema` troca o schema da tabela pelo do DataFrame. **Fica desligado por
    default de propósito**, e não por conservadorismo: com ele ligado, um transform que
    deixasse de produzir uma coluna apagaria essa coluna da tabela em silêncio, em vez de
    falhar. Coluna nova num bronze/silver é mudança deliberada, então pede um passo
    deliberado — chame com `overwrite_schema=True` uma vez, e os runs seguintes voltam a
    casar sozinhos.
    """
    bucket = settings.bucket_for_layer(layer)
    base_uri = settings.s3a_uri(bucket, table_path)

    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.save(base_uri)


def write_parquet(
    df: DataFrame,
    layer: str,
    table_path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    bucket = settings.bucket_for_layer(layer)
    base_uri = settings.s3a_uri(bucket, table_path)

    writer = df.write.format("parquet").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(base_uri)
