"""Manutenção do lake: recolher os arquivos que o log do Delta já declarou mortos.

O `overwrite` do medallion não apaga nada do disco — marca `remove` no `_delta_log` e grava
um conjunto novo. Depois de N escritas o diretório guarda N cópias da tabela e o log diz que
só a última conta. Em ordem de grandeza, com algumas semanas de runs: dezenas de MB ativos
contra centenas de MB em disco.

**Isto regride, e o padding do raw não.** O padding se corrigiu num campo do destino, uma vez
só; o tombstone volta a cada escrita, porque é consequência do `overwrite` — semântica
correta da camada acumuladora —, não defeito. Por isso manutenção aqui é política com
número, não gesto único:

    estado estacionário ≈ tamanho ativo × escritas por dia × dias de retenção

É a conta que escolhe a retenção: com o lake na casa das dezenas de MB ativos e duas
escritas por dia, uma semana estabiliza numa ordem de grandeza acima do ativo.

**A retenção é trava de segurança, não enfeite.** Um leitor que já resolveu a lista de
arquivos de uma versão perde o chão se o VACUUM apagar essa lista no meio da leitura. O
`catalog-api` lê silver sob demanda por HTTP e **não** participa do pool do Airflow, então
não existe janela em que o Airflow garanta que ninguém está lendo. Por isso o default aqui é
o default do Delta (168h), que não exige desligar a trava: retenção curta é decisão
deliberada, com janela escolhida a dedo, e o chamador precisa pedir por ela.
"""
import logging

from pyspark.sql import SparkSession

from src.config import settings

logger = logging.getLogger(__name__)

LAKE_LAYERS = ("bronze", "silver", "gold")

# O piso que o Delta aceita sem desligar a checagem de retenção.
DELTA_DEFAULT_RETAIN_HOURS = 168
DEFAULT_RETAIN_HOURS = DELTA_DEFAULT_RETAIN_HOURS

RETENTION_CHECK_CONF = "spark.databricks.delta.retentionDurationCheck.enabled"

# O VACUUM lista o diretório da tabela por um job Spark, e o default do Spark para a
# paralelização dessa listagem é 10.000 — pensado para lake em cluster. Em `local[*]` com
# quatro núcleos isso são 10.000 tasks por tabela, dezenas de tabelas: medido, uma passada
# levava mais de uma hora quase toda em escalonamento, não em I/O. Com a paralelização na
# ordem do número de núcleos, a mesma passada é questão de minutos.
LISTING_PARALLELISM_CONF = "spark.sql.sources.parallelPartitionDiscovery.parallelism"
LISTING_PARALLELISM = "16"

LOG_DIR = "_delta_log"

# `bronze/{platform}/{table}` e `gold/organic/{table}` ficam a dois níveis do bucket; a
# margem existe para não ter que mexer aqui se uma camada ganhar um nível.
MAX_DEPTH = 3


def requires_check_override(retain_hours: float) -> bool:
    """O Delta recusa retenção abaixo do default sem desligar a trava. Pura."""
    return retain_hours < DELTA_DEFAULT_RETAIN_HOURS


def _format_hours(retain_hours: float) -> str:
    """`RETAIN 0 HOURS`, não `RETAIN 0.0 HOURS` — o parser do Delta quer número limpo."""
    return f"{retain_hours:g}"


def discover_delta_tables(
    spark: SparkSession, layer: str, max_depth: int = MAX_DEPTH
) -> list[str]:
    """URIs `s3a://` das tabelas Delta do bucket da camada.

    Descobre pelo que **existe**, não pelos registries de `tables.py`. É de propósito: tabela
    órfã — de plataforma removida ou de stream renomeado — é exatamente o que precisa ser
    recolhido, e um registry não a listaria. O preço é que a descoberta não valida nomes;
    quem valida o que *deveria* existir são os testes das plataformas.

    Bucket inexistente devolve lista vazia — a camada simplesmente ainda não tem tabela.
    """
    bucket = settings.bucket_for_layer(layer)
    root_uri = settings.s3a_uri(bucket)

    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    hadoop_conf = spark._jsc.hadoopConfiguration()

    try:
        fs = hadoop_path(root_uri).getFileSystem(hadoop_conf)
    except Exception as exc:  # bucket ausente, credencial, endpoint
        logger.warning("Camada %s inacessível (%s) — nada a recolher", layer, exc)
        return []

    found: list[str] = []

    def walk(path, depth: int) -> None:
        if depth > max_depth:
            return
        try:
            entries = [e for e in fs.listStatus(path) if e.isDirectory()]
        except Exception:
            return
        if any(e.getPath().getName() == LOG_DIR for e in entries):
            found.append(path.toString())
            return  # tabela Delta não contém outra tabela Delta
        for entry in entries:
            walk(entry.getPath(), depth + 1)

    walk(hadoop_path(root_uri), 0)
    return sorted(found)


def vacuum_table(
    spark: SparkSession, uri: str, retain_hours: float, apply: bool
) -> int:
    """Recolhe os arquivos mortos de uma tabela. Devolve quantos o VACUUM remove.

    O `DRY RUN` roda sempre, inclusive quando vai aplicar: é dele que sai o número do
    relatório, porque o `VACUUM` real não devolve a lista de arquivos apagados.
    """
    hours = _format_hours(retain_hours)
    doomed = spark.sql(f"VACUUM delta.`{uri}` RETAIN {hours} HOURS DRY RUN").count()
    if apply and doomed:
        spark.sql(f"VACUUM delta.`{uri}` RETAIN {hours} HOURS")
    return doomed


def vacuum_lake(
    spark: SparkSession,
    layers: tuple[str, ...] = LAKE_LAYERS,
    retain_hours: float = DEFAULT_RETAIN_HOURS,
    apply: bool = False,
) -> list[tuple[str, int]]:
    """Passa por todas as tabelas Delta das camadas. Devolve `[(uri, arquivos)]`.

    Não recebe `raw`: o raw não é Delta, é landing do Airbyte em Parquet solto. Volume de
    raw é retenção de arquivo, problema de outra natureza.
    """
    for layer in layers:
        if layer not in LAKE_LAYERS:
            raise ValueError(f"Camada inválida para VACUUM: {layer}. Use {LAKE_LAYERS}.")

    spark.conf.set(LISTING_PARALLELISM_CONF, LISTING_PARALLELISM)

    if requires_check_override(retain_hours):
        logger.warning(
            "Retenção de %sh abaixo do default do Delta (%dh): desligando %s. Só é seguro "
            "com ninguém lendo as tabelas — inclusive o catalog-api, que lê silver por HTTP "
            "e não passa pelo pool do Airflow.",
            _format_hours(retain_hours),
            DELTA_DEFAULT_RETAIN_HOURS,
            RETENTION_CHECK_CONF,
        )
        spark.conf.set(RETENTION_CHECK_CONF, "false")

    results: list[tuple[str, int]] = []
    for layer in layers:
        tables = discover_delta_tables(spark, layer)
        logger.info("Camada %s: %d tabela(s) Delta", layer, len(tables))
        for uri in tables:
            doomed = vacuum_table(spark, uri, retain_hours, apply)
            results.append((uri, doomed))
            logger.info(
                "%s: %d arquivo(s) %s",
                uri,
                doomed,
                "recolhido(s)" if apply else "a recolher",
            )
    return results
