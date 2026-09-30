import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    input_file_name,
    lit,
    regexp_extract,
)
from pyspark.sql.utils import AnalysisException

from src.config import settings
from src.io.reader import read_delta, read_raw_parquet
from src.io.writer import write_delta
from src.transformers.base import BaseTransformer
from src.transformers.facebook_organic.tables import (
    FACEBOOK_STREAMS,
    PLATFORM,
    FacebookStream,
)
from src.transformers.snapshots import snapshot_date

logger = logging.getLogger(__name__)


class FacebookOrganicBronzeTransformer(BaseTransformer):
    """Bronze do facebook_organic: uma foto por dia de cada stream.

    Mesmo molde do TikTok (lê o raw, une com o bronze existente, deduplica, materializa e
    sobrescreve), com duas diferenças: o raw é lido com schema explícito, e a chave de
    dedupe inclui `snapshot_date` — deduplicar só pelo id apagaria o histórico que é a razão
    de ser desta plataforma.
    """

    @property
    def platform_name(self) -> str:
        return PLATFORM

    def run(self) -> None:
        for stream in FACEBOOK_STREAMS:
            self._process_stream(stream)

    def _process_stream(self, stream: FacebookStream) -> None:
        logger.info("Processing bronze stream: %s", stream.name)

        try:
            raw = read_raw_parquet(self.spark, stream.raw_path, schema=stream.schema)
        except AnalysisException as exc:
            self._handle_source_error(stream, exc)
            return
        except Exception as exc:
            if self._is_missing_path_error(exc):
                self._handle_source_error(stream, exc)
                return
            raise

        if raw.isEmpty():
            self._handle_empty_source(stream)
            return

        new = self.prepare(raw, stream.name)
        existing = self._read_existing_bronze(stream)
        combined = self._accumulate(existing, new)

        input_count = combined.count()
        cleaned = self.dedupe(combined, list(stream.dedupe_columns))
        cleaned.cache()
        output_count = cleaned.count()  # materializa antes de sobrescrever a origem

        write_delta(cleaned, "bronze", stream.bronze_table_name, mode="overwrite")

        logger.info(
            "Wrote bronze/%s: %d rows (deduped from %d accumulated)",
            stream.bronze_table_name,
            output_count,
            input_count,
        )

    @staticmethod
    def prepare(
        raw: DataFrame, stream_name: str, page_id: Column | None = None
    ) -> DataFrame:
        """Acrescenta `page_id` e `snapshot_date` ao raw. Pura — não lê nem escreve.

        `page_insights` só passou a trazer `id` depois da seleção de campos de 30/09; nas fotos
        anteriores ele é reconstruído no mesmo formato da API
        (`{page_id}/insights/{metric}/{period}`). Sem isso, as 15 métricas de uma foto antiga
        teriam a mesma chave nula e o dedupe as colapsaria numa linha só.

        `page_id` sai do caminho do arquivo; o parâmetro existe para os testes, que montam o
        DataFrame em memória, sem arquivo.
        """
        page_id = page_id if page_id is not None else page_id_from_path()
        df = raw.withColumn("page_id", page_id).withColumn(
            "snapshot_date", snapshot_date(col("_airbyte_extracted_at"))
        )
        if stream_name == "page_insights":
            df = df.withColumn(
                "id",
                coalesce(
                    col("id"),
                    concat_ws("/", col("page_id"), lit("insights"), col("name"), col("period")),
                ),
            )
        return df

    def _read_existing_bronze(self, stream: FacebookStream) -> DataFrame | None:
        """`None` na primeira execução, quando o bronze ainda não existe."""
        try:
            return read_delta(self.spark, "bronze", stream.bronze_table_name)
        except AnalysisException:
            return None
        except Exception as exc:
            if self._is_missing_path_error(exc):
                return None
            raise

    @staticmethod
    def _accumulate(existing: DataFrame | None, new: DataFrame) -> DataFrame:
        """Une o bronze já acumulado com o raw novo, antes do dedupe. Função pura."""
        if existing is None:
            return new
        return existing.unionByName(new, allowMissingColumns=True)

    def _handle_source_error(self, stream: FacebookStream, exc: Exception) -> None:
        message = f"Raw source missing or unreadable for {stream.name}: {exc}"
        if settings.etl_strict:
            raise RuntimeError(message) from exc
        logger.warning("%s — skipping stream", message)

    def _handle_empty_source(self, stream: FacebookStream) -> None:
        message = f"Raw source empty for {stream.name}"
        if settings.etl_strict:
            raise RuntimeError(message)
        logger.warning("%s — skipping stream", message)

    @staticmethod
    def _is_missing_path_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(
            marker in text
            for marker in (
                "path does not exist",
                "does not exist",
                "unable to infer schema",
                "no such file",
                "404",
            )
        )


def page_id_from_path() -> Column:
    """O raw fica em `airbyte/facebook_organic/{page_id}/{stream}/`."""
    return regexp_extract(input_file_name(), rf"{PLATFORM}/([^/]+)/", 1)
