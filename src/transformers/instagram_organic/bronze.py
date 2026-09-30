import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from src.config import settings
from src.io.reader import read_delta, read_raw_parquet
from src.io.writer import write_delta
from src.transformers.base import BaseTransformer
from src.transformers.instagram_organic.tables import (
    INSTAGRAM_STREAMS,
    PLATFORM,
    InstagramStream,
)
from src.transformers.snapshots import snapshot_date

logger = logging.getLogger(__name__)


class InstagramOrganicBronzeTransformer(BaseTransformer):
    """Bronze do instagram_organic.

    Mesmo molde do `facebook_organic`: raw lido com schema explícito, `snapshot_date`
    acrescentado, acumulação e dedupe. A chave de cada stream está no `tables.py` — foto
    diária (`id + snapshot_date`) nos streams full refresh, a própria data no `user_insights`,
    só o id nos stories (fica a última leitura).
    """

    @property
    def platform_name(self) -> str:
        return PLATFORM

    def run(self) -> None:
        for stream in INSTAGRAM_STREAMS:
            self._process_stream(stream)

    def _process_stream(self, stream: InstagramStream) -> None:
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

        new = self.prepare(raw, stream)
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
    def prepare(raw: DataFrame, stream: InstagramStream) -> DataFrame:
        """Acrescenta `snapshot_date` e descarta linhas que não são dado. Pura."""
        df = raw.withColumn("snapshot_date", snapshot_date(col("_airbyte_extracted_at")))
        if stream.required_column is not None:
            df = df.filter(col(stream.required_column).isNotNull())
        return df

    def _read_existing_bronze(self, stream: InstagramStream) -> DataFrame | None:
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

    def _handle_source_error(self, stream: InstagramStream, exc: Exception) -> None:
        message = f"Raw source missing or unreadable for {stream.name}: {exc}"
        if settings.etl_strict:
            raise RuntimeError(message) from exc
        logger.warning("%s — skipping stream", message)

    def _handle_empty_source(self, stream: InstagramStream) -> None:
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

