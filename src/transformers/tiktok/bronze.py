import logging

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from src.config import settings
from src.io.reader import read_delta, read_raw_parquet
from src.io.writer import write_delta
from src.transformers.base import BaseTransformer
from src.transformers.tiktok.tables import TIKTOK_STREAMS, TikTokStream

logger = logging.getLogger(__name__)


class TikTokBronzeTransformer(BaseTransformer):
    @property
    def platform_name(self) -> str:
        return "tiktok"

    def run(self) -> None:
        for stream in TIKTOK_STREAMS:
            self._process_stream(stream)

    def _process_stream(self, stream: TikTokStream) -> None:
        logger.info("Processing bronze stream: %s", stream.name)

        try:
            new = read_raw_parquet(self.spark, stream.raw_path)
        except AnalysisException as exc:
            self._handle_source_error(stream, exc)
            return
        except Exception as exc:
            if self._is_missing_path_error(exc):
                self._handle_source_error(stream, exc)
                return
            raise

        if new.isEmpty():
            self._handle_empty_source(stream)
            return

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

    def _read_existing_bronze(self, stream: TikTokStream) -> DataFrame | None:
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
        """Une o bronze já acumulado com o raw novo, antes do dedupe.

        Defesa contra retenção no raw ou conectores só-Overwrite: hoje o raw já guarda
        um arquivo por sync (redundante), mas o bronze deixa de depender disso. Função
        pura — não lê nem escreve, só decide o que combinar.
        """
        if existing is None:
            return new
        return existing.unionByName(new, allowMissingColumns=True)

    def _handle_source_error(self, stream: TikTokStream, exc: Exception) -> None:
        message = f"Raw source missing or unreadable for {stream.name}: {exc}"
        if settings.etl_strict:
            raise RuntimeError(message) from exc
        logger.warning("%s — skipping stream", message)

    def _handle_empty_source(self, stream: TikTokStream) -> None:
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
