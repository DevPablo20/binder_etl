import logging

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from src.config import settings
from src.io.reader import read_delta
from src.io.writer import write_delta
from src.transformers.base import BaseTransformer
from src.transformers.instagram_organic.tables import (
    PLATFORM,
    SILVER_TABLES,
    SilverTableConfig,
)
from src.transformers.instagram_organic.transforms.silver import SILVER_TRANSFORMS

logger = logging.getLogger(__name__)


class InstagramOrganicSilverTransformer(BaseTransformer):
    """Silver do instagram_organic. Como no `facebook_organic`, uma tabela silver pode vir de
    mais de um stream bronze, e o transform recebe um dict de DataFrames já lidos."""

    @property
    def platform_name(self) -> str:
        return PLATFORM

    def run(self) -> None:
        for table in SILVER_TABLES:
            self._process_table(table)

    def _process_table(self, table: SilverTableConfig) -> None:
        logger.info("Processing silver table: %s", table.name)

        sources = self._read_bronze_sources(table)
        if sources is None:
            return

        transformed = SILVER_TRANSFORMS[table.name](sources)
        transformed.cache()
        output_count = transformed.count()

        write_delta(transformed, "silver", table.silver_table_name, mode="overwrite")

        logger.info(
            "Wrote silver/%s: %d rows", table.silver_table_name, output_count
        )

        transformed.unpersist()

    def _read_bronze_sources(
        self, table: SilverTableConfig
    ) -> dict[str, DataFrame] | None:
        """`None` sinaliza que algum source falta ou está vazio — a tabela é pulada (ou
        levanta, com `ETL_STRICT`)."""
        sources: dict[str, DataFrame] = {}
        for source in table.bronze_sources:
            try:
                df = read_delta(self.spark, "bronze", f"{PLATFORM}/{source}")
            except AnalysisException as exc:
                self._handle_source_error(table, source, exc)
                return None
            except Exception as exc:
                if self._is_missing_path_error(exc):
                    self._handle_source_error(table, source, exc)
                    return None
                raise

            if df.isEmpty():
                self._handle_empty_source(table, source)
                return None

            sources[source] = df
        return sources

    def _handle_source_error(
        self, table: SilverTableConfig, source: str, exc: Exception
    ) -> None:
        message = f"Bronze source missing or unreadable for {table.name}/{source}: {exc}"
        if settings.etl_strict:
            raise RuntimeError(message) from exc
        logger.warning("%s — skipping table", message)

    def _handle_empty_source(self, table: SilverTableConfig, source: str) -> None:
        message = f"Bronze source empty for {table.name}/{source}"
        if settings.etl_strict:
            raise RuntimeError(message)
        logger.warning("%s — skipping table", message)

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
