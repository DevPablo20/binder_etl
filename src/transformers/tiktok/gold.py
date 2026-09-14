import logging

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from src.config import settings
from src.io.reader import read_delta
from src.io.writer import write_delta
from src.transformers.base import BaseTransformer
from src.transformers.tiktok.tables import GOLD_FACTS, GoldFactConfig, PLATFORM
from src.transformers.tiktok.transforms.gold import GOLD_TRANSFORMS

logger = logging.getLogger(__name__)


class TikTokGoldTransformer(BaseTransformer):
    @property
    def platform_name(self) -> str:
        return "tiktok"

    def run(self) -> None:
        for fact in GOLD_FACTS:
            self._process_fact(fact)

    def _process_fact(self, fact: GoldFactConfig) -> None:
        logger.info("Processing gold fact: %s", fact.name)

        sources = self._read_silver_sources(fact)
        if sources is None:
            return

        df = GOLD_TRANSFORMS[fact.name](sources, fact)
        if df.isEmpty():
            self._handle_empty_result(fact)
            return

        row_count = df.count()
        write_delta(df, "gold", fact.gold_table_name, mode="overwrite")

        logger.info(
            "Wrote gold/%s: %d rows",
            fact.gold_table_name,
            row_count,
        )

    def _read_silver_sources(
        self, fact: GoldFactConfig
    ) -> dict[str, DataFrame] | None:
        """Lê cada silver source uma vez e devolve pronto para o transform (função pura).

        `None` sinaliza que algum source falta ou está vazio — o fato inteiro é pulado
        (ou levanta, com `ETL_STRICT`), igual ao comportamento anterior.
        """
        sources: dict[str, DataFrame] = {}
        for source in fact.silver_sources:
            table_path = f"{PLATFORM}/{source}"
            try:
                df = read_delta(self.spark, "silver", table_path)
            except AnalysisException as exc:
                self._handle_missing_silver(fact, source, exc)
                return None
            except Exception as exc:
                if self._is_missing_path_error(exc):
                    self._handle_missing_silver(fact, source, exc)
                    return None
                raise

            if df.isEmpty():
                self._handle_empty_silver(fact, source)
                return None

            sources[source] = df

        return sources

    def _handle_missing_silver(
        self, fact: GoldFactConfig, source: str, exc: Exception
    ) -> None:
        message = f"Silver source missing or unreadable for {fact.name}/{source}: {exc}"
        if settings.etl_strict:
            raise RuntimeError(message) from exc
        logger.warning("%s — skipping fact", message)

    def _handle_empty_silver(self, fact: GoldFactConfig, source: str) -> None:
        message = f"Silver source empty for {fact.name}/{source}"
        if settings.etl_strict:
            raise RuntimeError(message)
        logger.warning("%s — skipping fact", message)

    def _handle_empty_result(self, fact: GoldFactConfig) -> None:
        message = f"Gold transform produced no rows for {fact.name}"
        if settings.etl_strict:
            raise RuntimeError(message)
        logger.warning("%s — skipping write", message)

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
