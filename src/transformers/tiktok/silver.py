import logging

from pyspark.sql.utils import AnalysisException

from src.config import settings
from src.io.reader import read_delta
from src.io.writer import write_delta
from src.transformers.base import BaseTransformer
from src.transformers.tiktok.transforms.silver import SILVER_TRANSFORMS
from src.transformers.tiktok.tables import TIKTOK_STREAMS, TikTokStream

logger = logging.getLogger(__name__)


class TikTokSilverTransformer(BaseTransformer):
    @property
    def platform_name(self) -> str:
        return "tiktok"

    def run(self) -> None:
        for stream in TIKTOK_STREAMS:
            self._process_stream(stream)

    def _process_stream(self, stream: TikTokStream) -> None:
        logger.info("Processing silver stream: %s", stream.name)

        try:
            df = read_delta(self.spark, "bronze", stream.bronze_table_name)
        except AnalysisException as exc:
            self._handle_source_error(stream, exc)
            return
        except Exception as exc:
            if self._is_missing_path_error(exc):
                self._handle_source_error(stream, exc)
                return
            raise

        if df.isEmpty():
            self._handle_empty_source(stream)
            return

        input_count = df.count()
        transformed = SILVER_TRANSFORMS[stream.name](df)
        output_count = transformed.count()

        write_delta(transformed, "silver", stream.silver_table_name, mode="overwrite")

        logger.info(
            "Wrote silver/%s: %d rows (from %d bronze rows)",
            stream.silver_table_name,
            output_count,
            input_count,
        )

    def _handle_source_error(self, stream: TikTokStream, exc: Exception) -> None:
        message = f"Bronze source missing or unreadable for {stream.name}: {exc}"
        if settings.etl_strict:
            raise RuntimeError(message) from exc
        logger.warning("%s — skipping stream", message)

    def _handle_empty_source(self, stream: TikTokStream) -> None:
        message = f"Bronze source empty for {stream.name}"
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
