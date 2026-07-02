"""JDBC helpers for Bridge read and serving write."""

from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession

from src.config import settings


def jdbc_properties() -> dict[str, str]:
    """Parse BINDER_DATABASE_URL into Spark JDBC connection properties."""
    url = settings.binder_database_url
    if not url:
        raise ValueError("BINDER_DATABASE_URL is not set")

    parsed = urlparse(url)
    if parsed.scheme not in ("postgresql", "postgres"):
        raise ValueError(f"Unsupported database scheme: {parsed.scheme}")

    jdbc_url = (
        f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}"
    )
    user = parsed.username or ""
    password = parsed.password or ""

    return {
        "url": jdbc_url,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def read_bridge_table(spark: SparkSession, table: str) -> DataFrame:
    """Read a Bridge table from backend Postgres via JDBC."""
    raise NotImplementedError(
        f"read_bridge_table({table!r}) is not implemented yet"
    )


def write_serving(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """Write enriched facts to serving schema in backend Postgres."""
    raise NotImplementedError(
        f"write_serving({table!r}, mode={mode!r}) is not implemented yet"
    )
