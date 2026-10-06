import os
from pathlib import Path

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path)


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


class Settings:
    endpoint: str = _get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = _get("MINIO_ROOT_USER", "minioadmin")
    secret_key: str = _get("MINIO_ROOT_PASSWORD", "minioadmin")

    bucket_raw: str = _get("MINIO_BUCKET_RAW", "raw")
    bucket_bronze: str = _get("MINIO_BUCKET_BRONZE", "bronze")
    bucket_silver: str = _get("MINIO_BUCKET_SILVER", "silver")
    bucket_gold: str = _get("MINIO_BUCKET_GOLD", "gold")

    etl_strict: bool = _get("ETL_STRICT", "false").lower() in ("true", "1", "yes")

    # API pública do Airbyte: os DAGs disparam o sync e acompanham o job por ela. O
    # `load_dotenv` acima não acha `.env` dentro do container do Airflow (só `./src` é
    # montado), então estas três chegam pelo `environment` do compose, como as de MinIO.
    airbyte_api_url: str = _get("AIRBYTE_API_URL", "http://localhost:8080").rstrip("/")
    airbyte_client_id: str = _get("AIRBYTE_CLIENT_ID")
    airbyte_client_secret: str = _get("AIRBYTE_CLIENT_SECRET")

    # A Catalog API é serviço sempre-ligado, lendo silver pequeno (centenas de linhas) —
    # não precisa de local[*]. Default pequeno para não disputar núcleo com pipelines
    # (CLI, DAG) rodando no mesmo host/máquina de dev.
    catalog_spark_master: str = _get("CATALOG_SPARK_MASTER", "local[2]")

    # Heap do driver. O default do Spark é 1 GiB — medido, não suposto — e o medallion roda
    # tudo no driver (`local[*]`), sem executor separado. Com o medallion serializado num
    # slot de pool, 4 GiB não disputam com nada.
    spark_driver_memory: str = _get("SPARK_DRIVER_MEMORY", "4g")

    def bucket_for_layer(self, layer: str) -> str:
        layer = layer.lower()
        if layer == "raw":
            return self.bucket_raw
        if layer == "bronze":
            return self.bucket_bronze
        if layer == "silver":
            return self.bucket_silver
        if layer == "gold":
            return self.bucket_gold
        raise ValueError(f"Invalid layer: {layer}. Use raw, bronze, silver, or gold.")

    def s3a_uri(self, bucket: str, path: str = "") -> str:
        p = path.strip("/")
        return f"s3a://{bucket}/{p}" if p else f"s3a://{bucket}"


settings = Settings()
