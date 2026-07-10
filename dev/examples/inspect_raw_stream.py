"""Inspect a raw Airbyte Parquet stream in MinIO.

Usage (from binder_etl/ with spark-dev running):

    docker compose exec spark-dev python dev/examples/inspect_raw_stream.py
    docker compose exec spark-dev python dev/examples/inspect_raw_stream.py airbyte/tiktok/campaigns
"""

from __future__ import annotations

import sys

from src.io.reader import read_raw_parquet
from src.spark_session import get_spark_session

DEFAULT_PATH = "airbyte/tiktok/ads"


def main(raw_path: str = DEFAULT_PATH) -> None:
    spark = get_spark_session("dev-inspect-raw")
    try:
        df = read_raw_parquet(spark, raw_path)
        print(f"path: {raw_path}")
        print(f"rows: {df.count()}")
        print("--- schema ---")
        df.printSchema()
        print("--- dtypes ---")
        for name, dtype in df.dtypes:
            print(f"  {name}: {dtype}")
        print("--- sample ---")
        df.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    main(path)
