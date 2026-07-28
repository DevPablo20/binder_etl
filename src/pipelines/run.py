import sys

from src.serving import land_ads_daily_metrics
from src.spark_session import get_spark_session
from src.transformers import PLATFORMS, TRANSFORMERS

MEDALLION_LAYERS = ("bronze", "silver", "gold")

PIPELINE_LAYERS = ("medallion", "land", *TRANSFORMERS)


def _run_layer(layer: str, platform: str) -> None:
    spark = get_spark_session(app_name=f"{layer}-{platform}")
    transformer = TRANSFORMERS[layer][platform](spark)
    transformer.run()
    spark.stop()
    print(f"{layer.capitalize()} completed for {platform}.")


def _run_land(platform: str) -> None:
    spark = get_spark_session(app_name=f"land-{platform}")
    try:
        count = land_ads_daily_metrics(spark, platform)
        print(f"Land completed for {platform}: {count} rows.")
    finally:
        spark.stop()


def main() -> None:
    layer = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    platform = (sys.argv[2] if len(sys.argv) > 2 else "").strip().lower()

    if not layer or layer not in PIPELINE_LAYERS:
        print("Usage: python -m src.pipelines.run <layer> <platform>")
        print(f"Layers: {', '.join(PIPELINE_LAYERS)}")
        sys.exit(1)

    if not platform or platform not in PLATFORMS:
        print(f"Usage: python -m src.pipelines.run {layer} <platform>")
        print(f"Platforms: {', '.join(PLATFORMS)}")
        sys.exit(1)

    if layer == "medallion":
        for lake_layer in MEDALLION_LAYERS:
            _run_layer(lake_layer, platform)
        print(f"Medallion completed for {platform}.")
        return

    if layer == "land":
        _run_land(platform)
        return

    _run_layer(layer, platform)


if __name__ == "__main__":
    main()
