import sys

from src.spark_session import get_spark_session
from src.transformers import TRANSFORMERS


def main() -> None:
    layer = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    platform = (sys.argv[2] if len(sys.argv) > 2 else "").strip().lower()

    if not layer or layer not in TRANSFORMERS:
        print("Usage: python -m src.pipelines.run <layer> <platform>")
        print(f"Layers: {', '.join(TRANSFORMERS)}")
        sys.exit(1)

    registry = TRANSFORMERS[layer]
    if not platform or platform not in registry:
        print(f"Usage: python -m src.pipelines.run {layer} <platform>")
        print(f"Platforms: {', '.join(registry)}")
        sys.exit(1)

    spark = get_spark_session(app_name=f"{layer}-{platform}")
    transformer = registry[platform](spark)
    transformer.run()
    spark.stop()
    print(f"{layer.capitalize()} completed for {platform}.")


if __name__ == "__main__":
    main()
