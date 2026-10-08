"""CLI de manutenção do lake: recolhe arquivos Delta que o log já declarou mortos.

Mecânica, política e aritmética do estado estacionário em `src/io/maintenance.py`.

    python -m src.pipelines.vacuum                    # relatório, não apaga nada
    python -m src.pipelines.vacuum --apply            # aplica com retenção de 168h
    python -m src.pipelines.vacuum --apply --retain-hours 0 --allow-short-retention

**Dry run é o default.** A ação que apaga precisa de `--apply`, e descer abaixo da retenção
do Delta precisa de `--allow-short-retention` por cima disso — não para criar burocracia,
mas porque retenção curta troca espaço por risco de leitor concorrente, e essa troca é
decisão de quem conhece a janela, não default de script.
"""
import argparse
import logging
import sys

from src.io.maintenance import (
    DEFAULT_RETAIN_HOURS,
    DELTA_DEFAULT_RETAIN_HOURS,
    LAKE_LAYERS,
    requires_check_override,
    vacuum_lake,
)
from src.spark_session import get_spark_session


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m src.pipelines.vacuum",
        description="Recolhe arquivos Delta superados em bronze/silver/gold.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apaga de verdade; sem isso, só relata (dry run)",
    )
    parser.add_argument(
        "--retain-hours",
        type=float,
        default=DEFAULT_RETAIN_HOURS,
        help=f"janela de retenção em horas (default: {DEFAULT_RETAIN_HOURS})",
    )
    parser.add_argument(
        "--allow-short-retention",
        action="store_true",
        help=(
            f"autoriza retenção abaixo de {DELTA_DEFAULT_RETAIN_HOURS}h, desligando a "
            "checagem do Delta — exige que nada esteja lendo as tabelas"
        ),
    )
    parser.add_argument(
        "--layers",
        nargs="+",
        default=list(LAKE_LAYERS),
        choices=list(LAKE_LAYERS),
        help="camadas a recolher (default: todas)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if requires_check_override(args.retain_hours) and not args.allow_short_retention:
        print(
            f"Retenção de {args.retain_hours:g}h está abaixo do default do Delta "
            f"({DELTA_DEFAULT_RETAIN_HOURS}h).\n"
            "Isso só é seguro com ninguém lendo as tabelas — inclusive o catalog-api, que "
            "lê silver por HTTP e não passa pelo pool do Airflow.\n"
            "Se a janela está garantida, repita com --allow-short-retention.",
            file=sys.stderr,
        )
        return 2

    # Sem isto o `logger.info` de `maintenance` é descartado (raiz sem handler, nível
    # WARNING), e uma passada de vários minutos não dá sinal de vida nenhum até o fim.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    spark = get_spark_session(app_name="lake-vacuum")
    try:
        results = vacuum_lake(
            spark,
            layers=tuple(args.layers),
            retain_hours=args.retain_hours,
            apply=args.apply,
        )
    finally:
        spark.stop()

    total = sum(count for _, count in results)
    verb = "recolhidos" if args.apply else "a recolher"
    print()
    for uri, count in results:
        if count:
            print(f"  {count:>6d}  {uri}")
    print(
        f"\n{len(results)} tabela(s) Delta | {total} arquivo(s) {verb} | "
        f"retenção {args.retain_hours:g}h"
    )
    if not args.apply and total:
        print("Dry run — nada foi apagado. Repita com --apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
