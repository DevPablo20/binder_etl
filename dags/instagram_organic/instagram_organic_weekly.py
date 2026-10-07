"""Instagram orgânico, nível de mídia: extração semanal e medallion em cima dela.

**Quarta, 02:00 São Paulo**, uma hora depois do Facebook — as duas redes escrevem as mesmas
tabelas de `gold/organic/`, e o pool do medallion serializa o resto.

`media` e `media_insights`, que é onde está o custo: o conector pede insights por mídia e não
tem incremental. Mesma janela do Facebook: o delta entre duas quartas cobre quarta → terça.
"""
import pendulum
from airflow import DAG
from organic_tasks import (
    DEFAULT_ARGS,
    INSTAGRAM_NAMESPACE,
    WEEKLY_TAG,
    organic_flow,
)

PLATFORM = "instagram_organic"

with DAG(
    dag_id=f"{PLATFORM}_weekly",
    default_args=DEFAULT_ARGS,
    description="Instagram, nível de mídia: quarta, recorte quarta → terça",
    schedule="0 2 * * 3",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=[PLATFORM, "medallion", "organic", "weekly"],
) as dag:
    organic_flow(WEEKLY_TAG, INSTAGRAM_NAMESPACE, PLATFORM)
