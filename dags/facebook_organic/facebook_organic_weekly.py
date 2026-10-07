"""Facebook orgânico, nível de post: extração semanal e medallion em cima dela.

**Quarta, 01:00 São Paulo.** O recorte que o monitoramento pediu é quarta da primeira semana
até terça da outra, e ele cai da mecânica que já existe: o sync da quarta à 01:00 fecha o
`snapshot_date` na terça (corte das 06:00), e o delta entre duas quartas cobre exatamente a
semana. O silver marca `gap_days` 7 e o gold expõe `days_covered` 7.

Semanal porque é aqui que está o custo: **uma chamada de insights por post, em todo sync**, e
nenhum dos streams tem incremental. A CAIXA sozinha são ~5.200 posts, 41 minutos de extração.
"""
import pendulum
from airflow import DAG
from organic_tasks import (
    DEFAULT_ARGS,
    FACEBOOK_NAMESPACE,
    WEEKLY_TAG,
    organic_flow,
)

PLATFORM = "facebook_organic"

with DAG(
    dag_id=f"{PLATFORM}_weekly",
    default_args=DEFAULT_ARGS,
    description="Facebook Pages, nível de post: quarta, recorte quarta → terça",
    schedule="0 1 * * 3",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=[PLATFORM, "medallion", "organic", "weekly"],
) as dag:
    organic_flow(WEEKLY_TAG, FACEBOOK_NAMESPACE, PLATFORM)
