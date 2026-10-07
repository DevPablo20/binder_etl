"""Instagram stories: extração diária e medallion em cima dela.

DAG próprio porque a conexão é outra e o horário é outro. **09:30 São Paulo**, pouco antes da
primeira janela de publicação das contas: o insight de story só existe enquanto o story está
vivo (24h), então a leitura tem de cair o mais tarde possível na vida dele — às 09:30 o story
da manhã anterior é lido com ~23h e ainda sobra mais de uma hora até expirar. Medido no ciclo
real: 21,3 a 22,6h nos stories da manhã, 14,6 a 16,9h nos da noite.

**Nunca pode virar semanal**, por isso o condutor é a tag `stories` e não `daily`/`weekly`: a
cadência aqui é mandatória, não uma escolha de custo.

Roda o medallion inteiro do Instagram, não só a trilha de stories — o runner é
`<layer> <platform>`. São ~7 min idempotentes.
"""
import pendulum
from airflow import DAG
from organic_tasks import (
    DEFAULT_ARGS,
    INSTAGRAM_NAMESPACE,
    STORIES_TAG,
    organic_flow,
)

PLATFORM = "instagram_organic"

# Atenção ao corte das 06:00 do `snapshot_date`: leitura às 09:30 fecha o **próprio** dia,
# diferente dos DAGs da madrugada. A data de negócio do gold de stories vem de
# `published_date`, então consumidor nenhum sente; só auditoria foto a foto.
with DAG(
    dag_id="instagram_stories_daily",
    default_args=DEFAULT_ARGS,
    description="Instagram stories: dispara, aguarda, monta o medallion",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=[PLATFORM, "medallion", "organic", "stories"],
) as dag:
    organic_flow(STORIES_TAG, INSTAGRAM_NAMESPACE, PLATFORM)
