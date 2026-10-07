"""Instagram orgânico, nível de conta: extração diária e medallion em cima dela.

`users`, `user_insights` e `user_lifetime_insights` — seguidores, alcance e novos seguidores
da conta, por dia. Um token vê todas as contas, então aqui é uma conexão só; o fan-out existe
no Facebook, onde cada página tem a sua.
"""
import pendulum
from airflow import DAG
from organic_tasks import (
    DAILY_TAG,
    DEFAULT_ARGS,
    INSTAGRAM_NAMESPACE,
    organic_flow,
)

PLATFORM = "instagram_organic"

# 02:00 São Paulo: antes do corte das 06:00, então a foto fecha o dia anterior.
with DAG(
    dag_id=f"{PLATFORM}_daily",
    default_args=DEFAULT_ARGS,
    description="Instagram, nível de conta: dispara, aguarda, monta o medallion",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=[PLATFORM, "medallion", "organic", "daily"],
) as dag:
    organic_flow(DAILY_TAG, INSTAGRAM_NAMESPACE, PLATFORM)
