"""Instagram orgânico: extração da conexão principal e medallion em cima dela.

Prova a forma que os três DAGs do orgânico seguem, com uma conexão só. As tarefas
compartilhadas estão em `organic_tasks.py`.
"""
import pendulum
from airflow import DAG
from organic_tasks import (
    DEFAULT_ARGS,
    INSTAGRAM_MAIN_CONNECTION,
    chain_medallion,
    trigger_sync,
    wait_for_sync,
)

PLATFORM = "instagram_organic"

# 02:00 São Paulo: antes do corte das 06:00 que define o `snapshot_date`, então a foto fecha
# o dia anterior. O schedule é lido no fuso do `start_date`, não em UTC.
with DAG(
    dag_id=f"{PLATFORM}_daily",
    default_args=DEFAULT_ARGS,
    description="Instagram orgânico: dispara o Airbyte, aguarda o job, monta o medallion",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=[PLATFORM, "medallion", "organic"],
) as dag:
    job_id = trigger_sync(INSTAGRAM_MAIN_CONNECTION)
    chain_medallion(wait_for_sync(job_id), PLATFORM)
