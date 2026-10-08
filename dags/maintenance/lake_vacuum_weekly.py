"""Manutenção semanal do lake: recolhe os arquivos Delta que o log já declarou mortos.

Por que existe um DAG e não um passo no fim do medallion: o tombstone volta a **cada**
escrita, então a limpeza é contínua por natureza — mas ela não pode competir com quem está
escrevendo. Pendurar o VACUUM no fim de cada medallion o faria rodar cinco vezes por dia,
sempre no pico, e cada run recolheria quase nada (os tombstones ainda estariam dentro da
janela de retenção). Semanal, fora do horário de todos os outros DAGs, recolhe de uma vez o
que envelheceu na semana.

**Retenção é a do Delta (168h), de propósito.** Abaixo disso o Delta exige desligar a
checagem de retenção, e a checagem protege contra um leitor que já resolveu a lista de
arquivos de uma versão — inclusive o `catalog-api`, que lê silver por HTTP e não passa pelo
pool do Airflow. Nenhum horário do Airflow garante que ele não está lendo, então retenção
curta é operação manual com janela escolhida, não default de DAG. A conta do que isso custa
em disco está em `src/io/maintenance.py`.

Roda no pool `spark_medallion` (um slot): `local[*]` em quatro núcleos não divide, e o VACUUM
lista tabela por tabela no MinIO. Com o pool, ele nunca cai em cima de um medallion.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# O nome do pool é SSOT do orgânico, que o criou no `airflow-init`. Importar evita duas
# strings iguais que podem divergir.
from organic_tasks import POOL_MEDALLION

default_args = {
    "owner": "binder",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="lake_vacuum_weekly",
    default_args=default_args,
    description="VACUUM semanal em bronze/silver/gold: recolhe arquivos Delta superados",
    # Domingo 04:00: os diários do orgânico são 01:00 e 02:00, stories 09:30, os semanais
    # quarta 01:00 e o `tiktok_daily` à meia-noite. Domingo às 04:00 não encosta em nenhum.
    schedule="0 4 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance", "delta"],
) as dag:
    vacuum_lake = BashOperator(
        task_id="vacuum_lake",
        bash_command="python -m src.pipelines.vacuum --apply",
        pool=POOL_MEDALLION,
        doc=(
            "Recolhe arquivos Delta fora da janela de 168h em bronze/silver/gold. "
            "Não altera nenhuma linha: só apaga arquivo que o `_delta_log` já marcou "
            "como `remove`."
        ),
    )
