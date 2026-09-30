from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PIPELINE_CMD = "python -m src.pipelines.run {layer} instagram_organic"

default_args = {
    "owner": "binder",
    "depends_on_past": False,
    "retries": 1,
}

# A conexão principal do Airbyte sincroniza às 01:00 de São Paulo (04:00 UTC); a de stories,
# de hora em hora. O DAG roda às 08:00 UTC (05:00 em São Paulo), antes do corte das 06:00 que
# define o `snapshot_date` — a foto da madrugada fecha o dia anterior.
with DAG(
    dag_id="instagram_organic_daily",
    default_args=default_args,
    description="Instagram orgânico medallion: raw (Airbyte) → bronze → silver → gold",
    schedule="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["instagram_organic", "medallion"],
) as dag:
    sync_raw = EmptyOperator(
        task_id="sync_raw",
        doc="Airbyte Instagram syncs to raw/airbyte/instagram_organic/ (main 01:00 SP; stories hourly).",
    )

    bronze_instagram_organic = BashOperator(
        task_id="bronze_instagram_organic",
        bash_command=PIPELINE_CMD.format(layer="bronze"),
    )

    silver_instagram_organic = BashOperator(
        task_id="silver_instagram_organic",
        bash_command=PIPELINE_CMD.format(layer="silver"),
    )

    gold_instagram_organic = BashOperator(
        task_id="gold_instagram_organic",
        bash_command=PIPELINE_CMD.format(layer="gold"),
    )

    sync_raw >> bronze_instagram_organic >> silver_instagram_organic >> gold_instagram_organic
