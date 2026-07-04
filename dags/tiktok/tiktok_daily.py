from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PIPELINE_CMD = "python -m src.pipelines.run {layer} tiktok"

default_args = {
    "owner": "binder",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="tiktok_daily",
    default_args=default_args,
    description="TikTok Ads medallion: raw (Airbyte) → bronze → silver → gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tiktok", "medallion"],
) as dag:
    sync_raw = EmptyOperator(
        task_id="sync_raw",
        doc="Run Airbyte TikTok sync to raw/airbyte/tiktok/ (external abctl).",
    )

    bronze_tiktok = BashOperator(
        task_id="bronze_tiktok",
        bash_command=PIPELINE_CMD.format(layer="bronze"),
    )

    silver_tiktok = BashOperator(
        task_id="silver_tiktok",
        bash_command=PIPELINE_CMD.format(layer="silver"),
    )

    gold_tiktok = BashOperator(
        task_id="gold_tiktok",
        bash_command=PIPELINE_CMD.format(layer="gold"),
    )

    sync_raw >> bronze_tiktok >> silver_tiktok >> gold_tiktok
