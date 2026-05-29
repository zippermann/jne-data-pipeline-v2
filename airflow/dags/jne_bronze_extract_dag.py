"""
JNE Bronze Extraction DAG
=========================
Relational Oracle → Parquet bronze extraction.

Pass {"keep_scope": true} in dag_run.conf to leave Oracle scope tables in place
for inspection after the run.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "jne-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=18),
}


with DAG(
    "jne_bronze_extract",
    default_args=default_args,
    description="JNE relational bronze extraction: Oracle source tables to Parquet",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["jne", "bronze", "oracle", "parquet"],
) as dag:
    extract_bronze = BashOperator(
        task_id="extract_bronze",
        bash_command=(
            "cd /opt/airflow/project && "
            "python -m src.bronze "
            "--config config/config.yaml "
            "--run-id {{ ts_nodash }} "
            '{{ "--keep-scope" if dag_run.conf.get("keep_scope", False) else "" }}'
        ),
    )
