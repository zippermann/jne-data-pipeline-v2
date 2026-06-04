"""
JNE Bronze Extraction DAG
=========================
Relational Oracle → Parquet bronze extraction, governance checks, and
Postgres inspection loading.

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
    description="JNE relational bronze extraction with governance and Postgres loading",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["jne", "bronze", "oracle", "parquet", "governance", "postgres"],
) as dag:
    run_context = (
        'RUN_ID="{{ ts_nodash }}" && '
        'EXTRACT_DATE="{{ dag_run.logical_date.strftime("%Y-%m-%d") }}" && '
        'BRONZE_RUN_PREFIX="$(python -m src.pipeline_context bronze-prefix '
        '--config config/config.yaml --run-id "$RUN_ID" --extract-date "$EXTRACT_DATE")" && '
        'GOVERNANCE_OUTPUT_PREFIX="$(python -m src.pipeline_context governance-prefix --run-id "$RUN_ID")" && '
        'EXTRACTION_WINDOW_START="$(python -m src.pipeline_context window --config config/config.yaml start)" && '
        'EXTRACTION_WINDOW_END="$(python -m src.pipeline_context window --config config/config.yaml end)" && '
        'export RUN_ID EXTRACT_DATE BRONZE_RUN_PREFIX GOVERNANCE_OUTPUT_PREFIX '
        'EXTRACTION_WINDOW_START EXTRACTION_WINDOW_END && '
    )

    extract_bronze = BashOperator(
        task_id="extract_bronze",
        bash_command=(
            "cd /opt/airflow/project && "
            f"{run_context}"
            "python -m src.bronze "
            "--config config/config.yaml "
            '--run-id "$RUN_ID" '
            '--extract-date "$EXTRACT_DATE" '
            '{{ "--keep-scope" if dag_run.conf.get("keep_scope", False) else "" }}'
        ),
    )

    run_governance = BashOperator(
        task_id="run_governance",
        bash_command=(
            "cd /opt/airflow/project && "
            f"{run_context}"
            "python -m src.runner --config config/governance.yaml"
        ),
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=(
            "cd /opt/airflow/project && "
            f"{run_context}"
            "python -m src.postgres_load --config config/governance.yaml"
        ),
    )

    extract_bronze >> run_governance >> load_postgres
