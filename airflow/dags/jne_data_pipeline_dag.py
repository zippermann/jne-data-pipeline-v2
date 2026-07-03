"""
JNE Data Pipeline DAG
=====================
Relational Oracle → Parquet bronze extraction, derived transformation,
governance checks, and mart loading.

Pass {"keep_scope": true} in dag_run.conf to leave Oracle scope tables in place
for inspection after the run.
Pass {"clickhouse_governance_only": true} to refresh only ClickHouse governance
outputs during the mart load task.
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
    "jne_data_pipeline",
    default_args=default_args,
    description="JNE relational bronze extraction with governance and derived mart tables",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["jne", "bronze", "oracle", "parquet", "governance"],
) as dag:
    run_context = (
        'RUN_ID="{{ ts_nodash }}" && '
        'EXTRACT_DATE="{{ dag_run.logical_date.strftime("%Y-%m-%d") }}" && '
        'BRONZE_RUN_PREFIX="$(python -m pipeline_context bronze-prefix '
        '--config config/config.yaml --run-id "$RUN_ID" --extract-date "$EXTRACT_DATE")" && '
        'EXTRACTION_WINDOW_START="$(python -m pipeline_context window --config config/config.yaml start)" && '
        'EXTRACTION_WINDOW_END="$(python -m pipeline_context window --config config/config.yaml end)" && '
        'export RUN_ID EXTRACT_DATE BRONZE_RUN_PREFIX '
        'EXTRACTION_WINDOW_START EXTRACTION_WINDOW_END && '
    )

    extract_oracle = BashOperator(
        task_id="extract_oracle",
        bash_command=(
            "cd /opt/airflow/project && "
            f"{run_context}"
            "python -m extractor.bronze "
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
            'python -m governance.runner '
            '--source minio '
            '--config config/config.yaml '
            '--bronze-run-prefix "$BRONZE_RUN_PREFIX" '
            '--output-dir "governance/outputs/$RUN_ID"'
        ),
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=(
            "cd /opt/airflow/project && "
            f"{run_context}"
            'python -m transform.transform_data '
            '--source minio '
            '--config config/config.yaml '
            '--bronze-run-prefix "$BRONZE_RUN_PREFIX"'
        ),
    )

    load_data_mart_clickhouse = BashOperator(
        task_id="load_data_mart_clickhouse",
        bash_command=(
            "cd /opt/airflow/project && "
            f"{run_context}"
            '{{ "python -m loader.mart_load_clickhouse --config config/mart_clickhouse.yaml " '
            '+ ("--governance-only" if dag_run.conf.get("clickhouse_governance_only", False) else "") '
            'if dag_run.conf.get("load_clickhouse", True) else '
            '"echo ClickHouse mart load disabled by dag_run.conf" }}'
        ),
    )

    extract_oracle >> transform_data >> run_governance >> load_data_mart_clickhouse
