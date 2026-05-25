"""
JNE Data Pipeline v2 — Bronze Extract DAG
==========================================
Extracts all Oracle source tables to MinIO as normalized Parquet files.
One Parquet directory per table, no joins, no transforms.

Runtime conf options (pass via Airflow UI or API):
  {"workers": 8}           — override parallel table workers (default: 4)
  {"chunksize": 500000}    — override rows per Parquet part (default: 100000)
  {"only": "cnote,mfbag"} — extract a subset of tables by out-name
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jne-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=12),
}

dag = DAG(
    'jne_bronze_extract',
    default_args=default_args,
    description='JNE pipeline v2: Oracle → MinIO bronze Parquet (normalized, no joins)',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['jne', 'bronze', 'extract', 'oracle', 'minio', 'v2'],
)

# ============================================================
# BRONZE EXTRACT: Oracle tables → MinIO Parquet
# ============================================================
# Reads every table in tables.yaml and writes one Parquet
# directory per table to s3://$MINIO_BUCKET/bronze/<table>/.
# Parallelism is handled inside the script via --workers.

bronze_extract = BashOperator(
    task_id='bronze_extract',
    bash_command=(
        'python /opt/airflow/scripts/extract/extract_bronze.py '
        '--config /opt/airflow/scripts/extract/tables.yaml '
        '--workers {{ dag_run.conf.get("workers", 4) }} '
        '--chunksize {{ dag_run.conf.get("chunksize", 100000) }} '
        '{% if dag_run.conf.get("only") %}--only {{ dag_run.conf["only"] }}{% endif %}'
    ),
    dag=dag,
)
