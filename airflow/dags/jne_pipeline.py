"""
JNE Data Pipeline v2
====================
Full pipeline DAG. Each layer is a task (or task group) in sequence.

Current layers:
  1. bronze_extract — Oracle source tables → MinIO Parquet (normalized, no joins)

Planned layers (not yet implemented):
  2. silver_load    — MinIO Parquet → PostgreSQL normalized tables
  3. gold_unify     — PostgreSQL silver → transport_unified (narrow join)

Runtime conf options (pass via Airflow UI or API):
  {"workers": 8}           — bronze: parallel table workers (default: 4)
  {"chunksize": 500000}    — bronze: rows per Parquet part (default: 100000)
  {"only": "cnote,mfbag"} — bronze: extract a subset of tables by out-name
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
    'jne_pipeline',
    default_args=default_args,
    description='JNE pipeline v2: Oracle → bronze → silver → gold',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['jne', 'v2', 'oracle', 'minio', 'bronze'],
)

# ============================================================
# LAYER 1 — BRONZE EXTRACT: Oracle tables → MinIO Parquet
# ============================================================

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

# ============================================================
# LAYER 2 — SILVER LOAD (placeholder)
# ============================================================
# silver_load = BashOperator(...)
# bronze_extract >> silver_load

# ============================================================
# LAYER 3 — GOLD UNIFY (placeholder)
# ============================================================
# gold_unify = BashOperator(...)
# silver_load >> gold_unify
