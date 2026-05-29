"""
JNE Data Pipeline — Airflow DAG
=================================
Three-step pipeline:
  1. unify_in_oracle  — runs unification + transform SQL in Oracle (JNE → HOA schema)
  2. copy_to_postgres — copies HOA.TRANSFORMED_UNIFIED_SHIPMENTS to Postgres for DQ
  3. run_data_quality — runs the governance scorer from Postgres

Run state is tracked in HOA.PIPELINE_RUN_LOG; incremental mode is automatic.
Pass {"full": true} in dag_run.conf to force a full run.
Pass {"dq_threshold": 90.0} in dag_run.conf to tune DQ pass/fail output.
Pass {"dq_batch_size": 1000} in dag_run.conf to lower governance memory use.
DQ batches are checkpointed by Airflow logical-run timestamp and resume on retry.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jne-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=18),
}

dag = DAG(
    'jne_etl_pipeline',
    default_args=default_args,
    description='JNE pipeline: Oracle (unify+transform) → PostgreSQL → DQ',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['jne', 'etl', 'production', 'oracle', 'governance'],
)

# ============================================================
# STEP 1: UNIFY + TRANSFORM IN ORACLE
# ============================================================
# Reads JNE source tables, runs the 38-table unification SQL,
# and creates HOA.UNIFIED_SHIPMENTS + HOA.TRANSFORMED_UNIFIED_SHIPMENTS.
# Incremental by default (cutoff from HOA.PIPELINE_RUN_LOG).

unify_in_oracle = BashOperator(
    task_id='unify_in_oracle',
    bash_command=(
        'python /opt/airflow/scripts/etl/unify_oracle.py '
        '--run-id {{ ts_nodash }} '
        '{{ "--force-full" if dag_run.conf.get("full", False) else "" }}'
    ),
    dag=dag,
)

# ============================================================
# STEP 2: COPY TRANSFORMED TABLE TO POSTGRESQL
# ============================================================
# Streams HOA.TRANSFORMED_UNIFIED_SHIPMENTS from Oracle into
# transformed.unified_shipments in PostgreSQL for the DQ step.

copy_to_postgres = BashOperator(
    task_id='copy_to_postgres',
    bash_command=(
        'python /opt/airflow/scripts/etl/copy_to_postgres.py '
        '{{ "--force-full" if dag_run.conf.get("full", False) else "" }}'
    ),
    dag=dag,
)

# ============================================================
# STEP 3: DATA QUALITY GOVERNANCE
# ============================================================

run_data_quality = BashOperator(
    task_id='run_data_quality',
    bash_command=(
        'python /opt/airflow/scripts/governance/main.py '
        '--from-postgres '
        '--save-to-postgres '
        '--skip-csv-output '
        '--skip-inline-output '
        '--compact-summary '
        '--resumable --resume '
        '--run-id {{ ts_nodash }} '
        '--batch-size {{ dag_run.conf.get("dq_batch_size", 2000) }} '
        '--threshold {{ dag_run.conf.get("dq_threshold", 85.0) }}'
    ),
    dag=dag,
)

# ============================================================
# TASK DEPENDENCIES
# ============================================================
unify_in_oracle >> copy_to_postgres >> run_data_quality
