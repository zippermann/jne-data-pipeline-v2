"""
JNE Pipeline Configuration
===========================
Central config for all pipeline scripts.

Pipeline flow (new):
  Oracle JNE.* (source)
    → unify_oracle.py    → HOA.UNIFIED_SHIPMENTS + HOA.TRANSFORMED_UNIFIED_SHIPMENTS  (Oracle)
    → copy_to_postgres.py → transformed.unified_shipments  (PostgreSQL, for DQ)

Last updated: 2026-04-20
"""

import os

# ============================================================
# POSTGRESQL CONNECTION (DQ / downstream)
# ============================================================
DB_HOST = os.getenv('DB_HOST', 'jne-postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'jne_user')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'jne_secure_password_2024')
DB_NAME = os.getenv('POSTGRES_DB', 'jne_dashboard')

DB_CONN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ============================================================
# ORACLE CONNECTION (source + destination HOA schema)
# ============================================================
ORACLE_HOST     = os.getenv('ORACLE_HOST', 'oracle-db')
ORACLE_PORT     = os.getenv('ORACLE_PORT', '1521')
ORACLE_SID      = os.getenv('ORACLE_SID', '')
ORACLE_SERVICE  = os.getenv('ORACLE_SERVICE', '')
ORACLE_USER     = os.getenv('ORACLE_USER', 'HOA')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD', 'changeme')

# JNE = source schema (read-only tables); HOA = destination schema (write access)
ORACLE_JNE_SCHEMA = os.getenv('ORACLE_SCHEMA', 'JNE')
ORACLE_HOA_SCHEMA = 'HOA'

# DSN: SID takes precedence over service name
if ORACLE_SID:
    ORACLE_DSN = (
        f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)"
        f"(HOST={ORACLE_HOST})(PORT={ORACLE_PORT}))"
        f"(CONNECT_DATA=(SID={ORACLE_SID})))"
    )
else:
    ORACLE_DSN = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"


# ============================================================
# EXTRACTION / UNIFICATION WINDOW
# ============================================================
# Full runs query this many days of data from Oracle.
EXTRACTION_WINDOW_MONTHS = int(os.getenv('EXTRACTION_WINDOW_MONTHS', '3'))

_window_days_env = os.getenv('EXTRACTION_WINDOW_DAYS', '').strip()
EXTRACTION_WINDOW_DAYS = (
    int(_window_days_env) if _window_days_env else EXTRACTION_WINDOW_MONTHS * 30
)

# Incremental runs overlap this many days before the last run to catch late arrivals.
INCREMENTAL_OVERLAP_DAYS = int(os.getenv('INCREMENTAL_OVERLAP_DAYS', '1'))


# ============================================================
# ORACLE UNIFICATION SQL
# ============================================================
ORACLE_UNIFICATION_SQL_FILE = (
    os.getenv(
        'ORACLE_UNIFICATION_SQL_FILE',
        '/opt/airflow/scripts/transformations/unify_jne_oracle.sql'
    )
)


# ============================================================
# COPY BATCH SIZE
# ============================================================
# Rows to fetch from Oracle / flush to Postgres per batch.
LOAD_BATCH_MAX_ROWS = int(os.getenv('LOAD_BATCH_MAX_ROWS', '50000'))


# ============================================================
# SCHEMAS (PostgreSQL)
# ============================================================
SCHEMA_TRANSFORMED = 'transformed'
SCHEMA_AUDIT       = 'audit'


# ============================================================
# AUDIT RETENTION
# ============================================================
AUDIT_RETENTION_MONTHS = int(os.getenv('AUDIT_RETENTION_MONTHS', '6'))
