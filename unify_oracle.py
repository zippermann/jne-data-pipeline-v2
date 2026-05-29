"""
JNE Oracle Unification + Transform
=====================================
Step 1 of the new pipeline.

Full run — phased scratch-table approach (same strategy as the old Postgres pipeline):
  Phase A: Materialize each dedup CTE into HOA.SCRATCH_<name> NOLOGGING in parallel.
           Each scratch table is indexed on its join key and ANALYZE'd so the query
           planner has accurate row counts before the final join.
  Phase B: Run dependent CTEs (mfcnote_pivoted) after their parent scratch tables exist.
  Phase C: Execute the final 38-table LEFT JOIN referencing HOA.SCRATCH_* tables.
           The result is written as HOA.UNIFIED_SHIPMENTS NOLOGGING.
  Cleanup: DROP all HOA.SCRATCH_* tables (always, even on failure).

Incremental run — inline CTEs, no scratch tables (daily slice is small enough):
  DELETE rows >= cutoff, INSERT from the SQL WITH…SELECT filtered by cutoff.

Transform step runs in Oracle after unification (adds DQ/manifest columns).
Run state is tracked in HOA.PIPELINE_RUN_LOG.
"""

import re
import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import oracledb

sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from pipeline_config import (
        ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD,
        ORACLE_HOA_SCHEMA, ORACLE_UNIFICATION_SQL_FILE,
        EXTRACTION_WINDOW_DAYS, INCREMENTAL_OVERLAP_DAYS,
    )
except ImportError:
    sys.path.insert(0, '/opt/airflow')
    from pipeline_config import (
        ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD,
        ORACLE_HOA_SCHEMA, ORACLE_UNIFICATION_SQL_FILE,
        EXTRACTION_WINDOW_DAYS, INCREMENTAL_OVERLAP_DAYS,
    )

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

UNIFIED_TABLE     = f"{ORACLE_HOA_SCHEMA}.UNIFIED_SHIPMENTS"
TRANSFORMED_TABLE = f"{ORACLE_HOA_SCHEMA}.TRANSFORMED_UNIFIED_SHIPMENTS"
RUN_LOG_TABLE     = f"{ORACLE_HOA_SCHEMA}.PIPELINE_RUN_LOG"
SCRATCH_PREFIX    = "SCRATCH_"

# Number of scratch tables to materialize in parallel.
# Each worker opens its own Oracle connection.
SCRATCH_PARALLEL_WORKERS = int(os.getenv('SCRATCH_PARALLEL_WORKERS', '2'))

# Degree of parallelism for Oracle parallel query within each scratch CTAS.
# Keep at 2: DOP=4 causes each parallel slave to claim its own TEMP sort area,
# which exhausts the TEMP tablespace on large CTEs (e.g. mfcnote_pivoted).
ORACLE_PARALLEL_DEGREE = int(os.getenv('ORACLE_PARALLEL_DEGREE', '2'))

# mfcnote_typed reads the full CMS_MFCNOTE table with no natural date boundary.
# Restrict it to start_date minus this many extra days so the scratch table stays
# manageable (avoids ORA-01652 on the mfcnote_pivoted aggregation step).
SCRATCH_DATE_LOOKBACK_DAYS = int(os.getenv('SCRATCH_DATE_LOOKBACK_DAYS', '30'))
SCRATCH_DATE_FILTERS = {
    # Transaction/event tables — filter to window + lookback so scratch tables
    # only hold relevant rows instead of the full historical dataset.
    # Lookup tables (mdt_city, courier, ora_zone, ora_user, drourate) are omitted
    # because they're already tiny and have no meaningful date boundary.
    'cnote_amo_deduped':     'CDATE',
    'mrcnote_deduped':       'MRCNOTE_DATE',
    'drsheet_deduped':       'DRSHEET_DATE',
    'mrsheet_deduped':       'MRSHEET_DATE',
    'drsheet_pra_deduped':   'CREATION_DATE',
    'dhicnote_deduped':      'DHICNOTE_TDATE',
    'mhicnote_deduped':      'MHICNOTE_DATE',
    'dhocnote_deduped':      'DHOCNOTE_TDATE',
    'mhocnote_deduped':      'MHOCNOTE_DATE',
    'dhoundel_deduped':      'CREATE_DATE',
    'mhoundel_deduped':      'MHOUNDEL_DATE',
    'mfcnote_typed':         'MFCNOTE_CRDATE',
    'manifest_deduped':      'MANIFEST_DATE',
    'dbag_ho_deduped':       'CDATE',
    'dmbag_deduped':         'ESB_TIME',
    'dhov_rsheet_deduped':   'CREATE_DATE',
    'dstatus_deduped':       'CREATE_DATE',
    'cost_dtransit_deduped': 'ESB_TIME',
    'cost_mtransit_deduped': 'MANIFEST_DATE',
    'rdsj_deduped':          'RDSJ_CDATE',
    'dsj_deduped':           'DSJ_CDATE',
    'msj_deduped':           'MSJ_DATE',
    'dsmu_deduped':          'ESB_TIME',
    'msmu_deduped':          'MSMU_DATE',
    'dhi_hoc_deduped':       'CDATE',
    'mhi_hoc_deduped':       'MHI_DATE',
    'mmbag_deduped':         'MMBAG_DATE',
    'mfbag_deduped':         'MFBAG_CRDATE',
    'dcorrect_deduped':      'DCORRECT_CDATE'
}

# mfcnote_pivoted must wait until mfcnote_typed is materialized first.
CTE_DEPENDENCIES = {
    'mfcnote_pivoted': 'mfcnote_typed',
}

# Join key columns per scratch table — used to create indexes before the final join.
SCRATCH_JOIN_KEYS = {
    'cnote_amo_deduped':       ['CNOTE_NO'],
    'mrcnote_deduped':         ['MRCNOTE_NO'],
    'drsheet_deduped':         ['DRSHEET_CNOTE_NO'],
    'mrsheet_deduped':         ['MRSHEET_NO'],
    'drsheet_pra_deduped':     ['DRSHEET_CNOTE_NO'],
    'dhicnote_deduped':        ['DHICNOTE_CNOTE_NO'],
    'mhicnote_deduped':        ['MHICNOTE_NO'],
    'dhocnote_deduped':        ['DHOCNOTE_CNOTE_NO'],
    'mhocnote_deduped':        ['MHOCNOTE_NO'],
    'dhoundel_deduped':        ['DHOUNDEL_CNOTE_NO'],
    'mhoundel_deduped':        ['MHOUNDEL_NO'],
    'mfcnote_typed':           ['MFCNOTE_NO'],
    'mfcnote_pivoted':         ['MFCNOTE_NO'],
    'manifest_deduped':        ['MANIFEST_NO'],
    'dbag_ho_deduped':         ['DBAG_CNOTE_NO'],
    'dmbag_deduped':           ['DMBAG_NO', 'DMBAG_BAG_NO'],
    'dhov_rsheet_deduped':     ['DHOV_RSHEET_CNOTE'],
    'dstatus_deduped':         ['DSTATUS_CNOTE_NO'],
    'cost_dtransit_deduped':   ['CNOTE_NO'],
    'cost_mtransit_deduped':   ['MANIFEST_NO'],
    'rdsj_deduped':            ['RDSJ_HVI_NO'],
    'dsj_deduped':             ['DSJ_HVO_NO'],
    'msj_deduped':             ['MSJ_NO'],
    'dsmu_deduped':            ['DSMU_NO', 'DSMU_BAG_NO'],
    'msmu_deduped':            ['MSMU_NO'],
    'drourate_deduped':        ['DROURATE_CODE', 'DROURATE_SERVICE'],
    'mdt_city_deduped':        ['CITY_CODE'],
    'courier_deduped':         ['COURIER_ID'],
    'ora_zone_deduped':        ['ZONE_CODE'],
    'dhi_hoc_deduped':         ['DHI_CNOTE_NO'],
    'mhi_hoc_deduped':         ['MHI_NO'],
    'ora_user_deduped':        ['USER_ID'],
    'mmbag_deduped':           ['MMBAG_NO'],
    'mfbag_deduped':           ['MFBAG_NO'],
    'dcorrect_deduped':      ['DCORRECT_CNOTE_NO']
}


# ============================================================
# CONNECTION
# ============================================================

def get_connection():
    return oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)


# ============================================================
# RUN LOG TABLE
# ============================================================

def ensure_run_log_table(cursor):
    cursor.execute("""
        DECLARE
            l_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO l_count
            FROM all_tables
            WHERE owner = :owner AND table_name = 'PIPELINE_RUN_LOG';
            IF l_count = 0 THEN
                EXECUTE IMMEDIATE '
                    CREATE TABLE """ + ORACLE_HOA_SCHEMA + """.PIPELINE_RUN_LOG (
                        RUN_ID        VARCHAR2(50)   NOT NULL,
                        RUN_MODE      VARCHAR2(20)   NOT NULL,
                        CUTOFF_DATE   TIMESTAMP,
                        STARTED_AT    TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
                        COMPLETED_AT  TIMESTAMP,
                        ROW_COUNT     NUMBER,
                        STATUS        VARCHAR2(20)   NOT NULL,
                        ERROR_DETAILS VARCHAR2(4000),
                        CONSTRAINT PK_PIPELINE_RUN_LOG PRIMARY KEY (RUN_ID)
                    )
                ';
            END IF;
        END;
    """, owner=ORACLE_HOA_SCHEMA)


def start_run_log(cursor, run_id, mode, cutoff_date):
    # DELETE first so retries with the same run_id don't hit ORA-00001.
    cursor.execute(f"DELETE FROM {RUN_LOG_TABLE} WHERE RUN_ID = :1", (run_id,))
    cursor.execute(
        f"INSERT INTO {RUN_LOG_TABLE} (RUN_ID, RUN_MODE, CUTOFF_DATE, STATUS) "
        f"VALUES (:1, :2, :3, 'RUNNING')",
        (run_id, mode, cutoff_date)
    )


def complete_run_log(cursor, run_id, row_count, status, error=None):
    cursor.execute(
        f"UPDATE {RUN_LOG_TABLE} "
        f"SET COMPLETED_AT = SYSTIMESTAMP, ROW_COUNT = :1, STATUS = :2, ERROR_DETAILS = :3 "
        f"WHERE RUN_ID = :4",
        (row_count, status, str(error)[:4000] if error else None, run_id)
    )


def get_last_successful_run(cursor):
    cursor.execute(
        f"SELECT CUTOFF_DATE, COMPLETED_AT FROM {RUN_LOG_TABLE} "
        f"WHERE STATUS = 'SUCCESS' ORDER BY COMPLETED_AT DESC FETCH FIRST 1 ROWS ONLY"
    )
    return cursor.fetchone()


# ============================================================
# UTILITIES
# ============================================================

def drop_table_if_exists(cursor, table_name):
    """Drop a table silently (ORA-00942 = table does not exist)."""
    cursor.execute(f"""
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {table_name} PURGE';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
    """)


def table_exists(cursor, schema, table):
    cursor.execute(
        "SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
        (schema.upper(), table.upper())
    )
    return cursor.fetchone()[0] > 0


def read_sql_file(path):
    with open(path, 'r') as f:
        return f.read().rstrip().rstrip(';').strip()


def scratch_table_name(cte_name):
    """HOA.SCRATCH_CNOTE_AMO_DEDUPED etc."""
    return f"{ORACLE_HOA_SCHEMA}.{SCRATCH_PREFIX}{cte_name.upper()}"


# ============================================================
# SQL PARSING
# ============================================================

def _parse_ctes_and_main_query(sql_content):
    """
    Parse a WITH…SELECT block into a list of (name, body) CTE pairs and the
    final SELECT. Uses bracket-depth counting to handle nested subqueries.
    """
    # Strip the leading WITH keyword that begins the file
    sql_body = re.sub(r'^\s*WITH\s+', '', sql_content, flags=re.IGNORECASE)

    cte_re = re.compile(r'^(\w+)\s+AS\s*\(', re.MULTILINE)
    matches = list(cte_re.finditer(sql_body))
    if not matches:
        return [], sql_body.strip()

    ctes = []
    last_end = 0
    for m in matches:
        name = m.group(1)
        pos = m.end()
        depth = 1
        while pos < len(sql_body) and depth > 0:
            ch = sql_body[pos]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == "'":
                pos += 1
                while pos < len(sql_body) and sql_body[pos] != "'":
                    pos += 1
            pos += 1
        body = sql_body[m.end():pos - 1].strip()
        ctes.append((name, body))
        last_end = pos

    main_query = sql_body[last_end:].strip()
    main_query = re.sub(r'^[,\s]+', '', main_query).rstrip(';').strip()
    return ctes, main_query


def _rewrite_cte_refs(sql_fragment, cte_names):
    """
    Replace every bare CTE name in sql_fragment with its HOA.SCRATCH_* equivalent.
    Uses word boundaries so 'mfcnote_typed' doesn't match 'mfcnote_typed_foo'.
    """
    result = sql_fragment
    for name in cte_names:
        scratch = scratch_table_name(name)
        result = re.sub(r'\b' + re.escape(name) + r'\b', scratch, result, flags=re.IGNORECASE)
    return result


# ============================================================
# SCRATCH TABLE MATERIALIZATION
# ============================================================

def _drop_all_scratch_tables(cte_names):
    """Drop all scratch tables — called in finally so it always runs."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        for name in cte_names:
            tbl = scratch_table_name(name)
            try:
                drop_table_if_exists(cursor, tbl)
            except Exception as exc:
                logger.warning(f"  Could not drop {tbl}: {exc}")
        conn.commit()
    finally:
        conn.close()


def _materialize_one_scratch(cte_name, cte_body, cte_names, start_date=None):
    """
    Materialize a single CTE as HOA.SCRATCH_<name> NOLOGGING, then:
      1. Create an index on the join key(s)
      2. Gather statistics (DBMS_STATS) so the planner has accurate cardinality

    Opens its own Oracle connection so parallel workers don't share state.
    Returns (cte_name, elapsed_seconds, row_count).
    """
    scratch = scratch_table_name(cte_name)
    short_name = f"{SCRATCH_PREFIX}{cte_name.upper()}"
    join_keys = SCRATCH_JOIN_KEYS.get(cte_name, [])

    # Rewrite any CTE references inside this CTE body to HOA.SCRATCH_* names.
    # Only mfcnote_pivoted references another CTE (mfcnote_typed).
    rewritten_body = _rewrite_cte_refs(cte_body, cte_names)

    # Inject a date filter for tables that read the full history with no natural
    # boundary (e.g. mfcnote_typed), to prevent ORA-01652 on the aggregation.
    outer_where = ''
    if start_date is not None and cte_name in SCRATCH_DATE_FILTERS:
        date_col = SCRATCH_DATE_FILTERS[cte_name]
        filter_date = start_date - timedelta(days=SCRATCH_DATE_LOOKBACK_DAYS)
        outer_where = f"\nWHERE {date_col} >= DATE '{filter_date.strftime('%Y-%m-%d')}'"

    ctas_sql = (
        f"CREATE TABLE {scratch} NOLOGGING PARALLEL {ORACLE_PARALLEL_DEGREE} AS\n"
        f"SELECT /*+ PARALLEL({ORACLE_PARALLEL_DEGREE}) */ * FROM (\n"
        f"{rewritten_body}\n"
        f"){outer_where}"
    )

    conn = get_connection()
    try:
        cursor = conn.cursor()
        start = datetime.now()

        drop_table_if_exists(cursor, scratch)
        cursor.execute(ctas_sql)

        # Index on join key(s) so the final LEFT JOIN can use index range scans
        if join_keys:
            key_list = ', '.join(join_keys)
            idx_name = f"IDX_{short_name[:20]}"
            cursor.execute(
                f"CREATE INDEX {ORACLE_HOA_SCHEMA}.{idx_name} "
                f"ON {scratch} ({key_list})"
            )

        # Gather stats so the query planner has accurate row counts
        cursor.execute(
            "BEGIN DBMS_STATS.GATHER_TABLE_STATS("
            "  ownname          => :1,"
            "  tabname          => :2,"
            "  estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,"
            "  degree           => :3"
            "); END;",
            (ORACLE_HOA_SCHEMA, short_name, ORACLE_PARALLEL_DEGREE)
        )

        conn.commit()

        cursor.execute(f"SELECT COUNT(*) FROM {scratch}")
        row_count = cursor.fetchone()[0]
        elapsed = (datetime.now() - start).total_seconds()
        return (cte_name, elapsed, row_count)
    finally:
        conn.close()


# ============================================================
# PHASED FULL UNIFICATION
# ============================================================

def run_full_unification_phased(sql_file, start_date):
    """
    Phased scratch-table unification for full runs on large datasets.

    Phase A — Parallel CTE materialization:
      Independent CTEs are materialized simultaneously (up to SCRATCH_PARALLEL_WORKERS).
      Each scratch table gets an index on its join key and fresh statistics.

    Phase B — Dependent CTEs:
      mfcnote_pivoted is materialized after mfcnote_typed (its parent).

    Phase C — Final 38-table LEFT JOIN:
      Runs with all CTE references rewritten to HOA.SCRATCH_* tables.
      PARALLEL hint applied for Oracle parallel query on the final CTAS.

    Cleanup — DROP all HOA.SCRATCH_* tables (always, even on failure).
    """
    logger.info(f"Full unification (phased): window start = {start_date.isoformat()}")

    raw_sql = read_sql_file(sql_file)
    ctes, main_query = _parse_ctes_and_main_query(raw_sql)
    if not ctes:
        raise ValueError("No CTEs found in SQL file — cannot run phased unification")

    cte_dict = {name: body for name, body in ctes}
    cte_names = [name for name, _ in ctes]

    dependent_ctes = set(CTE_DEPENDENCIES.keys()) & set(cte_dict)
    independent_ctes = [(n, b) for n, b in ctes if n not in dependent_ctes]

    logger.info(f"  {len(ctes)} CTEs parsed: "
                f"{len(independent_ctes)} independent, {len(dependent_ctes)} dependent")
    logger.info(f"  Parallel workers: {SCRATCH_PARALLEL_WORKERS}  "
                f"Oracle DOP: {ORACLE_PARALLEL_DEGREE}")

    phase_start = datetime.now()
    completed = 0
    total = len(ctes)

    # All scratch names (for cleanup and rewriting)
    all_scratch_names = cte_names

    try:
        # ------------------------------------------------------------------
        # Phase A: materialize independent CTEs in parallel
        # ------------------------------------------------------------------
        logger.info("Phase A: materializing independent scratch tables …")
        with ThreadPoolExecutor(max_workers=SCRATCH_PARALLEL_WORKERS) as pool:
            futures = {
                pool.submit(_materialize_one_scratch, name, body, all_scratch_names, start_date): name
                for name, body in independent_ctes
            }
            for fut in as_completed(futures):
                name, elapsed, rows = fut.result()
                completed += 1
                logger.info(f"  [{completed}/{total}] {scratch_table_name(name)}  "
                            f"{rows:,} rows  ({elapsed:.1f}s)")

        # ------------------------------------------------------------------
        # Phase B: materialize dependent CTEs sequentially after their parents
        # ------------------------------------------------------------------
        if dependent_ctes:
            logger.info("Phase B: materializing dependent scratch tables …")
        for dep_name in dependent_ctes:
            name, elapsed, rows = _materialize_one_scratch(
                dep_name, cte_dict[dep_name], all_scratch_names, start_date
            )
            completed += 1
            parent = CTE_DEPENDENCIES[dep_name]
            logger.info(f"  [{completed}/{total}] {scratch_table_name(name)}  "
                        f"{rows:,} rows  ({elapsed:.1f}s)  [after {parent}]")

        phase_a_b_elapsed = (datetime.now() - phase_start).total_seconds()
        logger.info(f"  All {total} scratch tables ready in {phase_a_b_elapsed:.1f}s")

        # ------------------------------------------------------------------
        # Phase C: final 38-table LEFT JOIN from scratch tables
        # ------------------------------------------------------------------
        logger.info("Phase C: running final 38-table LEFT JOIN …")
        rewritten_main = _rewrite_cte_refs(main_query, all_scratch_names)

        start_date_lit = f"DATE '{start_date.strftime('%Y-%m-%d')}'"
        ctas_sql = (
            f"CREATE TABLE {UNIFIED_TABLE} NOLOGGING PARALLEL {ORACLE_PARALLEL_DEGREE} AS\n"
            f"SELECT /*+ PARALLEL({ORACLE_PARALLEL_DEGREE}) */ *\n"
            f"FROM (\n{rewritten_main}\n)\n"
            f"WHERE CNOTE_CRDATE >= {start_date_lit}"
        )

        conn = get_connection()
        try:
            cursor = conn.cursor()
            drop_table_if_exists(cursor, UNIFIED_TABLE)
            join_start = datetime.now()
            cursor.execute(ctas_sql)
            conn.commit()
            join_elapsed = (datetime.now() - join_start).total_seconds()

            cursor.execute(f"SELECT COUNT(*) FROM {UNIFIED_TABLE}")
            row_count = cursor.fetchone()[0]
            logger.info(f"  {UNIFIED_TABLE}: {row_count:,} rows ({join_elapsed:.1f}s)")
            return row_count
        finally:
            conn.close()

    finally:
        # Always drop scratch tables, even on failure
        logger.info("Cleanup: dropping scratch tables …")
        _drop_all_scratch_tables(all_scratch_names)
        logger.info("  Scratch tables dropped.")


# ============================================================
# INCREMENTAL UNIFICATION (inline CTEs — daily slice is small)
# ============================================================

def run_incremental_unification(cursor, sql_file, cutoff):
    """DELETE rows >= cutoff then re-insert using inline CTEs (no scratch tables)."""
    logger.info(f"Incremental unification: cutoff = {cutoff.isoformat()}")

    sql = read_sql_file(sql_file)

    start = datetime.now()
    cursor.execute(
        f"DELETE FROM {UNIFIED_TABLE} WHERE CNOTE_CRDATE >= :cutoff",
        cutoff=cutoff
    )
    deleted = cursor.rowcount
    logger.info(f"  Deleted {deleted:,} stale rows")

    sql_body = re.sub(r'^\s*WITH\s+', '', sql, flags=re.IGNORECASE)
    insert_sql = (
        f"INSERT INTO {UNIFIED_TABLE}\n"
        f"WITH\n{sql_body}\n"
        f"WHERE CNOTE_CRDATE >= :cutoff"
    )
    cursor.execute(insert_sql, cutoff=cutoff)
    inserted = cursor.rowcount

    cursor.execute(f"SELECT COUNT(*) FROM {UNIFIED_TABLE}")
    total = cursor.fetchone()[0]
    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"  Inserted {inserted:,} rows; total = {total:,} ({elapsed:.1f}s)")
    return total


# ============================================================
# TRANSFORM (adds DQ/manifest columns to unified table)
# ============================================================

TRANSFORM_COLS = """
    (CASE WHEN OM_MAN_NO  IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM1_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM2_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM3_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM4_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM5_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM6_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM7_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM8_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM9_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN TM10_MAN_NO IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN IM_MAN_NO  IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN HM_MAN_NO  IS NOT NULL THEN 1 ELSE 0 END
    ) AS MANIFEST_COUNT,
    CASE WHEN COALESCE(
        TM1_MAN_NO, TM2_MAN_NO, TM3_MAN_NO, TM4_MAN_NO, TM5_MAN_NO,
        TM6_MAN_NO, TM7_MAN_NO, TM8_MAN_NO, TM9_MAN_NO, TM10_MAN_NO
    ) IS NOT NULL THEN 1 ELSE 0 END AS HAS_TRANSIT,
    CASE WHEN CNOTE_NO IS NULL OR CNOTE_DATE IS NULL
              OR CNOTE_ORIGIN IS NULL OR CNOTE_DESTINATION IS NULL
         THEN 1 ELSE 0 END AS DQ_HAS_NULLS,
    SYSTIMESTAMP AS DQ_CHECK_DATE,
    SYSTIMESTAMP AS TRANSFORMED_AT"""


def run_full_transform(cursor):
    logger.info(f"Full transform: {UNIFIED_TABLE} → {TRANSFORMED_TABLE}")
    drop_table_if_exists(cursor, TRANSFORMED_TABLE)
    start = datetime.now()
    cursor.execute(
        f"CREATE TABLE {TRANSFORMED_TABLE} NOLOGGING PARALLEL {ORACLE_PARALLEL_DEGREE} AS\n"
        f"SELECT /*+ PARALLEL({ORACLE_PARALLEL_DEGREE}) */ u.*,{TRANSFORM_COLS}\n"
        f"FROM {UNIFIED_TABLE} u"
    )
    elapsed = (datetime.now() - start).total_seconds()
    cursor.execute(f"SELECT COUNT(*) FROM {TRANSFORMED_TABLE}")
    row_count = cursor.fetchone()[0]
    logger.info(f"  {TRANSFORMED_TABLE}: {row_count:,} rows ({elapsed:.1f}s)")
    return row_count


def run_incremental_transform(cursor, cutoff):
    logger.info(f"Incremental transform: cutoff = {cutoff.isoformat()}")
    start = datetime.now()
    cursor.execute(
        f"DELETE FROM {TRANSFORMED_TABLE} WHERE CNOTE_CRDATE >= :cutoff",
        cutoff=cutoff
    )
    deleted = cursor.rowcount
    cursor.execute(
        f"INSERT INTO {TRANSFORMED_TABLE}\n"
        f"SELECT u.*,{TRANSFORM_COLS}\n"
        f"FROM {UNIFIED_TABLE} u WHERE CNOTE_CRDATE >= :cutoff",
        cutoff=cutoff
    )
    inserted = cursor.rowcount
    elapsed = (datetime.now() - start).total_seconds()
    cursor.execute(f"SELECT COUNT(*) FROM {TRANSFORMED_TABLE}")
    total = cursor.fetchone()[0]
    logger.info(
        f"  Transformed: deleted {deleted:,}, inserted {inserted:,}, total {total:,} ({elapsed:.1f}s)"
    )
    return total


# ============================================================
# MAIN
# ============================================================

def _open_conn():
    conn = get_connection()
    return conn, conn.cursor()


def _close_conn(conn, cursor):
    try:
        cursor.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description='JNE Oracle Unification + Transform')
    parser.add_argument('--run-id', default=None)
    parser.add_argument('--force-full', action='store_true',
                        help='Force full run even if prior run exists')
    args = parser.parse_args()

    run_id = args.run_id or datetime.now().strftime('%Y%m%dT%H%M%S')
    overall_start = datetime.now()

    # ----------------------------------------------------------------
    # Phase 0: detect mode + write RUNNING log, then CLOSE connection.
    # The scratch phase takes hours; holding a connection the whole time
    # triggers Oracle's IDLE_TIME profile limit (DPY-4033).
    # ----------------------------------------------------------------
    conn, cursor = _open_conn()
    try:
        ensure_run_log_table(cursor)
        conn.commit()

        cutoff = None
        mode = 'full'
        if not args.force_full:
            last_run = get_last_successful_run(cursor)
            if last_run is not None:
                cutoff = last_run[1] - timedelta(days=INCREMENTAL_OVERLAP_DAYS)
                mode = 'incremental'
                logger.info(f"Incremental mode: cutoff = {cutoff.isoformat()}")

        if mode == 'full':
            cutoff = datetime.now() - timedelta(days=EXTRACTION_WINDOW_DAYS)
            logger.info(f"Full mode: window start = {cutoff.isoformat()}")

        start_run_log(cursor, run_id, mode, cutoff)
        conn.commit()
    finally:
        _close_conn(conn, cursor)

    logger.info("=" * 60)
    logger.info(f"ORACLE UNIFICATION  run_id={run_id}  mode={mode}")
    logger.info("=" * 60)

    unified_rows = 0
    transformed_rows = 0
    run_status = 'FAILED'
    run_error = None

    try:
        # ----------------------------------------------------------------
        # Phase 1: Unification — uses its own per-worker connections
        # ----------------------------------------------------------------
        if mode == 'full':
            unified_rows = run_full_unification_phased(ORACLE_UNIFICATION_SQL_FILE, cutoff)
        else:
            conn, cursor = _open_conn()
            try:
                if not table_exists(cursor, ORACLE_HOA_SCHEMA, 'UNIFIED_SHIPMENTS'):
                    logger.info(f"{UNIFIED_TABLE} missing — falling back to full")
                    _close_conn(conn, cursor)
                    unified_rows = run_full_unification_phased(
                        ORACLE_UNIFICATION_SQL_FILE,
                        datetime.now() - timedelta(days=EXTRACTION_WINDOW_DAYS)
                    )
                    mode = 'full'
                else:
                    unified_rows = run_incremental_unification(cursor, ORACLE_UNIFICATION_SQL_FILE, cutoff)
                    conn.commit()
                    _close_conn(conn, cursor)
            except Exception:
                _close_conn(conn, cursor)
                raise

        # ----------------------------------------------------------------
        # Phase 2: Transform — fresh connection after the long unify phase
        # ----------------------------------------------------------------
        conn, cursor = _open_conn()
        try:
            if mode == 'full':
                transformed_rows = run_full_transform(cursor)
            else:
                if not table_exists(cursor, ORACLE_HOA_SCHEMA, 'TRANSFORMED_UNIFIED_SHIPMENTS'):
                    transformed_rows = run_full_transform(cursor)
                else:
                    transformed_rows = run_incremental_transform(cursor, cutoff)
            conn.commit()
        finally:
            _close_conn(conn, cursor)

        run_status = 'SUCCESS'

    except Exception as exc:
        run_error = exc
        logger.error(f"Unification failed: {exc}")
        raise

    finally:
        # ----------------------------------------------------------------
        # Always update the run log — fresh connection, always succeeds
        # ----------------------------------------------------------------
        try:
            conn, cursor = _open_conn()
            complete_run_log(cursor, run_id, transformed_rows, run_status, run_error)
            conn.commit()
            _close_conn(conn, cursor)
        except Exception as log_exc:
            logger.warning(f"Could not update run log: {log_exc}")

        elapsed = (datetime.now() - overall_start).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Complete in {elapsed:.1f}s  status={run_status}")
        if run_status == 'SUCCESS':
            logger.info(f"  {UNIFIED_TABLE}:     {unified_rows:,} rows")
            logger.info(f"  {TRANSFORMED_TABLE}: {transformed_rows:,} rows")
        logger.info("=" * 60)


if __name__ == '__main__':
    main()
