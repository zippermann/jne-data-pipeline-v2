"""Rule executors for governance checks."""

from __future__ import annotations

import glob
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from src.config import GovernanceConfig
from src.rules.registry import RuleSpec


VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class RuleResult:
    index_code: str
    element: str
    rule_family: str
    child_table: str
    child_fk: str
    parent_table: str
    parent_pk: str
    total_checked: int
    orphan_key_count: int
    orphan_row_count: int
    orphan_rate: float
    status: str
    needs_confirmation: bool
    skipped_reason: Optional[str]
    run_at: str


def _quote_identifier(name: str) -> str:
    if not VALID_IDENTIFIER.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return f'"{name}"'


def _relation_sql(path: str) -> str:
    escaped = path.replace("'", "''")
    return f"read_parquet('{escaped}', union_by_name=true)"


def _parse_s3_path(path: str) -> tuple[str, str]:
    if not path.startswith("s3://"):
        raise ValueError(f"Not an s3 path: {path}")
    body = path.removeprefix("s3://")
    bucket, key = body.split("/", 1)
    return bucket, key


def _path_exists(path: str, config: GovernanceConfig) -> bool:
    if path.startswith("s3://"):
        from minio import Minio

        bucket, key = _parse_s3_path(path)
        prefix = key.split("*", 1)[0]
        client = Minio(
            config.minio.endpoint,
            access_key=config.minio.access_key,
            secret_key=config.minio.secret_key,
            secure=config.minio.secure,
        )
        try:
            return next(client.list_objects(bucket, prefix=prefix, recursive=True), None) is not None
        except Exception:
            return False
    return bool(glob.glob(path))


def _columns(con: Any, path: str) -> set[str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {_relation_sql(path)} LIMIT 0").fetchall()
    return {row[0].upper() for row in rows}


def _skip_result(spec: RuleSpec, reason: str, run_at: str) -> RuleResult:
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        child_table=spec.child_table,
        child_fk=spec.child_fk,
        parent_table=spec.parent_table,
        parent_pk=spec.parent_pk,
        total_checked=0,
        orphan_key_count=0,
        orphan_row_count=0,
        orphan_rate=0.0,
        status="SKIPPED",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=reason,
        run_at=run_at,
    )


def run_intg1(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Run INTG1 referential integrity by anti-joining child FK to parent PK.

    NULL child foreign keys are ignored because missing FK values belong to
    Completeness, not orphan detection. Work is pushed into DuckDB over Parquet;
    only aggregate counts are returned to Python.
    """
    run_at = datetime.now(timezone.utc).isoformat()
    child_path = table_paths[spec.child_table]
    parent_path = table_paths[spec.parent_table]

    if not _path_exists(child_path, config):
        return _skip_result(spec, f"missing child parquet path: {child_path}", run_at)
    if not _path_exists(parent_path, config):
        return _skip_result(spec, f"missing parent parquet path: {parent_path}", run_at)

    child_columns = _columns(con, child_path)
    parent_columns = _columns(con, parent_path)
    if spec.child_fk.upper() not in child_columns:
        return _skip_result(spec, f"missing child column: {spec.child_fk}", run_at)
    if spec.parent_pk.upper() not in parent_columns:
        return _skip_result(spec, f"missing parent column: {spec.parent_pk}", run_at)

    use_boundary = bool(
        spec.child_date_column
        and spec.child_date_column.upper() in child_columns
        and config.extraction_window.get("start")
        and config.extraction_window.get("end")
    )
    boundary_expr = "NULL::BOOLEAN"
    if use_boundary:
        date_col = _quote_identifier(spec.child_date_column or "")
        boundary_expr = (
            f"BOOL_OR(TRY_CAST(c.{date_col} AS TIMESTAMP) < TIMESTAMP '{config.extraction_window['start']}' "
            f"OR TRY_CAST(c.{date_col} AS TIMESTAMP) >= TIMESTAMP '{config.extraction_window['end']}')"
        )

    child_fk = _quote_identifier(spec.child_fk)
    parent_pk = _quote_identifier(spec.parent_pk)
    child_sql = _relation_sql(child_path)
    parent_sql = _relation_sql(parent_path)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE failures_{spec.code} AS
        SELECT
            '{spec.code}' AS index_code,
            '{spec.child_table}' AS child_table,
            '{spec.child_fk}' AS child_fk,
            CAST(c.{child_fk} AS VARCHAR) AS child_fk_value,
            '{spec.parent_table}' AS parent_table,
            '{spec.parent_pk}' AS parent_pk,
            COUNT(*)::BIGINT AS affected_child_rows,
            {boundary_expr} AS boundary_suspect,
            '{run_at}' AS run_at
        FROM {child_sql} c
        WHERE c.{child_fk} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {parent_sql} p
              WHERE p.{parent_pk} = c.{child_fk}
          )
        GROUP BY c.{child_fk}
    """)

    con.execute(f"""
        INSERT INTO {failures_table}
        SELECT * FROM failures_{spec.code}
        LIMIT {int(config.governance.orphan_key_limit)}
    """)

    total_checked = con.execute(
        f"SELECT COUNT(*) FROM {child_sql} WHERE {child_fk} IS NOT NULL"
    ).fetchone()[0]
    orphan_key_count, orphan_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_child_rows), 0) FROM failures_{spec.code}"
    ).fetchone()
    orphan_rate = float(orphan_row_count / total_checked) if total_checked else 0.0

    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        child_table=spec.child_table,
        child_fk=spec.child_fk,
        parent_table=spec.parent_table,
        parent_pk=spec.parent_pk,
        total_checked=int(total_checked),
        orphan_key_count=int(orphan_key_count),
        orphan_row_count=int(orphan_row_count),
        orphan_rate=orphan_rate,
        status="FAIL" if orphan_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


EXECUTORS = {
    "INTG1": run_intg1,
}
