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
    table_name: str
    column_names: str
    compared_table: Optional[str]
    compared_columns: Optional[str]
    total_checked: int
    failed_key_count: int
    failed_row_count: int
    failure_rate: float
    status: str
    needs_confirmation: bool
    skipped_reason: Optional[str]
    run_at: str

    @property
    def total_checked_rows(self) -> int:
        return self.total_checked

    @property
    def orphan_key_count(self) -> int:
        return self.failed_key_count

    @property
    def orphan_row_count(self) -> int:
        return self.failed_row_count

    @property
    def orphan_rate(self) -> float:
        return self.failure_rate


@dataclass(frozen=True)
class PairCheck:
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_sql: str


PAIR_CHECKS: dict[str, PairCheck] = {
    # CONS1: CMS_CNOTE compared to CMS_APICUST through CNOTE_NO.
    "CONS1B3": PairCheck("CMS_CNOTE", "CNOTE_BRANCH_ID", "CMS_APICUST", "APICUST_BRANCH", 'l."CNOTE_NO" = r."APICUST_CNOTE_NO"'),
    "CONS1B10": PairCheck("CMS_CNOTE", "CNOTE_CUST_NO", "CMS_APICUST", "APICUST_CUST_NO", 'l."CNOTE_NO" = r."APICUST_CNOTE_NO"'),
    "CONS1B12": PairCheck("CMS_CNOTE", "CNOTE_ORIGIN", "CMS_APICUST", "APICUST_ORIGIN", 'l."CNOTE_NO" = r."APICUST_CNOTE_NO"'),
    "CONS1B13": PairCheck("CMS_CNOTE", "CNOTE_DESTINATION", "CMS_APICUST", "APICUST_DESTINATION", 'l."CNOTE_NO" = r."APICUST_CNOTE_NO"'),
    "CONS1B14": PairCheck("CMS_CNOTE", "CNOTE_QTY", "CMS_APICUST", "APICUST_QTY", 'l."CNOTE_NO" = r."APICUST_CNOTE_NO"'),
    "CONS1B15": PairCheck("CMS_CNOTE", "CNOTE_WEIGHT", "CMS_APICUST", "APICUST_WEIGHT", 'l."CNOTE_NO" = r."APICUST_CNOTE_NO"'),

    # CONS2: CNOTE-linked detail/header comparisons.
    "CONS2D3": PairCheck("CMS_DRCNOTE", "DRCNOTE_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."DRCNOTE_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2F6": PairCheck("CMS_DHI_HOC", "DHI_CNOTE_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."DHI_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2I4": PairCheck("CMS_MFCNOTE", "MFCNOTE_WEIGHT", "CMS_CNOTE", "CNOTE_WEIGHT", 'l."MFCNOTE_NO" = r."CNOTE_NO"'),
    "CONS2S8": PairCheck("CMS_DHOV_RSHEET", "DHOV_RSHEET_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."DHOV_RSHEET_CNOTE" = r."CNOTE_NO"'),
    "CONS2U4": PairCheck("CMS_DHOUNDEL_POD", "DHOUNDEL_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."DHOUNDEL_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2W4": PairCheck("CMS_DHOCNOTE", "DHOCNOTE_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."DHOCNOTE_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2AC4": PairCheck("CMS_DHICNOTE", "DHICNOTE_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."DHICNOTE_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2AE4": PairCheck("CMS_DBAG_HO", "DBAG_CNOTE_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."DBAG_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2AE5": PairCheck("CMS_DBAG_HO", "DBAG_CNOTE_WEIGHT", "CMS_CNOTE", "CNOTE_WEIGHT", 'l."DBAG_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2AE6": PairCheck("CMS_DBAG_HO", "DBAG_CNOTE_DESTINATION", "CMS_CNOTE", "CNOTE_DESTINATION", 'l."DBAG_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2Y5": PairCheck("CMS_COST_DTRANSIT_AGEN", "CNOTE_QTY", "CMS_CNOTE", "CNOTE_QTY", 'l."CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2Y6": PairCheck("CMS_COST_DTRANSIT_AGEN", "CNOTE_WEIGHT", "CMS_CNOTE", "CNOTE_WEIGHT", 'l."CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2Y8": PairCheck("CMS_COST_DTRANSIT_AGEN", "CNOTE_SERVICES_CODE", "CMS_CNOTE", "CNOTE_SERVICES_CODE", 'l."CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2R2": PairCheck("CMS_CNOTE_POD", "CNOTE_POD_DATE", "CMS_DRSHEET", "DRSHEET_DATE", 'l."CNOTE_POD_NO" = r."DRSHEET_CNOTE_NO"'),
    "CONS2R4": PairCheck("CMS_CNOTE_POD", "CNOTE_POD_STATUS", "CMS_DRSHEET", "DRSHEET_STATUS", 'l."CNOTE_POD_NO" = r."DRSHEET_CNOTE_NO"'),

    # CONS2: operational chains that do not require manifest pivoting.
    "CONS2C3": PairCheck("CMS_MRCNOTE", "MRCNOTE_BRANCH_ID", "CMS_CNOTE", "CNOTE_BRANCH_ID", 'l."MRCNOTE_NO" = j1."DRCNOTE_NO" AND j1."DRCNOTE_CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2M2": PairCheck("CMS_DSMU", "DSMU_FLIGHT_NO", "CMS_MSMU", "MSMU_FLIGHT_NO", 'l."DSMU_NO" = r."MSMU_NO"'),
    "CONS2M3": PairCheck("CMS_DSMU", "DSMU_FLIGHT_DATE", "CMS_MSMU", "MSMU_FLIGHT_DATE", 'l."DSMU_NO" = r."MSMU_NO"'),
    "CONS2M5": PairCheck("CMS_DSMU", "DSMU_WEIGHT", "CMS_DMBAG", "DMBAG_WEIGHT", 'l."DSMU_BAG_NO" = r."DMBAG_NO"'),
    "CONS2M6": PairCheck("CMS_DSMU", "DSMU_BAG_ORIGIN", "CMS_DMBAG", "DMBAG_ORIGIN", 'l."DSMU_BAG_NO" = r."DMBAG_NO"'),
    "CONS2M7": PairCheck("CMS_DSMU", "DSMU_BAG_DESTINATION", "CMS_DMBAG", "DMBAG_DESTINATION", 'l."DSMU_BAG_NO" = r."DMBAG_NO"'),
    "CONS2N3": PairCheck("CMS_MSMU", "MSMU_ORIGIN", "CMS_DSMU", "DSMU_BAG_ORIGIN", 'l."MSMU_NO" = r."DSMU_NO"'),
    "CONS2N4": PairCheck("CMS_MSMU", "MSMU_DESTINATION", "CMS_DSMU", "DSMU_BAG_DESTINATION", 'l."MSMU_NO" = r."DSMU_NO"'),
    "CONS2K5": PairCheck("CMS_DMBAG", "DMBAG_WEIGHT", "CMS_MFBAG", "MFBAG_ACT_WEIGHT", 'l."DMBAG_BAG_NO" = r."MFBAG_NO"'),
    "CONS2L3": PairCheck("CMS_MMBAG", "MMBAG_ORIGIN", "CMS_DMBAG", "DMBAG_ORIGIN", '(l."MMBAG_NO" = r."DMBAG_NO" OR l."MMBAG_NO" = r."DMBAG_BAG_NO")'),
    "CONS2L4": PairCheck("CMS_MMBAG", "MMBAG_DESTINATION", "CMS_DMBAG", "DMBAG_DESTINATION", '(l."MMBAG_NO" = r."DMBAG_NO" OR l."MMBAG_NO" = r."DMBAG_BAG_NO")'),
    "CONS2L7": PairCheck("CMS_MMBAG", "MMBAG_WEIGHT", "CMS_DMBAG", "DMBAG_WEIGHT", '(l."MMBAG_NO" = r."DMBAG_NO" OR l."MMBAG_NO" = r."DMBAG_BAG_NO")'),
    "CONS2J10": PairCheck("CMS_MFBAG", "MFBAG_ROUTE", "CMS_MANIFEST", "MANIFEST_ROUTE", 'l."MFBAG_MAN_NO" = r."MANIFEST_NO"'),
    "CONS2K3": PairCheck("CMS_DMBAG", "DMBAG_ORIGIN", "CMS_MANIFEST", "MANIFEST_FROM", 'l."DMBAG_BAG_NO" = j1."MFBAG_NO" AND j1."MFBAG_MAN_NO" = r."MANIFEST_NO"'),
    "CONS2K4": PairCheck("CMS_DMBAG", "DMBAG_DESTINATION", "CMS_MANIFEST", "MANIFEST_THRU", 'l."DMBAG_BAG_NO" = j1."MFBAG_NO" AND j1."MFBAG_MAN_NO" = r."MANIFEST_NO"'),
    "CONS2X4": PairCheck("CMS_COST_MTRANSIT_AGEN", "DESTINATION", "CMS_CNOTE", "CNOTE_DESTINATION", 'l."MANIFEST_NO" = j1."DMANIFEST_NO" AND j1."CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2X5": PairCheck("CMS_COST_MTRANSIT_AGEN", "CTC_WEIGHT", "CMS_CNOTE", "CNOTE_WEIGHT", 'l."MANIFEST_NO" = j1."DMANIFEST_NO" AND j1."CNOTE_NO" = r."CNOTE_NO"'),
    "CONS2X6": PairCheck("CMS_COST_MTRANSIT_AGEN", "ACT_WEIGHT", "CMS_CNOTE", "CNOTE_WEIGHT", 'l."MANIFEST_NO" = j1."DMANIFEST_NO" AND j1."CNOTE_NO" = r."CNOTE_NO"'),
}


TIMELINESS_CHECKS: dict[str, PairCheck] = {
    "TIME1B99": PairCheck("CMS_CNOTE", "CNOTE_CRDATE", "CMS_MHI_HOC", "MHI_APPROVE_DATE", 'l."CNOTE_NO" = j1."DHI_CNOTE_NO" AND j1."DHI_NO" = r."MHI_NO"'),
    "TIME1E7": PairCheck("CMS_MHI_HOC", "MHI_APPROVE_DATE", "CMS_MRCNOTE", "MRCNOTE_SIGNDATE", 'l."MHI_NO" = j1."DHI_NO" AND j1."DHI_CNOTE_NO" = j2."DRCNOTE_CNOTE_NO" AND j2."DRCNOTE_NO" = r."MRCNOTE_NO"'),
    "TIME1C9": PairCheck("CMS_MRCNOTE", "MRCNOTE_SIGNDATE", "CMS_MFBAG", "MFBAG_CRDATE", 'l."MRCNOTE_NO" = j1."DRCNOTE_NO" AND j1."DRCNOTE_CNOTE_NO" = j2."MFCNOTE_NO" AND j2."MFCNOTE_BAG_NO" = r."MFBAG_NO"'),
    "TIME1J18": PairCheck("CMS_MFBAG", "MFBAG_CRDATE", "CMS_MANIFEST", "MANIFEST_CRDATE", 'l."MFBAG_MAN_NO" = r."MANIFEST_NO"'),
    "TIME1Q3": PairCheck("CMS_MRSHEET", "MRSHEET_DATE", "CMS_CNOTE_POD", "CNOTE_POD_CREATION_DATE", 'l."MRSHEET_NO" = j1."DRSHEET_NO" AND j1."DRSHEET_CNOTE_NO" = r."CNOTE_POD_NO"'),
    "TIME1Z10": PairCheck("CMS_MSJ", "MSJ_SIGNDATE", "CMS_MHICNOTE", "MHICNOTE_DATE", 'l."MSJ_NO" = j1."DSJ_NO" AND j1."DSJ_HVO_NO" = j2."RDSJ_HVO_NO" AND j2."RDSJ_HVI_NO" = r."MHICNOTE_NO"'),
    "TIME1AB5": PairCheck("CMS_MHICNOTE", "MHICNOTE_DATE", "CMS_MRSHEET", "MRSHEET_DATE", 'l."MHICNOTE_NO" = j1."DHICNOTE_NO" AND j1."DHICNOTE_CNOTE_NO" = j2."DRSHEET_CNOTE_NO" AND j2."DRSHEET_NO" = r."MRSHEET_NO"'),
}


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


def _column_list(columns: tuple[str, ...]) -> str:
    return ", ".join(columns)


def _skip_result(spec: RuleSpec, reason: str, run_at: str) -> RuleResult:
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name=spec.table or spec.child_table,
        column_names=_column_list(spec.columns),
        compared_table=spec.parent_table or None,
        compared_columns=spec.parent_pk or None,
        total_checked=0,
        failed_key_count=0,
        failed_row_count=0,
        failure_rate=0.0,
        status="SKIPPED",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=reason,
        run_at=run_at,
    )


def _table_path(spec: RuleSpec, table_paths: dict[str, str]) -> str:
    return table_paths[spec.table]


def _ensure_table(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    run_at: str,
) -> tuple[str, set[str], Optional[RuleResult]]:
    path = _table_path(spec, table_paths)
    if not _path_exists(path, config):
        return path, set(), _skip_result(spec, f"missing parquet path: {path}", run_at)
    table_columns = _columns(con, path)
    missing = [column for column in spec.columns if column.upper() not in table_columns]
    if missing:
        return path, table_columns, _skip_result(spec, f"missing column(s): {', '.join(missing)}", run_at)
    return path, table_columns, None


def _failure_result(
    spec: RuleSpec,
    total_checked: int,
    failed_key_count: int,
    failed_row_count: int,
    run_at: str,
    skipped_reason: str | None = None,
) -> RuleResult:
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name=spec.table or spec.child_table,
        column_names=_column_list(spec.columns),
        compared_table=spec.parent_table or None,
        compared_columns=spec.parent_pk or None,
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=skipped_reason,
        run_at=run_at,
    )


def _insert_failures(
    con: Any,
    failures_table: str,
    temp_table: str,
    limit: int,
) -> None:
    con.execute(f"""
        INSERT INTO {failures_table}
        SELECT * FROM {temp_table}
        LIMIT {int(limit)}
    """)


def _non_null_predicate(alias: str, columns: tuple[str, ...]) -> str:
    return " AND ".join(f"{alias}.{_quote_identifier(column)} IS NOT NULL" for column in columns)


def _concat_columns(alias: str, columns: tuple[str, ...]) -> str:
    parts = [f"COALESCE(CAST({alias}.{_quote_identifier(column)} AS VARCHAR), '<NULL>')" for column in columns]
    if len(parts) == 1:
        return parts[0]
    return " || ' | ' || ".join(parts)


def _clean_sql(expr: str) -> str:
    return f"TRIM(CAST({expr} AS VARCHAR))"


def _alias_relation(alias: str, table: str, table_paths: dict[str, str]) -> str:
    return f"{_relation_sql(table_paths[table])} {alias}"


def _bridge_table(alias: str, check: PairCheck) -> str | None:
    code_tables = {
        "j1": None,
        "j2": None,
    }
    join_sql = check.join_sql
    if 'j1."DRCNOTE_' in join_sql:
        code_tables["j1"] = "CMS_DRCNOTE"
    elif 'j1."DHI_' in join_sql:
        code_tables["j1"] = "CMS_DHI_HOC"
    elif 'j1."MFCNOTE_' in join_sql:
        code_tables["j1"] = "CMS_MFCNOTE"
    elif 'j1."DRSHEET_' in join_sql:
        code_tables["j1"] = "CMS_DRSHEET"
    elif 'j1."DSJ_' in join_sql:
        code_tables["j1"] = "CMS_DSJ"
    elif 'j1."DHICNOTE_' in join_sql:
        code_tables["j1"] = "CMS_DHICNOTE"
    elif 'j1."MFBAG_' in join_sql:
        code_tables["j1"] = "CMS_MFBAG"
    elif 'j1."DMANIFEST_' in join_sql or 'j1."CNOTE_NO"' in join_sql:
        code_tables["j1"] = "CMS_COST_DTRANSIT_AGEN"

    if 'j2."DRCNOTE_' in join_sql:
        code_tables["j2"] = "CMS_DRCNOTE"
    elif 'j2."MFCNOTE_' in join_sql:
        code_tables["j2"] = "CMS_MFCNOTE"
    elif 'j2."RDSJ_' in join_sql:
        code_tables["j2"] = "CMS_RDSJ"
    elif 'j2."DRSHEET_' in join_sql:
        code_tables["j2"] = "CMS_DRSHEET"

    return code_tables[alias]


def _join_plan(check: PairCheck, table_paths: dict[str, str]) -> tuple[str, str]:
    parts = [part.strip() for part in check.join_sql.split(" AND ")]
    available = {"l."}
    joins = []

    for alias in ("j1", "j2"):
        table = _bridge_table(alias, check)
        if not table:
            continue
        alias_conditions = [
            part
            for part in parts
            if f"{alias}." in part
            and "r." not in part
            and all(other not in part or other in available or other == f"{alias}." for other in ("j1.", "j2."))
        ]
        if not alias_conditions:
            raise RuntimeError(f"Could not build join condition for bridge alias {alias}: {check.join_sql}")
        joins.append(f"JOIN {_alias_relation(alias, table, table_paths)} ON {' AND '.join(alias_conditions)}")
        available.add(f"{alias}.")

    right_conditions = [part for part in parts if "r." in part]
    if not right_conditions:
        right_conditions = parts
    return "\n".join(joins), " AND ".join(right_conditions)


def _check_required_columns(
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    required: dict[str, tuple[str, ...]],
) -> str | None:
    for table, columns in required.items():
        path = table_paths.get(table)
        if not path:
            return f"missing table path for {table}"
        if not _path_exists(path, config):
            return f"missing parquet path: {path}"
        available = _columns(con, path)
        missing = [column for column in columns if column.upper() not in available]
        if missing:
            return f"missing {table} column(s): {', '.join(missing)}"
    return None


def _pair_required_columns(check: PairCheck) -> dict[str, tuple[str, ...]]:
    required: dict[str, set[str]] = {
        check.left_table: {check.left_column},
        check.right_table: {check.right_column},
    }
    bridge_columns = {
        "CMS_DRCNOTE": ("DRCNOTE_NO", "DRCNOTE_CNOTE_NO"),
        "CMS_DHI_HOC": ("DHI_NO", "DHI_CNOTE_NO"),
        "CMS_MFCNOTE": ("MFCNOTE_NO", "MFCNOTE_BAG_NO"),
        "CMS_DRSHEET": ("DRSHEET_NO", "DRSHEET_CNOTE_NO"),
        "CMS_DSJ": ("DSJ_NO", "DSJ_HVO_NO"),
        "CMS_RDSJ": ("RDSJ_HVI_NO", "RDSJ_HVO_NO"),
        "CMS_DHICNOTE": ("DHICNOTE_NO", "DHICNOTE_CNOTE_NO"),
        "CMS_MFBAG": ("MFBAG_NO", "MFBAG_MAN_NO"),
        "CMS_COST_DTRANSIT_AGEN": ("DMANIFEST_NO", "CNOTE_NO"),
    }
    alias_tables = {"l": check.left_table, "r": check.right_table}
    for alias in ("j1", "j2"):
        table = _bridge_table(alias, check)
        if table:
            alias_tables[alias] = table

    for alias, table in alias_tables.items():
        for column in re.findall(rf'{alias}\."([^"]+)"', check.join_sql):
            required.setdefault(table, set()).add(column)

    for table, columns in bridge_columns.items():
        if table in required:
            required[table].update(column for column in columns if f'"{column}"' in check.join_sql)
    return {table: tuple(sorted(columns)) for table, columns in required.items()}


def _run_pair_check(
    spec: RuleSpec,
    check: PairCheck,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
    mode: str,
) -> RuleResult:
    run_at = datetime.now(timezone.utc).isoformat()
    missing_reason = _check_required_columns(con, config, table_paths, _pair_required_columns(check))
    if missing_reason:
        return _skip_result(spec, missing_reason, run_at)

    left_expr = f"l.{_quote_identifier(check.left_column)}"
    right_expr = f"r.{_quote_identifier(check.right_column)}"
    left_clean = _clean_sql(left_expr)
    right_clean = _clean_sql(right_expr)
    temp_table = f"failures_{spec.code.replace('-', '_').replace(' ', '_').replace('(', '').replace(')', '')}"
    bridge_joins, right_join_sql = _join_plan(check, table_paths)

    if mode == "timeliness":
        applicable = f"TRY_CAST({left_expr} AS TIMESTAMP) IS NOT NULL AND TRY_CAST({right_expr} AS TIMESTAMP) IS NOT NULL"
        failed = f"TRY_CAST({right_expr} AS TIMESTAMP) < TRY_CAST({left_expr} AS TIMESTAMP)"
        reason = f"{check.left_column} occurs after {check.right_column}"
    else:
        applicable = f"{left_clean} <> '' AND {right_clean} <> ''"
        failed = f"{left_clean} <> {right_clean}"
        reason = f"{check.left_column} does not match {check.right_column}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{check.left_table}' AS table_name,
            '{check.left_column}' AS column_names,
            {left_clean} || ' <> ' || {right_clean} AS failed_value,
            '{reason}' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM {_alias_relation("l", check.left_table, table_paths)}
        {bridge_joins}
        JOIN {_alias_relation("r", check.right_table, table_paths)}
          ON {right_join_sql}
        WHERE {applicable}
          AND {failed}
        GROUP BY {left_clean}, {right_clean}
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    from_sql = f"""
        FROM {_alias_relation("l", check.left_table, table_paths)}
        {bridge_joins}
        JOIN {_alias_relation("r", check.right_table, table_paths)}
          ON {right_join_sql}
    """
    total_checked = con.execute(f"SELECT COUNT(*) {from_sql} WHERE {applicable}").fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name=check.left_table,
        column_names=check.left_column,
        compared_table=check.right_table,
        compared_columns=check.right_column,
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


def run_completeness(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Check that required columns are not NULL."""
    run_at = datetime.now(timezone.utc).isoformat()
    path, _, skip = _ensure_table(spec, con, config, table_paths, run_at)
    if skip:
        return skip

    relation = _relation_sql(path)
    column = _quote_identifier(spec.columns[0])
    temp_table = f"failures_{spec.code.replace('-', '_')}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{spec.table}' AS table_name,
            '{_column_list(spec.columns)}' AS column_names,
            NULL::VARCHAR AS failed_value,
            'NULL value' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM {relation} c
        WHERE c.{column} IS NULL
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
    failed_row_count = con.execute(f"SELECT COALESCE(SUM(affected_rows), 0) FROM {temp_table}").fetchone()[0]
    failed_key_count = 1 if failed_row_count else 0
    return _failure_result(spec, total_checked, failed_key_count, failed_row_count, run_at)


def run_uniqueness(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Check that a single column or column tuple appears at most once."""
    run_at = datetime.now(timezone.utc).isoformat()
    path, _, skip = _ensure_table(spec, con, config, table_paths, run_at)
    if skip:
        return skip

    relation = _relation_sql(path)
    key_value = _concat_columns("c", spec.columns)
    group_by = ", ".join(f"c.{_quote_identifier(column)}" for column in spec.columns)
    non_null = _non_null_predicate("c", spec.columns)
    temp_table = f"failures_{spec.code.replace('-', '_')}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{spec.table}' AS table_name,
            '{_column_list(spec.columns)}' AS column_names,
            {key_value} AS failed_value,
            'duplicate key' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM {relation} c
        WHERE {non_null}
        GROUP BY {group_by}
        HAVING COUNT(*) > 1
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(f"SELECT COUNT(*) FROM {relation} c WHERE {non_null}").fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return _failure_result(spec, total_checked, failed_key_count, failed_row_count, run_at)


def _validity_predicate(spec: RuleSpec) -> tuple[str, str] | None:
    column = f"c.{_quote_identifier(spec.columns[0])}"
    value = f"CAST({column} AS VARCHAR)"
    clean_value = f"TRIM({value})"
    description = spec.description.lower()
    if "five-digit numeric" in description:
        return f"NOT REGEXP_MATCHES({clean_value}, '^[0-9]{{5}}$')", "not five-digit numeric"
    if "value 1 or 0" in description:
        return f"{clean_value} NOT IN ('1', '0')", "not 1 or 0"
    if "one digit" in description and "1 or 2" in description:
        return f"{clean_value} NOT IN ('1', '2')", "not 1 or 2"
    if "one digit integer from 1-3" in description:
        return f"{clean_value} NOT IN ('1', '2', '3')", "not 1, 2, or 3"
    if "'d' or 'u' and has a number at the end" in description:
        return f"NOT REGEXP_MATCHES(UPPER({clean_value}), '^[DU][0-9]+$')", "not D/U with numeric suffix"
    if "'d' or 'u'" in description:
        return f"UPPER({clean_value}) NOT IN ('D', 'U')", "not D or U"
    if "y or null" in description:
        return f"UPPER({clean_value}) <> 'Y'", "not Y or NULL"
    if "timestamp format" in description:
        return f"TRY_CAST({column} AS TIMESTAMP) IS NULL", "not timestamp"
    if "integer and does not contain a decimal" in description or "is the data an integer" in description:
        return f"NOT REGEXP_MATCHES({clean_value}, '^-?[0-9]+$')", "not integer"
    if "alphanumeric" in description:
        return f"NOT REGEXP_MATCHES({clean_value}, '^[A-Za-z0-9]+$')", "not alphanumeric"
    return None


def run_validity(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Run format/type validity checks that are explicit in the index workbook."""
    run_at = datetime.now(timezone.utc).isoformat()
    predicate = _validity_predicate(spec)
    if predicate is None:
        return _skip_result(spec, f"unsupported validity rule: {spec.description}", run_at)
    failed_predicate, reason = predicate

    path, _, skip = _ensure_table(spec, con, config, table_paths, run_at)
    if skip:
        return skip

    relation = _relation_sql(path)
    column = _quote_identifier(spec.columns[0])
    temp_table = f"failures_{spec.code.replace('-', '_')}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{spec.table}' AS table_name,
            '{_column_list(spec.columns)}' AS column_names,
            CAST(c.{column} AS VARCHAR) AS failed_value,
            '{reason}' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM {relation} c
        WHERE c.{column} IS NOT NULL
          AND ({failed_predicate})
        GROUP BY c.{column}
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(f"SELECT COUNT(*) FROM {relation} c WHERE c.{column} IS NOT NULL").fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return _failure_result(spec, total_checked, failed_key_count, failed_row_count, run_at)


def run_accuracy(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Run accuracy checks that are explicit scalar comparisons."""
    run_at = datetime.now(timezone.utc).isoformat()
    if ">=0" not in spec.description.replace(" ", ""):
        return _skip_result(spec, f"unsupported accuracy rule: {spec.description}", run_at)

    path, _, skip = _ensure_table(spec, con, config, table_paths, run_at)
    if skip:
        return skip

    relation = _relation_sql(path)
    column = _quote_identifier(spec.columns[0])
    temp_table = f"failures_{spec.code.replace('-', '_')}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{spec.table}' AS table_name,
            '{_column_list(spec.columns)}' AS column_names,
            CAST(c.{column} AS VARCHAR) AS failed_value,
            'negative or non-numeric value' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM {relation} c
        WHERE c.{column} IS NOT NULL
          AND (TRY_CAST(c.{column} AS DOUBLE) IS NULL OR TRY_CAST(c.{column} AS DOUBLE) < 0)
        GROUP BY c.{column}
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(f"SELECT COUNT(*) FROM {relation} c WHERE c.{column} IS NOT NULL").fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return _failure_result(spec, total_checked, failed_key_count, failed_row_count, run_at)


def run_dcorrect_accuracy(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Compare CNOTE origin/destination with the latest DCORRECT record."""
    run_at = datetime.now(timezone.utc).isoformat()
    if spec.code == "ACCU2B12":
        cnote_column = "CNOTE_ORIGIN"
        dcorrect_column = "DCORRECT_ORIGIN"
    else:
        cnote_column = "CNOTE_DESTINATION"
        dcorrect_column = "DCORRECT_DEST"

    required = {
        "CMS_CNOTE": ("CNOTE_NO", cnote_column),
        "CMS_DCORRECT_DEST": ("DCORRECT_CNOTE_NO", dcorrect_column),
    }
    missing_reason = _check_required_columns(con, config, table_paths, required)
    if missing_reason:
        return _skip_result(spec, missing_reason, run_at)

    dcorrect_columns = _columns(con, table_paths["CMS_DCORRECT_DEST"])
    if "DCORRECT_CNOTE_DATE" in dcorrect_columns:
        order_column = "DCORRECT_CNOTE_DATE"
    elif "DCORRECT_CDATE" in dcorrect_columns:
        order_column = "DCORRECT_CDATE"
    else:
        order_column = None

    dcorrect_relation = _relation_sql(table_paths["CMS_DCORRECT_DEST"])
    if order_column:
        dcorrect_sql = f"""
            SELECT *
            FROM (
                SELECT
                    d.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY d."DCORRECT_CNOTE_NO"
                        ORDER BY TRY_CAST(d.{_quote_identifier(order_column)} AS TIMESTAMP) DESC NULLS LAST
                    ) AS rn
                FROM {dcorrect_relation} d
                WHERE d."DCORRECT_CNOTE_NO" IS NOT NULL
            )
            WHERE rn = 1
        """
    else:
        dcorrect_sql = f"""
            SELECT *
            FROM {dcorrect_relation} d
            WHERE d."DCORRECT_CNOTE_NO" IS NOT NULL
        """

    cnote_expr = f'c.{_quote_identifier(cnote_column)}'
    dcorrect_expr = f'd.{_quote_identifier(dcorrect_column)}'
    cnote_clean = _clean_sql(cnote_expr)
    dcorrect_clean = _clean_sql(dcorrect_expr)
    temp_table = f"failures_{spec.code}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            'CMS_CNOTE' AS table_name,
            '{cnote_column}' AS column_names,
            {cnote_clean} || ' <> ' || {dcorrect_clean} AS failed_value,
            '{cnote_column} does not match {dcorrect_column}' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM {_relation_sql(table_paths["CMS_CNOTE"])} c
        JOIN ({dcorrect_sql}) d
          ON c."CNOTE_NO" = d."DCORRECT_CNOTE_NO"
        WHERE {cnote_clean} <> ''
          AND {dcorrect_clean} <> ''
          AND {cnote_clean} <> {dcorrect_clean}
        GROUP BY {cnote_clean}, {dcorrect_clean}
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    from_sql = f"""
        FROM {_relation_sql(table_paths["CMS_CNOTE"])} c
        JOIN ({dcorrect_sql}) d
          ON c."CNOTE_NO" = d."DCORRECT_CNOTE_NO"
    """
    applicable = f"{cnote_clean} <> '' AND {dcorrect_clean} <> ''"
    total_checked = con.execute(f"SELECT COUNT(*) {from_sql} WHERE {applicable}").fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()

    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name="CMS_CNOTE",
        column_names=cnote_column,
        compared_table="CMS_DCORRECT_DEST",
        compared_columns=dcorrect_column,
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


def run_service_reference_accuracy(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    run_at = datetime.now(timezone.utc).isoformat()
    left_table = "CMS_CNOTE" if spec.code == "ACCU6B6" else "CMS_APICUST"
    left_column = "CNOTE_SERVICES_CODE" if spec.code == "ACCU6B6" else "APICUST_SERVICES_CODE"
    required = {
        left_table: (left_column,),
        "CMS_CNOTE": ("CNOTE_NO", "CNOTE_ROUTE_CODE"),
        "CMS_DROURATE": ("DROURATE_CODE", "DROURATE_SERVICE"),
    }
    if left_table == "CMS_APICUST":
        required[left_table] = ("APICUST_CNOTE_NO", left_column)

    missing_reason = _check_required_columns(con, config, table_paths, required)
    if missing_reason:
        return _skip_result(spec, missing_reason, run_at)

    temp_table = f"failures_{spec.code}"
    left_service = f"l.{_quote_identifier(left_column)}"
    cnote_alias = "l" if left_table == "CMS_CNOTE" else "c"
    route = f'{cnote_alias}."CNOTE_ROUTE_CODE"'
    from_sql = f"FROM {_alias_relation('l', left_table, table_paths)}"
    if left_table == "CMS_APICUST":
        from_sql += f"\nJOIN {_alias_relation('c', 'CMS_CNOTE', table_paths)} ON l.\"APICUST_CNOTE_NO\" = c.\"CNOTE_NO\""

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{left_table}' AS table_name,
            '{left_column}' AS column_names,
            {_clean_sql(left_service)} AS failed_value,
            'service code is not present in CMS_DROURATE for route' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        {from_sql}
        WHERE {_clean_sql(left_service)} <> ''
          AND {_clean_sql(route)} <> ''
          AND NOT EXISTS (
              SELECT 1
              FROM {_relation_sql(table_paths["CMS_DROURATE"])} d
              WHERE {_clean_sql('d."DROURATE_CODE"')} = {_clean_sql(route)}
                AND {_clean_sql('d."DROURATE_SERVICE"')} = {_clean_sql(left_service)}
          )
        GROUP BY {_clean_sql(left_service)}
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(
        f"SELECT COUNT(*) {from_sql} WHERE {_clean_sql(left_service)} <> '' AND {_clean_sql(route)} <> ''"
    ).fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name=left_table,
        column_names=left_column,
        compared_table="CMS_DROURATE",
        compared_columns="DROURATE_CODE, DROURATE_SERVICE",
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


def run_consistency(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    if spec.code == "CONS3H4":
        return run_manifest_route_consistency(spec, con, config, table_paths, failures_table)
    if spec.code in {"CONS3J3", "CONS3J4", "CONS3N10", "CONS4N9", "CONS4L6"}:
        return run_aggregate_consistency(spec, con, config, table_paths, failures_table)
    check = PAIR_CHECKS.get(spec.code)
    if check is None:
        return run_unsupported(spec, con, config, table_paths, failures_table)
    return _run_pair_check(spec, check, con, config, table_paths, failures_table, "consistency")


def run_manifest_route_consistency(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Compare MANIFEST_ROUTE letters 9-11 with CNOTE destination."""
    run_at = datetime.now(timezone.utc).isoformat()
    required = {
        "CMS_MANIFEST": ("MANIFEST_NO", "MANIFEST_ROUTE"),
        "CMS_MFCNOTE": ("MFCNOTE_MAN_NO", "MFCNOTE_NO"),
        "CMS_CNOTE": ("CNOTE_NO", "CNOTE_DESTINATION"),
    }
    missing_reason = _check_required_columns(con, config, table_paths, required)
    if missing_reason:
        return _skip_result(spec, missing_reason, run_at)

    route_segment = 'SUBSTRING(TRIM(CAST(m."MANIFEST_ROUTE" AS VARCHAR)), 9, 3)'
    destination = _clean_sql('c."CNOTE_DESTINATION"')
    temp_table = f"failures_{spec.code}"
    from_sql = f"""
        FROM {_alias_relation("m", "CMS_MANIFEST", table_paths)}
        JOIN {_alias_relation("mf", "CMS_MFCNOTE", table_paths)}
          ON m."MANIFEST_NO" = mf."MFCNOTE_MAN_NO"
        JOIN {_alias_relation("c", "CMS_CNOTE", table_paths)}
          ON mf."MFCNOTE_NO" = c."CNOTE_NO"
    """
    applicable = f'{route_segment} <> \'\' AND LENGTH(TRIM(CAST(m."MANIFEST_ROUTE" AS VARCHAR))) >= 11 AND {destination} <> \'\''
    failed = f"{route_segment} <> {destination}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            'CMS_MANIFEST' AS table_name,
            'MANIFEST_ROUTE' AS column_names,
            {route_segment} || ' <> ' || {destination} AS failed_value,
            'MANIFEST_ROUTE letters 9-11 do not match CNOTE_DESTINATION' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        {from_sql}
        WHERE {applicable}
          AND {failed}
        GROUP BY {route_segment}, {destination}
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(f"SELECT COUNT(*) {from_sql} WHERE {applicable}").fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name="CMS_MANIFEST",
        column_names="MANIFEST_ROUTE",
        compared_table="CMS_CNOTE",
        compared_columns="CNOTE_DESTINATION",
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


def run_aggregate_consistency(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    run_at = datetime.now(timezone.utc).isoformat()
    temp_table = f"failures_{spec.code}"

    if spec.code in {"CONS3J3", "CONS3J4"}:
        left_col = "MFBAG_ACT_WEIGHT" if spec.code == "CONS3J3" else "MFBAG_CTC_WEIGHT"
        required = {
            "CMS_MFBAG": ("MFBAG_NO", left_col),
            "CMS_MFCNOTE": ("MFCNOTE_BAG_NO", "MFCNOTE_WEIGHT"),
        }
        missing_reason = _check_required_columns(con, config, table_paths, required)
        if missing_reason:
            return _skip_result(spec, missing_reason, run_at)
        left_table = "CMS_MFBAG"
        compared_table = "CMS_MFCNOTE"
        from_sql = f"""
            FROM {_alias_relation("l", "CMS_MFBAG", table_paths)}
            JOIN (
                SELECT "MFCNOTE_BAG_NO" AS agg_key, SUM(TRY_CAST("MFCNOTE_WEIGHT" AS DOUBLE)) AS expected_value
                FROM {_relation_sql(table_paths["CMS_MFCNOTE"])}
                WHERE "MFCNOTE_BAG_NO" IS NOT NULL
                GROUP BY "MFCNOTE_BAG_NO"
            ) r ON l."MFBAG_NO" = r.agg_key
        """
    elif spec.code == "CONS3N10":
        left_col = "MSMU_WEIGHT"
        required = {
            "CMS_MSMU": ("MSMU_NO", left_col),
            "CMS_DSMU": ("DSMU_NO", "DSMU_WEIGHT"),
        }
        missing_reason = _check_required_columns(con, config, table_paths, required)
        if missing_reason:
            return _skip_result(spec, missing_reason, run_at)
        left_table = "CMS_MSMU"
        compared_table = "CMS_DSMU"
        from_sql = f"""
            FROM {_alias_relation("l", "CMS_MSMU", table_paths)}
            JOIN (
                SELECT "DSMU_NO" AS agg_key, SUM(TRY_CAST("DSMU_WEIGHT" AS DOUBLE)) AS expected_value
                FROM {_relation_sql(table_paths["CMS_DSMU"])}
                WHERE "DSMU_NO" IS NOT NULL
                GROUP BY "DSMU_NO"
            ) r ON l."MSMU_NO" = r.agg_key
        """
    elif spec.code == "CONS4N9":
        left_col = "MSMU_QTY"
        required = {
            "CMS_MSMU": ("MSMU_NO", left_col),
            "CMS_DSMU": ("DSMU_NO", "DSMU_BAG_NO"),
        }
        missing_reason = _check_required_columns(con, config, table_paths, required)
        if missing_reason:
            return _skip_result(spec, missing_reason, run_at)
        left_table = "CMS_MSMU"
        compared_table = "CMS_DSMU"
        from_sql = f"""
            FROM {_alias_relation("l", "CMS_MSMU", table_paths)}
            JOIN (
                SELECT "DSMU_NO" AS agg_key, COUNT(DISTINCT "DSMU_BAG_NO")::DOUBLE AS expected_value
                FROM {_relation_sql(table_paths["CMS_DSMU"])}
                WHERE "DSMU_NO" IS NOT NULL
                GROUP BY "DSMU_NO"
            ) r ON l."MSMU_NO" = r.agg_key
        """
    else:
        left_col = "MMBAG_QTY"
        required = {
            "CMS_MMBAG": ("MMBAG_NO", left_col),
            "CMS_DMBAG": ("DMBAG_NO", "DMBAG_BAG_NO"),
            "CMS_MFCNOTE": ("MFCNOTE_NO", "MFCNOTE_BAG_NO"),
        }
        missing_reason = _check_required_columns(con, config, table_paths, required)
        if missing_reason:
            return _skip_result(spec, missing_reason, run_at)
        left_table = "CMS_MMBAG"
        compared_table = "CMS_MFCNOTE"
        from_sql = f"""
            FROM {_alias_relation("l", "CMS_MMBAG", table_paths)}
            JOIN (
                SELECT d."DMBAG_NO" AS agg_key, COUNT(DISTINCT m."MFCNOTE_NO")::DOUBLE AS expected_value
                FROM {_relation_sql(table_paths["CMS_DMBAG"])} d
                JOIN {_relation_sql(table_paths["CMS_MFCNOTE"])} m
                  ON d."DMBAG_BAG_NO" = m."MFCNOTE_BAG_NO"
                GROUP BY d."DMBAG_NO"
            ) r ON l."MMBAG_NO" = r.agg_key
        """

    actual = f'TRY_CAST(l.{_quote_identifier(left_col)} AS DOUBLE)'
    applicable = f"{actual} IS NOT NULL AND r.expected_value IS NOT NULL"
    failed = f"ABS({actual} - r.expected_value) > 0.000001"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{left_table}' AS table_name,
            '{left_col}' AS column_names,
            CAST({actual} AS VARCHAR) || ' <> ' || CAST(r.expected_value AS VARCHAR) AS failed_value,
            'aggregate expected value mismatch' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        {from_sql}
        WHERE {applicable}
          AND {failed}
        GROUP BY {actual}, r.expected_value
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)
    total_checked = con.execute(f"SELECT COUNT(*) {from_sql} WHERE {applicable}").fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name=left_table,
        column_names=left_col,
        compared_table=compared_table,
        compared_columns="aggregate",
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


def run_timeliness(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    if spec.code.startswith("TIME1H15"):
        return run_manifest_sequence_timeliness(spec, con, config, table_paths, failures_table)
    if spec.code == "TIME1V9":
        return run_mhocnote_to_msj_timeliness(spec, con, config, table_paths, failures_table)
    if spec.code == "TIME1X2":
        return run_im_manifest_to_msj_timeliness(spec, con, config, table_paths, failures_table)
    check = TIMELINESS_CHECKS.get(spec.code)
    if check is None:
        return run_unsupported(spec, con, config, table_paths, failures_table)
    return _run_pair_check(spec, check, con, config, table_paths, failures_table, "timeliness")


def _msj_by_cnote_sql(table_paths: dict[str, str]) -> str:
    return f"""
        SELECT
            dhic."DHICNOTE_CNOTE_NO" AS cnote_no,
            msj."MSJ_NO" AS msj_no,
            TRY_CAST(msj."MSJ_SIGNDATE" AS TIMESTAMP) AS msj_signdate
        FROM {_alias_relation("dhic", "CMS_DHICNOTE", table_paths)}
        JOIN {_alias_relation("rdsj", "CMS_RDSJ", table_paths)}
          ON dhic."DHICNOTE_NO" = rdsj."RDSJ_HVI_NO"
        JOIN {_alias_relation("dsj", "CMS_DSJ", table_paths)}
          ON rdsj."RDSJ_HVO_NO" = dsj."DSJ_HVO_NO"
        JOIN {_alias_relation("msj", "CMS_MSJ", table_paths)}
          ON dsj."DSJ_NO" = msj."MSJ_NO"
    """


def _run_timeline_pairs(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    failures_table: str,
    pairs_sql: str,
    table_name: str,
    column_names: str,
    compared_table: str,
    compared_columns: str,
    reason: str,
) -> RuleResult:
    run_at = datetime.now(timezone.utc).isoformat()
    temp_table = f"failures_{spec.code.replace(' ', '_').replace('(', '').replace(')', '')}"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{table_name}' AS table_name,
            '{column_names}' AS column_names,
            left_id || '@' || CAST(left_ts AS VARCHAR)
                || ' > ' ||
                right_id || '@' || CAST(right_ts AS VARCHAR) AS failed_value,
            '{reason}' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM ({pairs_sql}) pairs
        WHERE left_ts IS NOT NULL
          AND right_ts IS NOT NULL
          AND right_ts < left_ts
        GROUP BY left_id, left_ts, right_id, right_ts
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(f"""
        SELECT COUNT(*)
        FROM ({pairs_sql}) pairs
        WHERE left_ts IS NOT NULL
          AND right_ts IS NOT NULL
    """).fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name=table_name,
        column_names=column_names,
        compared_table=compared_table,
        compared_columns=compared_columns,
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


def run_mhocnote_to_msj_timeliness(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    required = {
        "CMS_DHOCNOTE": ("DHOCNOTE_CNOTE_NO", "DHOCNOTE_NO"),
        "CMS_MHOCNOTE": ("MHOCNOTE_NO", "MHOCNOTE_SIGNDATE"),
        "CMS_DHICNOTE": ("DHICNOTE_CNOTE_NO", "DHICNOTE_NO"),
        "CMS_RDSJ": ("RDSJ_HVI_NO", "RDSJ_HVO_NO"),
        "CMS_DSJ": ("DSJ_HVO_NO", "DSJ_NO"),
        "CMS_MSJ": ("MSJ_NO", "MSJ_SIGNDATE"),
    }
    run_at = datetime.now(timezone.utc).isoformat()
    missing_reason = _check_required_columns(con, config, table_paths, required)
    if missing_reason:
        return _skip_result(spec, missing_reason, run_at)

    pairs_sql = f"""
        WITH msj_by_cnote AS ({_msj_by_cnote_sql(table_paths)})
        SELECT
            mhoc."MHOCNOTE_NO" AS left_id,
            TRY_CAST(mhoc."MHOCNOTE_SIGNDATE" AS TIMESTAMP) AS left_ts,
            msj.msj_no AS right_id,
            msj.msj_signdate AS right_ts
        FROM {_alias_relation("dhoc", "CMS_DHOCNOTE", table_paths)}
        JOIN {_alias_relation("mhoc", "CMS_MHOCNOTE", table_paths)}
          ON dhoc."DHOCNOTE_NO" = mhoc."MHOCNOTE_NO"
        JOIN msj_by_cnote msj
          ON dhoc."DHOCNOTE_CNOTE_NO" = msj.cnote_no
    """
    return _run_timeline_pairs(
        spec,
        con,
        config,
        failures_table,
        pairs_sql,
        "CMS_MHOCNOTE",
        "MHOCNOTE_SIGNDATE",
        "CMS_MSJ",
        "MSJ_SIGNDATE",
        "MHOCNOTE_SIGNDATE occurs after MSJ_SIGNDATE",
    )


def run_im_manifest_to_msj_timeliness(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    required = {
        "CMS_MFCNOTE": ("MFCNOTE_NO", "MFCNOTE_MAN_NO", "MFCNOTE_CRDATE"),
        "CMS_MANIFEST": ("MANIFEST_NO", "MANIFEST_CRDATE", "MANIFEST_DATE"),
        "CMS_DHICNOTE": ("DHICNOTE_CNOTE_NO", "DHICNOTE_NO"),
        "CMS_RDSJ": ("RDSJ_HVI_NO", "RDSJ_HVO_NO"),
        "CMS_DSJ": ("DSJ_HVO_NO", "DSJ_NO"),
        "CMS_MSJ": ("MSJ_NO", "MSJ_SIGNDATE"),
    }
    run_at = datetime.now(timezone.utc).isoformat()
    missing_reason = _check_required_columns(con, config, table_paths, required)
    if missing_reason:
        return _skip_result(spec, missing_reason, run_at)

    pairs_sql = f"""
        WITH {_manifest_events_ctes(table_paths)},
        msj_by_cnote AS ({_msj_by_cnote_sql(table_paths)})
        SELECT
            im.man_no AS left_id,
            im.manifest_crdate AS left_ts,
            msj.msj_no AS right_id,
            msj.msj_signdate AS right_ts
        FROM events im
        JOIN msj_by_cnote msj
          ON im.cnote_no = msj.cnote_no
        WHERE im.man_type = 'IM'
          AND im.type_seq = 1
    """
    return _run_timeline_pairs(
        spec,
        con,
        config,
        failures_table,
        pairs_sql,
        "CMS_MANIFEST",
        "MANIFEST_CRDATE (IM)",
        "CMS_MSJ",
        "MSJ_SIGNDATE",
        "IM manifest creation occurs after MSJ_SIGNDATE",
    )


def _manifest_events_ctes(table_paths: dict[str, str]) -> str:
    return f"""
        mfc_typed AS (
            SELECT
                CAST(m."MFCNOTE_NO" AS VARCHAR) AS cnote_no,
                CAST(m."MFCNOTE_MAN_NO" AS VARCHAR) AS man_no,
                UPPER(SPLIT_PART(CAST(m."MFCNOTE_MAN_NO" AS VARCHAR), '/', 2)) AS man_type,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        CAST(m."MFCNOTE_NO" AS VARCHAR),
                        UPPER(SPLIT_PART(CAST(m."MFCNOTE_MAN_NO" AS VARCHAR), '/', 2))
                    ORDER BY TRY_CAST(m."MFCNOTE_CRDATE" AS TIMESTAMP) ASC NULLS LAST
                ) AS type_seq
            FROM {_relation_sql(table_paths["CMS_MFCNOTE"])} m
            WHERE m."MFCNOTE_MAN_NO" IS NOT NULL
              AND UPPER(SPLIT_PART(CAST(m."MFCNOTE_MAN_NO" AS VARCHAR), '/', 2)) IN ('OM', 'TM', 'IM')
        ),
        manifest_deduped AS (
            SELECT *
            FROM (
                SELECT
                    man.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY man."MANIFEST_NO"
                        ORDER BY TRY_CAST(man."MANIFEST_DATE" AS TIMESTAMP) DESC NULLS LAST
                    ) AS rn
                FROM {_relation_sql(table_paths["CMS_MANIFEST"])} man
            )
            WHERE rn = 1
        ),
        events AS (
            SELECT
                mfc.cnote_no,
                mfc.man_no,
                mfc.man_type,
                mfc.type_seq,
                TRY_CAST(man."MANIFEST_CRDATE" AS TIMESTAMP) AS manifest_crdate
            FROM mfc_typed mfc
            JOIN manifest_deduped man
              ON mfc.man_no = CAST(man."MANIFEST_NO" AS VARCHAR)
        )
    """


def run_manifest_sequence_timeliness(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    """Check OM/TM/IM manifest creation order per CNOTE."""
    run_at = datetime.now(timezone.utc).isoformat()
    required = {
        "CMS_MFCNOTE": ("MFCNOTE_NO", "MFCNOTE_MAN_NO", "MFCNOTE_CRDATE"),
        "CMS_MANIFEST": ("MANIFEST_NO", "MANIFEST_CRDATE", "MANIFEST_DATE"),
    }
    missing_reason = _check_required_columns(con, config, table_paths, required)
    if missing_reason:
        return _skip_result(spec, missing_reason, run_at)

    events_ctes = _manifest_events_ctes(table_paths)
    temp_table = f"failures_{spec.code.replace(' ', '_').replace('(', '').replace(')', '')}"

    if spec.code == "TIME1H15 (OM)":
        pairs_sql = f"""
            WITH {events_ctes}
            SELECT
                om.cnote_no,
                om.man_no AS left_manifest_no,
                'OM' AS left_manifest_type,
                om.manifest_crdate AS left_crdate,
                tm.man_no AS right_manifest_no,
                'TM1' AS right_manifest_type,
                tm.manifest_crdate AS right_crdate
            FROM events om
            JOIN events tm
              ON om.cnote_no = tm.cnote_no
             AND tm.man_type = 'TM'
             AND tm.type_seq = 1
            WHERE om.man_type = 'OM'
              AND om.type_seq = 1
        """
        compared_columns = "MANIFEST_CRDATE (TM1)"
        reason = "OM manifest is after first TM manifest"
    elif spec.code == "TIME1H15 (TM)":
        pairs_sql = f"""
            WITH {events_ctes}
            SELECT
                tm.cnote_no,
                tm.man_no AS left_manifest_no,
                'TM' || CAST(tm.type_seq AS VARCHAR) AS left_manifest_type,
                tm.manifest_crdate AS left_crdate,
                next_tm.man_no AS right_manifest_no,
                'TM' || CAST(next_tm.type_seq AS VARCHAR) AS right_manifest_type,
                next_tm.manifest_crdate AS right_crdate
            FROM events tm
            JOIN events next_tm
              ON tm.cnote_no = next_tm.cnote_no
             AND next_tm.man_type = 'TM'
             AND next_tm.type_seq = tm.type_seq + 1
            WHERE tm.man_type = 'TM'
        """
        compared_columns = "MANIFEST_CRDATE (next TM)"
        reason = "TM manifest is after next TM manifest"
    else:
        pairs_sql = f"""
            WITH {events_ctes},
            last_tm AS (
                SELECT *
                FROM (
                    SELECT
                        e.*,
                        ROW_NUMBER() OVER (PARTITION BY e.cnote_no ORDER BY e.type_seq DESC) AS rn
                    FROM events e
                    WHERE e.man_type = 'TM'
                )
                WHERE rn = 1
            ),
            prior_manifest AS (
                SELECT
                    last_tm.cnote_no,
                    last_tm.man_no,
                    'TM' || CAST(last_tm.type_seq AS VARCHAR) AS man_type,
                    last_tm.manifest_crdate
                FROM last_tm
                UNION ALL
                SELECT
                    om.cnote_no,
                    om.man_no,
                    'OM' AS man_type,
                    om.manifest_crdate
                FROM events om
                WHERE om.man_type = 'OM'
                  AND om.type_seq = 1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM last_tm
                      WHERE last_tm.cnote_no = om.cnote_no
                  )
            )
            SELECT
                prior.cnote_no,
                prior.man_no AS left_manifest_no,
                prior.man_type AS left_manifest_type,
                prior.manifest_crdate AS left_crdate,
                im.man_no AS right_manifest_no,
                'IM' AS right_manifest_type,
                im.manifest_crdate AS right_crdate
            FROM prior_manifest prior
            JOIN events im
              ON prior.cnote_no = im.cnote_no
             AND im.man_type = 'IM'
             AND im.type_seq = 1
        """
        compared_columns = "MANIFEST_CRDATE (IM)"
        reason = "final OM/TM manifest is after IM manifest"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            'CMS_MANIFEST' AS table_name,
            'MANIFEST_CRDATE' AS column_names,
            left_manifest_type || ':' || left_manifest_no || '@' || CAST(left_crdate AS VARCHAR)
                || ' > ' ||
                right_manifest_type || ':' || right_manifest_no || '@' || CAST(right_crdate AS VARCHAR)
                AS failed_value,
            '{reason}' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
            NULL::BOOLEAN AS boundary_suspect,
            '{run_at}' AS run_at
        FROM ({pairs_sql}) pairs
        WHERE left_crdate IS NOT NULL
          AND right_crdate IS NOT NULL
          AND right_crdate < left_crdate
        GROUP BY
            left_manifest_type,
            left_manifest_no,
            left_crdate,
            right_manifest_type,
            right_manifest_no,
            right_crdate
    """)
    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(f"""
        SELECT COUNT(*)
        FROM ({pairs_sql}) pairs
        WHERE left_crdate IS NOT NULL
          AND right_crdate IS NOT NULL
    """).fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()
    return RuleResult(
        index_code=spec.code,
        element=spec.element,
        rule_family=spec.rule_family,
        table_name="CMS_MANIFEST",
        column_names="MANIFEST_CRDATE",
        compared_table="CMS_MANIFEST",
        compared_columns=compared_columns,
        total_checked=int(total_checked),
        failed_key_count=int(failed_key_count),
        failed_row_count=int(failed_row_count),
        failure_rate=float(failed_row_count / total_checked) if total_checked else 0.0,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=spec.needs_confirmation,
        skipped_reason=None,
        run_at=run_at,
    )


def run_unsupported(
    spec: RuleSpec,
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    failures_table: str,
) -> RuleResult:
    run_at = datetime.now(timezone.utc).isoformat()
    return _skip_result(
        spec,
        "relational join mapping is not implemented for this workbook index",
        run_at,
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
    temp_table = f"failures_{spec.code.replace('-', '_')}"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        SELECT
            '{spec.code}' AS index_code,
            '{spec.child_table}' AS table_name,
            '{spec.child_fk}' AS column_names,
            CAST(c.{child_fk} AS VARCHAR) AS failed_value,
            'orphan key: missing {spec.parent_table}.{spec.parent_pk}' AS failure_reason,
            COUNT(*)::BIGINT AS affected_rows,
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

    _insert_failures(con, failures_table, temp_table, config.governance.orphan_key_limit)

    total_checked = con.execute(
        f"SELECT COUNT(*) FROM {child_sql} WHERE {child_fk} IS NOT NULL"
    ).fetchone()[0]
    failed_key_count, failed_row_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(affected_rows), 0) FROM {temp_table}"
    ).fetchone()

    return _failure_result(spec, total_checked, failed_key_count, failed_row_count, run_at)


EXECUTORS = {
    "COMP": run_completeness,
    "UNIQ": run_uniqueness,
    "UNIQ1": run_uniqueness,
    "UNIQ2": run_uniqueness,
    "VALD": run_validity,
    "VALD1": run_validity,
    "VALD2": run_validity,
    "VALD3": run_validity,
    "VALD4": run_validity,
    "VALD5": run_validity,
    "VALD7": run_validity,
    "VALD8": run_validity,
    "VALD9": run_validity,
    "VALD10": run_validity,
    "VALD11": run_validity,
    "VALD12": run_validity,
    "VALD13": run_validity,
    "ACCU": run_accuracy,
    "ACCU1": run_accuracy,
    "ACCU2": run_dcorrect_accuracy,
    "ACCU3": run_dcorrect_accuracy,
    "ACCU4": run_accuracy,
    "ACCU5": run_service_reference_accuracy,
    "ACCU6": run_service_reference_accuracy,
    "CONS": run_consistency,
    "CONS1": run_consistency,
    "CONS2": run_consistency,
    "CONS3": run_consistency,
    "CONS4": run_consistency,
    "TIME": run_timeliness,
    "TIME1": run_timeliness,
    "INTG1": run_intg1,
}
