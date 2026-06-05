"""Build a CNOTE-by-index governance status matrix."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import pyarrow as pa

from src.config import GovernanceConfig
from src.rules.executors import (
    PAIR_CHECKS,
    TIMELINESS_CHECKS,
    RuleResult,
    _alias_relation,
    _clean_sql,
    _column_list,
    _columns,
    _concat_columns,
    _join_plan,
    _non_null_predicate,
    _quote_identifier,
    _relation_sql,
    _sql_literal,
    _validity_predicate,
)
from src.rules.registry import RuleSpec, active_rules


STATUS_COLUMNS = [
    "run_id",
    "cnote_no",
    "index_code",
    "element",
    "rule_family",
    "status",
    "failure_reason",
    "failed_value",
    "source_table",
    "source_column",
    "compared_table",
    "compared_column",
    "run_at",
]


DIRECT_CNOTE_COLUMNS = {
    "CMS_CNOTE": "CNOTE_NO",
    "CMS_APICUST": "APICUST_CNOTE_NO",
    "CMS_CNOTE_AMO": "CNOTE_NO",
    "CMS_CNOTE_POD": "CNOTE_POD_NO",
    "CMS_DRCNOTE": "DRCNOTE_CNOTE_NO",
    "CMS_DRSHEET": "DRSHEET_CNOTE_NO",
    "CMS_DRSHEET_PRA": "DRSHEET_CNOTE_NO",
    "CMS_DHICNOTE": "DHICNOTE_CNOTE_NO",
    "CMS_DHOCNOTE": "DHOCNOTE_CNOTE_NO",
    "CMS_DHOUNDEL_POD": "DHOUNDEL_CNOTE_NO",
    "CMS_DHOV_RSHEET": "DHOV_RSHEET_CNOTE",
    "CMS_DBAG_HO": "DBAG_CNOTE_NO",
    "CMS_DHI_HOC": "DHI_CNOTE_NO",
    "CMS_MFCNOTE": "MFCNOTE_NO",
    "CMS_COST_DTRANSIT_AGEN": "CNOTE_NO",
    "CMS_DSTATUS": "DSTATUS_CNOTE_NO",
    "CMS_DCORRECT_DEST": "DCORRECT_CNOTE_NO",
}


def _create_rule_metadata(con: Any, rules: list[RuleSpec], results: list[RuleResult]) -> None:
    scorecard = {result.index_code: result for result in results}
    rows = []
    for rule in rules:
        result = scorecard.get(rule.code)
        rows.append({
            "index_code": rule.code,
            "element": rule.element,
            "rule_family": rule.rule_family,
            "source_table": rule.table or rule.child_table,
            "source_column": _column_list(rule.columns),
            "compared_table": rule.parent_table or (result.compared_table if result else None),
            "compared_column": rule.parent_pk or (result.compared_columns if result else None),
            "scorecard_status": result.status if result else "SKIPPED",
            "skipped_reason": result.skipped_reason if result else "missing scorecard result",
            "run_at": result.run_at if result else "",
        })

    table = pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("index_code", pa.string()),
        pa.field("element", pa.string()),
        pa.field("rule_family", pa.string()),
        pa.field("source_table", pa.string()),
        pa.field("source_column", pa.string()),
        pa.field("compared_table", pa.string()),
        pa.field("compared_column", pa.string()),
        pa.field("scorecard_status", pa.string()),
        pa.field("skipped_reason", pa.string()),
        pa.field("run_at", pa.string()),
    ]))
    con.register("rule_metadata_arrow", table)
    con.execute("CREATE OR REPLACE TEMP TABLE cnote_rule_metadata AS SELECT * FROM rule_metadata_arrow")
    con.unregister("rule_metadata_arrow")


def _table_relation(table: str, table_paths: dict[str, str]) -> str:
    return _relation_sql(table_paths[table])


def _table_has_path(table: str, table_paths: dict[str, str]) -> bool:
    return table in table_paths


def _direct_related_sql(table: str, table_paths: dict[str, str]) -> str | None:
    column = DIRECT_CNOTE_COLUMNS.get(table)
    if not column or not _table_has_path(table, table_paths):
        return None
    return f"""
        SELECT DISTINCT CAST({_quote_identifier(column)} AS VARCHAR) AS cnote_no
        FROM {_table_relation(table, table_paths)}
        WHERE {_quote_identifier(column)} IS NOT NULL
    """


def _related_sql_for_table(table: str, table_paths: dict[str, str]) -> str | None:
    direct = _direct_related_sql(table, table_paths)
    if direct:
        return direct

    if table == "CMS_MRCNOTE" and {"CMS_DRCNOTE", "CMS_MRCNOTE"} <= set(table_paths):
        return f"""
            SELECT DISTINCT CAST(d."DRCNOTE_CNOTE_NO" AS VARCHAR) AS cnote_no
            FROM {_table_relation("CMS_DRCNOTE", table_paths)} d
            JOIN {_table_relation("CMS_MRCNOTE", table_paths)} m
              ON d."DRCNOTE_NO" = m."MRCNOTE_NO"
            WHERE d."DRCNOTE_CNOTE_NO" IS NOT NULL
        """
    if table == "CMS_MRSHEET" and {"CMS_DRSHEET", "CMS_MRSHEET"} <= set(table_paths):
        return f"""
            SELECT DISTINCT CAST(d."DRSHEET_CNOTE_NO" AS VARCHAR) AS cnote_no
            FROM {_table_relation("CMS_DRSHEET", table_paths)} d
            JOIN {_table_relation("CMS_MRSHEET", table_paths)} m
              ON d."DRSHEET_NO" = m."MRSHEET_NO"
            WHERE d."DRSHEET_CNOTE_NO" IS NOT NULL
        """
    if table == "CMS_MHICNOTE" and {"CMS_DHICNOTE", "CMS_MHICNOTE"} <= set(table_paths):
        return f"""
            SELECT DISTINCT CAST(d."DHICNOTE_CNOTE_NO" AS VARCHAR) AS cnote_no
            FROM {_table_relation("CMS_DHICNOTE", table_paths)} d
            JOIN {_table_relation("CMS_MHICNOTE", table_paths)} m
              ON d."DHICNOTE_NO" = m."MHICNOTE_NO"
            WHERE d."DHICNOTE_CNOTE_NO" IS NOT NULL
        """
    if table == "CMS_MHOCNOTE" and {"CMS_DHOCNOTE", "CMS_MHOCNOTE"} <= set(table_paths):
        return f"""
            SELECT DISTINCT CAST(d."DHOCNOTE_CNOTE_NO" AS VARCHAR) AS cnote_no
            FROM {_table_relation("CMS_DHOCNOTE", table_paths)} d
            JOIN {_table_relation("CMS_MHOCNOTE", table_paths)} m
              ON d."DHOCNOTE_NO" = m."MHOCNOTE_NO"
            WHERE d."DHOCNOTE_CNOTE_NO" IS NOT NULL
        """
    if table == "CMS_MHOUNDEL_POD" and {"CMS_DHOUNDEL_POD", "CMS_MHOUNDEL_POD"} <= set(table_paths):
        return f"""
            SELECT DISTINCT CAST(d."DHOUNDEL_CNOTE_NO" AS VARCHAR) AS cnote_no
            FROM {_table_relation("CMS_DHOUNDEL_POD", table_paths)} d
            JOIN {_table_relation("CMS_MHOUNDEL_POD", table_paths)} m
              ON d."DHOUNDEL_NO" = m."MHOUNDEL_NO"
            WHERE d."DHOUNDEL_CNOTE_NO" IS NOT NULL
        """
    if table in {"CMS_MANIFEST", "CMS_MFBAG", "CMS_DMBAG", "CMS_MMBAG", "CMS_DSMU", "CMS_MSMU"}:
        required = {"CMS_MFCNOTE", "CMS_MFBAG", "CMS_DMBAG", "CMS_DSMU", "CMS_MSMU"}
        if not required <= set(table_paths):
            return None
        joins = {
            "CMS_MANIFEST": f"""
                FROM {_table_relation("CMS_MFCNOTE", table_paths)} mfc
                JOIN {_table_relation("CMS_MANIFEST", table_paths)} src
                  ON mfc."MFCNOTE_MAN_NO" = src."MANIFEST_NO"
            """,
            "CMS_MFBAG": f"""
                FROM {_table_relation("CMS_MFCNOTE", table_paths)} mfc
                JOIN {_table_relation("CMS_MFBAG", table_paths)} src
                  ON mfc."MFCNOTE_BAG_NO" = src."MFBAG_NO"
            """,
            "CMS_DMBAG": f"""
                FROM {_table_relation("CMS_MFCNOTE", table_paths)} mfc
                JOIN {_table_relation("CMS_MFBAG", table_paths)} mfbag
                  ON mfc."MFCNOTE_BAG_NO" = mfbag."MFBAG_NO"
                JOIN {_table_relation("CMS_DMBAG", table_paths)} src
                  ON mfbag."MFBAG_NO" = src."DMBAG_BAG_NO"
            """,
            "CMS_MMBAG": f"""
                FROM {_table_relation("CMS_MFCNOTE", table_paths)} mfc
                JOIN {_table_relation("CMS_MFBAG", table_paths)} mfbag
                  ON mfc."MFCNOTE_BAG_NO" = mfbag."MFBAG_NO"
                JOIN {_table_relation("CMS_DMBAG", table_paths)} dmbag
                  ON mfbag."MFBAG_NO" = dmbag."DMBAG_BAG_NO"
                JOIN {_table_relation("CMS_MMBAG", table_paths)} src
                  ON dmbag."DMBAG_NO" = src."MMBAG_NO"
                  OR dmbag."DMBAG_BAG_NO" = src."MMBAG_NO"
            """,
            "CMS_DSMU": f"""
                FROM {_table_relation("CMS_MFCNOTE", table_paths)} mfc
                JOIN {_table_relation("CMS_MFBAG", table_paths)} mfbag
                  ON mfc."MFCNOTE_BAG_NO" = mfbag."MFBAG_NO"
                JOIN {_table_relation("CMS_DMBAG", table_paths)} dmbag
                  ON mfbag."MFBAG_NO" = dmbag."DMBAG_BAG_NO"
                JOIN {_table_relation("CMS_DSMU", table_paths)} src
                  ON dmbag."DMBAG_NO" = src."DSMU_BAG_NO"
            """,
            "CMS_MSMU": f"""
                FROM {_table_relation("CMS_MFCNOTE", table_paths)} mfc
                JOIN {_table_relation("CMS_MFBAG", table_paths)} mfbag
                  ON mfc."MFCNOTE_BAG_NO" = mfbag."MFBAG_NO"
                JOIN {_table_relation("CMS_DMBAG", table_paths)} dmbag
                  ON mfbag."MFBAG_NO" = dmbag."DMBAG_BAG_NO"
                JOIN {_table_relation("CMS_DSMU", table_paths)} dsmu
                  ON dmbag."DMBAG_NO" = dsmu."DSMU_BAG_NO"
                JOIN {_table_relation("CMS_MSMU", table_paths)} src
                  ON dsmu."DSMU_NO" = src."MSMU_NO"
            """,
        }
        return f"""
            SELECT DISTINCT CAST(mfc."MFCNOTE_NO" AS VARCHAR) AS cnote_no
            {joins[table]}
            WHERE mfc."MFCNOTE_NO" IS NOT NULL
        """
    if table in {"CMS_RDSJ", "CMS_DSJ", "CMS_MSJ"}:
        if not {"CMS_DHICNOTE", "CMS_RDSJ", "CMS_DSJ", "CMS_MSJ"} <= set(table_paths):
            return None
        joins = {
            "CMS_RDSJ": f"""
                FROM {_table_relation("CMS_DHICNOTE", table_paths)} dhic
                JOIN {_table_relation("CMS_RDSJ", table_paths)} src
                  ON dhic."DHICNOTE_NO" = src."RDSJ_HVI_NO"
            """,
            "CMS_DSJ": f"""
                FROM {_table_relation("CMS_DHICNOTE", table_paths)} dhic
                JOIN {_table_relation("CMS_RDSJ", table_paths)} rdsj
                  ON dhic."DHICNOTE_NO" = rdsj."RDSJ_HVI_NO"
                JOIN {_table_relation("CMS_DSJ", table_paths)} src
                  ON rdsj."RDSJ_HVO_NO" = src."DSJ_HVO_NO"
            """,
            "CMS_MSJ": f"""
                FROM {_table_relation("CMS_DHICNOTE", table_paths)} dhic
                JOIN {_table_relation("CMS_RDSJ", table_paths)} rdsj
                  ON dhic."DHICNOTE_NO" = rdsj."RDSJ_HVI_NO"
                JOIN {_table_relation("CMS_DSJ", table_paths)} dsj
                  ON rdsj."RDSJ_HVO_NO" = dsj."DSJ_HVO_NO"
                JOIN {_table_relation("CMS_MSJ", table_paths)} src
                  ON dsj."DSJ_NO" = src."MSJ_NO"
            """,
        }
        return f"""
            SELECT DISTINCT CAST(dhic."DHICNOTE_CNOTE_NO" AS VARCHAR) AS cnote_no
            {joins[table]}
            WHERE dhic."DHICNOTE_CNOTE_NO" IS NOT NULL
        """
    if table == "CMS_COST_MTRANSIT_AGEN" and {"CMS_COST_DTRANSIT_AGEN", "CMS_COST_MTRANSIT_AGEN"} <= set(table_paths):
        return f"""
            SELECT DISTINCT CAST(d."CNOTE_NO" AS VARCHAR) AS cnote_no
            FROM {_table_relation("CMS_COST_DTRANSIT_AGEN", table_paths)} d
            JOIN {_table_relation("CMS_COST_MTRANSIT_AGEN", table_paths)} m
              ON d."DMANIFEST_NO" = m."MANIFEST_NO"
            WHERE d."CNOTE_NO" IS NOT NULL
        """
    return None


def _create_rule_related(con: Any, rules: list[RuleSpec], table_paths: dict[str, str]) -> set[str]:
    union_parts = []
    applicable = set()
    for rule in rules:
        table = rule.table or rule.child_table
        related_sql = _related_sql_for_table(table, table_paths)
        if related_sql:
            applicable.add(rule.code)
            union_parts.append(f"SELECT {_sql_literal(rule.code)} AS index_code, cnote_no FROM ({related_sql})")
    if union_parts:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE cnote_rule_related AS
            SELECT DISTINCT index_code, cnote_no
            FROM ({" UNION ALL ".join(union_parts)})
            WHERE cnote_no IS NOT NULL
        """)
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE cnote_rule_related(index_code VARCHAR, cnote_no VARCHAR)")
    return applicable


def _create_applicable_rules(con: Any, applicable_codes: set[str]) -> None:
    if applicable_codes:
        values = ", ".join(f"({_sql_literal(code)})" for code in sorted(applicable_codes))
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE cnote_applicable_rules AS
            SELECT *
            FROM (VALUES {values}) AS v(index_code)
        """)
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE cnote_applicable_rules(index_code VARCHAR)")


def _cnote_expr_for_alias(alias: str, table: str) -> str | None:
    column = DIRECT_CNOTE_COLUMNS.get(table)
    if column:
        return f"CAST({alias}.{_quote_identifier(column)} AS VARCHAR)"
    return None


def _pair_alias_tables(check: Any) -> dict[str, str]:
    alias_tables = {"l": check.left_table, "r": check.right_table}
    for alias in ("j1", "j2"):
        table = None
        join_sql = check.join_sql
        if f'{alias}."DRCNOTE_' in join_sql:
            table = "CMS_DRCNOTE"
        elif f'{alias}."DHI_' in join_sql:
            table = "CMS_DHI_HOC"
        elif f'{alias}."MFCNOTE_' in join_sql:
            table = "CMS_MFCNOTE"
        elif f'{alias}."DRSHEET_' in join_sql:
            table = "CMS_DRSHEET"
        elif f'{alias}."DSJ_' in join_sql:
            table = "CMS_DSJ"
        elif f'{alias}."DHICNOTE_' in join_sql:
            table = "CMS_DHICNOTE"
        elif f'{alias}."MFBAG_' in join_sql:
            table = "CMS_MFBAG"
        elif f'{alias}."DMANIFEST_' in join_sql or f'{alias}."CNOTE_NO"' in join_sql:
            table = "CMS_COST_DTRANSIT_AGEN"
        if table:
            alias_tables[alias] = table
    return alias_tables


def _pair_cnote_expr(check: Any) -> str:
    exprs = [
        expr
        for alias, table in _pair_alias_tables(check).items()
        for expr in [_cnote_expr_for_alias(alias, table)]
        if expr
    ]
    if not exprs:
        return "NULL::VARCHAR"
    return "COALESCE(" + ", ".join(exprs) + ")"


def _insert_generic_scalar_failures(con: Any, rule: RuleSpec, table_paths: dict[str, str]) -> bool:
    table = rule.table
    cnote_col = DIRECT_CNOTE_COLUMNS.get(table)
    if not cnote_col or not table or table not in table_paths:
        return False
    available_columns = _columns(con, table_paths[table])
    required_columns = {cnote_col.upper(), *(column.upper() for column in rule.columns)}
    if not required_columns <= available_columns:
        return False
    relation = _table_relation(table, table_paths)
    cnote = f"CAST(c.{_quote_identifier(cnote_col)} AS VARCHAR)"
    column = _quote_identifier(rule.columns[0])
    if rule.rule_family == "COMP":
        predicate = f"c.{column} IS NULL"
        failed_value = "NULL::VARCHAR"
        reason = "NULL value"
    elif rule.element == "VALD":
        validity = _validity_predicate(rule)
        if validity is None:
            return False
        predicate, reason = validity
        predicate = f"c.{column} IS NOT NULL AND ({predicate})"
        failed_value = f"CAST(c.{column} AS VARCHAR)"
    elif rule.element == "ACCU" and ">=0" in rule.description.replace(" ", ""):
        predicate = (
            f"c.{column} IS NOT NULL AND "
            f"(TRY_CAST(c.{column} AS DOUBLE) IS NULL OR TRY_CAST(c.{column} AS DOUBLE) < 0)"
        )
        failed_value = f"CAST(c.{column} AS VARCHAR)"
        reason = "negative or non-numeric value"
    else:
        return False
    con.execute(f"""
        INSERT INTO cnote_index_failures
        SELECT
            {cnote} AS cnote_no,
            {_sql_literal(rule.code)} AS index_code,
            {_sql_literal(reason)} AS failure_reason,
            {failed_value} AS failed_value
        FROM {relation} c
        WHERE {cnote} IS NOT NULL
          AND {predicate}
    """)
    return True


def _insert_uniqueness_failures(con: Any, rule: RuleSpec, table_paths: dict[str, str]) -> bool:
    table = rule.table
    cnote_col = DIRECT_CNOTE_COLUMNS.get(table)
    if not cnote_col or not table or table not in table_paths:
        return False
    available_columns = _columns(con, table_paths[table])
    required_columns = {cnote_col.upper(), *(column.upper() for column in rule.columns)}
    if not required_columns <= available_columns:
        return False
    relation = _table_relation(table, table_paths)
    non_null = _non_null_predicate("c", rule.columns)
    group_by = ", ".join(f"c.{_quote_identifier(column)}" for column in rule.columns)
    key_value = _concat_columns("c", rule.columns)
    con.execute(f"""
        INSERT INTO cnote_index_failures
        WITH duplicate_keys AS (
            SELECT {group_by}
            FROM {relation} c
            WHERE {non_null}
            GROUP BY {group_by}
            HAVING COUNT(*) > 1
        )
        SELECT
            CAST(c.{_quote_identifier(cnote_col)} AS VARCHAR) AS cnote_no,
            {_sql_literal(rule.code)} AS index_code,
            'duplicate key' AS failure_reason,
            {key_value} AS failed_value
        FROM {relation} c
        JOIN duplicate_keys d
          ON {" AND ".join(f"c.{_quote_identifier(column)} = d.{_quote_identifier(column)}" for column in rule.columns)}
        WHERE c.{_quote_identifier(cnote_col)} IS NOT NULL
    """)
    return True


def _insert_pair_failures(con: Any, rule: RuleSpec, check: Any, table_paths: dict[str, str], mode: str) -> bool:
    if not {check.left_table, check.right_table} <= set(table_paths):
        return False
    try:
        bridge_joins, right_join_sql = _join_plan(check, table_paths)
    except Exception:
        return False
    cnote_expr = _pair_cnote_expr(check)
    if cnote_expr == "NULL::VARCHAR":
        return False
    left_expr = f"l.{_quote_identifier(check.left_column)}"
    right_expr = f"r.{_quote_identifier(check.right_column)}"
    if mode == "timeliness":
        applicable = f"TRY_CAST({left_expr} AS TIMESTAMP) IS NOT NULL AND TRY_CAST({right_expr} AS TIMESTAMP) IS NOT NULL"
        failed = f"TRY_CAST({right_expr} AS TIMESTAMP) < TRY_CAST({left_expr} AS TIMESTAMP)"
        reason = f"{check.left_column} occurs after {check.right_column}"
        failed_value = f"CAST({left_expr} AS VARCHAR) || ' > ' || CAST({right_expr} AS VARCHAR)"
    else:
        left_clean = _clean_sql(left_expr)
        right_clean = _clean_sql(right_expr)
        applicable = f"{left_clean} <> '' AND {right_clean} <> ''"
        failed = f"{left_clean} <> {right_clean}"
        reason = f"{check.left_column} does not match {check.right_column}"
        failed_value = f"{left_clean} || ' <> ' || {right_clean}"
    con.execute(f"""
        INSERT INTO cnote_index_failures
        SELECT
            {cnote_expr} AS cnote_no,
            {_sql_literal(rule.code)} AS index_code,
            {_sql_literal(reason)} AS failure_reason,
            {failed_value} AS failed_value
        FROM {_alias_relation("l", check.left_table, table_paths)}
        {bridge_joins}
        JOIN {_alias_relation("r", check.right_table, table_paths)}
          ON {right_join_sql}
        WHERE {cnote_expr} IS NOT NULL
          AND {applicable}
          AND {failed}
    """)
    return True


def _insert_intg_failures(con: Any, rule: RuleSpec, table_paths: dict[str, str]) -> bool:
    table = rule.child_table
    cnote_col = DIRECT_CNOTE_COLUMNS.get(table)
    if not cnote_col or table not in table_paths or rule.parent_table not in table_paths:
        return False
    child_fk = _quote_identifier(rule.child_fk)
    parent_pk = _quote_identifier(rule.parent_pk)
    con.execute(f"""
        INSERT INTO cnote_index_failures
        SELECT
            CAST(c.{_quote_identifier(cnote_col)} AS VARCHAR) AS cnote_no,
            {_sql_literal(rule.code)} AS index_code,
            {_sql_literal(f"orphan key: missing {rule.parent_table}.{rule.parent_pk}")} AS failure_reason,
            CAST(c.{child_fk} AS VARCHAR) AS failed_value
        FROM {_table_relation(table, table_paths)} c
        WHERE c.{_quote_identifier(cnote_col)} IS NOT NULL
          AND c.{child_fk} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {_table_relation(rule.parent_table, table_paths)} p
              WHERE p.{parent_pk} = c.{child_fk}
          )
    """)
    return True


def _insert_dcorrect_failures(con: Any, rule: RuleSpec, table_paths: dict[str, str]) -> bool:
    if not {"CMS_CNOTE", "CMS_DCORRECT_DEST"} <= set(table_paths) or rule.code not in {"ACCU2B12", "ACCU3B13"}:
        return False
    cnote_column = "CNOTE_ORIGIN" if rule.code == "ACCU2B12" else "CNOTE_DESTINATION"
    dcorrect_column = "DCORRECT_ORIGIN" if rule.code == "ACCU2B12" else "DCORRECT_DEST"
    if {"CNOTE_NO", cnote_column} - _columns(con, table_paths["CMS_CNOTE"]):
        return False
    if {"DCORRECT_CNOTE_NO", dcorrect_column} - _columns(con, table_paths["CMS_DCORRECT_DEST"]):
        return False
    cnote_clean = _clean_sql(f'c."{cnote_column}"')
    dcorrect_clean = _clean_sql(f'd."{dcorrect_column}"')
    con.execute(f"""
        INSERT INTO cnote_index_failures
        SELECT
            CAST(c."CNOTE_NO" AS VARCHAR) AS cnote_no,
            {_sql_literal(rule.code)} AS index_code,
            {_sql_literal(f"{cnote_column} does not match {dcorrect_column}")} AS failure_reason,
            {cnote_clean} || ' <> ' || {dcorrect_clean} AS failed_value
        FROM {_table_relation("CMS_CNOTE", table_paths)} c
        JOIN {_table_relation("CMS_DCORRECT_DEST", table_paths)} d
          ON c."CNOTE_NO" = d."DCORRECT_CNOTE_NO"
        WHERE c."CNOTE_NO" IS NOT NULL
          AND {cnote_clean} <> ''
          AND {dcorrect_clean} <> ''
          AND {cnote_clean} <> {dcorrect_clean}
    """)
    return True


def _insert_manifest_route_failures(con: Any, rule: RuleSpec, table_paths: dict[str, str]) -> bool:
    if rule.code != "CONS3H4" or not {"CMS_MANIFEST", "CMS_MFCNOTE", "CMS_CNOTE"} <= set(table_paths):
        return False
    if {"MANIFEST_NO", "MANIFEST_ROUTE"} - _columns(con, table_paths["CMS_MANIFEST"]):
        return False
    if {"MFCNOTE_MAN_NO", "MFCNOTE_NO"} - _columns(con, table_paths["CMS_MFCNOTE"]):
        return False
    if {"CNOTE_NO", "CNOTE_DESTINATION"} - _columns(con, table_paths["CMS_CNOTE"]):
        return False
    route_segment = 'SUBSTRING(TRIM(CAST(m."MANIFEST_ROUTE" AS VARCHAR)), 9, 3)'
    destination = _clean_sql('c."CNOTE_DESTINATION"')
    con.execute(f"""
        INSERT INTO cnote_index_failures
        SELECT
            CAST(c."CNOTE_NO" AS VARCHAR) AS cnote_no,
            {_sql_literal(rule.code)} AS index_code,
            'MANIFEST_ROUTE letters 9-11 do not match CNOTE_DESTINATION' AS failure_reason,
            {route_segment} || ' <> ' || {destination} AS failed_value
        FROM {_table_relation("CMS_MANIFEST", table_paths)} m
        JOIN {_table_relation("CMS_MFCNOTE", table_paths)} mf
          ON m."MANIFEST_NO" = mf."MFCNOTE_MAN_NO"
        JOIN {_table_relation("CMS_CNOTE", table_paths)} c
          ON mf."MFCNOTE_NO" = c."CNOTE_NO"
        WHERE c."CNOTE_NO" IS NOT NULL
          AND {route_segment} <> ''
          AND LENGTH(TRIM(CAST(m."MANIFEST_ROUTE" AS VARCHAR))) >= 11
          AND {destination} <> ''
          AND {route_segment} <> {destination}
    """)
    return True


def _create_mapped_failure_rules(con: Any, mapped_codes: set[str]) -> None:
    if mapped_codes:
        values = ", ".join(f"({_sql_literal(code)})" for code in sorted(mapped_codes))
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE cnote_failure_mapped_rules AS
            SELECT *
            FROM (VALUES {values}) AS v(index_code)
        """)
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE cnote_failure_mapped_rules(index_code VARCHAR)")


def _create_cnote_failures(con: Any, rules: list[RuleSpec], table_paths: dict[str, str]) -> set[str]:
    con.execute("""
        CREATE OR REPLACE TEMP TABLE cnote_index_failures (
            cnote_no VARCHAR,
            index_code VARCHAR,
            failure_reason VARCHAR,
            failed_value VARCHAR
        )
    """)
    mapped_codes = set()
    for rule in rules:
        handled = False
        if rule.rule_family == "COMP" or rule.element in {"VALD", "ACCU"}:
            handled = (
                _insert_dcorrect_failures(con, rule, table_paths)
                or _insert_generic_scalar_failures(con, rule, table_paths)
            )
        if not handled and rule.element == "UNIQ":
            handled = _insert_uniqueness_failures(con, rule, table_paths)
        if not handled and rule.rule_family == "INTG1":
            handled = _insert_intg_failures(con, rule, table_paths)
        if not handled and rule.element == "CONS":
            handled = (
                _insert_manifest_route_failures(con, rule, table_paths)
                or (rule.code in PAIR_CHECKS and _insert_pair_failures(con, rule, PAIR_CHECKS[rule.code], table_paths, "consistency"))
            )
        if not handled and rule.element == "TIME" and rule.code in TIMELINESS_CHECKS:
            handled = _insert_pair_failures(con, rule, TIMELINESS_CHECKS[rule.code], table_paths, "timeliness")
        if handled:
            mapped_codes.add(rule.code)
    _create_mapped_failure_rules(con, mapped_codes)
    print(f"Built cnote-level failure details for {len(mapped_codes)} rule(s)", flush=True)
    return mapped_codes


def write_cnote_index_status(
    con: Any,
    config: GovernanceConfig,
    table_paths: dict[str, str],
    results: list[RuleResult],
    output_path: Path,
) -> None:
    """Write full CNOTE/index status matrix to a local Parquet file."""
    rules = active_rules()
    run_id = config.bronze.run_prefix.rsplit("run_id=", 1)[-1] if "run_id=" in config.bronze.run_prefix else ""
    cnote_path = table_paths.get("CMS_CNOTE")
    if not cnote_path:
        raise RuntimeError("CMS_CNOTE table path is required for cnote_index_status output")

    _create_rule_metadata(con, rules, results)
    applicable_codes = _create_rule_related(con, rules, table_paths)
    _create_applicable_rules(con, applicable_codes)
    _create_cnote_failures(con, rules, table_paths)

    output_sql = f"""
        WITH cnotes AS (
            SELECT DISTINCT CAST("CNOTE_NO" AS VARCHAR) AS cnote_no
            FROM {_relation_sql(cnote_path)}
            WHERE "CNOTE_NO" IS NOT NULL
        ),
        failures AS (
            SELECT
                cnote_no,
                index_code,
                STRING_AGG(DISTINCT failure_reason, '; ') AS failure_reason,
                STRING_AGG(DISTINCT failed_value, '; ') AS failed_value
            FROM cnote_index_failures
            GROUP BY cnote_no, index_code
        )
        SELECT
            {_sql_literal(run_id)} AS run_id,
            c.cnote_no,
            r.index_code,
            r.element,
            r.rule_family,
            CASE
                WHEN r.scorecard_status = 'SKIPPED' THEN 'SKIPPED'
                WHEN f.index_code IS NOT NULL THEN 'FAIL'
                WHEN ar.index_code IS NULL THEN 'NOT_APPLICABLE'
                WHEN rel.index_code IS NULL THEN 'NO_RELATED_RECORD'
                WHEN r.scorecard_status = 'FAIL' AND mr.index_code IS NULL THEN 'NOT_APPLICABLE'
                ELSE 'PASS'
            END AS status,
            CASE
                WHEN r.scorecard_status = 'SKIPPED' THEN r.skipped_reason
                ELSE f.failure_reason
            END AS failure_reason,
            f.failed_value,
            r.source_table,
            r.source_column,
            r.compared_table,
            r.compared_column,
            r.run_at
        FROM cnotes c
        CROSS JOIN cnote_rule_metadata r
        LEFT JOIN cnote_applicable_rules ar
          ON r.index_code = ar.index_code
        LEFT JOIN cnote_failure_mapped_rules mr
          ON r.index_code = mr.index_code
        LEFT JOIN cnote_rule_related rel
          ON c.cnote_no = rel.cnote_no
         AND r.index_code = rel.index_code
        LEFT JOIN failures f
          ON c.cnote_no = f.cnote_no
         AND r.index_code = f.index_code
    """
    escaped = str(output_path).replace("'", "''")
    con.execute(f"COPY ({output_sql}) TO '{escaped}' (FORMAT PARQUET)")
