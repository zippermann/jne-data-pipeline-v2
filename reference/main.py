"""
JNE DQ — entry point
====================
Thin orchestrator: load → run checks → save.

Usage:
  python main.py --input unified_shipments.csv --output dq_output.csv
  python main.py --from-postgres --output dq_output.csv
  python main.py --from-postgres --row-limit 100000 --output dq_sample.csv
  python main.py --from-postgres --save-to-postgres --skip-csv-output
  python main.py --from-postgres --save-to-postgres --skip-csv-output \
    --skip-inline-output --compact-summary --resumable --resume --run-id dq_20260525
  python main.py --input unified_shipments.csv --threshold 90.0
"""
import argparse
import json
import re
import sys
from pathlib import Path
from uuid import uuid4

import pandas as pd
import config as cfg
import scorer as sc


class ConsoleProgress:
    """Render concise, dependency-free progress on one terminal line."""

    def __init__(self, label: str, total: int | None = None, width: int = 28):
        self.label = label
        self.total = total
        self.width = width
        self._last_length = 0

    def update(self, completed: int, detail: str = "") -> None:
        if self.total:
            ratio = min(max(completed / self.total, 0.0), 1.0)
            filled = int(self.width * ratio)
            bar = "#" * filled + "-" * (self.width - filled)
            status = f"[{bar}] {completed:,}/{self.total:,} {ratio * 100:5.1f}%"
        else:
            status = f"{completed:,} complete"
        message = f"\r{self.label}: {status}"
        if detail:
            message += f" | {detail}"
        message = message.ljust(self._last_length)
        self._last_length = len(message)
        print(message, end="", flush=True)

    def close(self, detail: str = "") -> None:
        if detail:
            message = f"\r{self.label}: {detail}".ljust(self._last_length)
            print(message, flush=True)
        else:
            print()

sys.path.append(str(Path(__file__).parent.parent.parent)) # allow importing pipeline_config from project root
try:
    from pipeline_config import DB_CONN, SCHEMA_TRANSFORMED
except ImportError:
    sys.path.insert(0, "/opt/airflow")
    try:
        from pipeline_config import DB_CONN, SCHEMA_TRANSFORMED
    except ImportError:
        DB_CONN = None
        SCHEMA_TRANSFORMED = "transformed"


def _get_sqlalchemy():
    """Import SQLAlchemy only for Postgres-backed workflows."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError as exc:
        raise RuntimeError(
            "SQLAlchemy is required for --from-postgres or --save-to-postgres. "
            "Install project dependencies, or run with --input for CSV mode."
        ) from exc
    return create_engine, text


def load_csv(path: str, row_limit: int | None = None) -> pd.DataFrame:
    """Load the unified shipments CSV."""
    print(f"Loading {path}...")
    return pd.read_csv(path, low_memory=False, nrows=row_limit)


def load_from_postgres(table: str, row_limit: int | None = None) -> pd.DataFrame:
    """Load the unified shipments table directly from PostgreSQL."""
    if DB_CONN is None:
        raise RuntimeError("DB_CONN is not configured; cannot read from PostgreSQL.")
    create_engine, text = _get_sqlalchemy()
    print(f"Loading PostgreSQL table {table}...")
    if row_limit:
        print(f"  Limiting governance input to {row_limit:,} rows...") #row limit configuration
    engine = create_engine(DB_CONN)
    try:
        with engine.connect() as conn:
            if row_limit:
                query = text(f"SELECT * FROM {table} LIMIT :row_limit")
                df = pd.read_sql_query(query, conn, params={"row_limit": row_limit})
            else:
                df = pd.read_sql_query(text(f"SELECT * FROM {table}"), conn)
    finally:
        engine.dispose()
    return df


def _split_table_name(table: str) -> tuple[str, str]: #s
    """Return (schema, table) from a schema-qualified table name."""
    parts = table.split(".", 1)
    if len(parts) == 1:
        schema, table_name = "public", parts[0]
    else:
        schema, table_name = parts[0], parts[1]

    ident = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    if not ident.fullmatch(schema) or not ident.fullmatch(table_name):
        raise ValueError(
            "PostgreSQL output table must be schema-qualified with simple "
            "identifiers, e.g. governance.dq_scores"
        )
    return schema, table_name


def _inline_table_name(table: str) -> str:
    """Return the companion inline table name for a score output table."""
    schema, table_name = _split_table_name(table)
    return f"{schema}.{table_name}_inline"


def _inline_csv_path(path: str) -> str:
    """Return the companion inline CSV path for a score output CSV."""
    p = Path(path)
    return str(p.with_name(f"{p.stem}_inline{p.suffix or '.csv'}"))


def _summary_csv_path(path: str) -> str:
    """Return the companion index-check summary CSV path."""
    p = Path(path)
    return str(p.with_name(f"{p.stem}_summary{p.suffix or '.csv'}"))


def _summary_table_name(table: str) -> str:
    """Return the companion index-check summary table name."""
    schema, table_name = _split_table_name(table)
    return f"{schema}.{table_name}_summary"


def _compact_summary_table_name(table: str) -> str:
    """Return the scalable companion summary table for production runs."""
    schema, table_name = _split_table_name(table)
    return f"{schema}.{table_name}_summary_compact"


def _integrity_csv_path(path: str) -> str:
    """Return the companion column-level integrity CSV path."""
    p = Path(path)
    return str(p.with_name(f"{p.stem}_integrity{p.suffix or '.csv'}"))


def _integrity_table_name(table: str) -> str:
    """Return the companion column-level integrity table name."""
    schema, table_name = _split_table_name(table)
    return f"{schema}.{table_name}_integrity"


def _integrity_batch_table_name(table: str) -> str:
    """Return internal per-batch Integrity stats used for resumable runs."""
    schema, table_name = _split_table_name(table)
    return f"{schema}.{table_name}_integrity_batches"


def _postgres_write_chunksize(column_count: int) -> int:
    """Keep multi-row INSERT parameter counts below PostgreSQL's limit."""
    return max(1, min(10000, 60000 // max(column_count, 1)))


def save_to_postgres(
    df_scores: pd.DataFrame,
    table: str,
    source_table: str,
    run_id: str,
    row_limit: int | None = None,
    if_exists: str = "replace",
) -> None:
    """
    Save row-level DQ scores to PostgreSQL for downstream marts.

    By default the table is replaced for each run so ClickHouse or dashboard
    jobs can read the latest score set from one stable location.  Use append
    mode when historical DQ runs need to be retained in Postgres.
    """
    if DB_CONN is None:
        raise RuntimeError("DB_CONN is not configured; cannot save to PostgreSQL.")
    create_engine, text = _get_sqlalchemy()
    schema, table_name = _split_table_name(table)
    out = df_scores.copy()
    out.insert(0, "dq_run_id", run_id)
    out.insert(1, "dq_run_ts", pd.Timestamp.now("UTC"))
    out.insert(2, "source_table", source_table)
    out.insert(3, "row_limit", row_limit)

    print(f"Saving DQ scores to PostgreSQL table {schema}.{table_name}...")
    engine = create_engine(DB_CONN)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.execute(text("SET max_parallel_workers_per_gather = 0"))
            out.to_sql(
                table_name,
                conn,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=_postgres_write_chunksize(len(out.columns)),
                method="multi",
            )
    finally:
        engine.dispose()
    print(f"Saved PostgreSQL table: {schema}.{table_name} ({len(out):,} rows)")


def add_business_flags(df: pd.DataFrame, df_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Add non-scoring business flags to the DQ output.

    DCORRECT destination records are correction/history signals: the corrected
    destination should already be reflected in CMS_CNOTE.  This flag is
    therefore informational and must not affect Accuracy or Overall scores.
    """
    out = df_scores.copy()

    if "dcorrect_destination" not in df.columns:
        return out

    dcorrect_dest = sc._clean(df["dcorrect_destination"])

    if "dcorrect_cnote_no" in df.columns:
        has_dcorrect = sc._clean(df["dcorrect_cnote_no"]).ne("")
    else:
        has_dcorrect = dcorrect_dest.ne("")

    out["has_dcorrect_destination_record"] = has_dcorrect.astype("int8")

    return out


def build_inline_checks(
    df: pd.DataFrame,
    per_element: dict,
) -> pd.DataFrame:
    """
    Build a human-readable audit table with each checked field followed by a
    {field}_check column containing PASS or FAIL: element names.
    """
    column_row_checks = {col: [None] * len(df) for col in df.columns}

    for element, rows in per_element.items():
        for idx, row_checks in enumerate(rows):
            if not row_checks:
                continue
            for field, result in row_checks.items():
                if field not in column_row_checks:
                    continue
                if column_row_checks[field][idx] is None:
                    column_row_checks[field][idx] = {}
                column_row_checks[field][idx][element] = result

    inline_results = {}
    for col_name, row_checks_list in column_row_checks.items():
        if not any(row_checks is not None for row_checks in row_checks_list):
            continue

        check_col = f"{col_name}_check"
        inline_results[check_col] = []
        for row_checks in row_checks_list:
            if not row_checks:
                inline_results[check_col].append(None)
                continue

            failed = [element for element, result in row_checks.items() if result == 0]
            inline_results[check_col].append(
                "PASS" if not failed else f"FAIL: {', '.join(sorted(failed))}"
            )

    df_checks = pd.DataFrame(inline_results)
    final_cols = []
    for col in df.columns:
        final_cols.append(col)
        check_col = f"{col}_check"
        if check_col in df_checks.columns:
            final_cols.append(check_col)

    combined = pd.concat(
        [df.reset_index(drop=True), df_checks.reset_index(drop=True)],
        axis=1,
    )
    return combined[[col for col in final_cols if col in combined.columns]]


def _rule_output_label(rule) -> str | None:
    """Return the scorer result key produced by a catalog rule."""
    if not rule.columns:
        return None

    if rule.element == "Completeness":
        if rule.rule_family == "COMP2" and "value rule" in rule.implementation:
            return rule.condition_label or rule.columns[-1]
        return rule.columns[-1]

    if rule.element == "Consistency":
        return rule.columns[0]

    if rule.element == "Validity":
        return rule.columns[0]

    if rule.element == "Timeliness":
        if rule.rule_family == "TIME1":
            return rule.assign_to or rule.columns[-1]
        return rule.columns[-1]

    if rule.element == "Uniqueness":
        return rule.columns[0]

    if rule.element == "Accuracy":
        return rule.columns[0]

    return rule.columns[0]


def _manifest_prefixes(df_columns) -> list[str]:
    """Return manifest prefixes present in the flat input."""
    prefixes = set()
    for pfx in ("om", "im", "hm"):
        if f"{pfx}_man_no" in df_columns:
            prefixes.add(pfx)

    tm_pat = re.compile(r"^(tm\d+)_man_no$")
    for col in df_columns:
        match = tm_pat.match(col)
        if match:
            prefixes.add(match.group(1))

    return sorted(prefixes)


def _prefixed_code(base_code: str, pfx: str) -> str:
    """Append a manifest prefix to an index code, e.g. CONS2H4-OM."""
    return f"{base_code}-{pfx.upper()}"


def _manifest_rule(
    index_code: str,
    element: str,
    rule_family: str,
    table: str,
    columns: tuple[str, ...],
    description: str,
    implementation: str,
    assign_to: str | None = None,
    condition_label: str | None = None,
):
    return cfg.Rule(
        index_code=index_code,
        element=element,
        rule_family=rule_family,
        table=table,
        columns=columns,
        description=description,
        implementation=implementation,
        assign_to=assign_to,
        condition_label=condition_label,
    )


def _dynamic_manifest_rules(df_columns) -> list:
    """
    Build index-code metadata for generated OM/TM/IM/HM manifest checks.

    The base Excel index still references H=CMS_MANIFEST and I=CMS_MFCNOTE.
    The suffix after '-' identifies the physical flat-table prefix.
    """
    rules = []
    manifest_fields = {
        "manifest_date": ("H3", "Completeness", "COMP1", "Validity", "VALD4"),
        "manifest_route": ("H4", "Completeness", "COMP1", "Validity", "VALD1"),
        "manifest_from": ("H5", "Completeness", "COMP1", "Validity", "VALD1"),
        "manifest_thru": ("H6", "Completeness", "COMP1", "Validity", "VALD1"),
        "manifest_approved": ("H8", "Completeness", "COMP1", "Validity", "VALD8"),
        "manifest_origin": ("H9", "Completeness", "COMP1", "Validity", "VALD1"),
        "manifest_code": ("H10", "Completeness", "COMP1", "Validity", "VALD5"),
        "manifest_uid": ("H11", "Completeness", "COMP1", None, None),
        "manifest_crdate": ("H12", "Completeness", "COMP1", "Validity", "VALD4"),
        "manifest_canceled": ("H17", "Completeness", "COMP1", None, None),
    }
    manifest_validity_only = {
        "man_no": ("H1", "VALD1"),
    }
    mfc_fields = {
        "man_no": ("I1", "Completeness", "COMP1", "Validity", "VALD1"),
        "bag_no": ("I2", "Completeness", "COMP1", "Validity", "VALD1"),
        "mfc_weight": ("I4", "Completeness", "COMP1", "Validity", "VALD2"),
        "mfc_crdate": ("I5", "Completeness", "COMP1", "Validity", "VALD4"),
    }

    manifest_prefixes = _manifest_prefixes(df_columns)
    transit_prefixes = sorted(
        (pfx for pfx in manifest_prefixes if pfx.startswith("tm")),
        key=lambda pfx: int(pfx[2:]),
    )

    for pfx in manifest_prefixes:
        pfx_label = pfx.upper()
        manifest_table = f"CMS_MANIFEST_{pfx_label}"
        mfc_table = f"CMS_MFCNOTE_{pfx_label}"

        for suffix, (field_code, *_meta) in manifest_fields.items():
            col = f"{pfx}_{suffix}"
            if col not in df_columns:
                continue

            rules.append(_manifest_rule(
                _prefixed_code(f"COMP1{field_code}", pfx),
                "Completeness",
                "COMP1",
                manifest_table,
                (col,),
                f"{col} must be present and non-empty.",
                "rules.check_completeness -> COMP1 mandatory field mask",
            ))

            validity_family = _meta[3] if len(_meta) > 3 else None
            if validity_family:
                rules.append(_manifest_rule(
                    _prefixed_code(f"{validity_family}{field_code}", pfx),
                    "Validity",
                    validity_family,
                    manifest_table,
                    (col,),
                    f"{col} must satisfy {validity_family}.",
                    "rules.check_validity -> generated manifest rule",
                ))

        for suffix, (field_code, validity_family) in manifest_validity_only.items():
            col = f"{pfx}_{suffix}"
            if col in df_columns:
                rules.append(_manifest_rule(
                    _prefixed_code(f"{validity_family}{field_code}", pfx),
                    "Validity",
                    validity_family,
                    manifest_table,
                    (col,),
                    f"{col} must satisfy {validity_family}.",
                    "rules.check_validity -> generated manifest rule",
                ))

        route_col = f"{pfx}_manifest_route"
        if route_col in df_columns and "cnote_destination" in df_columns:
            rules.append(_manifest_rule(
                _prefixed_code("CONS2H4", pfx),
                "Consistency",
                "CONS2",
                manifest_table,
                (route_col, "cnote_destination"),
                f"{route_col} must match cnote_destination.",
                "rules.check_consistency -> generated manifest pair",
            ))

        crdate_col = f"{pfx}_manifest_crdate"
        if pfx == "om" and transit_prefixes:
            first_tm_crdate = f"{transit_prefixes[0]}_manifest_crdate"
            if crdate_col in df_columns and first_tm_crdate in df_columns:
                rules.append(_manifest_rule(
                    "TIME1H15-OM",
                    "Timeliness",
                    "TIME1",
                    manifest_table,
                    (crdate_col, first_tm_crdate),
                    f"{crdate_col} must be earlier than or equal to {first_tm_crdate}.",
                    f"rules.check_timeliness -> assigned to {crdate_col}",
                    assign_to=crdate_col,
                ))
        elif pfx in transit_prefixes and crdate_col in df_columns:
            tm_pos = transit_prefixes.index(pfx)
            next_col = (
                f"{transit_prefixes[tm_pos + 1]}_manifest_crdate"
                if tm_pos + 1 < len(transit_prefixes)
                else "im_manifest_crdate"
            )
            if next_col in df_columns:
                rules.append(_manifest_rule(
                    "TIME1H15-TM",
                    "Timeliness",
                    "TIME1",
                    manifest_table,
                    (crdate_col, next_col),
                    f"{crdate_col} must be earlier than or equal to {next_col}.",
                    f"rules.check_timeliness -> assigned to {crdate_col}",
                    assign_to=crdate_col,
                ))

        canceled_col = f"{pfx}_manifest_canceled"
        canceled_uid_col = f"{pfx}_manifest_canceled_uid"
        if canceled_col in df_columns and canceled_uid_col in df_columns:
            rules.append(_manifest_rule(
                _prefixed_code("COMP2H17", pfx),
                "Completeness",
                "COMP2",
                manifest_table,
                (canceled_col, canceled_uid_col),
                f"If {canceled_col} = Y, {canceled_uid_col} must be present.",
                f"rules.check_completeness -> COMP2 value rule ({pfx}_canceled_requires_uid)",
                condition_label=f"{pfx}_canceled_requires_uid",
            ))

        for suffix, (field_code, *_meta) in mfc_fields.items():
            col = f"{pfx}_{suffix}"
            if col not in df_columns:
                continue

            rules.append(_manifest_rule(
                _prefixed_code(f"COMP1{field_code}", pfx),
                "Completeness",
                "COMP1",
                mfc_table,
                (col,),
                f"{col} must be present and non-empty.",
                "rules.check_completeness -> COMP1 mandatory field mask",
            ))

            validity_family = _meta[3] if len(_meta) > 3 else None
            if validity_family:
                rules.append(_manifest_rule(
                    _prefixed_code(f"{validity_family}{field_code}", pfx),
                    "Validity",
                    validity_family,
                    mfc_table,
                    (col,),
                    f"{col} must satisfy {validity_family}.",
                    "rules.check_validity -> generated MFCNOTE rule",
                ))

        mfc_weight_col = f"{pfx}_mfc_weight"
        if mfc_weight_col in df_columns and "cnote_weight" in df_columns:
            rules.append(_manifest_rule(
                _prefixed_code("CONS2I4", pfx),
                "Consistency",
                "CONS2",
                mfc_table,
                (mfc_weight_col, "cnote_weight"),
                f"{mfc_weight_col} must match cnote_weight.",
                "rules.check_consistency -> generated MFCNOTE pair",
            ))

        man_col = f"{pfx}_man_no"
        if man_col in df_columns:
            rules.append(_manifest_rule(
                _prefixed_code("UNIQ1I1", pfx),
                "Uniqueness",
                "UNIQ1",
                mfc_table,
                (man_col,),
                f"{man_col} must be unique among eligible rows.",
                "rules.check_uniqueness -> generated MFCNOTE key",
            ))

    return rules


def _active_rules(df_columns) -> list:
    """Return static catalog rules plus generated manifest rules."""
    return list(cfg.RULE_CATALOG) + _dynamic_manifest_rules(df_columns)


def _rules_by_result_key(df_columns) -> dict:
    """Map (element, scorer output label) to catalog rules with index codes."""
    mapped = {}
    seen = set()
    for rule in _active_rules(df_columns):
        if not rule.index_code or rule.implementation == "deferred":
            continue
        label = _rule_output_label(rule)
        if not label:
            continue
        signature = (rule.element, label, rule.index_code, rule.columns[0])
        if signature in seen:
            continue
        seen.add(signature)
        mapped.setdefault((rule.element, label), []).append(rule)
    return mapped


def _index_suffix(index_code: str) -> str | None:
    """Return the table/field suffix from an index code, e.g. COMP1A2 -> A2."""
    match = re.match(r"^[A-Z]+[0-9]+(.+)$", index_code)
    return match.group(1) if match else None


def _index_sort_key(index_code: str) -> tuple:
    """Sort index-like codes by rule number, table letter, then field number."""
    match = re.match(r"^[A-Z]+([0-9]+)([A-Z]+)([0-9].*)$", index_code)
    if not match:
        return (999, "ZZZ", 999999, index_code)

    rule_no, table_code, field_code = match.groups()
    field_no = re.match(r"([0-9]+)", field_code)
    return (
        int(rule_no),
        table_code,
        int(field_no.group(1)) if field_no else 999999,
        field_code,
    )


def _integrity_code(rule) -> str | None:
    """Return the derived Integrity code for a catalog rule."""
    if not rule.index_code:
        return None
    suffix = _index_suffix(rule.index_code)
    return f"INTG1{suffix}" if suffix else None


def _display_value(value) -> str:
    """Format a failed value for the summary output."""
    if pd.isna(value):
        return "NULL"
    text = str(value).strip()
    return text if text else "NULL"


def _failure_value(df: pd.DataFrame, row_idx: int, rule) -> str:
    """Return the value shown after FAIL: for one failed index check."""
    present_cols = [col for col in rule.columns if col in df.columns]
    if not present_cols:
        return "NULL"

    if len(present_cols) == 1:
        return _display_value(df[present_cols[0]].iloc[row_idx])

    parts = [
        f"{col}={_display_value(df[col].iloc[row_idx])}"
        for col in present_cols
    ]
    return "; ".join(parts)


def build_index_summary(
    df: pd.DataFrame,
    per_element: dict,
) -> pd.DataFrame:
    """
    Build one row per shipment with one column per index code.

    Each index-code cell is PASS, FAIL: value, or blank when that rule was not
    applicable to that row.
    """
    rule_map = _rules_by_result_key(df.columns)
    index_codes = []
    for rule in _active_rules(df.columns):
        if rule.index_code and rule.implementation != "deferred":
            index_codes.append(rule.index_code)

    index_codes = sorted(dict.fromkeys(index_codes), key=_index_sort_key)
    metric_cols = {
        f"{element.lower()}_pass_count": [0] * len(df)
        for element in cfg.DQ_ELEMENTS
    }
    metric_cols.update({
        f"{element.lower()}_fail_count": [0] * len(df)
        for element in cfg.DQ_ELEMENTS
    })
    summary = pd.DataFrame(
        {
            "cnote_no": df["cnote_no"].reset_index(drop=True),
            **metric_cols,
            **{index_code: [None] * len(df) for index_code in index_codes},
        }
    )

    for element, rows in per_element.items():
        for row_idx, row_checks in enumerate(rows):
            if not row_checks:
                continue
            for label, result in row_checks.items():
                metric_name = (
                    f"{element.lower()}_pass_count"
                    if bool(result)
                    else f"{element.lower()}_fail_count"
                )
                summary.at[row_idx, metric_name] += 1
                for rule in rule_map.get((element, label), []):
                    if bool(result):
                        summary.at[row_idx, rule.index_code] = "PASS"
                    else:
                        value = _failure_value(df, row_idx, rule)
                        summary.at[row_idx, rule.index_code] = f"FAIL: {value}"

    return summary


def build_compact_index_summary(
    df: pd.DataFrame,
    per_element: dict,
) -> pd.DataFrame:
    """
    Build scalable per-shipment audit detail.

    The wide summary repeats hundreds of PASS values for every shipment. For
    production runs, counters preserve coverage while failed checks alone are
    serialized as JSON for investigation.
    """
    rule_map = _rules_by_result_key(df.columns)
    metric_cols = {
        f"{element.lower()}_pass_count": [0] * len(df)
        for element in cfg.DQ_ELEMENTS
    }
    metric_cols.update({
        f"{element.lower()}_fail_count": [0] * len(df)
        for element in cfg.DQ_ELEMENTS
    })
    failed_checks = [{} for _ in range(len(df))]

    for element, rows in per_element.items():
        for row_idx, row_checks in enumerate(rows):
            if not row_checks:
                continue
            for label, result in row_checks.items():
                metric_name = (
                    f"{element.lower()}_pass_count"
                    if bool(result)
                    else f"{element.lower()}_fail_count"
                )
                metric_cols[metric_name][row_idx] += 1
                if bool(result):
                    continue
                for rule in rule_map.get((element, label), []):
                    failed_checks[row_idx][rule.index_code] = _failure_value(
                        df, row_idx, rule
                    )

    out = pd.DataFrame({
        "cnote_no": df["cnote_no"].reset_index(drop=True),
        **metric_cols,
    })
    pass_columns = [f"{element.lower()}_pass_count" for element in cfg.DQ_ELEMENTS]
    fail_columns = [f"{element.lower()}_fail_count" for element in cfg.DQ_ELEMENTS]
    out["applicable_check_count"] = out[pass_columns + fail_columns].sum(axis=1)
    out["failed_check_count"] = out[fail_columns].sum(axis=1)
    out["failed_checks"] = [
        json.dumps(checks, ensure_ascii=True, separators=(",", ":"))
        for checks in failed_checks
    ]
    return out


def build_integrity_summary(
    df: pd.DataFrame,
    per_element: dict,
) -> pd.DataFrame:
    """
    Build column-level Integrity analytics from the six scoring elements.

    Integrity is derived from existing pass/fail outcomes. It does not create
    new row-level rules and does not affect the overall score.
    """
    active_rules = [
        rule for rule in _active_rules(df.columns)
        if rule.index_code and rule.implementation != "deferred" and rule.columns
    ]
    rule_map = _rules_by_result_key(df.columns)
    stats = {}

    for rule in active_rules:
        column_name = rule.columns[0]
        integrity_code = _integrity_code(rule)
        if not integrity_code:
            continue

        entry = stats.setdefault(
            column_name,
            {
                "integrity_code": integrity_code,
                "column_name": column_name,
                "source_table": rule.table,
                "checked_elements": set(),
                "checked_index_codes": set(),
                "pass_count": 0,
                "fail_count": 0,
                "element_pass_counts": {
                    elem: 0 for elem in cfg.DQ_ELEMENTS
                },
                "element_fail_counts": {
                    elem: 0 for elem in cfg.DQ_ELEMENTS
                },
            },
        )
        entry["checked_elements"].add(rule.element)
        entry["checked_index_codes"].add(rule.index_code)

    for element, rows in per_element.items():
        for row_checks in rows:
            if not row_checks:
                continue
            for label, result in row_checks.items():
                for rule in rule_map.get((element, label), []):
                    if not rule.columns:
                        continue

                    column_name = rule.columns[0]
                    integrity_code = _integrity_code(rule)
                    if not integrity_code:
                        continue

                    entry = stats.setdefault(
                        column_name,
                        {
                            "integrity_code": integrity_code,
                            "column_name": column_name,
                            "source_table": rule.table,
                            "checked_elements": set(),
                            "checked_index_codes": set(),
                            "pass_count": 0,
                            "fail_count": 0,
                            "element_pass_counts": {
                                elem: 0 for elem in cfg.DQ_ELEMENTS
                            },
                            "element_fail_counts": {
                                elem: 0 for elem in cfg.DQ_ELEMENTS
                            },
                        },
                    )
                    entry["checked_elements"].add(rule.element)
                    entry["checked_index_codes"].add(rule.index_code)

                    if bool(result):
                        entry["pass_count"] += 1
                        entry["element_pass_counts"][rule.element] += 1
                    else:
                        entry["fail_count"] += 1
                        entry["element_fail_counts"][rule.element] += 1

    rows = []
    for entry in stats.values():
        total = entry["pass_count"] + entry["fail_count"]
        pass_rate = round(100.0 * entry["pass_count"] / total, 2) if total else None
        column = entry["column_name"]
        null_count = (
            int(sc._clean(df[column]).eq("").sum())
            if column in df.columns else None
        )
        rows.append({
            "integrity_code": entry["integrity_code"],
            "column_name": entry["column_name"],
            "source_table": entry["source_table"],
            "checked_elements": ", ".join(sorted(entry["checked_elements"])),
            "checked_index_codes": ", ".join(sorted(entry["checked_index_codes"])),
            "null_count": null_count,
            "pass_count": entry["pass_count"],
            "fail_count": entry["fail_count"],
            "total_checks": total,
            "pass_rate": pass_rate,
            "accuracy_pass_count": entry["element_pass_counts"]["Accuracy"],
            "accuracy_fail_count": entry["element_fail_counts"]["Accuracy"],
            "completeness_pass_count": entry["element_pass_counts"]["Completeness"],
            "completeness_fail_count": entry["element_fail_counts"]["Completeness"],
            "consistency_pass_count": entry["element_pass_counts"]["Consistency"],
            "consistency_fail_count": entry["element_fail_counts"]["Consistency"],
            "timeliness_pass_count": entry["element_pass_counts"]["Timeliness"],
            "timeliness_fail_count": entry["element_fail_counts"]["Timeliness"],
            "validity_pass_count": entry["element_pass_counts"]["Validity"],
            "validity_fail_count": entry["element_fail_counts"]["Validity"],
            "uniqueness_pass_count": entry["element_pass_counts"]["Uniqueness"],
            "uniqueness_fail_count": entry["element_fail_counts"]["Uniqueness"],
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["_sort_key"] = out["integrity_code"].map(_index_sort_key)
    return (
        out.sort_values("_sort_key", ignore_index=True)
        .drop(columns="_sort_key")
    )


def run_dq(
    df: pd.DataFrame,
    threshold: float = 85.0,
    include_inline: bool = True,
    compact_summary: bool = False,
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Runs all six DQ elements across every table in the config and returns
    scored, inline, index-summary, and Integrity output DataFrames.
    """
    n = len(df)
    cols = list(df.columns)

    # Dynamic manifest config (OM/IM/HM/TM1..TMN detected from columns)
    if verbose:
        print("Detecting manifest transit columns...")
    manifest_meta = cfg.generate_manifest_config(cols)
    value_cond_rules = cfg.generate_value_conditionals(cols)

    # Build the full table registry: static tables + dynamic manifest tables
    all_tables = {**{t: {} for t in cfg.PRIMARY_KEYS}, **manifest_meta}

    # Per-element accumulators — one dict per row, merged across tables
    acc: dict = {e: [None] * n for e in cfg.DQ_ELEMENTS}

    def _merge_into(elem: str, row_list):
        for i, v in enumerate(row_list):
            acc[elem][i] = sc._merge(acc[elem][i], v)

    if verbose:
        print(f"Running DQ on {len(all_tables)} tables...")
    for table_name in all_tables:
        is_dynamic = table_name in manifest_meta
        if is_dynamic:
            meta = manifest_meta[table_name]
            pk     = meta.get("primary_key", [])
            mand   = meta.get("mandatory", [])
            cpairs = meta.get("consistency_pairs", [])
            regex  = meta.get("regex_rules", {})
            dtimes = meta.get("datetime_rules", [])
            trules = meta.get("timeliness_rules", [])
            ukeys  = meta.get("uniqueness_keys", [])
        else:
            if table_name not in cfg.PRIMARY_KEYS:
                continue
            pk     = cfg.PRIMARY_KEYS.get(table_name, [])
            mand   = cfg.COMPLETENESS_FIELDS.get(table_name, [])
            cpairs = cfg.CONSISTENCY_PAIRS.get(table_name, [])
            regex  = cfg.VALIDITY_REGEX.get(table_name, {})
            dtimes = cfg.VALIDITY_DATETIMES.get(table_name, [])
            trules = cfg.TIMELINESS_RULES.get(table_name, [])
            ukeys  = cfg.UNIQUENESS_KEYS.get(table_name, [])

        if verbose:
            print(f"  Auditing {table_name}...")

        _merge_into("Completeness", sc.check_completeness(
            df, mand, pk, [], [],  # conditional rules handled globally below
        ))
        _merge_into("Consistency",  sc.check_consistency(df, cpairs, pk))
        _merge_into("Validity",     sc.check_validity(df, regex, dtimes, pk))
        _merge_into("Timeliness",   sc.check_timeliness(df, trules, pk))
        _merge_into("Uniqueness",   sc.check_uniqueness(df, ukeys, pk))

    # Cross-table completeness rules (conditional + value-conditional)
    # Use cnote_no as the eligibility key — every shipment row has one.
    global_pk = ["cnote_no"]
    if verbose:
        print("Running conditional completeness (PP-04)...")
    _merge_into("Completeness", sc.check_completeness(
        df, [], global_pk,
        cfg.CONDITIONAL_COMPLETENESS,
        value_cond_rules,
    ))

    # Accuracy
    if verbose:
        print("Running accuracy checks...")
    _merge_into("Accuracy", sc.check_accuracy(df))

    if verbose:
        print("Computing scores...")
    df_scores = sc.compute_scores(n, acc)
    df_scores.insert(0, "cnote_no", df["cnote_no"])
    df_scores["decision"] = (
        df_scores["overall_score"].ge(threshold).map({True: "PASS", False: "FAIL"})
    )
    df_scores = add_business_flags(df, df_scores)
    df_inline = build_inline_checks(df, acc) if include_inline else pd.DataFrame()
    df_summary = (
        build_compact_index_summary(df, acc)
        if compact_summary else build_index_summary(df, acc)
    )
    df_integrity = build_integrity_summary(df, acc)
    return df_scores, df_inline, df_summary, df_integrity


def _sql_ident(name: str) -> str:
    """Quote a known PostgreSQL identifier after validating it."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid PostgreSQL identifier: {name}")
    return f'"{name}"'


def _source_table_sql(table: str) -> str:
    schema, table_name = _split_table_name(table)
    return f"{_sql_ident(schema)}.{_sql_ident(table_name)}"


def _postgres_source_columns(table: str) -> list[str]:
    """Read only the PostgreSQL input schema, without loading data rows."""
    if DB_CONN is None:
        raise RuntimeError("DB_CONN is not configured; cannot read from PostgreSQL.")
    create_engine, text = _get_sqlalchemy()
    engine = create_engine(DB_CONN)
    try:
        with engine.connect() as conn:
            return list(pd.read_sql_query(
                text(f"SELECT * FROM {_source_table_sql(table)} LIMIT 0"), conn
            ).columns)
    finally:
        engine.dispose()


def _postgres_uniqueness_specs(columns: list[str]) -> list[tuple[list[str], list[str]]]:
    """Return uniqueness key/eligibility combinations evaluated by the scorer."""
    manifest_meta = cfg.generate_manifest_config(columns)
    specs = []
    seen = set()
    for table_name in {**{t: {} for t in cfg.PRIMARY_KEYS}, **manifest_meta}:
        if table_name in manifest_meta:
            pk_cols = manifest_meta[table_name].get("primary_key", [])
            key_groups = manifest_meta[table_name].get("uniqueness_keys", [])
        else:
            pk_cols = cfg.PRIMARY_KEYS.get(table_name, [])
            key_groups = cfg.UNIQUENESS_KEYS.get(table_name, [])
        present_pk = [c for c in pk_cols if c in columns]
        if not present_pk:
            continue
        for key_group in key_groups:
            present_keys = [c for c in key_group if c in columns]
            if not present_keys:
                continue
            signature = (tuple(present_keys), tuple(present_pk))
            if signature not in seen:
                specs.append((present_keys, present_pk))
                seen.add(signature)
    return specs


def _clean_sql(alias: str, column: str) -> str:
    """Return the PostgreSQL equivalent of scorer._clean() for key matching."""
    return (
        f"UPPER(BTRIM(REGEXP_REPLACE(COALESCE({alias}.{_sql_ident(column)}::text, ''), "
        r"'\.0$', '')))"
    )


def _prepare_postgres_scoring_query(
    conn,
    text,
    table: str,
    columns: list[str],
    row_limit: int | None,
    batch_size: int | None = None,
) -> str:
    """
    Create narrow helper tables and return the streamed scoring query.

    Helper calculations run individually instead of as dozens of window
    functions over the wide shipment table. This bounds PostgreSQL temp usage.
    """
    # Parallel workers each consume temp_file_limit independently; disabling them
    # keeps all sort/hash work in a single backend and avoids the 4 GB cap.
    conn.execute(text("SET max_parallel_workers_per_gather = 0"))
    source = _source_table_sql(table)
    if row_limit:
        conn.execute(text(
            f"CREATE TEMP TABLE dq_source_rows ON COMMIT PRESERVE ROWS AS "
            f"SELECT * FROM {source} "
            f"ORDER BY {_sql_ident('cnote_no')} NULLS FIRST, ctid "
            f"LIMIT {int(row_limit)}"
        ))
        source = "dq_source_rows"

    joins = []
    extra_cols = []
    where_sql = ""

    if batch_size:
        print("  Preparing deterministic batch map...")
        conn.execute(text(
            "CREATE TEMP TABLE dq_batch_rows ON COMMIT PRESERVE ROWS AS "
            "SELECT source_ctid, "
            f"(((ROW_NUMBER() OVER (ORDER BY cnote_no NULLS FIRST, source_ctid)) - 1) "
            f"/ {int(batch_size)} + 1)::INTEGER AS batch_no "
            "FROM ("
            f"SELECT ctid AS source_ctid, {_sql_ident('cnote_no')} AS cnote_no "
            f"FROM {source}"
            ") ordered_rows"
        ))
        conn.execute(text("CREATE INDEX ON dq_batch_rows (batch_no)"))
        conn.execute(text("CREATE INDEX ON dq_batch_rows (source_ctid)"))
        joins.append("JOIN dq_batch_rows ON s.ctid = dq_batch_rows.source_ctid")
        where_sql = "WHERE dq_batch_rows.batch_no = :dq_batch_no\n"

    def has(*names):
        return all(name in columns for name in names)

    def numeric_expr(alias: str, col: str) -> str:
        value = f"NULLIF(BTRIM({alias}.{_sql_ident(col)}::text), '')"
        return (
            f"CASE WHEN {value} ~ '^-?[0-9]+([.][0-9]+)?$' "
            f"THEN {value}::numeric END"
        )

    aggregate_specs = []
    if "mfbag_calculated_weight" not in columns and has("mfbag_no", "mfcnote_weight"):
        aggregate_specs.append((
            "dq_mfbag_weight", "mfbag_no", "mfbag_calculated_weight",
            f"SUM({numeric_expr('a', 'mfcnote_weight')})",
        ))
    if "mmbag_calculated_qty" not in columns and has("mmbag_no", "cnote_no"):
        aggregate_specs.append((
            "dq_mmbag_qty", "mmbag_no", "mmbag_calculated_qty",
            f"COUNT(a.{_sql_ident('cnote_no')})",
        ))
    if "msmu_calculated_weight" not in columns and has("msmu_no", "dsmu_weight"):
        aggregate_specs.append((
            "dq_msmu_weight", "msmu_no", "msmu_calculated_weight",
            f"SUM({numeric_expr('a', 'dsmu_weight')})",
        ))
    if "msmu_calculated_qty" not in columns and has("msmu_no", "dsmu_bag_no"):
        aggregate_specs.append((
            "dq_msmu_qty", "msmu_no", "msmu_calculated_qty",
            f"COUNT(DISTINCT a.{_sql_ident('dsmu_bag_no')})",
        ))

    uniqueness_specs = _postgres_uniqueness_specs(columns)
    helper_total = len(aggregate_specs) + len(uniqueness_specs)
    helper_progress = ConsoleProgress("Preparing global helpers", helper_total)
    helper_done = 0

    for helper_table, key_col, output_col, calculation in aggregate_specs:
        helper_progress.update(helper_done, f"aggregate {output_col}")
        conn.execute(text(
            f"CREATE TEMP TABLE {helper_table} ON COMMIT PRESERVE ROWS AS "
            f"SELECT a.{_sql_ident(key_col)} AS key_value, {calculation} AS helper_value "
            f"FROM {source} a GROUP BY a.{_sql_ident(key_col)}"
        ))
        conn.execute(text(f"CREATE INDEX ON {helper_table} (key_value)"))
        joins.append(
            f"LEFT JOIN {helper_table} ON "
            f"s.{_sql_ident(key_col)} IS NOT DISTINCT FROM {helper_table}.key_value"
        )
        extra_cols.append(f'{helper_table}.helper_value AS "{output_col}"')
        helper_done += 1
        helper_progress.update(helper_done, f"aggregate {output_col}")

    for idx, (key_cols, pk_cols) in enumerate(uniqueness_specs):
        helper_table = f"dq_uniq_{idx}"
        helper_col = sc._uniqueness_count_column(key_cols, pk_cols)
        key_select = ", ".join(
            f"{_clean_sql('u', col)} AS k{pos}" for pos, col in enumerate(key_cols)
        )
        key_group = ", ".join(_clean_sql("u", col) for col in key_cols)
        eligible = " AND ".join(f"{_clean_sql('u', col)} <> ''" for col in pk_cols)
        helper_progress.update(
            helper_done, f"uniqueness {idx + 1:,}/{len(uniqueness_specs):,}"
        )
        conn.execute(text(
            f"CREATE TEMP TABLE {helper_table} ON COMMIT PRESERVE ROWS AS "
            f"SELECT {key_select}, 1 AS duplicate_key "
            f"FROM {source} u WHERE {eligible} "
            f"GROUP BY {key_group} HAVING COUNT(*) > 1"
        ))
        index_cols = ", ".join(f"k{pos}" for pos in range(len(key_cols)))
        conn.execute(text(f"CREATE INDEX ON {helper_table} ({index_cols})"))
        match = " AND ".join(
            f"{_clean_sql('s', col)} = {helper_table}.k{pos}"
            for pos, col in enumerate(key_cols)
        )
        joins.append(f"LEFT JOIN {helper_table} ON {match}")
        extra_cols.append(
            f'CASE WHEN {helper_table}.duplicate_key IS NULL THEN 1 ELSE 2 END '
            f'AS "{helper_col}"'
        )
        helper_done += 1
        helper_progress.update(
            helper_done, f"uniqueness {idx + 1:,}/{len(uniqueness_specs):,}"
        )

    if helper_total:
        helper_progress.close(f"complete ({helper_total:,} helpers)")

    select_extra = f",\n    {', '.join(extra_cols)}" if extra_cols else ""
    joins_sql = "\n".join(joins)
    return (
        f"SELECT s.*{select_extra}\nFROM {source} s\n{joins_sql}\n"
        f"{where_sql}"
        f"ORDER BY s.{_sql_ident('cnote_no')} NULLS FIRST, s.ctid"
    )


def _combine_integrity_batches(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine small per-batch integrity reports into one full-run report."""
    if not frames:
        return pd.DataFrame()
    all_rows = pd.concat(frames, ignore_index=True)
    id_cols = [
        "integrity_code", "column_name", "source_table",
        "checked_elements", "checked_index_codes",
    ]
    count_cols = [
        col for col in all_rows.columns
        if col in {"null_count", "pass_count", "fail_count"} or
        col.endswith("_pass_count") or col.endswith("_fail_count")
    ]
    out = all_rows.groupby(id_cols, dropna=False, as_index=False)[count_cols].sum()
    out["total_checks"] = out["pass_count"] + out["fail_count"]
    out["pass_rate"] = (
        (100.0 * out["pass_count"] / out["total_checks"])
        .where(out["total_checks"].ne(0))
        .round(2)
    )
    ordered = [
        *id_cols, "null_count", "pass_count", "fail_count",
        "total_checks", "pass_rate",
        *[col for col in count_cols if col not in {"null_count", "pass_count", "fail_count"}],
    ]
    out["_sort_key"] = out["integrity_code"].map(_index_sort_key)
    return out.sort_values("_sort_key").drop(columns="_sort_key")[ordered].reset_index(drop=True)


def _write_csv_batch(df: pd.DataFrame, path: str, first: bool) -> None:
    df.to_csv(path, mode="w" if first else "a", header=first, index=False)


def _with_run_metadata(
    df: pd.DataFrame,
    source_table: str,
    run_id: str,
    row_limit: int | None,
    batch_no: int | None,
) -> pd.DataFrame:
    """Add stable execution metadata to a frame written for a DQ run."""
    out = df.copy()
    out.insert(0, "dq_run_id", run_id)
    out.insert(1, "dq_batch_no", batch_no)
    out.insert(2, "dq_run_ts", pd.Timestamp.now("UTC"))
    out.insert(3, "source_table", source_table)
    out.insert(4, "row_limit", row_limit)
    return out


def _checkpoint_tables(output_table: str) -> tuple[str, str]:
    schema, _table_name = _split_table_name(output_table)
    quoted_schema = _sql_ident(schema)
    return f"{quoted_schema}.dq_runs", f"{quoted_schema}.dq_batches"


def _ensure_checkpoint_tables(conn, text, output_table: str) -> None:
    """Create lightweight control tables for resumable Postgres DQ runs."""
    schema, _table_name = _split_table_name(output_table)
    quoted_schema = _sql_ident(schema)
    runs_table, batches_table = _checkpoint_tables(output_table)
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}"))
    conn.execute(text(
        f"CREATE TABLE IF NOT EXISTS {runs_table} ("
        "dq_run_id TEXT PRIMARY KEY, "
        "source_table TEXT NOT NULL, "
        "output_table TEXT NOT NULL, "
        "status TEXT NOT NULL, "
        "batch_size INTEGER NOT NULL, "
        "row_limit BIGINT, "
        "threshold DOUBLE PRECISION NOT NULL, "
        "compact_summary BOOLEAN NOT NULL, "
        "inline_output BOOLEAN NOT NULL, "
        "started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
        "completed_at TIMESTAMPTZ, "
        "error_details TEXT)"
    ))
    conn.execute(text(
        f"CREATE TABLE IF NOT EXISTS {batches_table} ("
        "dq_run_id TEXT NOT NULL, "
        "batch_no INTEGER NOT NULL, "
        "status TEXT NOT NULL, "
        "row_count INTEGER, "
        "passed_count INTEGER, "
        "first_cnote_no TEXT, "
        "last_cnote_no TEXT, "
        "started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
        "completed_at TIMESTAMPTZ, "
        "error_details TEXT, "
        "PRIMARY KEY (dq_run_id, batch_no))"
    ))
    conn.execute(text(
        f"CREATE INDEX IF NOT EXISTS dq_batches_status_idx "
        f"ON {batches_table} (dq_run_id, status)"
    ))
    existing_output_tables = [
        output_table,
        _inline_table_name(output_table),
        _compact_summary_table_name(output_table),
        _integrity_table_name(output_table),
        _integrity_batch_table_name(output_table),
    ]
    for table in existing_output_tables:
        table_schema, table_name = _split_table_name(table)
        exists = conn.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{table_schema}.{table_name}"},
        ).scalar()
        if exists:
            conn.execute(text(
                f"ALTER TABLE {_source_table_sql(table)} "
                "ADD COLUMN IF NOT EXISTS dq_batch_no INTEGER"
            ))


def _start_or_resume_run(conn, text, args, run_id: str) -> set[int]:
    """Validate a checkpointed run and return batches already completed."""
    runs_table, batches_table = _checkpoint_tables(args.postgres_output_table)
    existing = conn.execute(
        text(f"SELECT * FROM {runs_table} WHERE dq_run_id = :run_id"),
        {"run_id": run_id},
    ).mappings().first()
    expected = {
        "source_table": args.postgres_table,
        "output_table": args.postgres_output_table,
        "batch_size": args.batch_size,
        "row_limit": args.row_limit,
        "threshold": args.threshold,
        "compact_summary": args.compact_summary,
        "inline_output": not args.skip_inline_output,
    }
    if existing:
        if not args.resume:
            raise RuntimeError(
                f"DQ run {run_id} already exists. Pass --resume to continue it."
            )
        mismatched = [
            key for key, value in expected.items()
            if existing[key] != value
        ]
        if mismatched:
            raise RuntimeError(
                f"Cannot resume DQ run {run_id}; changed options: "
                + ", ".join(mismatched)
            )
        conn.execute(text(
            f"UPDATE {runs_table} SET status = 'RUNNING', error_details = NULL "
            "WHERE dq_run_id = :run_id AND status <> 'COMPLETED'"
        ), {"run_id": run_id})
    else:
        conn.execute(text(
            f"INSERT INTO {runs_table} ("
            "dq_run_id, source_table, output_table, status, batch_size, "
            "row_limit, threshold, compact_summary, inline_output"
            ") VALUES ("
            ":run_id, :source_table, :output_table, 'RUNNING', :batch_size, "
            ":row_limit, :threshold, :compact_summary, :inline_output)"
        ), {
            "run_id": run_id,
            "source_table": args.postgres_table,
            "output_table": args.postgres_output_table,
            "batch_size": args.batch_size,
            "row_limit": args.row_limit,
            "threshold": args.threshold,
            "compact_summary": args.compact_summary,
            "inline_output": not args.skip_inline_output,
        })

    completed = conn.execute(text(
        f"SELECT batch_no FROM {batches_table} "
        "WHERE dq_run_id = :run_id AND status = 'COMPLETED'"
    ), {"run_id": run_id}).scalars()
    return set(completed)


def _delete_batch_rows(conn, text, table: str, run_id: str, batch_no: int) -> None:
    """Delete an interrupted batch before re-inserting it transactionally."""
    schema, table_name = _split_table_name(table)
    exists = conn.execute(
        text("SELECT to_regclass(:name)"),
        {"name": f"{schema}.{table_name}"},
    ).scalar()
    if exists:
        conn.execute(text(
            f"DELETE FROM {_source_table_sql(table)} "
            "WHERE dq_run_id = :run_id AND dq_batch_no = :batch_no"
        ), {"run_id": run_id, "batch_no": batch_no})


def _append_batch_frame(conn, frame: pd.DataFrame, table: str) -> None:
    schema, table_name = _split_table_name(table)
    frame.to_sql(
        table_name,
        conn,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=_postgres_write_chunksize(len(frame.columns)),
        method="multi",
    )


def _save_checkpointed_batch(
    engine,
    text,
    args,
    run_id: str,
    batch_no: int,
    df_batch: pd.DataFrame,
    df_out: pd.DataFrame,
    df_inline: pd.DataFrame,
    df_summary: pd.DataFrame,
    df_integrity: pd.DataFrame,
) -> None:
    """Write one batch's outputs and completion record in one transaction."""
    outputs = [
        (df_out, args.postgres_output_table),
        (df_summary, _compact_summary_table_name(args.postgres_output_table)),
        (df_integrity, _integrity_batch_table_name(args.postgres_output_table)),
    ]
    if not args.skip_inline_output:
        outputs.append((df_inline, _inline_table_name(args.postgres_output_table)))

    first_cnote = _display_value(df_batch["cnote_no"].iloc[0]) if len(df_batch) else None
    last_cnote = _display_value(df_batch["cnote_no"].iloc[-1]) if len(df_batch) else None
    _runs_table, batches_table = _checkpoint_tables(args.postgres_output_table)
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {batches_table} (dq_run_id, batch_no, status) "
            "VALUES (:run_id, :batch_no, 'RUNNING') "
            "ON CONFLICT (dq_run_id, batch_no) DO UPDATE SET "
            "status = 'RUNNING', started_at = NOW(), completed_at = NULL, error_details = NULL"
        ), {"run_id": run_id, "batch_no": batch_no})
        for frame, table in outputs:
            _delete_batch_rows(conn, text, table, run_id, batch_no)
            enriched = _with_run_metadata(
                frame, args.postgres_table, run_id, args.row_limit, batch_no
            )
            _append_batch_frame(conn, enriched, table)
        conn.execute(text(
            f"UPDATE {batches_table} SET status = 'COMPLETED', "
            "row_count = :row_count, passed_count = :passed_count, "
            "first_cnote_no = :first_cnote, last_cnote_no = :last_cnote, "
            "completed_at = NOW() "
            "WHERE dq_run_id = :run_id AND batch_no = :batch_no"
        ), {
            "row_count": len(df_out),
            "passed_count": int(df_out["decision"].eq("PASS").sum()),
            "first_cnote": first_cnote,
            "last_cnote": last_cnote,
            "run_id": run_id,
            "batch_no": batch_no,
        })


def _mark_checkpoint_run_failed(engine, text, output_table: str, run_id: str, exc: Exception) -> None:
    runs_table, _batches_table = _checkpoint_tables(output_table)
    with engine.begin() as conn:
        conn.execute(text(
            f"UPDATE {runs_table} SET status = 'FAILED', error_details = :error "
            "WHERE dq_run_id = :run_id"
        ), {"run_id": run_id, "error": str(exc)[:4000]})


def _finish_checkpointed_run(engine, text, args, run_id: str) -> pd.DataFrame:
    """Combine persisted Integrity batches and mark the resumed run complete."""
    batch_integrity_table = _integrity_batch_table_name(args.postgres_output_table)
    final_integrity_table = _integrity_table_name(args.postgres_output_table)
    runs_table, _batches_table = _checkpoint_tables(args.postgres_output_table)
    with engine.connect() as conn:
        integrity_frames = [pd.read_sql_query(
            text(
                f"SELECT * FROM {_source_table_sql(batch_integrity_table)} "
                "WHERE dq_run_id = :run_id"
            ),
            conn,
            params={"run_id": run_id},
        )]
    df_integrity = _combine_integrity_batches(integrity_frames)
    with engine.begin() as conn:
        schema, table_name = _split_table_name(final_integrity_table)
        exists = conn.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table_name}"},
        ).scalar()
        if exists:
            conn.execute(text(
                f"DELETE FROM {_source_table_sql(final_integrity_table)} "
                "WHERE dq_run_id = :run_id"
            ), {"run_id": run_id})
        _append_batch_frame(
            conn,
            _with_run_metadata(
                df_integrity, args.postgres_table, run_id, args.row_limit, None
            ),
            final_integrity_table,
        )
        if args.postgres_if_exists == "replace":
            latest_only_tables = [
                args.postgres_output_table,
                _compact_summary_table_name(args.postgres_output_table),
                final_integrity_table,
                _integrity_batch_table_name(args.postgres_output_table),
            ]
            if not args.skip_inline_output:
                latest_only_tables.append(_inline_table_name(args.postgres_output_table))
            for table in latest_only_tables:
                conn.execute(text(
                    f"DELETE FROM {_source_table_sql(table)} "
                    "WHERE dq_run_id <> :run_id"
                ), {"run_id": run_id})
        conn.execute(text(
            f"UPDATE {runs_table} SET status = 'COMPLETED', completed_at = NOW(), "
            "error_details = NULL WHERE dq_run_id = :run_id"
        ), {"run_id": run_id})
    return df_integrity


def _print_checkpointed_stats(engine, text, args, run_id: str) -> None:
    """Print full-run metrics from persisted score rows, including resumed batches."""
    score_table = _source_table_sql(args.postgres_output_table)
    projections = [
        "COUNT(*) AS total_rows",
        "SUM(CASE WHEN decision = 'PASS' THEN 1 ELSE 0 END) AS passed",
        "AVG(overall_score) AS overall_avg",
    ]
    for element in cfg.DQ_ELEMENTS:
        col = _sql_ident(f"{element.lower()}_score")
        label = element.lower()
        projections.extend([
            f"AVG({col}) AS {label}_avg",
            f"MIN({col}) AS {label}_min",
            f"MAX({col}) AS {label}_max",
        ])
    with engine.connect() as conn:
        stats = conn.execute(text(
            f"SELECT {', '.join(projections)} FROM {score_table} "
            "WHERE dq_run_id = :run_id"
        ), {"run_id": run_id}).mappings().one()

    print(f"\n{'Element':<15} {'Avg':>7} {'Min':>7} {'Max':>7}")
    print("-" * 40)
    for element in cfg.DQ_ELEMENTS:
        label = element.lower()
        if stats[f"{label}_avg"] is None:
            print(f"{element:<15} {'N/A':>7}")
        else:
            print(
                f"{element:<15} {float(stats[f'{label}_avg']):>6.1f}% "
                f"{float(stats[f'{label}_min']):>6.1f}% "
                f"{float(stats[f'{label}_max']):>6.1f}%"
            )
    total_rows = int(stats["total_rows"] or 0)
    passed = int(stats["passed"] or 0)
    if stats["overall_avg"] is not None:
        print(f"\n{'OVERALL':<15} {float(stats['overall_avg']):>6.1f}%")
    print(
        f"Pass rate: {passed}/{total_rows} "
        f"({100.0 * passed / total_rows if total_rows else 0:.1f}%)"
    )


def run_postgres_in_batches(args, run_id: str) -> None:
    """Stream Postgres input, score bounded batches, and write outputs incrementally."""
    if DB_CONN is None:
        raise RuntimeError("DB_CONN is not configured; cannot read from PostgreSQL.")
    if args.resumable and (
        not args.save_to_postgres
        or not args.skip_csv_output
        or not args.skip_inline_output
        or not args.compact_summary
    ):
        raise RuntimeError(
            "--resumable requires --save-to-postgres --skip-csv-output "
            "--skip-inline-output --compact-summary"
        )
    create_engine, text = _get_sqlalchemy()
    columns = _postgres_source_columns(args.postgres_table)
    print(f"Loading PostgreSQL table {args.postgres_table} in batches of {args.batch_size:,} rows...")
    if args.row_limit:
        print(f"  Limiting governance input to {args.row_limit:,} rows...")

    paths = [
        args.output, _inline_csv_path(args.output),
        _summary_csv_path(args.output), _integrity_csv_path(args.output),
    ]
    integrity_frames = []
    score_stats = {
        element: {"sum": 0.0, "count": 0, "min": None, "max": None}
        for element in cfg.DQ_ELEMENTS
    }
    overall_sum = overall_count = passed = total_rows = 0
    engine = create_engine(DB_CONN)
    first = True
    completed_batches = set()
    try:
        if args.resumable:
            with engine.begin() as control_conn:
                _ensure_checkpoint_tables(control_conn, text, args.postgres_output_table)
                completed_batches = _start_or_resume_run(control_conn, text, args, run_id)
            if completed_batches:
                print(f"  Resuming run {run_id}: {len(completed_batches):,} batches already complete.")
        with engine.connect() as conn:
            query = _prepare_postgres_scoring_query(
                conn,
                text,
                args.postgres_table,
                columns,
                args.row_limit,
                args.batch_size if args.resumable else None,
            )
            if args.resumable:
                batch_numbers = conn.execute(text(
                    "SELECT DISTINCT batch_no FROM dq_batch_rows ORDER BY batch_no"
                )).scalars()
                pending_batches = [
                    batch_no for batch_no in batch_numbers
                    if batch_no not in completed_batches
                ]
                if completed_batches:
                    print("  Completed batch rows will not be reloaded for scoring.")
                scoring_progress = ConsoleProgress(
                    "Scoring batches",
                    len(pending_batches) + len(completed_batches),
                )
                processed_batch_count = len(completed_batches)
                scoring_progress.update(
                    processed_batch_count, "resuming"
                )
                batches = (
                    (
                        batch_no,
                        pd.read_sql_query(
                            text(query), conn, params={"dq_batch_no": batch_no}
                        ),
                    )
                    for batch_no in pending_batches
                )
            else:
                stream_conn = conn.execution_options(
                    stream_results=True,
                    max_row_buffer=args.batch_size,
                )
                chunks = pd.read_sql_query(
                    text(query), stream_conn, chunksize=args.batch_size
                )
                batches = enumerate(chunks, start=1)
                scoring_progress = ConsoleProgress("Scoring batches")
                processed_batch_count = 0
                scoring_progress.update(0, "starting")

            for batch_number, df_batch in batches:
                scoring_progress.update(
                    processed_batch_count,
                    f"running batch {batch_number:,} ({len(df_batch):,} rows)",
                )
                df_out, df_inline, df_summary, df_integrity = run_dq(
                    df_batch,
                    threshold=args.threshold,
                    include_inline=not args.skip_inline_output,
                    compact_summary=args.compact_summary,
                    verbose=False,
                )
                if not args.skip_inline_output:
                    internal_cols = [
                        col for col in df_inline.columns
                        if col.startswith("__dq_uniq_count_") or col in {
                            "mfbag_calculated_weight", "mmbag_calculated_qty",
                            "msmu_calculated_weight", "msmu_calculated_qty",
                        }
                    ]
                    df_inline = df_inline.drop(columns=internal_cols, errors="ignore")
                total_rows += len(df_out)
                passed += int(df_out["decision"].eq("PASS").sum())
                for element in cfg.DQ_ELEMENTS:
                    values = df_out[f"{element.lower()}_score"].dropna()
                    if values.empty:
                        continue
                    stat = score_stats[element]
                    stat["sum"] += float(values.sum())
                    stat["count"] += int(len(values))
                    stat["min"] = float(values.min()) if stat["min"] is None else min(stat["min"], float(values.min()))
                    stat["max"] = float(values.max()) if stat["max"] is None else max(stat["max"], float(values.max()))
                overall = df_out["overall_score"].dropna()
                overall_sum += float(overall.sum())
                overall_count += int(len(overall))
                if not args.resumable:
                    integrity_frames.append(df_integrity)

                if args.save_to_postgres:
                    if args.resumable:
                        _save_checkpointed_batch(
                            engine, text, args, run_id, batch_number, df_batch,
                            df_out, df_inline, df_summary, df_integrity,
                        )
                    else:
                        mode = args.postgres_if_exists if first else "append"
                        summary_table = (
                            _compact_summary_table_name(args.postgres_output_table)
                            if args.compact_summary else _summary_table_name(args.postgres_output_table)
                        )
                        frames = [
                            (df_out, args.postgres_output_table),
                            (df_summary, summary_table),
                        ]
                        if not args.skip_inline_output:
                            frames.append((df_inline, _inline_table_name(args.postgres_output_table)))
                        for frame, target in frames:
                            save_to_postgres(
                                frame, target, args.postgres_table, run_id, args.row_limit, mode
                            )

                if not args.skip_csv_output:
                    output_frames = [(df_out, paths[0]), (df_summary, paths[2])]
                    if not args.skip_inline_output:
                        output_frames.append((df_inline, paths[1]))
                    for frame, path in output_frames:
                        _write_csv_batch(frame, path, first)
                first = False
                processed_batch_count += 1
                scoring_progress.update(
                    processed_batch_count, f"{total_rows:,} new rows processed"
                )
            scoring_progress.close(f"complete ({total_rows:,} new rows processed)")
    except Exception as exc:
        if args.resumable:
            _mark_checkpoint_run_failed(
                engine, text, args.postgres_output_table, run_id, exc
            )
        raise
    finally:
        engine.dispose()

    engine = create_engine(DB_CONN)
    try:
        if args.resumable:
            df_integrity = _finish_checkpointed_run(engine, text, args, run_id)
        else:
            df_integrity = _combine_integrity_batches(integrity_frames)
            if args.save_to_postgres:
                save_to_postgres(
                    df_integrity, _integrity_table_name(args.postgres_output_table),
                    args.postgres_table, run_id, args.row_limit, args.postgres_if_exists,
                )
    finally:
        engine.dispose()
    if not args.skip_csv_output:
        df_integrity.to_csv(paths[3], index=False)
        print(f"\nSaved to: {args.output} ({total_rows:,} rows)")
        if not args.skip_inline_output:
            print(f"Saved inline checks to: {paths[1]} ({total_rows:,} rows)")
        print(f"Saved index summary to: {paths[2]} ({total_rows:,} rows)")
        print(f"Saved integrity analytics to: {paths[3]} ({len(df_integrity):,} rows)")

    if args.resumable:
        engine = create_engine(DB_CONN)
        try:
            _print_checkpointed_stats(engine, text, args, run_id)
        finally:
            engine.dispose()
        return

    print(f"\n{'Element':<15} {'Avg':>7} {'Min':>7} {'Max':>7}")
    print("-" * 40)
    for element, stat in score_stats.items():
        if stat["count"]:
            print(f"{element:<15} {stat['sum']/stat['count']:>6.1f}% {stat['min']:>6.1f}% {stat['max']:>6.1f}%")
        else:
            print(f"{element:<15} {'N/A':>7}")
    if overall_count:
        print(f"\n{'OVERALL':<15} {overall_sum/overall_count:>6.1f}%")
    print(f"Pass rate: {passed}/{total_rows} ({100.0 * passed / total_rows if total_rows else 0:.1f}%)")


def main() -> None:
    parser = argparse.ArgumentParser(description="JNE 7-Elements DQ Framework")
    parser.add_argument("--input",     default=None,          help="Path to unified CSV")
    parser.add_argument("--from-postgres", action="store_true",
                        help="Read unified shipments directly from PostgreSQL")
    parser.add_argument("--postgres-table",
                        default=f"{SCHEMA_TRANSFORMED}.unified_shipments",
                        help="PostgreSQL table to read when --from-postgres is used")
    parser.add_argument("--row-limit", type=int, default=None,
                        help="Optional row limit for testing large inputs")
    parser.add_argument("--batch-size", type=int, default=2000,
                        help="Rows per in-memory scoring batch for PostgreSQL input")
    parser.add_argument("--output",    default="dq_output.csv", help="Output scores CSV")
    parser.add_argument("--skip-csv-output", action="store_true",
                        help="Do not write the scores CSV file")
    parser.add_argument("--skip-inline-output", action="store_true",
                        help="Do not create the wide source-plus-check inline output")
    parser.add_argument("--compact-summary", action="store_true",
                        help="Store counters and failed checks JSON instead of wide PASS/FAIL columns")
    parser.add_argument("--save-to-postgres", action="store_true",
                        help="Save row-level DQ scores to PostgreSQL")
    parser.add_argument("--postgres-output-table",
                        default="governance.dq_scores",
                        help="PostgreSQL score table when --save-to-postgres is used")
    parser.add_argument("--postgres-if-exists", choices=["replace", "append"],
                        default="replace",
                        help="Replace latest score table or append run history")
    parser.add_argument("--run-id", default=None,
                        help="Optional DQ run id; defaults to a generated id")
    parser.add_argument("--resumable", action="store_true",
                        help="Checkpoint completed PostgreSQL batches for retry/resume")
    parser.add_argument("--resume", action="store_true",
                        help="Continue an existing --resumable run with the same --run-id")
    parser.add_argument("--threshold", type=float, default=85.0, help="Pass/fail threshold %%")
    args = parser.parse_args()
    if args.resume and not args.resumable:
        parser.error("--resume requires --resumable")
    if args.resumable and not args.from_postgres:
        parser.error("--resumable is available only with --from-postgres")
    run_id = args.run_id or f"dq_{pd.Timestamp.now('UTC'):%Y%m%dT%H%M%S}_{uuid4().hex[:8]}"

    if args.from_postgres:
        run_postgres_in_batches(args, run_id)
        return
    elif args.input:
        df_all = load_csv(args.input, row_limit=args.row_limit)
    else:
        parser.error("Either --input or --from-postgres is required")

    df_out, df_inline, df_summary, df_integrity = run_dq(
        df_all,
        threshold=args.threshold,
        include_inline=not args.skip_inline_output,
        compact_summary=args.compact_summary,
    )

    if args.save_to_postgres:
        source_table = args.postgres_table if args.from_postgres else args.input
        save_to_postgres(
            df_out,
            args.postgres_output_table,
            source_table=source_table,
            run_id=run_id,
            row_limit=args.row_limit,
            if_exists=args.postgres_if_exists,
        )
        if not args.skip_inline_output:
            save_to_postgres(
                df_inline,
                _inline_table_name(args.postgres_output_table),
                source_table=source_table,
                run_id=run_id,
                row_limit=args.row_limit,
                if_exists=args.postgres_if_exists,
            )
        save_to_postgres(
            df_summary,
            (
                _compact_summary_table_name(args.postgres_output_table)
                if args.compact_summary else _summary_table_name(args.postgres_output_table)
            ),
            source_table=source_table,
            run_id=run_id,
            row_limit=args.row_limit,
            if_exists=args.postgres_if_exists,
        )
        save_to_postgres(
            df_integrity,
            _integrity_table_name(args.postgres_output_table),
            source_table=source_table,
            run_id=run_id,
            row_limit=args.row_limit,
            if_exists=args.postgres_if_exists,
        )

    if not args.skip_csv_output:
        df_out.to_csv(args.output, index=False)
        inline_path = _inline_csv_path(args.output)
        summary_path = _summary_csv_path(args.output)
        integrity_path = _integrity_csv_path(args.output)
        if not args.skip_inline_output:
            df_inline.to_csv(inline_path, index=False)
        df_summary.to_csv(summary_path, index=False)
        df_integrity.to_csv(integrity_path, index=False)
        print(f"\nSaved to: {args.output} ({len(df_out):,} rows)")
        if not args.skip_inline_output:
            print(f"Saved inline checks to: {inline_path} ({len(df_inline):,} rows)")
        print(f"Saved index summary to: {summary_path} ({len(df_summary):,} rows)")
        print(f"Saved integrity analytics to: {integrity_path} ({len(df_integrity):,} rows)")

    print(f"\n{'Element':<15} {'Avg':>7} {'Min':>7} {'Max':>7}")
    print("-" * 40)
    for elem in cfg.DQ_ELEMENTS:
        col = f"{elem.lower()}_score"
        v = df_out[col].dropna()
        if len(v):
            print(f"{elem:<15} {v.mean():>6.1f}% {v.min():>6.1f}% {v.max():>6.1f}%")
        else:
            print(f"{elem:<15} {'N/A':>7}")

    overall = df_out["overall_score"].dropna()
    if len(overall):
        print(f"\n{'OVERALL':<15} {overall.mean():>6.1f}%")
    passed = df_out["decision"].eq("PASS").sum()
    print(f"Pass rate: {passed}/{len(df_out)} ({100 * passed / len(df_out):.1f}%)")


if __name__ == "__main__":
    main()
