"""One-time connected CNOTE sample export for Tableau analysis.

This script is intentionally outside the pipeline/DAG path. It samples a CNOTE
anchor set, derives the same operational scopes the bronze extractor uses, and
writes each relational table locally as Parquet or CSV.
"""

from __future__ import annotations

import argparse
import csv
import logging
import shutil
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bronze import (
    CODE_VERSION,
    OracleSettings,
    PartitionedParquetWriter,
    ScopeSettings,
    Stage,
    TableResult,
    Window,
    _build_sql,
    _drop_table,
    _extract_date_label,
    _load_pii_exclusions,
    _oracle_arrow_schema,
    _projection,
    _run_dir_for_extract_date,
    cleanup_scope_tables,
    configure_logging,
    connect,
    load_config,
    required_scopes_for_specs,
    resolve_window,
    sanitize_run_id,
    scope_predicate,
    selected_specs,
    table_columns,
)


logger = logging.getLogger(__name__)


def _create_scope(conn: Any, table_name: str, key_column: str, query: str) -> int:
    _drop_table(conn, table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {table_name} NOLOGGING AS\n"
            f"SELECT DISTINCT {key_column} FROM (\n{query}\n) WHERE {key_column} IS NOT NULL"
        )
        cursor.execute(f"CREATE INDEX IDX_{table_name.split('.')[-1][:24]} ON {table_name} ({key_column})")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = int(cursor.fetchone()[0])
    conn.commit()
    return count


def _sampled_cnote_query(
    source_schema: str,
    anchor_table: str,
    anchor_date_column: str,
    window: Window,
    sample_size: int,
    seed: int,
) -> str:
    return f"""
            SELECT CNOTE_NO
            FROM (
                SELECT CNOTE_NO
                FROM {source_schema}.{anchor_table}
                WHERE {anchor_date_column} >= DATE '{window.start_label}'
                  AND {anchor_date_column} < DATE '{window.end_label}'
                ORDER BY ORA_HASH(CNOTE_NO, 4294967295, {seed}), CNOTE_NO
            )
            WHERE ROWNUM <= {sample_size}
            """


def materialize_sample_scope_tables(
    conn: Any,
    settings: ScopeSettings,
    window: Window,
    anchor_table: str,
    anchor_date_column: str,
    sample_size: int,
    seed: int,
    required_scopes: set[str],
) -> dict[str, int]:
    src = settings.source_schema
    cnote_scope = settings.table("CNOTE")
    counts = {}

    logger.info("Creating sampled CNOTE scope (%s rows requested)", f"{sample_size:,}")
    counts["CNOTE"] = _create_scope(
        conn,
        cnote_scope,
        "CNOTE_NO",
        _sampled_cnote_query(src, anchor_table, anchor_date_column, window, sample_size, seed),
    )

    scope_queries = {
        "DRCNOTE": ("DRCNOTE_NO", f"SELECT DRCNOTE_NO FROM {src}.CMS_DRCNOTE WHERE DRCNOTE_CNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "DHI_HOC": ("DHI_NO", f"SELECT DHI_NO FROM {src}.CMS_DHI_HOC WHERE DHI_CNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "DHOUNDEL": ("DHOUNDEL_NO", f"SELECT DHOUNDEL_NO FROM {src}.CMS_DHOUNDEL_POD WHERE DHOUNDEL_CNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "DRSHEET": ("DRSHEET_NO", f"SELECT DRSHEET_NO FROM {src}.CMS_DRSHEET WHERE DRSHEET_CNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "MANIFEST": ("MANIFEST_NO", f"SELECT MFCNOTE_MAN_NO AS MANIFEST_NO FROM {src}.CMS_MFCNOTE WHERE MFCNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "MFBAG": (
            "MFBAG_NO",
            f"""
            SELECT MFCNOTE_BAG_NO AS MFBAG_NO FROM {src}.CMS_MFCNOTE WHERE MFCNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})
            UNION
            SELECT MFBAG_NO FROM {src}.CMS_MFBAG WHERE MFBAG_MAN_NO IN (SELECT MANIFEST_NO FROM {settings.table("MANIFEST")})
            """,
        ),
        "DMBAG": ("DMBAG_NO", f"SELECT DMBAG_NO FROM {src}.CMS_DMBAG WHERE DMBAG_BAG_NO IN (SELECT MFBAG_NO FROM {settings.table('MFBAG')})"),
        "SMU": ("SMU_NO", f"SELECT DSMU_NO AS SMU_NO FROM {src}.CMS_DSMU WHERE DSMU_BAG_NO IN (SELECT DMBAG_NO FROM {settings.table('DMBAG')})"),
        "MMBAG": ("MMBAG_NO", f"SELECT DMBAG_NO AS MMBAG_NO FROM {settings.table('DMBAG')}"),
        "COST_MANIFEST": ("MANIFEST_NO", f"SELECT DMANIFEST_NO AS MANIFEST_NO FROM {src}.CMS_COST_DTRANSIT_AGEN WHERE CNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "HVI": ("HVI_NO", f"SELECT DHICNOTE_NO AS HVI_NO FROM {src}.CMS_DHICNOTE WHERE DHICNOTE_CNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "HVO": ("HVO_NO", f"SELECT DHOCNOTE_NO AS HVO_NO FROM {src}.CMS_DHOCNOTE WHERE DHOCNOTE_CNOTE_NO IN (SELECT CNOTE_NO FROM {cnote_scope})"),
        "RDSJ_HVO": ("HVO_NO", f"SELECT RDSJ_HVO_NO AS HVO_NO FROM {src}.CMS_RDSJ WHERE RDSJ_HVI_NO IN (SELECT HVI_NO FROM {settings.table('HVI')})"),
        "MSJ": ("MSJ_NO", f"SELECT DSJ_NO AS MSJ_NO FROM {src}.CMS_DSJ WHERE DSJ_HVO_NO IN (SELECT HVO_NO FROM {settings.table('RDSJ_HVO')})"),
    }

    for scope_name, (key_column, query) in scope_queries.items():
        if scope_name not in required_scopes:
            continue
        logger.info("Creating derived scope %s", scope_name)
        counts[scope_name] = _create_scope(conn, settings.table(scope_name), key_column, query)
    logger.info("Scope counts: %s", counts)
    return counts


def _table_sql(
    config: dict,
    spec: Any,
    columns: list[str],
    scope: ScopeSettings,
    source_columns: Sequence[str],
) -> tuple[str, dict]:
    if spec.stage != Stage.ANCHOR:
        try:
            return _build_sql(config, spec, columns, scope, source_columns)
        except TypeError as exc:
            if "positional argument" not in str(exc) and "positional arguments" not in str(exc):
                raise
            return _build_sql(config, spec, columns, scope)

    source_schema = config["oracle"].get("source_schema", "JNE").upper()
    alias = "src"
    date_col = config["extraction"]["anchor_date_column"]
    column_sql = ", ".join(f"{alias}.{col}" for col in columns)
    sql = (
        f"SELECT {column_sql} FROM {source_schema}.{spec.table} {alias} "
        f"WHERE ({alias}.{date_col} >= :start_date AND {alias}.{date_col} < :end_date) "
        f"AND ({scope_predicate(scope, alias, 'CNOTE', 'CNOTE_NO')})"
    )
    return sql, {"start_date": None, "end_date": None}


def _write_csv(cursor: Any, output_dir: Path, columns: list[str], arraysize: int) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-00001.csv"
    row_count = 0
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        while True:
            rows = cursor.fetchmany(arraysize)
            if not rows:
                break
            writer.writerows(rows)
            row_count += len(rows)
    (output_dir / "_SUCCESS").write_text(f"{row_count}\n", encoding="ascii")
    return row_count


def export_table(
    config: dict,
    oracle_settings: OracleSettings,
    scope: ScopeSettings,
    window: Window,
    run_dir: Path,
    spec: Any,
    output_format: str,
) -> TableResult:
    start = time.monotonic()
    output_dir = run_dir / spec.output_name
    if output_dir.exists():
        shutil.rmtree(output_dir)

    exclusions = _load_pii_exclusions(config)
    with connect(oracle_settings) as conn:
        source_schema = config["oracle"].get("source_schema", "JNE")
        source_columns = table_columns(conn, source_schema, spec.table)
        columns = _projection(source_columns, spec, exclusions)
        sql, binds = _table_sql(config, spec, columns, scope, source_columns)
        if ":start_date" in sql or ":end_date" in sql:
            binds = {"start_date": window.start, "end_date": window.end}

        logger.info("Exporting %s to %s", spec.table, output_dir)
        with conn.cursor() as cursor:
            cursor.arraysize = oracle_settings.fetch_arraysize
            cursor.execute(sql, binds)
            if output_format == "csv":
                row_count = _write_csv(cursor, output_dir, columns, oracle_settings.fetch_arraysize)
            else:
                arrow_schema = _oracle_arrow_schema(cursor.description)
                compression = config["output"].get("compression", "zstd")
                compression_level = int(config["output"].get("zstd_level", 9)) if compression == "zstd" else None
                with PartitionedParquetWriter(
                    output_dir,
                    columns,
                    int(config["output"].get("rows_per_file", 250000)),
                    compression,
                    compression_level,
                    schema=arrow_schema,
                    overwrite=True,
                ) as writer:
                    while True:
                        rows = cursor.fetchmany(oracle_settings.fetch_arraysize)
                        if not rows:
                            break
                        writer.write_rows(rows)
                    row_count = writer.row_count

    files = list(output_dir.glob("part-*"))
    return TableResult(
        table=spec.table,
        output_name=spec.output_name,
        stage=spec.stage.value,
        row_count=row_count,
        file_count=len(files),
        size_bytes=sum(path.stat().st_size for path in files),
        elapsed_seconds=time.monotonic() - start,
    )


def run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    window = resolve_window(config)
    run_id = sanitize_run_id(args.run_id)
    extract_date = args.extract_date or _extract_date_label(run_id)
    specs = selected_specs(config)
    oracle_settings = OracleSettings.from_config(config)
    scope = ScopeSettings.from_config(config, f"TABLEAU_{run_id}")
    required_scopes = required_scopes_for_specs(specs)
    required_scopes.add("CNOTE")
    run_dir = _run_dir_for_extract_date(Path(args.output_root), window, run_id, extract_date)
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Connected sample export run_id=%s code_version=%s window=[%s, %s)",
        run_id,
        CODE_VERSION,
        window.start_label,
        window.end_label,
    )

    with connect(oracle_settings) as conn:
        scope_counts = materialize_sample_scope_tables(
            conn,
            scope,
            window,
            config["extraction"]["anchor_table"],
            config["extraction"]["anchor_date_column"],
            args.sample_size,
            args.seed,
            required_scopes,
        )

    results = []
    try:
        for spec in specs:
            if spec.stage == Stage.REFERENCE and config["scoping"].get("reference_tables_mode", "full") == "skip":
                continue
            results.append(export_table(config, oracle_settings, scope, window, run_dir, spec, args.format))
    finally:
        if args.keep_scope:
            logger.info("Keeping Oracle scope tables for inspection")
        else:
            with connect(oracle_settings) as conn:
                cleanup_scope_tables(conn, scope)
            logger.info("Oracle scope tables cleaned up")

    manifest = {
        "run_id": run_id,
        "window_start": window.start_label,
        "window_end": window.end_label,
        "sample_size_requested": args.sample_size,
        "sample_seed": args.seed,
        "format": args.format,
        "scope_counts": scope_counts,
        "tables": [asdict(result) for result in results],
    }
    import json

    (run_dir / "sample_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    logger.info("Wrote %s", run_dir / "sample_manifest.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a connected one-time CNOTE sample for Tableau.")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--run-id", default="tableau_sample")
    parser.add_argument("--extract-date")
    parser.add_argument("--output-root", default="data/tableau_samples")
    parser.add_argument("--sample-size", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--keep-scope", action="store_true")
    args = parser.parse_args()
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be greater than zero")
    if args.seed < 0:
        raise ValueError("--seed must be zero or greater")
    configure_logging()
    run(args)


if __name__ == "__main__":
    main()
