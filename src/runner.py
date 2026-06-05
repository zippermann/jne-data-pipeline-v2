"""Governance runner CLI."""

from __future__ import annotations

import argparse
import tempfile
from dataclasses import asdict
from pathlib import Path

from src.config import load_governance_config, table_path
from src.duck import connect_duckdb
from src.rules.executors import EXECUTORS, run_completeness_batch
from src.rules.explain import print_explanation
from src.rules.registry import active_rules


SCORECARD_COLUMNS = [
    "index_code",
    "element",
    "rule_family",
    "table_name",
    "column_names",
    "compared_table",
    "compared_columns",
    "total_checked",
    "failed_key_count",
    "failed_row_count",
    "failure_rate",
    "status",
    "needs_confirmation",
    "skipped_reason",
    "run_at",
]


FAILURES_TABLE = "all_governance_failures"


def _create_failures_table(con) -> None:
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {FAILURES_TABLE} (
            index_code VARCHAR,
            table_name VARCHAR,
            column_names VARCHAR,
            failed_value VARCHAR,
            failure_reason VARCHAR,
            affected_rows BIGINT,
            boundary_suspect BOOLEAN,
            run_at VARCHAR
        )
    """)


def _table_paths(config, rules) -> dict[str, str]:
    tables = {rule.table for rule in rules if rule.table}
    tables |= {rule.child_table for rule in rules if rule.child_table}
    tables |= {rule.parent_table for rule in rules if rule.parent_table}
    if any(rule.code in {"ACCU2B12", "ACCU3B13"} for rule in rules):
        tables.add("CMS_DCORRECT_DEST")
    if any(rule.code.startswith("TIME1H15") for rule in rules):
        tables.add("CMS_MFCNOTE")
    if any(rule.code == "TIME1V9" for rule in rules):
        tables |= {"CMS_DHOCNOTE", "CMS_DHICNOTE", "CMS_RDSJ", "CMS_DSJ", "CMS_MSJ"}
    if any(rule.code == "TIME1X2" for rule in rules):
        tables |= {"CMS_MFCNOTE", "CMS_MANIFEST", "CMS_DHICNOTE", "CMS_RDSJ", "CMS_DSJ", "CMS_MSJ"}
    return {table: table_path(config, table) for table in tables}


def _parse_s3_path(path: str) -> tuple[str, str]:
    if not path.startswith("s3://"):
        raise ValueError(f"Not an s3 path: {path}")
    bucket, key = path.removeprefix("s3://").split("/", 1)
    return bucket, key


def _minio_client(config):
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "minio is required for governance checks over bronze objects. Install "
            "dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    return Minio(
        config.minio.endpoint,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=config.minio.secure,
    )


def _localize_table_paths(config, table_paths: dict[str, str], tmpdir: Path) -> dict[str, str]:
    """Download S3/MinIO table parquet parts so DuckDB reads local files.

    This avoids DuckDB/httpfs multifile assertion failures seen against MinIO
    while keeping the governance SQL unchanged.
    """
    client = _minio_client(config)
    localized = {}
    for table, path in table_paths.items():
        if not path.startswith("s3://"):
            localized[table] = path
            continue

        bucket, key = _parse_s3_path(path)
        prefix = key.split("*", 1)[0]
        table_dir = tmpdir / table.lower()
        table_dir.mkdir(parents=True, exist_ok=True)

        downloaded = 0
        for item in client.list_objects(bucket, prefix=prefix, recursive=True):
            object_name = item.object_name
            if not object_name.endswith(".parquet"):
                continue
            local_path = table_dir / Path(object_name).name
            client.fget_object(bucket, object_name, str(local_path))
            downloaded += 1

        print(f"Localized {table}: {downloaded} parquet object(s) from {path}")
        localized[table] = str(table_dir / "*.parquet")
    return localized


def _print_scorecard(results) -> None:
    print(",".join(SCORECARD_COLUMNS))
    for result in results:
        row = asdict(result)
        print(",".join("" if row[column] is None else str(row[column]) for column in SCORECARD_COLUMNS))


def run(config_path: str) -> list:
    from src.output import write_outputs

    config = load_governance_config(config_path)
    rules = active_rules()
    with tempfile.TemporaryDirectory() as tmp:
        table_paths = _localize_table_paths(config, _table_paths(config, rules), Path(tmp))
        con = connect_duckdb(config)
        try:
            _create_failures_table(con)
            results = []
            completeness_results = {}
            completeness_rules = [rule for rule in rules if rule.rule_family == "COMP"]
            if completeness_rules:
                tables = sorted({rule.table for rule in completeness_rules})
                print(f"Running COMP batch: {len(completeness_rules)} rule(s) across {len(tables)} table(s)")
                try:
                    completeness_results = run_completeness_batch(
                        completeness_rules,
                        con,
                        config,
                        table_paths,
                        FAILURES_TABLE,
                    )
                except Exception as exc:
                    raise RuntimeError(f"Governance completeness batch failed: {exc}") from exc
            for rule in rules:
                if rule.rule_family == "COMP":
                    results.append(completeness_results[rule.code])
                    continue
                executor = EXECUTORS.get(rule.rule_family)
                if executor is None:
                    executor = EXECUTORS[rule.element]
                print(f"Running {rule.code}: {rule.element}/{rule.rule_family} {rule.table or rule.child_table}")
                try:
                    results.append(executor(rule, con, config, table_paths, FAILURES_TABLE))
                except Exception as exc:
                    raise RuntimeError(
                        f"Governance rule {rule.code} failed "
                        f"({rule.element}/{rule.rule_family}, table={rule.table or rule.child_table}, "
                        f"columns={', '.join(rule.columns)}): {exc}"
                    ) from exc
            _print_scorecard(results)
            outputs = write_outputs(config, con, results, FAILURES_TABLE, table_paths)
            print("\nWrote outputs:")
            for path in outputs:
                print(path)
            return results
        finally:
            con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run JNE relational governance checks.")
    parser.add_argument("--config", default="config/governance.yaml")
    parser.add_argument("--explain", help="Print a rule explanation and exit.")
    args = parser.parse_args()

    if args.explain:
        print_explanation(args.explain)
        return

    run(args.config)


if __name__ == "__main__":
    main()
