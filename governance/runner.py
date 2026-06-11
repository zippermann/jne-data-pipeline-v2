"""Governance runner for bronze Parquet pipeline outputs.

The production path reads the run manifest written by ``extractor.bronze``,
loads only the bronze tables needed by runnable catalog entries, and emits a
scorecard plus row-level failures. A synthetic mode remains available for local
rule tests and demos, but Airflow uses the bronze manifest path.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random
from typing import Any, Iterable

import pandas as pd

from extractor.bronze import MinioSettings, load_config
from governance.catalog import CATALOG
from governance.output import write_failures, write_scorecard
from governance.rules import FAILURE_COLUMNS, RULE_FUNCTIONS, RuleOutcome


RUN_WINDOW_START = "2026-06-01"
RUN_WINDOW_END = "2026-06-08"
DEFAULT_OUTPUT_DIR = Path("governance/outputs")
TABLE_PARAM_KEYS = (
    "left_table",
    "right_table",
    "start_table",
    "end_table",
    "child_table",
    "parent_table",
    "reference_table",
    "master_table",
)


@dataclass(frozen=True)
class BronzeTable:
    table: str
    output_name: str
    row_count: int | None = None


@dataclass(frozen=True)
class GovernanceSource:
    manifest: dict[str, Any]
    tables: dict[str, BronzeTable]
    client: Any | None = None
    bucket: str | None = None
    prefix: str | None = None
    run_path: Path | None = None
    tmpdir: Path | None = None


def _base_cnotes() -> list[str]:
    return [f"CNOTE{number:04d}" for number in range(1, 51)]


def load_tables(table_names: set[str]) -> dict[str, pd.DataFrame]:
    """Synthetic fixture used only by ``--source synthetic`` and unit tests."""
    random = Random(20260610)
    cnotes = _base_cnotes()
    start = datetime.fromisoformat(RUN_WINDOW_START)

    data = {
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": cnotes,
            "CNOTE_SERVICES_CODE": ["REG"] * 50,
            "CNOTE_ORIGIN": ["CGK"] * 50,
            "CNOTE_DESTINATION": ["SUB"] * 50,
            "CNOTE_WEIGHT": [float(random.randint(1, 20)) for _ in cnotes],
            "CNOTE_DATE": [start + timedelta(hours=index) for index in range(50)],
            "CNOTE_CRDATE": [start + timedelta(hours=index) for index in range(50)],
        }),
        "CMS_DRCNOTE": pd.DataFrame({
            "DRCNOTE_NO": [f"DR{number:04d}" for number in range(1, 51)],
            "DRCNOTE_CNOTE_NO": cnotes,
        }),
        "CMS_APICUST": pd.DataFrame({
            "APICUST_CNOTE_NO": cnotes,
            "APICUST_WEIGHT": [0.0] * 50,
            "APICUST_ORIGIN": ["CGK"] * 50,
        }),
        "CMS_MHI_HOC": pd.DataFrame({
            "MHI_CNOTE_NO": cnotes,
            "MHI_APPROVE_DATE": [start + timedelta(hours=index + 1) for index in range(50)],
        }),
        "CMS_MHOCNOTE": pd.DataFrame({
            "MHOCNOTE_NO": [f"HOC{number:04d}" for number in range(1, 51)],
            "MHOCNOTE_APPROVE": ["Y"] * 50,
            "MHOCNOTE_APP_DATE": [start + timedelta(hours=index + 2) for index in range(50)],
            "MHOCNOTE_SIGNDATE": [start + timedelta(hours=index + 3) for index in range(50)],
        }),
        "CMS_MSJ": pd.DataFrame({
            "MSJ_NO": [f"MSJ{number:04d}" for number in range(1, 51)],
            "MSJ_APPROVE": ["Y"] * 50,
            "MSJ_SIGNDATE": [start + timedelta(hours=index + 4) for index in range(50)],
        }),
        "CMS_DRSHEET": pd.DataFrame({
            "DRSHEET_NO": [f"RS{number:04d}" for number in range(1, 51)],
            "DRSHEET_CNOTE_NO": cnotes,
            "DRSHEET_DATE": [start + timedelta(days=1, hours=index) for index in range(50)],
        }),
        "CMS_CNOTE_POD": pd.DataFrame({
            "CNOTE_POD_NO": cnotes,
            "CNOTE_POD_DATE": [start + timedelta(days=2, hours=index) for index in range(50)],
        }),
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": cnotes,
            "MFCNOTE_MAN_NO": [f"MAN{number:04d}" for number in range(1, 51)],
            "MFCNOTE_WEIGHT": [0.0] * 50,
        }),
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": [f"MAN{number:04d}" for number in range(1, 51)],
            "MANIFEST_APPROVED": ["Y"] * 50,
            "MANIFEST_ROUTE": ["CGK-SUB"] * 50,
            "MANIFEST_THRU": ["SUB"] * 50,
            "MANIFEST_ORIGIN": ["CGK"] * 50,
            "MANIFEST_CRDATE": [start + timedelta(hours=index + 5) for index in range(50)],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": [f"BAG{number:04d}" for number in range(1, 51)],
            "MFBAG_MAN_NO": [f"MAN{number:04d}" for number in range(1, 51)],
            "MFBAG_CRDATE": [start + timedelta(hours=index + 6) for index in range(50)],
        }),
        "CMS_MMBAG": pd.DataFrame({
            "MMBAG_NO": [f"MBAG{number:04d}" for number in range(1, 51)],
            "MMBAG_APPROVE": ["Y"] * 50,
            "MMBAG_DATE_APPROVE": [start + timedelta(hours=index + 7) for index in range(50)],
        }),
        "CMS_DCORRECT_DEST": pd.DataFrame({
            "DCORRECT_CNOTE_NO": cnotes,
            "DCORRECT_ORIGIN": ["CGK"] * 50,
            "DCORRECT_DEST": ["SUB"] * 50,
        }),
    }

    data["CMS_APICUST"]["APICUST_WEIGHT"] = data["CMS_CNOTE"]["CNOTE_WEIGHT"].copy()
    data["CMS_MFCNOTE"]["MFCNOTE_WEIGHT"] = data["CMS_CNOTE"]["CNOTE_WEIGHT"].copy()

    data["CMS_CNOTE"].loc[1, "CNOTE_NO"] = None
    data["CMS_DRCNOTE"].loc[2, "DRCNOTE_CNOTE_NO"] = ""
    data["CMS_CNOTE"].loc[3, "CNOTE_SERVICES_CODE"] = "bad service"
    data["CMS_CNOTE"].loc[4, "CNOTE_ORIGIN"] = "CGK1"
    data["CMS_CNOTE"].loc[6, "CNOTE_NO"] = data["CMS_CNOTE"].loc[5, "CNOTE_NO"]
    data["CMS_DRCNOTE"].loc[8, "DRCNOTE_NO"] = data["CMS_DRCNOTE"].loc[7, "DRCNOTE_NO"]
    data["CMS_APICUST"].loc[10, "APICUST_WEIGHT"] = data["CMS_CNOTE"].loc[10, "CNOTE_WEIGHT"] + 5
    data["CMS_APICUST"].loc[12, "APICUST_ORIGIN"] = "SUB"
    data["CMS_MHI_HOC"].loc[14, "MHI_APPROVE_DATE"] = data["CMS_CNOTE"].loc[14, "CNOTE_CRDATE"] - timedelta(hours=2)
    data["CMS_CNOTE_POD"].loc[16, "CNOTE_POD_DATE"] = data["CMS_DRSHEET"].loc[16, "DRSHEET_DATE"] - timedelta(hours=3)
    data["CMS_DRCNOTE"].loc[18, "DRCNOTE_CNOTE_NO"] = "CNOTE9999"
    data["CMS_MFCNOTE"].loc[20, "MFCNOTE_MAN_NO"] = "MAN9999"
    data["CMS_APICUST"].loc[22, "APICUST_WEIGHT"] = None
    data["CMS_DCORRECT_DEST"].loc[24, "DCORRECT_ORIGIN"] = "BDO"
    data["CMS_DCORRECT_DEST"].loc[25, "DCORRECT_ORIGIN"] = "CGK9"
    data["CMS_DCORRECT_DEST"].loc[26, "DCORRECT_DEST"] = "BDO"
    data["CMS_DCORRECT_DEST"].loc[27, "DCORRECT_DEST"] = "SUB9"
    data["CMS_MHOCNOTE"].loc[28, "MHOCNOTE_APP_DATE"] = None
    data["CMS_MHOCNOTE"].loc[29, "MHOCNOTE_SIGNDATE"] = None
    data["CMS_MSJ"].loc[30, "MSJ_SIGNDATE"] = None
    data["CMS_MANIFEST"].loc[31, "MANIFEST_ROUTE"] = None
    data["CMS_MANIFEST"].loc[32, "MANIFEST_THRU"] = None
    data["CMS_MANIFEST"].loc[33, "MANIFEST_ORIGIN"] = None
    data["CMS_MMBAG"].loc[34, "MMBAG_DATE_APPROVE"] = None
    data["CMS_MFCNOTE"].loc[35, "MFCNOTE_WEIGHT"] = data["CMS_CNOTE"].loc[35, "CNOTE_WEIGHT"] + 2
    data["CMS_MHI_HOC"].loc[36, "MHI_APPROVE_DATE"] = data["CMS_CNOTE"].loc[36, "CNOTE_DATE"] - timedelta(hours=1)
    data["CMS_MFBAG"].loc[37, "MFBAG_CRDATE"] = data["CMS_MANIFEST"].loc[37, "MANIFEST_CRDATE"] - timedelta(hours=1)

    return {table: frame for table, frame in data.items() if table in table_names}


def _required_tables() -> set[str]:
    tables: set[str] = set()
    for entry in CATALOG:
        tables.update(_entry_tables(entry))
    return tables


def _entry_tables(entry: dict) -> set[str]:
    params = entry["params"]
    tables = {entry["table"]}
    for key in TABLE_PARAM_KEYS:
        if key in params:
            tables.add(params[key])
    for key in ("detail_table", "master_table"):
        if key in params:
            tables.add(params[key])
    for step in params.get("joins", []):
        if "table" in step:
            tables.add(step["table"])
    for reference in params.get("references", []):
        if "table" in reference:
            tables.add(reference["table"])
    rule_family = entry.get("rule_family")
    if rule_family == "manifest_code_sequence":
        tables.update({"CMS_MFCNOTE", "CMS_MANIFEST"})
    if rule_family == "cnote_im_manifest_before_msj":
        tables.update({"CMS_MFCNOTE", "CMS_MANIFEST", "CMS_DHICNOTE", "CMS_RDSJ", "CMS_DSJ", "CMS_MSJ"})
    return {table.upper() for table in tables if table}


def _entry_columns(entry: dict) -> dict[str, set[str]]:
    params = entry["params"]
    table = entry["table"]
    columns: dict[str, set[str]] = {}

    def add(table_name: str | None, column: str | None) -> None:
        if table_name and column:
            columns.setdefault(table_name.upper(), set()).add(column)

    add(table, params.get("column"))
    add(table, params.get("cnote_column"))
    add(table, params.get("condition_column"))
    for column in params.get("columns", []):
        add(table, column)

    for prefix in ("left", "right", "start", "end"):
        add(params.get(f"{prefix}_table"), params.get(f"{prefix}_column"))
        add(params.get(f"{prefix}_table"), params.get(f"{prefix}_join_key") or params.get("join_key"))

    add(params.get("left_table"), params.get("left_column"))
    add(params.get("left_table"), params.get("start_column"))
    add(params.get("left_table"), params.get("cnote_column"))
    add(params.get("right_table"), params.get("right_column"))
    current_table = params.get("left_table") or params.get("detail_table")
    for step in params.get("joins", []):
        add(current_table, step.get("left_on"))
        add(step.get("table"), step.get("right_on"))
        current_table = step.get("table")
    add(current_table, params.get("right_column"))
    add(current_table, params.get("end_column"))
    add(current_table, params.get("detail_key"))

    add(params.get("child_table"), params.get("child_column"))
    add(params.get("child_table"), params.get("child_key"))
    add(params.get("child_table"), params.get("count_column"))
    add(params.get("parent_table"), params.get("parent_column"))
    add(params.get("reference_table"), params.get("reference_column"))
    for reference in params.get("references", []):
        add(reference.get("table"), reference.get("column"))
    add(params.get("master_table"), params.get("master_key"))
    add(params.get("master_table"), params.get("master_column"))
    add(params.get("master_table"), params.get("cnote_column"))
    add(params.get("master_table"), params.get("master_key"))
    add(params.get("master_table"), params.get("master_value_column"))
    add(params.get("master_table"), params.get("master_count_column"))
    add(params.get("master_table"), params.get("cnote_column"))
    add(params.get("detail_table"), params.get("detail_key"))
    add(params.get("detail_table"), params.get("detail_value_column"))
    add(params.get("detail_table"), params.get("detail_count_column"))
    rule_family = entry.get("rule_family")
    if rule_family == "manifest_code_sequence":
        add("CMS_MFCNOTE", "MFCNOTE_NO")
        add("CMS_MFCNOTE", "MFCNOTE_MAN_NO")
        add("CMS_MANIFEST", "MANIFEST_NO")
        add("CMS_MANIFEST", params.get("manifest_code_column", "MANIFEST_CODE"))
        add("CMS_MANIFEST", params.get("date_column", "MANIFEST_CRDATE"))
    if rule_family == "cnote_im_manifest_before_msj":
        add("CMS_MFCNOTE", "MFCNOTE_NO")
        add("CMS_MFCNOTE", "MFCNOTE_MAN_NO")
        add("CMS_MANIFEST", "MANIFEST_NO")
        add("CMS_MANIFEST", params.get("manifest_code_column", "MANIFEST_CODE"))
        add("CMS_MANIFEST", params.get("manifest_date_column", "MANIFEST_DATE"))
        add("CMS_DHICNOTE", "DHICNOTE_NO")
        add("CMS_DHICNOTE", "DHICNOTE_CNOTE_NO")
        add("CMS_RDSJ", "RDSJ_HVI_NO")
        add("CMS_RDSJ", "RDSJ_HVO_NO")
        add("CMS_DSJ", "DSJ_HVO_NO")
        add("CMS_DSJ", "DSJ_NO")
        add("CMS_MSJ", "MSJ_NO")
        add("CMS_MSJ", params.get("msj_date_column", "MSJ_SIGNDATE"))
    return columns


def _required_columns(entries: Iterable[dict]) -> dict[str, set[str]]:
    required: dict[str, set[str]] = {}
    for entry in entries:
        for table, columns in _entry_columns(entry).items():
            required.setdefault(table, set()).update(columns)
    return required


def _missing_entry_columns(entry: dict, data: dict[str, pd.DataFrame]) -> list[str]:
    missing = []
    for table, columns in _entry_columns(entry).items():
        if table not in data:
            missing.append(f"{table}.*")
            continue
        available = set(data[table].columns)
        for column in sorted(columns - available):
            missing.append(f"{table}.{column}")
    return missing


def _read_json_response(response: Any) -> dict[str, Any]:
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def _minio_client(config: dict):
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "minio is required for governance --source minio. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the Airflow image."
        ) from exc

    settings = MinioSettings.from_config(config)
    return settings, Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )


def _manifest_tables(manifest: dict[str, Any]) -> dict[str, BronzeTable]:
    tables: dict[str, BronzeTable] = {}
    for item in manifest.get("tables", []):
        source_name = str(item["table"]).upper()
        tables[source_name] = BronzeTable(
            table=source_name,
            output_name=str(item["output_name"]),
            row_count=item.get("row_count"),
        )
    return tables


def _source_from_minio(config_path: str, run_prefix: str | None, tmpdir: Path) -> GovernanceSource:
    config = load_config(config_path)
    settings, client = _minio_client(config)
    prefix = (run_prefix or os.getenv("BRONZE_RUN_PREFIX") or "").strip("/")
    if not prefix:
        raise ValueError("BRONZE_RUN_PREFIX or --bronze-run-prefix is required for governance --source minio")
    response = client.get_object(settings.bucket, f"{prefix}/run_manifest.json")
    manifest = _read_json_response(response)
    return GovernanceSource(
        manifest=manifest,
        tables=_manifest_tables(manifest),
        client=client,
        bucket=settings.bucket,
        prefix=prefix,
        tmpdir=tmpdir,
    )


def _source_from_local(run_path: str | Path) -> GovernanceSource:
    path = Path(run_path)
    manifest = json.loads((path / "run_manifest.json").read_text(encoding="utf-8"))
    return GovernanceSource(manifest=manifest, tables=_manifest_tables(manifest), run_path=path)


def _list_minio_parquet_objects(source: GovernanceSource, table: BronzeTable) -> list[str]:
    assert source.client is not None and source.bucket is not None and source.prefix is not None
    prefix = f"{source.prefix}/{table.output_name}/"
    return sorted(
        item.object_name
        for item in source.client.list_objects(source.bucket, prefix=prefix, recursive=True)
        if item.object_name.endswith(".parquet")
    )


def _read_parquet_files(paths: Iterable[Path], columns: list[str]) -> pd.DataFrame:
    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyarrow is required to run governance against bronze Parquet. "
            "Install dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    frames = [pq.read_table(path, columns=columns).to_pandas() for path in paths]
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def _load_table_from_minio(source: GovernanceSource, table: BronzeTable, columns: set[str]) -> pd.DataFrame:
    assert source.client is not None and source.bucket is not None and source.tmpdir is not None
    object_names = _list_minio_parquet_objects(source, table)
    local_paths = []
    for object_name in object_names:
        local_path = source.tmpdir / f"{table.output_name}-{Path(object_name).name}"
        source.client.fget_object(source.bucket, object_name, str(local_path))
        local_paths.append(local_path)
    return _read_parquet_files(local_paths, sorted(columns))


def _load_table_from_local(source: GovernanceSource, table: BronzeTable, columns: set[str]) -> pd.DataFrame:
    assert source.run_path is not None
    paths = sorted((source.run_path / table.output_name).glob("part-*.parquet"))
    return _read_parquet_files(paths, sorted(columns))


def _load_bronze_tables(source: GovernanceSource, required_columns: dict[str, set[str]]) -> dict[str, pd.DataFrame]:
    data = {}
    for table_name, columns in sorted(required_columns.items()):
        table = source.tables[table_name]
        started = time.monotonic()
        if source.run_path is not None:
            data[table_name] = _load_table_from_local(source, table, columns)
        else:
            data[table_name] = _load_table_from_minio(source, table, columns)
        print(
            f"Loaded {table_name} from bronze as {table.output_name}: "
            f"{len(data[table_name]):,} rows, {len(data[table_name].columns)} columns "
            f"in {time.monotonic() - started:.1f}s",
            flush=True,
        )
    return data


def _error_outcome(message: str) -> RuleOutcome:
    failures = pd.DataFrame([[None, message, "rule raised an exception"]], columns=FAILURE_COLUMNS)
    return RuleOutcome(total_checked=0, total_failed=0, failures=failures)


def _skip_outcome(message: str) -> RuleOutcome:
    failures = pd.DataFrame([[None, message, "rule skipped"]], columns=FAILURE_COLUMNS)
    return RuleOutcome(total_checked=0, total_failed=0, failures=failures)


def _append_result(
    results: list[dict],
    entry: dict,
    outcome: RuleOutcome,
    run_at: str,
    status: str,
    error_message: str = "",
) -> None:
    results.append({
        "index_code": entry["index_code"],
        "element": entry["element"],
        "rule_family": entry["rule_family"],
        "table": entry["table"],
        "total_checked": outcome.total_checked,
        "total_failed": outcome.total_failed,
        "status": status,
        "error_message": error_message,
        "run_at": run_at,
    })


def _add_failures(
    all_failures: list[pd.DataFrame],
    entry: dict,
    outcome: RuleOutcome,
    run_at: str,
    status: str,
) -> None:
    failures = outcome.failures.copy()
    if failures.empty:
        return
    failures["run_at"] = run_at
    failures["index_code"] = entry["index_code"]
    failures["element"] = entry["element"]
    failures["status"] = status
    all_failures.append(failures)


def _run_entries(
    entries: list[dict],
    data: dict[str, pd.DataFrame],
    skipped: dict[str, str],
    output_dir: Path,
    strict: bool,
) -> None:
    run_at = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []
    all_failures: list[pd.DataFrame] = []
    error_count = 0

    for entry in CATALOG:
        if entry["index_code"] in skipped:
            outcome = _skip_outcome(skipped[entry["index_code"]])
            _append_result(results, entry, outcome, run_at, "SKIPPED", skipped[entry["index_code"]])
            _add_failures(all_failures, entry, outcome, run_at, "SKIPPED")
            continue
        if entry not in entries:
            continue

        params = dict(entry["params"])
        params.setdefault("table", entry["table"])
        try:
            outcome = RULE_FUNCTIONS[entry["rule_family"]](data, params)
            status = "FAIL" if outcome.total_failed else "PASS"
            error_message = ""
        except Exception as exc:
            error_count += 1
            status = "ERROR"
            error_message = str(exc)
            print(f"ERROR: {entry['index_code']} failed: {exc}", flush=True)
            outcome = _error_outcome(error_message)

        _append_result(results, entry, outcome, run_at, status, error_message)
        _add_failures(all_failures, entry, outcome, run_at, status)

    failure_frame = pd.concat(all_failures, ignore_index=True) if all_failures else pd.DataFrame()
    scorecard_path = write_scorecard(results, output_dir / "scorecard.csv")
    failures_path = write_failures(failure_frame, output_dir / "failures.csv")
    status_counts = pd.Series([row["status"] for row in results]).value_counts().to_dict()

    print(f"Catalog entries evaluated: {len(results)}")
    print(f"Status counts: {status_counts}")
    print(f"Total data failures: {int(sum(result['total_failed'] for result in results))}")
    print(f"Outputs: {scorecard_path}, {failures_path}")
    if strict and error_count:
        raise RuntimeError(f"Governance completed with {error_count} rule implementation error(s)")


def run(
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    source: str = "minio",
    config_path: str = "config/config.yaml",
    bronze_run_prefix: str | None = None,
    bronze_run_path: str | Path | None = None,
    strict: bool = True,
) -> None:
    output_dir = Path(output_dir)
    if source == "synthetic":
        data = load_tables(_required_tables())
        runnable = []
        skipped = {}
        available_tables = set(data)
        for entry in CATALOG:
            missing_tables = sorted(_entry_tables(entry) - available_tables)
            missing_columns = _missing_entry_columns(entry, data) if not missing_tables else []
            if missing_tables:
                skipped[entry["index_code"]] = "missing synthetic table(s): " + ", ".join(missing_tables)
            elif missing_columns:
                skipped[entry["index_code"]] = "missing synthetic column(s): " + ", ".join(missing_columns)
            else:
                runnable.append(entry)
        _run_entries(runnable, data, skipped, output_dir, strict)
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        if source == "local":
            if bronze_run_path is None:
                raise ValueError("--bronze-run-path is required for governance --source local")
            bronze_source = _source_from_local(bronze_run_path)
        elif source == "minio":
            bronze_source = _source_from_minio(config_path, bronze_run_prefix, tmpdir)
        else:
            raise ValueError(f"Unsupported governance source: {source}")

        available_tables = set(bronze_source.tables)
        runnable = []
        skipped = {}
        for entry in CATALOG:
            missing_tables = sorted(_entry_tables(entry) - available_tables)
            if missing_tables:
                skipped[entry["index_code"]] = "missing bronze table(s): " + ", ".join(missing_tables)
            else:
                runnable.append(entry)

        required_columns = _required_columns(runnable)
        data = _load_bronze_tables(bronze_source, required_columns)
        manifest_path = output_dir / "bronze_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(bronze_source.manifest, indent=2, sort_keys=True), encoding="utf-8")
        _run_entries(runnable, data, skipped, output_dir, strict)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run JNE governance checks against a bronze run.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--source", choices=("minio", "local", "synthetic"), default="minio")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--bronze-run-prefix", default=None)
    parser.add_argument("--bronze-run-path", default=None)
    parser.add_argument("--no-strict", action="store_true", help="Do not fail the process on rule implementation errors")
    args = parser.parse_args()
    run(
        output_dir=args.output_dir,
        source=args.source,
        config_path=args.config,
        bronze_run_prefix=args.bronze_run_prefix,
        bronze_run_path=args.bronze_run_path,
        strict=not args.no_strict,
    )


if __name__ == "__main__":
    main()
