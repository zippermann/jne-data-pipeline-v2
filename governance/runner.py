"""Governance runner for bronze Parquet pipeline outputs.

The production path reads the run manifest written by ``extractor.bronze``,
loads only the bronze tables needed by runnable catalog entries, and emits a
single long CNOTE-level result. A synthetic mode remains available for local
rule tests and demos, but Airflow uses the bronze manifest path.
"""

from __future__ import annotations

import argparse
import json
import os
import re
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
from governance.output import (
    GovernanceResultWriter,
    RESULT_CNOTE_COLUMNS,
    write_rule_summary,
    write_rule_summary_parquet,
)
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
DROURATE_TABLE = "CMS_DROURATE"
DROURATE_CODE_PATTERN = re.compile(r"^([A-Z]{3}[0-9]{5})([A-Z]{3}[0-9]{5})$")


@dataclass(frozen=True)
class BronzeTable:
    table: str
    output_name: str
    row_count: int | None = None
    source_prefix: str | None = None


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
        "CMS_DROURATE": pd.DataFrame({
            "DROURATE_CODE": ["CGKSUB", "CGK10000SUB10000"],
            "DROURATE_SERVICE": ["REG", "YES"],
            "__origin_component": ["CGK", "CGK10000"],
            "__destination_component": ["SUB", "SUB10000"],
        }),
        "ORA_BRANCH": pd.DataFrame({
            "BRANCH_CODE": ["CGK", "SUB"],
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
        if entry.get("enabled") is False:
            continue
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
    if rule_family == "transit_manifest_required_for_origin_mismatch":
        tables.update({"CMS_DSMU", "CMS_MSMU", "CMS_MFBAG"})
    return {table.upper() for table in tables if table}


def _entry_columns(entry: dict) -> dict[str, set[str]]:
    params = entry["params"]
    table = entry["table"]
    columns: dict[str, set[str]] = {}

    def add(table_name: str | None, column: str | None) -> None:
        if table_name and column:
            columns.setdefault(table_name.upper(), set()).add(column)

    add(table, params.get("column"))
    add(table, params.get("condition_column"))
    for column in params.get("columns", []):
        add(table, column)

    for prefix in ("left", "right", "start", "end"):
        add(params.get(f"{prefix}_table"), params.get(f"{prefix}_column"))
        add(params.get(f"{prefix}_table"), params.get(f"{prefix}_join_key") or params.get("join_key"))

    add(params.get("left_table"), params.get("left_column"))
    add(params.get("left_table"), params.get("start_column"))
    add(params.get("right_table"), params.get("right_column"))
    current_table = params.get("left_table") or params.get("detail_table")
    for step in params.get("joins", []):
        add(current_table, step.get("left_on"))
        add(step.get("table"), step.get("right_on"))
        current_table = step.get("table")
    if params.get("joins") or params.get("detail_table"):
        add(current_table, params.get("right_column"))
        add(current_table, params.get("end_column"))
        add(current_table, params.get("detail_key"))

    add(params.get("child_table"), params.get("child_column"))
    add(params.get("child_table"), params.get("child_key"))
    add(params.get("child_table"), params.get("count_column"))
    add(params.get("parent_table"), params.get("parent_column"))
    reference_column = {
        "origin": "__origin_component",
        "destination": "__destination_component",
    }.get(params.get("reference_component"), params.get("reference_column"))
    add(params.get("reference_table"), reference_column)
    for reference in params.get("references", []):
        add(reference.get("table"), reference.get("column"))
    add(params.get("master_table"), params.get("master_key"))
    add(params.get("master_table"), params.get("master_column"))
    add(params.get("master_table"), params.get("cnote_column"))
    add(params.get("master_table"), params.get("master_key"))
    add(params.get("master_table"), params.get("master_value_column"))
    add(params.get("master_table"), params.get("master_count_column"))
    add(params.get("master_table"), params.get("cnote_column"))
    if not params.get("joins"):
        add(params.get("detail_table"), params.get("detail_key"))
    add(params.get("detail_table"), params.get("detail_value_column"))
    add(params.get("detail_table"), params.get("detail_count_column"))
    rule_family = entry.get("rule_family")
    if rule_family == "manifest_code_sequence":
        add("CMS_MFCNOTE", "MFCNOTE_NO")
        add("CMS_MFCNOTE", "MFCNOTE_MAN_NO")
        add("CMS_MANIFEST", "MANIFEST_NO")
        add("CMS_MANIFEST", params.get("manifest_code_column", "MANIFEST_CODE"))
        add(params.get("date_table", "CMS_MANIFEST"), params.get("date_column", "MANIFEST_CRDATE"))
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
    if rule_family == "transit_manifest_required_for_origin_mismatch":
        add("CMS_DSMU", "DSMU_NO")
        add("CMS_DSMU", "DSMU_BAG_NO")
        add("CMS_DSMU", "DSMU_BAG_ORIGIN")
        add("CMS_MSMU", "MSMU_NO")
        add("CMS_MSMU", "MSMU_ORIGIN")
        add("CMS_MFBAG", "MFBAG_NO")
        add("CMS_MFBAG", "MFBAG_MAN_NO")
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
            source_prefix=item.get("source_prefix"),
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


def _upload_governance_outputs_to_minio(source: GovernanceSource, paths: Iterable[Path]) -> list[str]:
    assert source.client is not None and source.bucket is not None and source.prefix is not None
    uploaded = []
    for path in paths:
        object_name = f"{source.prefix}/governance/{path.name}"
        source.client.fput_object(source.bucket, object_name, str(path))
        uploaded.append(f"s3://{source.bucket}/{object_name}")
    return uploaded


def _list_minio_parquet_objects(source: GovernanceSource, table: BronzeTable) -> list[str]:
    assert source.client is not None and source.bucket is not None and source.prefix is not None
    prefix = table.source_prefix or f"{source.prefix}/{table.output_name}/"
    return sorted(
        item.object_name
        for item in source.client.list_objects(source.bucket, prefix=prefix, recursive=True)
        if item.object_name.endswith(".parquet")
    )


def _read_parquet_files(paths: Iterable[Path], columns: list[str], *, distinct: bool = False) -> pd.DataFrame:
    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyarrow is required to run governance against bronze Parquet. "
            "Install dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    requested = list(dict.fromkeys(columns))
    frames = []
    for path in paths:
        parquet_file = pq.ParquetFile(path)
        available = set(parquet_file.schema_arrow.names)
        read_columns = [column for column in requested if column in available]
        if distinct and read_columns:
            for batch in parquet_file.iter_batches(batch_size=100_000, columns=read_columns):
                frames.append(batch.to_pandas().drop_duplicates())
        elif read_columns:
            frame = pq.read_table(path, columns=read_columns).to_pandas()
            if distinct:
                frame = frame.drop_duplicates()
        else:
            frame = pd.DataFrame(index=range(parquet_file.metadata.num_rows))
        if not distinct or not read_columns:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    return result.drop_duplicates(ignore_index=True) if distinct else result


def _normalized_candidate_values(values: pd.Series) -> set[str]:
    strings = values.fillna("").astype("string").str.strip().str.replace(r"\.0+$", "", regex=True)
    return set(strings[strings.ne("")].dropna().astype(str))


def _drourate_reference_column(params: dict) -> str:
    return {
        "origin": "__origin_component",
        "destination": "__destination_component",
    }.get(params.get("reference_component"), params["reference_column"])


def _frame_from_column_values(columns: dict[str, set[str]]) -> pd.DataFrame:
    ordered = {column: sorted(values) for column, values in columns.items()}
    max_length = max((len(values) for values in ordered.values()), default=0)
    return pd.DataFrame({
        column: values + [pd.NA] * (max_length - len(values))
        for column, values in ordered.items()
    })


def _streamed_reference_tables(entries: Iterable[dict], source: GovernanceSource) -> set[str]:
    streamed = set()
    for entry in entries:
        reference_table = entry.get("params", {}).get("reference_table")
        if reference_table != DROURATE_TABLE:
            continue
        table = source.tables.get(DROURATE_TABLE)
        if table:
            streamed.add(DROURATE_TABLE)
    return streamed


def _build_drourate_candidates(entries: Iterable[dict], data: dict[str, pd.DataFrame]) -> dict[str, set[str]]:
    candidates = {
        "DROURATE_CODE": set(),
        "DROURATE_SERVICE": set(),
        "__origin_component": set(),
        "__destination_component": set(),
    }
    for entry in entries:
        params = entry.get("params", {})
        if params.get("reference_table") != DROURATE_TABLE:
            continue
        table_name = entry["table"]
        column = params["column"]
        if table_name not in data or column not in data[table_name].columns:
            continue
        reference_column = _drourate_reference_column(params)
        candidates[reference_column].update(_normalized_candidate_values(data[table_name][column]))
    return candidates


def _drourate_scan_columns(candidates: dict[str, set[str]]) -> list[str]:
    columns = []
    if candidates["DROURATE_CODE"] or candidates["__origin_component"] or candidates["__destination_component"]:
        columns.append("DROURATE_CODE")
    if candidates["DROURATE_SERVICE"]:
        columns.append("DROURATE_SERVICE")
    return columns


def _drourate_done(candidates: dict[str, set[str]], matched: dict[str, set[str]]) -> bool:
    return all(candidates[column] <= matched[column] for column in candidates)


def _scan_drourate_path(path: Path, candidates: dict[str, set[str]], matched: dict[str, set[str]]) -> tuple[int, int]:
    import pyarrow.parquet as pq

    batches = 0
    malformed_codes = 0
    columns = _drourate_scan_columns(candidates)
    if not columns:
        return batches, malformed_codes

    parquet_file = pq.ParquetFile(path)
    available = set(parquet_file.schema_arrow.names)
    read_columns = [column for column in columns if column in available]
    if not read_columns:
        return batches, malformed_codes

    for batch in parquet_file.iter_batches(batch_size=1_000_000, columns=read_columns):
        batches += 1
        frame = batch.to_pandas()
        if "DROURATE_SERVICE" in frame.columns and candidates["DROURATE_SERVICE"]:
            matched["DROURATE_SERVICE"].update(
                _normalized_candidate_values(frame["DROURATE_SERVICE"]) & candidates["DROURATE_SERVICE"]
            )
        if "DROURATE_CODE" in frame.columns:
            code_candidates = candidates["DROURATE_CODE"]
            origin_candidates = candidates["__origin_component"]
            destination_candidates = candidates["__destination_component"]
            if code_candidates or origin_candidates or destination_candidates:
                for code in _normalized_candidate_values(frame["DROURATE_CODE"]):
                    if code in code_candidates:
                        matched["DROURATE_CODE"].add(code)
                    code_match = DROURATE_CODE_PATTERN.fullmatch(code)
                    if not code_match:
                        malformed_codes += 1
                        continue
                    origin, destination = code_match.groups()
                    if origin in origin_candidates:
                        matched["__origin_component"].add(origin)
                    if destination in destination_candidates:
                        matched["__destination_component"].add(destination)
        if _drourate_done(candidates, matched):
            break
    return batches, malformed_codes


def _stream_drourate_reference(source: GovernanceSource, entries: list[dict], data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    candidates = _build_drourate_candidates(entries, data)
    matched = {column: set() for column in candidates}
    started = time.monotonic()
    batches = 0
    malformed_codes = 0

    if source.run_path is not None:
        table = source.tables[DROURATE_TABLE]
        paths = sorted((source.run_path / table.output_name).glob("part-*.parquet"))
        for path in paths:
            path_batches, path_malformed = _scan_drourate_path(path, candidates, matched)
            batches += path_batches
            malformed_codes += path_malformed
            if _drourate_done(candidates, matched):
                break
    else:
        assert source.client is not None and source.bucket is not None and source.tmpdir is not None
        table = source.tables[DROURATE_TABLE]
        for object_name in _list_minio_parquet_objects(source, table):
            local_path = source.tmpdir / f"{table.output_name}-{Path(object_name).name}"
            source.client.fget_object(source.bucket, object_name, str(local_path))
            try:
                path_batches, path_malformed = _scan_drourate_path(local_path, candidates, matched)
                batches += path_batches
                malformed_codes += path_malformed
            finally:
                local_path.unlink(missing_ok=True)
            if _drourate_done(candidates, matched):
                break

    candidate_counts = {column: len(values) for column, values in candidates.items()}
    matched_counts = {column: len(values) for column, values in matched.items()}
    unmatched_counts = {column: len(candidates[column] - matched[column]) for column in candidates}
    print(
        "Streamed CMS_DROURATE reference candidates: "
        f"candidates={candidate_counts}, matched={matched_counts}, unmatched={unmatched_counts}, "
        f"batches={batches}, malformed_codes={malformed_codes}, elapsed={time.monotonic() - started:.1f}s",
        flush=True,
    )
    return _frame_from_column_values(matched)


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


def _entry_column_name(entry: dict) -> str:
    params = entry["params"]
    candidates = []
    for key in (
        "column",
        "left_column",
        "right_column",
        "start_column",
        "end_column",
        "master_value_column",
        "master_count_column",
        "detail_value_column",
        "detail_count_column",
        "child_column",
        "parent_column",
    ):
        value = params.get(key)
        if value and value not in candidates:
            candidates.append(str(value))
    for value in params.get("columns", []):
        if value and value not in candidates:
            candidates.append(str(value))
    return ", ".join(candidates)


def _string_key_values(values: pd.Series) -> pd.Series:
    return values.fillna("").astype("string").str.strip()


def _group_unique_strings(frame: pd.DataFrame, key_column: str, value_column: str) -> dict[str, list[str]]:
    if frame.empty or key_column not in frame.columns or value_column not in frame.columns:
        return {}
    work = pd.DataFrame({
        "key": _string_key_values(frame[key_column]),
        "value": _string_key_values(frame[value_column]),
    })
    work = work.loc[work["key"].ne("") & work["value"].ne("")].drop_duplicates()
    if work.empty:
        return {}
    grouped = work.groupby("key", sort=False)["value"].agg(list)
    return {str(key): [str(value) for value in values] for key, values in grouped.items()}


def _bag_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    mfcnote = data.get("CMS_MFCNOTE")
    if mfcnote is None:
        return {}
    return _group_unique_strings(mfcnote, "MFCNOTE_BAG_NO", "MFCNOTE_NO")


def _dmbag_to_cnotes(data: dict[str, pd.DataFrame], bag_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    dmbag = data.get("CMS_DMBAG")
    if dmbag is None or not bag_to_cnotes:
        return {}

    bridge = dict(bag_to_cnotes)
    if "DMBAG_NO" not in dmbag.columns or "DMBAG_BAG_NO" not in dmbag.columns:
        return bridge

    bag_rows = pd.DataFrame({
        "dmbag_no": _string_key_values(dmbag["DMBAG_NO"]),
        "bag_no": _string_key_values(dmbag["DMBAG_BAG_NO"]),
    })
    bag_rows = bag_rows.loc[bag_rows["dmbag_no"].ne("") & bag_rows["bag_no"].ne("")].drop_duplicates()
    for dmbag_no, group in bag_rows.groupby("dmbag_no", sort=False):
        cnotes: list[str] = []
        seen: set[str] = set()
        for bag_no in group["bag_no"]:
            for cnote_no in bag_to_cnotes.get(str(bag_no), []):
                if cnote_no not in seen:
                    seen.add(cnote_no)
                    cnotes.append(cnote_no)
        if cnotes:
            bridge[str(dmbag_no)] = cnotes
    return bridge


def _entity_bridges(data: dict[str, pd.DataFrame]) -> dict[str, dict[str, list[str]]]:
    bag_to_cnotes = _bag_to_cnotes(data)
    dmbag_to_cnotes = _dmbag_to_cnotes(data, bag_to_cnotes)
    mfcnote_to_cnote: dict[str, list[str]] = {}
    mfcnote = data.get("CMS_MFCNOTE")
    if mfcnote is not None and "MFCNOTE_NO" in mfcnote.columns:
        values = _string_key_values(mfcnote["MFCNOTE_NO"])
        mfcnote_to_cnote = {str(value): [str(value)] for value in values[values.ne("")].drop_duplicates()}
    return {
        "CMS_MFCNOTE": mfcnote_to_cnote,
        "CMS_MFBAG": bag_to_cnotes,
        "CMS_DMBAG": dmbag_to_cnotes,
    }


def _cnote_universe(data: dict[str, pd.DataFrame]) -> set[str]:
    cnote = data.get("CMS_CNOTE")
    if cnote is None or "CNOTE_NO" not in cnote.columns:
        return set()
    values = _string_key_values(cnote["CNOTE_NO"])
    return set(values[values.ne("")].drop_duplicates())


def _safe_linked_cnotes(values: Iterable[str], cnote_universe: set[str]) -> list[str]:
    linked: list[str] = []
    seen: set[str] = set()
    for value in values:
        cnote_no = str(value)
        if not cnote_no or cnote_no in seen:
            continue
        if cnote_universe and cnote_no not in cnote_universe:
            continue
        seen.add(cnote_no)
        linked.append(cnote_no)
    return linked


def _entity_type(entry: dict) -> str:
    table = str(entry.get("table", "")).upper()
    if table.startswith("CMS_"):
        return table.removeprefix("CMS_")
    return table


def _check_rows_frame(
    entry: dict,
    outcome: RuleOutcome,
    entity_bridges: dict[str, dict[str, list[str]]] | None = None,
    cnote_universe: set[str] | None = None,
) -> pd.DataFrame:
    checks = outcome.checks
    if checks is None or checks.empty:
        return pd.DataFrame()
    rows = checks.copy()
    rows = rows.rename(columns={"cnote_no": "entity_id"})
    rows["entity_id"] = _string_key_values(rows["entity_id"])
    table_name = str(entry["table"]).upper()
    rows["entity_type"] = _entity_type(entry)
    cnotes = cnote_universe or set()

    bridge_map = entity_bridges or {}
    bridge = bridge_map.get(table_name, {})
    if table_name in bridge_map:
        rows["_linked_cnotes"] = rows["entity_id"].map(lambda value: _safe_linked_cnotes(bridge.get(str(value), []), cnotes))
        rows["_link_method"] = f"{table_name.lower()}_bridge"
    elif table_name == "CMS_CNOTE":
        rows["_linked_cnotes"] = rows["entity_id"].map(lambda value: [str(value)] if (not cnotes or str(value) in cnotes) else [])
        rows["_link_method"] = "direct_cnote"
    else:
        rows["_linked_cnotes"] = rows["entity_id"].map(lambda value: [str(value)] if str(value) in cnotes else [])
        rows["_link_method"] = "direct_cnote_value"

    rows["cnote_no"] = rows["_linked_cnotes"].map(lambda values: values[0] if len(values) == 1 else "")

    rows["index_code"] = entry["index_code"]
    rows["main_indicator"] = entry.get("indicator", "")
    rows["column_name"] = _entry_column_name(entry)
    rows["table_name"] = entry["table"]
    rows["impact_billing"] = entry.get("impact_billing", "")
    rows["impact_operational"] = entry.get("impact_operational", "")
    return rows


def _result_cnote_rows(rows: pd.DataFrame) -> pd.DataFrame:
    link_rows: list[dict[str, str]] = []
    for _, row in rows.iterrows():
        result_id = str(row["result_id"])
        link_method = str(row["_link_method"])
        cnotes = row["_linked_cnotes"]
        if not isinstance(cnotes, list):
            continue
        for cnote_no in cnotes:
            link_rows.append({
                "result_id": result_id,
                "cnote_no": str(cnote_no),
                "link_method": link_method,
                "link_confidence": "safe",
            })
    return pd.DataFrame(link_rows, columns=RESULT_CNOTE_COLUMNS)


def _rule_summary_row(
    entry: dict,
    status: str,
    total_checked: int = 0,
    total_failed: int = 0,
    result_rows: int = 0,
    skip_reason: str = "",
    error_message: str = "",
) -> dict:
    return {
        "index_code": entry["index_code"],
        "element": entry.get("element", ""),
        "main_indicator": entry.get("indicator", ""),
        "rule_family": entry.get("rule_family", ""),
        "table_name": entry.get("table", ""),
        "status": status,
        "total_checked": total_checked,
        "total_failed": total_failed,
        "result_rows": result_rows,
        "skip_reason": skip_reason,
        "error_message": error_message,
        "impact_billing": entry.get("impact_billing", ""),
        "impact_operational": entry.get("impact_operational", ""),
    }


def _run_entries(
    entries: list[dict],
    data: dict[str, pd.DataFrame],
    skipped: dict[str, str],
    output_dir: Path,
    strict: bool,
    fail_on_skipped: bool = False,
    upload_source: GovernanceSource | None = None,
) -> None:
    run_at = datetime.now(timezone.utc).isoformat()
    summary_rows: list[dict] = []
    status_counts: dict[str, int] = {}
    row_status_counts: dict[str, int] = {}
    result_row_total = 0
    error_count = 0
    disabled_count = 0
    skipped_count = 0
    results_path = output_dir / "governance_results.csv"
    results_parquet_path = output_dir / "governance_results.parquet"
    result_cnotes_path = output_dir / "governance_result_cnotes.csv"
    result_cnotes_parquet_path = output_dir / "governance_result_cnotes.parquet"
    bridges = _entity_bridges(data)
    cnotes = _cnote_universe(data)
    next_result_number = 1

    with (
        GovernanceResultWriter(results_path, results_parquet_path) as result_writer,
        GovernanceResultWriter(
            result_cnotes_path,
            result_cnotes_parquet_path,
            columns=RESULT_CNOTE_COLUMNS,
        ) as result_cnote_writer,
    ):
        for entry in CATALOG:
            if entry.get("enabled") is False:
                disabled_count += 1
                continue
            if entry["index_code"] in skipped:
                skipped_count += 1
                summary_rows.append(_rule_summary_row(entry, "SKIPPED", skip_reason=skipped[entry["index_code"]]))
                continue
            if entry not in entries:
                continue

            params = dict(entry["params"])
            params.setdefault("table", entry["table"])
            try:
                outcome = RULE_FUNCTIONS[entry["rule_family"]](data, params)
                if outcome.checks is None or len(outcome.checks) == 0:
                    status = "NO_ROWS"
                else:
                    status = "FAIL" if outcome.total_failed else "PASS"
                error_message = ""
            except Exception as exc:
                error_count += 1
                status = "ERROR"
                error_message = str(exc)
                print(f"ERROR: {entry['index_code']} failed: {exc}", flush=True)
                outcome = _error_outcome(error_message)

            status_counts[status] = status_counts.get(status, 0) + 1
            check_rows = _check_rows_frame(entry, outcome, bridges, cnotes)
            result_rows = len(check_rows)
            if not check_rows.empty:
                result_ids = [
                    f"R{result_number:012d}"
                    for result_number in range(next_result_number, next_result_number + result_rows)
                ]
                next_result_number += result_rows
                check_rows.insert(0, "result_id", result_ids)
                result_cnote_rows = _result_cnote_rows(check_rows)
                check_rows = check_rows.drop(columns=["_linked_cnotes", "_link_method"])
                for row_status, count in check_rows["status"].value_counts().items():
                    row_status_counts[str(row_status)] = row_status_counts.get(str(row_status), 0) + int(count)
                result_row_total += result_writer.write(check_rows)
                result_cnote_writer.write(result_cnote_rows)
            summary_rows.append(
                _rule_summary_row(
                    entry,
                    status,
                    total_checked=outcome.total_checked,
                    total_failed=outcome.total_failed,
                    result_rows=result_rows,
                    error_message=error_message,
                )
            )

    summary_frame = pd.DataFrame(summary_rows)
    summary_path = write_rule_summary(summary_frame, output_dir / "governance_rule_summary.csv")
    summary_parquet_path = write_rule_summary_parquet(summary_frame, output_dir / "governance_rule_summary.parquet")
    uploaded_paths = (
        _upload_governance_outputs_to_minio(
            upload_source,
            [
                results_path,
                results_parquet_path,
                result_cnotes_path,
                result_cnotes_parquet_path,
                summary_path,
                summary_parquet_path,
            ],
        )
        if upload_source is not None
        else []
    )

    print(f"Catalog entries evaluated: {sum(status_counts.values())}")
    print(f"Rule status counts: {status_counts}")
    print(f"Disabled entries: {disabled_count}")
    print(f"Skipped entries: {skipped_count}")
    print(f"CNOTE result rows: {result_row_total:,}")
    print(f"CNOTE row status counts: {row_status_counts}")
    print(f"Output: {results_path}")
    print(f"Output parquet: {results_parquet_path}")
    print(f"Rule summary: {summary_path}")
    print(f"Rule summary parquet: {summary_parquet_path}")
    for uploaded_path in uploaded_paths:
        print(f"Uploaded governance output: {uploaded_path}")
    if skipped_count:
        print(f"WARNING: Governance skipped {skipped_count} active rule(s); see {summary_path}", flush=True)
    if fail_on_skipped and skipped_count:
        raise RuntimeError(f"Governance skipped {skipped_count} active rule(s); see {summary_path}")
    if strict and error_count:
        raise RuntimeError(f"Governance completed with {error_count} rule implementation error(s)")


def run(
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    source: str = "minio",
    config_path: str = "config/config.yaml",
    bronze_run_prefix: str | None = None,
    bronze_run_path: str | Path | None = None,
    strict: bool = True,
    fail_on_skipped: bool = False,
) -> None:
    output_dir = Path(output_dir)
    if source == "synthetic":
        data = load_tables(_required_tables())
        runnable = []
        skipped = {}
        available_tables = set(data)
        for entry in CATALOG:
            if entry.get("enabled") is False:
                continue
            missing_tables = sorted(_entry_tables(entry) - available_tables)
            missing_columns = _missing_entry_columns(entry, data) if not missing_tables else []
            if missing_tables:
                skipped[entry["index_code"]] = "missing synthetic table(s): " + ", ".join(missing_tables)
            elif missing_columns:
                skipped[entry["index_code"]] = "missing synthetic column(s): " + ", ".join(missing_columns)
            else:
                runnable.append(entry)
        _run_entries(runnable, data, skipped, output_dir, strict, fail_on_skipped=fail_on_skipped)
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
            if entry.get("enabled") is False:
                continue
            missing_tables = sorted(_entry_tables(entry) - available_tables)
            if missing_tables:
                skipped[entry["index_code"]] = "missing bronze table(s): " + ", ".join(missing_tables)
            else:
                runnable.append(entry)

        streamed_reference_tables = _streamed_reference_tables(runnable, bronze_source)
        required_columns = _required_columns(runnable)
        load_columns = {
            table: columns
            for table, columns in required_columns.items()
            if table not in streamed_reference_tables
        }
        data = _load_bronze_tables(bronze_source, load_columns)
        if DROURATE_TABLE in streamed_reference_tables:
            data[DROURATE_TABLE] = _stream_drourate_reference(bronze_source, runnable, data)
        available_runnable = []
        for entry in runnable:
            missing_columns = _missing_entry_columns(entry, data)
            if missing_columns:
                skipped[entry["index_code"]] = "missing bronze column(s): " + ", ".join(missing_columns)
            else:
                available_runnable.append(entry)
        _run_entries(
            available_runnable,
            data,
            skipped,
            output_dir,
            strict,
            fail_on_skipped=fail_on_skipped,
            upload_source=bronze_source if source == "minio" else None,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run JNE governance checks against a bronze run.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--source", choices=("minio", "local", "synthetic"), default="minio")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--bronze-run-prefix", default=None)
    parser.add_argument("--bronze-run-path", default=None)
    parser.add_argument("--no-strict", action="store_true", help="Do not fail the process on rule implementation errors")
    parser.add_argument("--fail-on-skipped", action="store_true", help="Fail when any active rule is skipped")
    args = parser.parse_args()
    run(
        output_dir=args.output_dir,
        source=args.source,
        config_path=args.config,
        bronze_run_prefix=args.bronze_run_prefix,
        bronze_run_path=args.bronze_run_path,
        strict=not args.no_strict,
        fail_on_skipped=args.fail_on_skipped,
    )


if __name__ == "__main__":
    main()
