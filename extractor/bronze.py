"""Relational bronze extraction for the JNE pipeline.

This module intentionally keeps the first bronze implementation in one place:
config loading, window resolution, Oracle scope-table creation, table inventory,
Parquet writing, and the CLI runner. If the pipeline grows, split it back out
around stable boundaries; for now one file is easier to audit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


logger = logging.getLogger(__name__)
CODE_VERSION = "bronze-minio-v2026-06-01-01"
ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")
VALID_IDENTIFIER = re.compile(r"[^A-Z0-9_]")


# ============================================================
# CONFIG
# ============================================================

def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def repl(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            return os.getenv(name, default or "")

        return ENV_PATTERN.sub(repl, value)
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    return value


def load_config(path: str | Path = "config/config.yaml") -> dict[str, Any]:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyYAML is required to load config files. Install dependencies with "
            "`pip install -r requirements.txt` or rebuild the Airflow image."
        ) from exc

    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    return _expand_env(raw)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


# ============================================================
# WINDOW
# ============================================================

@dataclass(frozen=True)
class Window:
    start: date
    end: date

    @property
    def start_label(self) -> str:
        return self.start.isoformat()

    @property
    def end_label(self) -> str:
        return self.end.isoformat()


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def resolve_window(config: dict) -> Window:
    window = config["extraction"]["window"]
    mode = window.get("mode", "relative")
    if mode == "relative":
        year, month = [int(part) for part in window["anchor_month"].split("-", 1)]
        start = date(year, month, 1)
        end = _add_months(start, int(window.get("num_months", 1)))
        return Window(start=start, end=end)
    if mode == "explicit":
        return Window(
            start=date.fromisoformat(window["start_date"]),
            end=date.fromisoformat(window["end_date"]),
        )
    raise ValueError(f"Unsupported extraction.window.mode: {mode}")


# ============================================================
# ORACLE
# ============================================================

@dataclass(frozen=True)
class OracleSettings:
    host: str
    port: int
    user: str
    password: str
    source_schema: str = "JNE"
    sid: str = ""
    service_name: str = ""
    fetch_arraysize: int = 50000
    prefetch_rows: int | None = None

    @classmethod
    def from_config(cls, config: dict) -> "OracleSettings":
        oracle = config["oracle"]
        return cls(
            host=oracle.get("host", ""),
            port=int(oracle.get("port", 1521)),
            sid=oracle.get("sid", ""),
            service_name=oracle.get("service_name", ""),
            user=oracle.get("user", ""),
            password=oracle.get("password", ""),
            source_schema=oracle.get("source_schema", "JNE"),
            fetch_arraysize=int(oracle.get("fetch_arraysize", 50000)),
            prefetch_rows=int(oracle.get("prefetch_rows", int(oracle.get("fetch_arraysize", 50000)) + 1)),
        )

    @property
    def dsn(self) -> str:
        if self.sid:
            return (
                f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})"
                f"(PORT={self.port}))(CONNECT_DATA=(SID={self.sid})))"
            )
        return f"{self.host}:{self.port}/{self.service_name}"


@contextmanager
def connect(settings: OracleSettings) -> Iterator[Any]:
    try:
        import oracledb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "oracledb is required for extraction. Install dependencies with "
            "`pip install -r requirements.txt` or rebuild the Airflow image."
        ) from exc

    conn = oracledb.connect(
        user=settings.user,
        password=settings.password,
        dsn=settings.dsn,
    )
    try:
        yield conn
    finally:
        conn.close()


def table_columns(conn: Any, owner: str, table: str) -> list[str]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM all_tab_columns
            WHERE owner = :owner AND table_name = :table_name
            ORDER BY column_id
            """,
            owner=owner.upper(),
            table_name=table.upper(),
        )
        columns = [row[0] for row in cursor.fetchall()]
    if not columns:
        raise RuntimeError(f"No Oracle metadata found for {owner}.{table}")
    return columns


# ============================================================
# TABLE INVENTORY
# ============================================================

class Stage(str, Enum):
    ANCHOR = "anchor"
    CNOTE = "cnote"
    BAG_MANIFEST = "bag_manifest"
    RUNSHEET_DO = "runsheet_do"
    REFERENCE = "reference"


@dataclass(frozen=True)
class TableSpec:
    table: str
    output_name: str
    stage: Stage
    scope_column: str | None = None
    scope_name: str | None = None
    date_guard_column: str | None = None


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec("CMS_CNOTE", "cms_cnote", Stage.ANCHOR),
    TableSpec("CMS_APICUST", "cms_apicust", Stage.CNOTE, "APICUST_CNOTE_NO", "CNOTE"),
    TableSpec("CMS_CNOTE_AMO", "cms_cnote_amo", Stage.CNOTE, "CNOTE_NO", "CNOTE", "CDATE"),
    TableSpec("CMS_DRCNOTE", "cms_drcnote", Stage.CNOTE, "DRCNOTE_CNOTE_NO", "CNOTE"),
    TableSpec("CMS_MRCNOTE", "cms_mrcnote", Stage.CNOTE, "MRCNOTE_NO", "DRCNOTE", "MRCNOTE_DATE"),
    TableSpec("CMS_DHI_HOC", "cms_dhi_hoc", Stage.CNOTE, "DHI_CNOTE_NO", "CNOTE", "CDATE"),
    TableSpec("CMS_MHI_HOC", "cms_mhi_hoc", Stage.CNOTE, "MHI_NO", "DHI_HOC", "MHI_DATE"),
    TableSpec("CMS_DSTATUS", "cms_dstatus", Stage.CNOTE, "DSTATUS_CNOTE_NO", "CNOTE", "CREATE_DATE"),
    TableSpec("CMS_CNOTE_POD", "cms_cnote_pod", Stage.CNOTE, "CNOTE_POD_NO", "CNOTE", "CNOTE_POD_DATE"),
    TableSpec("CMS_DHOV_RSHEET", "cms_dhov_rsheet", Stage.CNOTE, "DHOV_RSHEET_CNOTE", "CNOTE", "CREATE_DATE"),
    TableSpec("CMS_DHOUNDEL_POD", "cms_dhoundel_pod", Stage.CNOTE, "DHOUNDEL_CNOTE_NO", "CNOTE", "CREATE_DATE"),
    TableSpec("CMS_MHOUNDEL_POD", "cms_mhoundel_pod", Stage.CNOTE, "MHOUNDEL_NO", "DHOUNDEL", "MHOUNDEL_DATE"),
    TableSpec("CMS_DRSHEET", "cms_drsheet", Stage.CNOTE, "DRSHEET_CNOTE_NO", "CNOTE", "DRSHEET_DATE"),
    TableSpec("CMS_DRSHEET_PRA", "cms_drsheet_pra", Stage.CNOTE, "DRSHEET_CNOTE_NO", "CNOTE", "CREATION_DATE"),
    TableSpec("CMS_DBAG_HO", "cms_dbag_ho", Stage.CNOTE, "DBAG_CNOTE_NO", "CNOTE", "CDATE"),
    TableSpec("CMS_DHOCNOTE", "cms_dhocnote", Stage.CNOTE, "DHOCNOTE_CNOTE_NO", "CNOTE", "DHOCNOTE_TDATE"),
    TableSpec("CMS_DHICNOTE", "cms_dhicnote", Stage.CNOTE, "DHICNOTE_CNOTE_NO", "CNOTE", "DHICNOTE_TDATE"),
    TableSpec("CMS_COST_DTRANSIT_AGEN", "cms_cost_dtransit_agen", Stage.CNOTE, "CNOTE_NO", "CNOTE", "ESB_TIME"),
    TableSpec("CMS_MFCNOTE", "cms_mfcnote", Stage.CNOTE, "MFCNOTE_NO", "CNOTE", "MFCNOTE_CRDATE"),
    TableSpec("CMS_DCORRECT_DEST", "cms_dcorrect_dest", Stage.CNOTE, "DCORRECT_CNOTE_NO", "CNOTE", "DCORRECT_CDATE"),
    TableSpec("CMS_MANIFEST", "cms_manifest", Stage.BAG_MANIFEST, "MANIFEST_NO", "MANIFEST", "MANIFEST_DATE"),
    TableSpec("CMS_MFBAG", "cms_mfbag", Stage.BAG_MANIFEST, "MFBAG_MAN_NO", "MANIFEST", "MFBAG_CRDATE"),
    TableSpec("CMS_DMBAG", "cms_dmbag", Stage.BAG_MANIFEST, "DMBAG_BAG_NO", "MFBAG", "ESB_TIME"),
    TableSpec("CMS_MMBAG", "cms_mmbag", Stage.BAG_MANIFEST, "MMBAG_NO", "DMBAG", "MMBAG_DATE"),
    TableSpec("CMS_DSMU", "cms_dsmu", Stage.BAG_MANIFEST, "DSMU_BAG_NO", "DMBAG", "ESB_TIME"),
    TableSpec("CMS_MSMU", "cms_msmu", Stage.BAG_MANIFEST, "MSMU_NO", "SMU", "MSMU_DATE"),
    TableSpec("CMS_COST_MTRANSIT_AGEN", "cms_cost_mtransit_agen", Stage.BAG_MANIFEST, "MANIFEST_NO", "COST_MANIFEST", "MANIFEST_DATE"),
    TableSpec("CMS_MRSHEET", "cms_mrsheet", Stage.RUNSHEET_DO, "MRSHEET_NO", "DRSHEET", "MRSHEET_DATE"),
    TableSpec("CMS_MSJ", "cms_msj", Stage.RUNSHEET_DO, "MSJ_NO", "MSJ", "MSJ_DATE"),
    TableSpec("CMS_RDSJ", "cms_rdsj", Stage.RUNSHEET_DO, "RDSJ_HVI_NO", "HVI", "RDSJ_CDATE"),
    TableSpec("CMS_MHICNOTE", "cms_mhicnote", Stage.RUNSHEET_DO, "MHICNOTE_NO", "HVI", "MHICNOTE_DATE"),
    TableSpec("CMS_MHOCNOTE", "cms_mhocnote", Stage.RUNSHEET_DO, "MHOCNOTE_NO", "HVO", "MHOCNOTE_DATE"),
    TableSpec("CMS_DSJ", "cms_dsj", Stage.RUNSHEET_DO, "DSJ_HVO_NO", "RDSJ_HVO", "DSJ_CDATE"),
    TableSpec("CMS_DROURATE", "cms_drourate", Stage.REFERENCE),
    TableSpec("ORA_ZONE", "ora_zone", Stage.REFERENCE),
    TableSpec("ORA_USER", "ora_user", Stage.REFERENCE),
    TableSpec("T_MDT_CITY_ORIGIN", "t_mdt_city_origin", Stage.REFERENCE),
    TableSpec("LASTMILE_COURIER", "lastmile_courier", Stage.REFERENCE),
)


def specs_for_stage(stage: Stage) -> list[TableSpec]:
    return [spec for spec in TABLE_SPECS if spec.stage == stage]


def selected_specs(config: dict) -> list[TableSpec]:
    requested = config.get("extraction", {}).get("tables") or []
    if not requested:
        return list(TABLE_SPECS)
    requested_normalized = {_normalize_table_name(item) for item in requested}
    specs = [
        spec for spec in TABLE_SPECS
        if _normalize_table_name(spec.table) in requested_normalized
        or _normalize_table_name(spec.output_name) in requested_normalized
    ]
    found = {_normalize_table_name(spec.table) for spec in specs} | {_normalize_table_name(spec.output_name) for spec in specs}
    missing = sorted(item for item in requested_normalized if item not in found)
    if missing:
        raise ValueError(f"Unknown extraction table(s): {missing}")
    return specs


def specs_for_stage_from(specs: Sequence[TableSpec], stage: Stage) -> list[TableSpec]:
    return [spec for spec in specs if spec.stage == stage]


def _normalize_table_name(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


# ============================================================
# SCOPING
# ============================================================

def sanitize_run_id(run_id: str) -> str:
    cleaned = VALID_IDENTIFIER.sub("_", run_id.upper())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = "RUN"
    if cleaned[0].isdigit():
        cleaned = f"R_{cleaned}"
    return cleaned[:40]


@dataclass(frozen=True)
class ScopeSettings:
    source_schema: str
    scope_schema: str
    prefix: str
    run_id: str

    @classmethod
    def from_config(cls, config: dict, run_id: str) -> "ScopeSettings":
        scoping = config["scoping"]
        return cls(
            source_schema=config["oracle"].get("source_schema", "JNE").upper(),
            scope_schema=scoping.get("scope_schema", "HOA").upper(),
            prefix=scoping.get("scope_table_prefix", "BRONZE_SCOPE_").upper(),
            run_id=sanitize_run_id(run_id),
        )

    def table(self, scope_name: str) -> str:
        return f"{self.scope_schema}.{self.prefix}{scope_name.upper()}_{self.run_id}"


def _drop_table(conn: Any, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {table_name} PURGE';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN RAISE; END IF;
            END;
            """
        )


def _parallel_hint(degree: int | None) -> str:
    if not degree or degree <= 1:
        return ""
    return f"/*+ PARALLEL({degree}) */ "


def _create_scope(
    conn: Any,
    table_name: str,
    key_column: str,
    query: str,
    binds: dict,
    ctas_parallel_degree: int = 1,
) -> int:
    _drop_table(conn, table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {table_name} NOLOGGING AS\n"
            f"SELECT {_parallel_hint(ctas_parallel_degree)}DISTINCT {key_column} FROM (\n{query}\n) WHERE {key_column} IS NOT NULL",
            binds,
        )
        cursor.execute(f"CREATE INDEX {_scope_index_name(table_name)} ON {table_name} ({key_column})")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
    conn.commit()
    return count


def _scope_index_name(table_name: str) -> str:
    suffix = table_name.split(".")[-1].upper()
    digest = hashlib.sha1(suffix.encode("utf-8")).hexdigest()[:6].upper()
    return f"IDX_{suffix[:19]}_{digest}"


def _cnote_limit(config: dict) -> int | None:
    raw_limit = config.get("extraction", {}).get("cnote_limit")
    if raw_limit in (None, ""):
        return None
    limit = int(raw_limit)
    if limit <= 0:
        raise ValueError("extraction.cnote_limit must be greater than zero when set")
    return limit


def _scope_date_filter(alias: str, column: str, window: Window, lookback_days: int, lookahead_days: int) -> str:
    return (
        f"{alias}.{column} >= DATE '{window.start_label}' - {lookback_days} "
        f"AND {alias}.{column} < DATE '{window.end_label}' + {lookahead_days}"
    )


def _scope_join_query(
    source_table: str,
    source_alias: str,
    output_expr: str,
    scope_table: str,
    scope_alias: str,
    source_key: str,
    scope_key: str,
    date_filter: str | None = None,
) -> str:
    where_sql = f"\n            WHERE {date_filter}" if date_filter else ""
    return f"""
            SELECT /*+ LEADING({scope_alias} {source_alias}) USE_HASH({source_alias}) */ {output_expr}
            FROM {scope_table} {scope_alias}
            JOIN {source_table} {source_alias}
              ON {source_alias}.{source_key} = {scope_alias}.{scope_key}{where_sql}
            """


def _scope_dependencies() -> dict[str, set[str]]:
    return {
        "DRCNOTE": {"CNOTE"},
        "DHI_HOC": {"CNOTE"},
        "DHOUNDEL": {"CNOTE"},
        "DRSHEET": {"CNOTE"},
        "MANIFEST": {"CNOTE"},
        "MFBAG": {"MANIFEST"},
        "DMBAG": {"MFBAG"},
        "SMU": {"DMBAG"},
        "MMBAG": {"DMBAG"},
        "COST_MANIFEST": {"CNOTE"},
        "HVI": {"CNOTE"},
        "HVO": {"CNOTE"},
        "RDSJ_HVO": {"HVI"},
        "MSJ": {"RDSJ_HVO"},
    }


@dataclass(frozen=True)
class ScopeJob:
    name: str
    table_name: str
    key_column: str
    query: str


def _run_scope_job(
    job: ScopeJob,
    conn: Any,
    ctas_parallel_degree: int,
) -> tuple[str, int]:
    logger.info("Creating Oracle scope %s", job.name)
    count = _create_scope(
        conn,
        job.table_name,
        job.key_column,
        job.query,
        {},
        ctas_parallel_degree,
    )
    logger.info("Oracle scope %s: %s rows", job.name, f"{count:,}")
    return job.name, count


def _run_scope_job_with_new_connection(
    job: ScopeJob,
    oracle_settings: OracleSettings,
    ctas_parallel_degree: int,
) -> tuple[str, int]:
    with connect(oracle_settings) as conn:
        return _run_scope_job(job, conn, ctas_parallel_degree)


def _run_scope_jobs(
    jobs: dict[str, ScopeJob],
    conn: Any,
    oracle_settings: OracleSettings | None,
    ctas_parallel_degree: int,
    scope_workers: int,
    existing_counts: dict[str, int] | None = None,
) -> dict[str, int]:
    counts: dict[str, int] = dict(existing_counts or {})
    created_counts: dict[str, int] = {}
    dependencies = _scope_dependencies()
    pending = set(jobs)
    scope_workers = max(scope_workers, 1)
    if scope_workers > 1 and oracle_settings is None:
        logger.warning("scope_workers=%s requested but oracle_settings was not provided; running scopes sequentially", scope_workers)
        scope_workers = 1

    while pending:
        ready = sorted(
            name
            for name in pending
            if dependencies.get(name, set()) <= set(counts)
        )
        if not ready:
            raise RuntimeError(f"Scope dependency cycle or missing parent for: {sorted(pending)}")

        wave = ready[:scope_workers]
        logger.info("Creating Oracle scope wave: %s", ", ".join(wave))
        if scope_workers == 1:
            name, count = _run_scope_job(jobs[wave[0]], conn, ctas_parallel_degree)
            counts[name] = count
            created_counts[name] = count
        else:
            with ThreadPoolExecutor(max_workers=min(scope_workers, len(wave))) as executor:
                futures = {
                    executor.submit(
                        _run_scope_job_with_new_connection,
                        jobs[name],
                        oracle_settings,
                        ctas_parallel_degree,
                    ): name
                    for name in wave
                }
                for future in as_completed(futures):
                    name, count = future.result()
                    counts[name] = count
                    created_counts[name] = count
        pending.difference_update(wave)
    return created_counts


def materialize_scope_tables(
    conn: Any,
    settings: ScopeSettings,
    window: Window,
    anchor_table: str,
    anchor_date_column: str,
    cnote_limit: int | None = None,
    required_scopes: set[str] | None = None,
    oracle_settings: OracleSettings | None = None,
    date_guard_lookback_days: int = 0,
    date_guard_lookahead_days: int = 30,
    ctas_parallel_degree: int = 1,
    scope_workers: int = 1,
) -> dict[str, int]:
    required_scopes = _expand_required_scopes(required_scopes or _all_scope_names())
    if not required_scopes:
        logger.info("No Oracle scope tables required for selected extraction tables")
        return {}

    src = settings.source_schema
    cnote_scope = settings.table("CNOTE")
    start_literal = f"DATE '{window.start_label}'"
    end_literal = f"DATE '{window.end_label}'"
    counts = {}
    ctas_parallel_degree = max(int(ctas_parallel_degree), 1)
    if "CNOTE" in required_scopes:
        cnote_query = f"""
            SELECT CNOTE_NO
            FROM {src}.{anchor_table}
            WHERE {anchor_date_column} >= {start_literal}
              AND {anchor_date_column} < {end_literal}
            ORDER BY {anchor_date_column}, CNOTE_NO
            """
        if cnote_limit is not None:
            cnote_query = f"""
            SELECT CNOTE_NO
            FROM (
{cnote_query}
            )
            WHERE ROWNUM <= {cnote_limit}
            """
        logger.info("Creating Oracle scope CNOTE")
        counts["CNOTE"] = _create_scope(
            conn,
            cnote_scope,
            "CNOTE_NO",
            cnote_query,
            {},
            ctas_parallel_degree,
        )
        logger.info("Oracle scope CNOTE: %s rows", f"{counts['CNOTE']:,}")

    jobs = {
        "DRCNOTE": ScopeJob(
            "DRCNOTE",
            settings.table("DRCNOTE"),
            "DRCNOTE_NO",
            _scope_join_query(f"{src}.CMS_DRCNOTE", "src", "src.DRCNOTE_NO", cnote_scope, "scope", "DRCNOTE_CNOTE_NO", "CNOTE_NO"),
        ),
        "DHI_HOC": ScopeJob(
            "DHI_HOC",
            settings.table("DHI_HOC"),
            "DHI_NO",
            _scope_join_query(f"{src}.CMS_DHI_HOC", "src", "src.DHI_NO", cnote_scope, "scope", "DHI_CNOTE_NO", "CNOTE_NO"),
        ),
        "DHOUNDEL": ScopeJob(
            "DHOUNDEL",
            settings.table("DHOUNDEL"),
            "DHOUNDEL_NO",
            _scope_join_query(f"{src}.CMS_DHOUNDEL_POD", "src", "src.DHOUNDEL_NO", cnote_scope, "scope", "DHOUNDEL_CNOTE_NO", "CNOTE_NO"),
        ),
        "DRSHEET": ScopeJob(
            "DRSHEET",
            settings.table("DRSHEET"),
            "DRSHEET_NO",
            _scope_join_query(
                f"{src}.CMS_DRSHEET",
                "src",
                "src.DRSHEET_NO",
                cnote_scope,
                "scope",
                "DRSHEET_CNOTE_NO",
                "CNOTE_NO",
                _scope_date_filter("src", "DRSHEET_DATE", window, date_guard_lookback_days, date_guard_lookahead_days),
            ),
        ),
        "MANIFEST": ScopeJob(
            "MANIFEST",
            settings.table("MANIFEST"),
            "MANIFEST_NO",
            _scope_join_query(
                f"{src}.CMS_MFCNOTE",
                "src",
                "src.MFCNOTE_MAN_NO AS MANIFEST_NO",
                cnote_scope,
                "scope",
                "MFCNOTE_NO",
                "CNOTE_NO",
                _scope_date_filter("src", "MFCNOTE_CRDATE", window, date_guard_lookback_days, date_guard_lookahead_days),
            ),
        ),
        "MFBAG": ScopeJob(
            "MFBAG",
            settings.table("MFBAG"),
            "MFBAG_NO",
            f"""
            {_scope_join_query(f"{src}.CMS_MFCNOTE", "src", "src.MFCNOTE_BAG_NO AS MFBAG_NO", cnote_scope, "scope", "MFCNOTE_NO", "CNOTE_NO", _scope_date_filter("src", "MFCNOTE_CRDATE", window, date_guard_lookback_days, date_guard_lookahead_days))}
            UNION
            {_scope_join_query(f"{src}.CMS_MFBAG", "src", "src.MFBAG_NO", settings.table("MANIFEST"), "scope", "MFBAG_MAN_NO", "MANIFEST_NO")}
            """,
        ),
        "DMBAG": ScopeJob(
            "DMBAG",
            settings.table("DMBAG"),
            "DMBAG_NO",
            _scope_join_query(f"{src}.CMS_DMBAG", "src", "src.DMBAG_NO", settings.table("MFBAG"), "scope", "DMBAG_BAG_NO", "MFBAG_NO"),
        ),
        "SMU": ScopeJob(
            "SMU",
            settings.table("SMU"),
            "SMU_NO",
            _scope_join_query(f"{src}.CMS_DSMU", "src", "src.DSMU_NO AS SMU_NO", settings.table("DMBAG"), "scope", "DSMU_BAG_NO", "DMBAG_NO"),
        ),
        "MMBAG": ScopeJob(
            "MMBAG",
            settings.table("MMBAG"),
            "MMBAG_NO",
            f"""
            SELECT DMBAG_NO AS MMBAG_NO
            FROM {settings.table("DMBAG")}
            """,
        ),
        "COST_MANIFEST": ScopeJob(
            "COST_MANIFEST",
            settings.table("COST_MANIFEST"),
            "MANIFEST_NO",
            _scope_join_query(f"{src}.CMS_COST_DTRANSIT_AGEN", "src", "src.DMANIFEST_NO AS MANIFEST_NO", cnote_scope, "scope", "CNOTE_NO", "CNOTE_NO"),
        ),
        "HVI": ScopeJob(
            "HVI",
            settings.table("HVI"),
            "HVI_NO",
            _scope_join_query(f"{src}.CMS_DHICNOTE", "src", "src.DHICNOTE_NO AS HVI_NO", cnote_scope, "scope", "DHICNOTE_CNOTE_NO", "CNOTE_NO"),
        ),
        "HVO": ScopeJob(
            "HVO",
            settings.table("HVO"),
            "HVO_NO",
            _scope_join_query(f"{src}.CMS_DHOCNOTE", "src", "src.DHOCNOTE_NO AS HVO_NO", cnote_scope, "scope", "DHOCNOTE_CNOTE_NO", "CNOTE_NO"),
        ),
        "RDSJ_HVO": ScopeJob(
            "RDSJ_HVO",
            settings.table("RDSJ_HVO"),
            "HVO_NO",
            _scope_join_query(f"{src}.CMS_RDSJ", "src", "src.RDSJ_HVO_NO AS HVO_NO", settings.table("HVI"), "scope", "RDSJ_HVI_NO", "HVI_NO"),
        ),
        "MSJ": ScopeJob(
            "MSJ",
            settings.table("MSJ"),
            "MSJ_NO",
            _scope_join_query(f"{src}.CMS_DSJ", "src", "src.DSJ_NO AS MSJ_NO", settings.table("RDSJ_HVO"), "scope", "DSJ_HVO_NO", "HVO_NO"),
        ),
    }
    selected_jobs = {name: job for name, job in jobs.items() if name in required_scopes}
    counts.update(_run_scope_jobs(selected_jobs, conn, oracle_settings, ctas_parallel_degree, scope_workers, counts))
    return counts


def cleanup_scope_tables(conn: Any, settings: ScopeSettings) -> None:
    for name in (
        "MSJ", "RDSJ_HVO", "HVO", "HVI", "DRSHEET", "DHOUNDEL", "DHI_HOC",
        "DRCNOTE", "COST_MANIFEST", "MMBAG", "SMU", "DMBAG", "MFBAG",
        "MANIFEST", "CNOTE",
    ):
        _drop_table(conn, settings.table(name))
    conn.commit()


def required_scopes_for_specs(specs: Sequence[TableSpec]) -> set[str]:
    scopes = {
        spec.scope_name
        for spec in specs
        if spec.stage not in {Stage.ANCHOR, Stage.REFERENCE} and spec.scope_name
    }
    return _expand_required_scopes({scope for scope in scopes if scope})


def _all_scope_names() -> set[str]:
    return {
        "CNOTE",
        "DRCNOTE",
        "DHI_HOC",
        "DHOUNDEL",
        "DRSHEET",
        "MANIFEST",
        "MFBAG",
        "DMBAG",
        "SMU",
        "MMBAG",
        "COST_MANIFEST",
        "HVI",
        "HVO",
        "RDSJ_HVO",
        "MSJ",
    }


def _expand_required_scopes(scopes: set[str]) -> set[str]:
    dependencies = _scope_dependencies()
    expanded = {scope.upper() for scope in scopes}
    changed = True
    while changed:
        changed = False
        for scope in list(expanded):
            for dependency in dependencies.get(scope, set()):
                if dependency not in expanded:
                    expanded.add(dependency)
                    changed = True
    return expanded


def scope_predicate(scope: ScopeSettings, table_alias: str, scope_name: str, scope_column: str) -> str:
    key_column = {
        "CNOTE": "CNOTE_NO",
        "DRCNOTE": "DRCNOTE_NO",
        "DHI_HOC": "DHI_NO",
        "DHOUNDEL": "DHOUNDEL_NO",
        "DRSHEET": "DRSHEET_NO",
        "MANIFEST": "MANIFEST_NO",
        "MFBAG": "MFBAG_NO",
        "DMBAG": "DMBAG_NO",
        "SMU": "SMU_NO",
        "MMBAG": "MMBAG_NO",
        "COST_MANIFEST": "MANIFEST_NO",
        "HVI": "HVI_NO",
        "HVO": "HVO_NO",
        "RDSJ_HVO": "HVO_NO",
        "MSJ": "MSJ_NO",
    }[scope_name]
    return f"{table_alias}.{scope_column} IN (SELECT {key_column} FROM {scope.table(scope_name)})"


# ============================================================
# PARQUET + MANIFEST
# ============================================================

class PartitionedParquetWriter:
    def __init__(
        self,
        output_dir: Path,
        columns: Sequence[str],
        rows_per_file: int,
        compression: str,
        compression_level: int | None,
        schema: Any | None = None,
        overwrite: bool = False,
    ) -> None:
        self.output_dir = output_dir
        self.columns = list(columns)
        self.rows_per_file = rows_per_file
        self.compression = compression
        self.compression_level = compression_level
        self.overwrite = overwrite
        self.writer = None
        self.schema = schema
        self.part_no = 0
        self.rows_in_part = 0
        self.row_count = 0

    def __enter__(self) -> "PartitionedParquetWriter":
        existing_parts = list(self.output_dir.glob("part-*.parquet"))
        if existing_parts and self.overwrite:
            shutil.rmtree(self.output_dir)
        elif existing_parts:
            raise RuntimeError(f"{self.output_dir} already contains Parquet parts")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()

    def write_rows(self, rows: Sequence[tuple]) -> None:
        if not rows:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        if self.schema is None:
            self.schema = _infer_arrow_schema(self.columns, rows)
        arrays = [
            _arrow_array_for_field(field, values)
            for field, values in zip(self.schema, zip(*rows))
        ]
        table = pa.Table.from_arrays(arrays, schema=self.schema)
        if self.writer is None or self.rows_in_part >= self.rows_per_file:
            self._open_next_part(pq)
        self.writer.write_table(table)
        self.rows_in_part += table.num_rows
        self.row_count += table.num_rows

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        (self.output_dir / "_SUCCESS").write_text(f"{self.row_count}\n", encoding="ascii")

    def _open_next_part(self, pq: Any) -> None:
        if self.writer is not None:
            self.writer.close()
        self.part_no += 1
        self.rows_in_part = 0
        part_path = self.output_dir / f"part-{self.part_no:05d}.parquet"
        logger.info("Starting Parquet part %s", part_path)
        self.writer = pq.ParquetWriter(
            part_path,
            self.schema,
            compression=self.compression,
            compression_level=self.compression_level,
            use_dictionary=True,
        )


def _infer_arrow_schema(columns: Sequence[str], rows: Sequence[tuple]) -> Any:
    import pyarrow as pa

    fields = []
    for column, values in zip(columns, zip(*rows)):
        if all(value is None for value in values):
            fields.append(pa.field(column, pa.string()))
        else:
            fields.append(pa.field(column, pa.array(values).type))
    return pa.schema(fields)


def _arrow_array_for_field(field: Any, values: Iterable[Any]) -> Any:
    import pyarrow as pa

    values = list(values)
    try:
        if pa.types.is_string(field.type):
            return pa.array(
                (None if value is None else str(value) for value in values),
                type=field.type,
            )
        return pa.array(values, type=field.type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Could not convert column {field.name} to Arrow type {field.type}: {exc}"
        ) from exc


def _description_item(item: Any, index: int, attr: str | None = None) -> Any:
    if attr and hasattr(item, attr):
        return getattr(item, attr)
    try:
        return item[index]
    except (IndexError, TypeError):
        return None


def _oracle_arrow_schema(description: Sequence[Any]) -> Any:
    import pyarrow as pa

    fields = []
    for item in description:
        name = _description_item(item, 0, "name")
        type_code = _description_item(item, 1, "type_code")
        precision = _description_item(item, 4, "precision")
        scale = _description_item(item, 5, "scale")
        fields.append(pa.field(name, _arrow_type_from_oracle(type_code, precision, scale)))
    return pa.schema(fields)


def _arrow_type_from_oracle(type_code: Any, precision: Any, scale: Any) -> Any:
    import pyarrow as pa

    type_name = getattr(type_code, "name", str(type_code)).upper()
    if "CHAR" in type_name or "CLOB" in type_name or "JSON" in type_name or "ROWID" in type_name:
        return pa.string()
    if "BLOB" in type_name or "RAW" in type_name:
        return pa.string()
    if "TIMESTAMP" in type_name:
        return pa.timestamp("us")
    if type_name.endswith("DATE") or "DB_TYPE_DATE" in type_name:
        return pa.timestamp("us")
    if "BINARY_DOUBLE" in type_name or "BINARY_FLOAT" in type_name or "DOUBLE" in type_name or "FLOAT" in type_name:
        return pa.float64()
    if "BOOLEAN" in type_name:
        return pa.bool_()
    if "NUMBER" in type_name or "DECIMAL" in type_name or "INTEGER" in type_name:
        return pa.string()
    return pa.string()


@dataclass
class TableResult:
    table: str
    output_name: str
    stage: str
    row_count: int
    file_count: int
    size_bytes: int
    elapsed_seconds: float


class RunManifest:
    def __init__(self, path: Path, run_id: str, window: Window) -> None:
        self.path = path
        self.data: dict[str, Any] = {
            "run_id": run_id,
            "window_start": window.start_label,
            "window_end": window.end_label,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": None,
            "scope_counts": {},
            "tables": [],
            "totals": {"row_count": 0, "file_count": 0, "size_bytes": 0},
            "minio": {"enabled": False, "bucket": None, "prefix": None, "uploaded_files": 0},
        }
        self._table_index: dict[str, int] = {}

    def set_scope_counts(self, counts: dict[str, int]) -> None:
        self.data["scope_counts"] = counts
        self.write()

    def add_table(self, result: TableResult) -> None:
        self.data["tables"].append(asdict(result))
        self._table_index[result.output_name] = len(self.data["tables"]) - 1
        self.data["totals"]["row_count"] += result.row_count
        self.data["totals"]["file_count"] += result.file_count
        self.data["totals"]["size_bytes"] += result.size_bytes
        self.write()

    def set_minio_upload(self, result: "MinioUploadResult") -> None:
        self.data["minio"] = {
            "enabled": True,
            "bucket": result.bucket,
            "prefix": result.prefix,
            "uploaded_files": result.file_count,
            "uploaded_bytes": result.size_bytes,
            "elapsed_seconds": result.elapsed_seconds,
        }
        self.write()

    def complete(self) -> None:
        self.data["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.write()

    def write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")


# ============================================================
# EXTRACTION
# ============================================================

def _load_pii_exclusions(config: dict) -> dict[str, set[str]]:
    if config.get("columns", {}).get("mode", "all") != "exclude_pii":
        return {}
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyYAML is required when columns.mode=exclude_pii") from exc

    path = Path(config["columns"]["pii_exclude_file"])
    if not path.exists():
        raise FileNotFoundError(f"PII exclusion file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return {table.upper(): {col.upper() for col in cols or []} for table, cols in raw.items()}


def _projection(columns: Iterable[str], spec: TableSpec, exclusions: dict[str, set[str]]) -> list[str]:
    excluded = exclusions.get(spec.table.upper(), set())
    selected = [col for col in columns if col.upper() not in excluded]
    if not selected:
        raise RuntimeError(f"All columns excluded for {spec.table}")
    return selected


def _output_dir(root: Path, window: Window, run_id: str, output_name: str, extract_date: str | None = None) -> Path:
    extract_date = extract_date or _extract_date_label(run_id)
    return (
        root
        / f"window_start={window.start_label}"
        / f"window_end={window.end_label}"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / output_name
    )


def _extract_date_label(run_id: str) -> str:
    match = re.search(r"(\d{4})(\d{2})(\d{2})T", run_id)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return date.today().isoformat()


def _existing_table_result(table_dir: Path, spec: TableSpec, start: float) -> TableResult | None:
    success_file = table_dir / "_SUCCESS"
    part_files = list(table_dir.glob("part-*.parquet"))
    if not success_file.exists() or not part_files:
        return None
    row_count = int(success_file.read_text(encoding="ascii").strip() or "0")
    size_bytes = sum(path.stat().st_size for path in part_files)
    elapsed = time.monotonic() - start
    logger.info(
        "Skipping %s because completed Parquet output already exists: %s rows, %s part(s)",
        spec.table,
        f"{row_count:,}",
        len(part_files),
    )
    return TableResult(
        table=spec.table,
        output_name=spec.output_name,
        stage=spec.stage.value,
        row_count=row_count,
        file_count=len(part_files),
        size_bytes=size_bytes,
        elapsed_seconds=elapsed,
    )


def _prepare_table_output_dir(table_dir: Path) -> None:
    if table_dir.exists():
        existing_parts = list(table_dir.glob("part-*.parquet"))
        if existing_parts:
            logger.info("Removing existing partial output before retry: %s", table_dir)
            shutil.rmtree(table_dir)


def _date_guardrails_enabled(config: dict) -> bool:
    return _as_bool(config.get("scoping", {}).get("date_guardrails_enabled", True))


def _date_guardrail_days(config: dict, key: str, default: int) -> int:
    value = int(config.get("scoping", {}).get(key, default))
    if value < 0:
        raise ValueError(f"scoping.{key} must be zero or greater")
    return value


def _date_guard_column(spec: TableSpec, source_columns: Sequence[str]) -> str | None:
    if not spec.date_guard_column:
        return None
    available = {column.upper() for column in source_columns}
    if spec.date_guard_column.upper() not in available:
        logger.warning(
            "Skipping date guardrail for %s because column %s is not present",
            spec.table,
            spec.date_guard_column,
        )
        return None
    return spec.date_guard_column


def _build_sql(
    config: dict,
    spec: TableSpec,
    columns: list[str],
    scope: ScopeSettings,
    source_columns: Sequence[str] | None = None,
) -> tuple[str, dict]:
    source_schema = config["oracle"].get("source_schema", "JNE").upper()
    alias = "src"
    column_sql = ", ".join(f"{alias}.{col}" for col in columns)
    sql = f"SELECT {column_sql} FROM {source_schema}.{spec.table} {alias}"
    binds = {}
    predicates = []

    if spec.stage == Stage.ANCHOR:
        date_col = config["extraction"]["anchor_date_column"]
        predicates.append(f"{alias}.{date_col} >= :start_date AND {alias}.{date_col} < :end_date")
        if _cnote_limit(config) is not None:
            predicates.append(scope_predicate(scope, alias, "CNOTE", "CNOTE_NO"))
    elif spec.stage != Stage.REFERENCE:
        if not spec.scope_name or not spec.scope_column:
            raise RuntimeError(f"Missing scope declaration for {spec.table}")
        predicates.append(table_scope_predicate(source_schema, scope, spec, alias))
        guard_column = None
        if _date_guardrails_enabled(config):
            guard_column = _date_guard_column(spec, source_columns or columns)
        if guard_column:
            lookback_days = _date_guardrail_days(config, "date_guardrail_lookback_days", 0)
            lookahead_days = _date_guardrail_days(config, "date_guardrail_lookahead_days", 30)
            predicates.append(
                f"{alias}.{guard_column} >= :start_date - {lookback_days} "
                f"AND {alias}.{guard_column} < :end_date + {lookahead_days}"
            )
            logger.info(
                "Applying date guardrail for %s on %s: start-%s day(s), end+%s day(s)",
                spec.table,
                guard_column,
                lookback_days,
                lookahead_days,
            )
    if predicates:
        sql += " WHERE " + " AND ".join(f"({predicate})" for predicate in predicates)
    return sql, binds


def table_scope_predicate(source_schema: str, scope: ScopeSettings, spec: TableSpec, table_alias: str) -> str:
    return scope_predicate(scope, table_alias, spec.scope_name, spec.scope_column)


def extract_table(
    config: dict,
    oracle_settings: OracleSettings,
    scope: ScopeSettings,
    window: Window,
    run_id: str,
    spec: TableSpec,
    extract_date: str | None = None,
) -> TableResult:
    start = time.monotonic()
    output_root = Path(config["output"]["root"])
    rows_per_file = int(config["output"].get("rows_per_file", 250000))
    progress_rows = int(config["output"].get("progress_rows", rows_per_file))
    compression = config["output"].get("compression", "zstd")
    zstd_level = int(config["output"].get("zstd_level", 9))
    compression_level = zstd_level if compression == "zstd" else None
    exclusions = _load_pii_exclusions(config)
    table_dir = _output_dir(output_root, window, run_id, spec.output_name, extract_date)
    existing_result = _existing_table_result(table_dir, spec, start)
    if existing_result is not None:
        return existing_result
    _prepare_table_output_dir(table_dir)

    with connect(oracle_settings) as conn:
        source_schema = config["oracle"].get("source_schema", "JNE")
        source_columns = table_columns(conn, source_schema, spec.table)
        columns = _projection(source_columns, spec, exclusions)
        sql, binds = _build_sql(config, spec, columns, scope, source_columns)
        if ":start_date" in sql or ":end_date" in sql:
            binds = {"start_date": window.start, "end_date": window.end}

        logger.info("Extracting %s to %s", spec.table, table_dir)
        with conn.cursor() as cursor:
            cursor.arraysize = oracle_settings.fetch_arraysize
            if oracle_settings.prefetch_rows is not None:
                cursor.prefetchrows = oracle_settings.prefetch_rows
            try:
                cursor.execute(sql, binds)
            except Exception as exc:
                raise RuntimeError(f"Oracle query failed for {spec.table}: {exc}\nSQL: {sql}") from exc
            arrow_schema = _oracle_arrow_schema(cursor.description)
            with PartitionedParquetWriter(
                table_dir,
                columns,
                rows_per_file,
                compression,
                compression_level,
                schema=arrow_schema,
                overwrite=True,
            ) as writer:
                last_logged_rows = 0
                while True:
                    rows = cursor.fetchmany(oracle_settings.fetch_arraysize)
                    if not rows:
                        break
                    writer.write_rows(rows)
                    if writer.row_count - last_logged_rows >= progress_rows:
                        elapsed = time.monotonic() - start
                        rows_per_second = writer.row_count / elapsed if elapsed else 0
                        logger.info(
                            "%s progress: %s rows, %s parquet part(s), %.0f rows/sec",
                            spec.table,
                            f"{writer.row_count:,}",
                            writer.part_no,
                            rows_per_second,
                        )
                        last_logged_rows = writer.row_count

    part_files = list(table_dir.glob("part-*.parquet"))
    size_bytes = sum(path.stat().st_size for path in part_files)
    elapsed = time.monotonic() - start
    result = TableResult(
        table=spec.table,
        output_name=spec.output_name,
        stage=spec.stage.value,
        row_count=int((table_dir / "_SUCCESS").read_text(encoding="ascii").strip() or "0"),
        file_count=len(part_files),
        size_bytes=size_bytes,
        elapsed_seconds=elapsed,
    )
    logger.info("Finished %s: %s rows in %.1fs", spec.table, f"{result.row_count:,}", elapsed)
    return result


# ============================================================
# MINIO LAKE
# ============================================================

@dataclass(frozen=True)
class MinioSettings:
    enabled: bool
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool
    prefix: str

    @classmethod
    def from_config(cls, config: dict) -> "MinioSettings":
        minio_config = config.get("minio", {})
        return cls(
            enabled=_as_bool(minio_config.get("enabled", False)),
            endpoint=minio_config.get("endpoint", "localhost:9000"),
            access_key=minio_config.get("access_key", "minioadmin"),
            secret_key=minio_config.get("secret_key", "minioadmin"),
            bucket=minio_config.get("bucket", "jne-bronze"),
            secure=_as_bool(minio_config.get("secure", False)),
            prefix=minio_config.get("prefix", "bronze/jne"),
        )


@dataclass(frozen=True)
class MinioUploadResult:
    bucket: str
    prefix: str
    file_count: int
    size_bytes: int
    elapsed_seconds: float


def _minio_client(settings: MinioSettings) -> Any:
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "minio is required to write/read bronze Parquet objects. "
            "Install dependencies with `pip install -r requirements.txt` or rebuild the Airflow image."
        ) from exc

    return Minio(
        endpoint=settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )


def _run_dir(root: Path, window: Window, run_id: str) -> Path:
    extract_date = _extract_date_label(run_id)
    return (
        root
        / f"window_start={window.start_label}"
        / f"window_end={window.end_label}"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )


def _run_dir_for_extract_date(root: Path, window: Window, run_id: str, extract_date: str) -> Path:
    return (
        root
        / f"window_start={window.start_label}"
        / f"window_end={window.end_label}"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )


def lake_prefix(settings: MinioSettings, window: Window, run_id: str, extract_date: str | None = None) -> str:
    extract_date = extract_date or _extract_date_label(run_id)
    prefix = settings.prefix.strip("/")
    return (
        f"{prefix}/window_start={window.start_label}/window_end={window.end_label}/"
        f"extract_date={extract_date}/run_id={run_id}"
    )


def upload_run_to_minio(
    config: dict,
    window: Window,
    run_id: str,
    manifest: RunManifest,
    extract_date: str | None = None,
) -> MinioUploadResult | None:
    settings = MinioSettings.from_config(config)
    if not settings.enabled:
        logger.info("Skipping MinIO upload because minio.enabled=false")
        return None

    start = time.monotonic()
    client = _minio_client(settings)
    if not client.bucket_exists(settings.bucket):
        client.make_bucket(settings.bucket)

    run_dir = (
        _run_dir_for_extract_date(Path(config["output"]["root"]), window, run_id, extract_date)
        if extract_date
        else _run_dir(Path(config["output"]["root"]), window, run_id)
    )
    prefix = lake_prefix(settings, window, run_id, extract_date)
    uploaded_files = 0
    uploaded_bytes = 0
    for path in sorted(run_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(run_dir).as_posix()
        object_name = f"{prefix}/{relative}"
        client.fput_object(settings.bucket, object_name, str(path))
        uploaded_files += 1
        uploaded_bytes += path.stat().st_size

    elapsed = time.monotonic() - start
    result = MinioUploadResult(settings.bucket, prefix, uploaded_files, uploaded_bytes, elapsed)
    manifest.set_minio_upload(result)
    logger.info(
        "Uploaded %s files (%s bytes) to minio://%s/%s in %.1fs",
        uploaded_files,
        uploaded_bytes,
        settings.bucket,
        prefix,
        elapsed,
    )
    return result


def upload_manifest_to_minio(
    config: dict,
    window: Window,
    run_id: str,
    manifest: RunManifest,
    extract_date: str | None = None,
) -> None:
    settings = MinioSettings.from_config(config)
    if not settings.enabled:
        return
    client = _minio_client(settings)
    if not client.bucket_exists(settings.bucket):
        client.make_bucket(settings.bucket)
    object_name = f"{lake_prefix(settings, window, run_id, extract_date)}/run_manifest.json"
    client.fput_object(settings.bucket, object_name, str(manifest.path))


# ============================================================
# RUNNER
# ============================================================

def _manifest_path(config: dict, window: Window, run_id: str, extract_date: str | None = None) -> Path:
    extract_date = extract_date or _extract_date_label(run_id)
    return (
        Path(config["output"]["root"])
        / f"window_start={window.start_label}"
        / f"window_end={window.end_label}"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "run_manifest.json"
    )


def _extract_stage(
    specs: Sequence[TableSpec],
    config: dict,
    settings: OracleSettings,
    scope: ScopeSettings,
    window: Window,
    run_id: str,
    stage: Stage,
    workers: int,
    manifest: RunManifest,
    extract_date: str | None = None,
) -> None:
    specs = specs_for_stage_from(specs, stage)
    if stage == Stage.REFERENCE and config["scoping"].get("reference_tables_mode", "full") == "skip":
        logger.info("Skipping reference tables because reference_tables_mode=skip")
        return
    if not specs:
        logger.info("Skipping stage %s: no selected tables", stage.value)
        return
    logger.info("Extracting stage %s (%s tables)", stage.value, len(specs))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(extract_table, config, settings, scope, window, run_id, spec, extract_date): spec.table
            for spec in specs
        }
        for future in as_completed(futures):
            manifest.add_table(future.result())


def run(config_path: str, run_id: str, keep_scope: bool = False, extract_date: str | None = None) -> None:
    config = load_config(config_path)
    window = resolve_window(config)
    safe_run_id = sanitize_run_id(run_id)
    specs = selected_specs(config)
    oracle_settings = OracleSettings.from_config(config)
    scope = ScopeSettings.from_config(config, safe_run_id)
    workers = int(os.getenv("BRONZE_WORKERS", "4"))
    manifest = RunManifest(_manifest_path(config, window, safe_run_id, extract_date), safe_run_id, window)
    cnote_limit = _cnote_limit(config)
    scoping_config = config.get("scoping", {})
    scope_workers = int(os.getenv("BRONZE_SCOPE_WORKERS", scoping_config.get("scope_workers", 1)))
    ctas_parallel_degree = int(os.getenv("BRONZE_SCOPE_PARALLEL_DEGREE", scoping_config.get("ctas_parallel_degree", 1)))
    date_guard_lookback_days = _date_guardrail_days(config, "date_guardrail_lookback_days", 0)
    date_guard_lookahead_days = _date_guardrail_days(config, "date_guardrail_lookahead_days", 30)
    started = time.monotonic()

    logger.info(
        "Bronze run %s code_version=%s window=[%s, %s) workers=%s scope_workers=%s ctas_parallel_degree=%s cnote_limit=%s",
        safe_run_id,
        CODE_VERSION,
        window.start_label,
        window.end_label,
        workers,
        scope_workers,
        ctas_parallel_degree,
        f"{cnote_limit:,}" if cnote_limit is not None else "none",
    )
    logger.info("Selected tables: %s", ", ".join(spec.table for spec in specs))

    with connect(oracle_settings) as conn:
        required_scopes = required_scopes_for_specs(specs)
        if cnote_limit is not None:
            required_scopes.add("CNOTE")
        counts = materialize_scope_tables(
            conn,
            scope,
            window,
            config["extraction"]["anchor_table"],
            config["extraction"]["anchor_date_column"],
            cnote_limit,
            required_scopes,
            oracle_settings,
            date_guard_lookback_days,
            date_guard_lookahead_days,
            ctas_parallel_degree,
            scope_workers,
        )
        manifest.set_scope_counts(counts)
        logger.info("Scope counts: %s", counts)

    try:
        _extract_stage(specs, config, oracle_settings, scope, window, safe_run_id, Stage.ANCHOR, 1, manifest, extract_date)
        _extract_stage(specs, config, oracle_settings, scope, window, safe_run_id, Stage.CNOTE, workers, manifest, extract_date)
        _extract_stage(specs, config, oracle_settings, scope, window, safe_run_id, Stage.BAG_MANIFEST, workers, manifest, extract_date)
        _extract_stage(specs, config, oracle_settings, scope, window, safe_run_id, Stage.RUNSHEET_DO, workers, manifest, extract_date)
        _extract_stage(specs, config, oracle_settings, scope, window, safe_run_id, Stage.REFERENCE, workers, manifest, extract_date)
        upload_run_to_minio(config, window, safe_run_id, manifest, extract_date)
        manifest.complete()
        upload_manifest_to_minio(config, window, safe_run_id, manifest, extract_date)
    finally:
        if keep_scope:
            logger.info("Keeping scope tables for inspection")
        else:
            with connect(oracle_settings) as conn:
                cleanup_scope_tables(conn, scope)
            logger.info("Scope tables cleaned up")

    logger.info("Bronze run complete in %.1fs", time.monotonic() - started)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract JNE relational bronze Parquet datasets.")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--run-id", default=datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    parser.add_argument("--extract-date", help="Date partition label for local and MinIO output, YYYY-MM-DD.")
    parser.add_argument("--keep-scope", action="store_true")
    args = parser.parse_args()
    configure_logging()
    run(args.config, args.run_id, args.keep_scope, args.extract_date)


if __name__ == "__main__":
    main()
