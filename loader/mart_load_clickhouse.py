"""Load bronze and governance outputs into a ClickHouse mart."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable


ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")


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


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool


@dataclass(frozen=True)
class BronzeConfig:
    bucket: str
    run_prefix: str


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    secure: bool


@dataclass(frozen=True)
class SchemaConfig:
    bronze: str
    bronze_staging: str
    derived: str
    derived_staging: str
    governance: str


@dataclass(frozen=True)
class GovernanceConfig:
    enabled: bool
    results_path: Path
    result_cnotes_path: Path
    summary_path: Path
    results_table: str = "governance_results"
    result_cnotes_table: str = "governance_result_cnotes"
    summary_table: str = "governance_rule_summary"
    build_document_links: bool = True
    document_links_table: str = "document_cnote_links"
    build_dashboard_table: bool = False
    dashboard_table: str = "governance_results_dashboard"
    execution_mode: str = "clickhouse"
    result_cnotes_statuses: tuple[str, ...] = ("FAIL",)


@dataclass(frozen=True)
class UnifiedMartConfig:
    enabled: bool
    schema: str
    table: str
    sql_path: Path


@dataclass(frozen=True)
class MartClickHouseConfig:
    minio: MinioConfig
    bronze: BronzeConfig
    clickhouse: ClickHouseConfig
    schemas: SchemaConfig
    governance: GovernanceConfig
    unified_mart: UnifiedMartConfig
    skip_stages: tuple[str, ...] = ()
    reuse_existing_stages: tuple[str, ...] = ()
    load_mode: str = "latest_snapshot"


UNIFIED_REQUIRED_TABLES = (
    "cms_cnote",
    "cms_apicust",
    "t_cancel_cnote_api",
    "cms_drcnote",
    "cms_mrcnote",
    "cms_dhi_hoc",
    "cms_mhi_hoc",
    "cms_mfcnote",
    "cms_manifest",
    "cms_mrsheet_pra",
    "cms_drsheet_pra",
    "cms_dsmu",
    "cms_msmu",
    "cms_mhocnote",
    "cms_dhocnote",
    "cms_mhicnote",
    "cms_dhicnote",
    "cms_cost_dtransit_agen",
    "cms_cost_mtransit_agen",
    "cms_mhov_rsheet",
    "cms_dhov_rsheet",
    "cms_mrsheet",
    "cms_drsheet",
    "cms_mstatus",
    "cms_dstatus",
)


def load_config(path: str | Path = "config/mart_clickhouse.yaml") -> MartClickHouseConfig:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyYAML is required to load ClickHouse mart config files. Install "
            "dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    with Path(path).open("r", encoding="utf-8") as handle:
        raw = _expand_env(yaml.safe_load(handle) or {})

    minio = raw.get("minio", {})
    bronze = raw.get("bronze", {})
    clickhouse = raw.get("clickhouse", {})
    schemas = raw.get("schemas", {})
    governance = raw.get("governance", {})
    unified_mart = raw.get("unified_mart", {})
    mart = raw.get("mart", {})
    run_id = os.getenv("RUN_ID", "")
    default_governance_path = Path("governance/outputs") / run_id / "governance_results.csv"
    default_result_cnotes_path = Path("governance/outputs") / run_id / "governance_result_cnotes.csv"
    default_summary_path = Path("governance/outputs") / run_id / "governance_rule_summary.csv"

    config = MartClickHouseConfig(
        minio=MinioConfig(
            endpoint=minio.get("endpoint", "minio:9000"),
            access_key=minio.get("access_key", "minioadmin"),
            secret_key=minio.get("secret_key", "minioadmin"),
            secure=_as_bool(minio.get("secure", False)),
        ),
        bronze=BronzeConfig(
            bucket=bronze["bucket"],
            run_prefix=bronze["run_prefix"].strip("/"),
        ),
        clickhouse=ClickHouseConfig(
            host=clickhouse.get("host", "mart-clickhouse"),
            port=int(clickhouse.get("port", 8123)),
            database=clickhouse.get("database", "mart"),
            user=clickhouse.get("user", "default"),
            password=clickhouse.get("password", ""),
            secure=_as_bool(clickhouse.get("secure", False)),
        ),
        schemas=SchemaConfig(
            bronze=schemas.get("bronze", "bronze"),
            bronze_staging=schemas.get("bronze_staging", "bronze_staging"),
            derived=schemas.get("derived", "derived"),
            derived_staging=schemas.get("derived_staging", "derived_staging"),
            governance=schemas.get("governance", "governance"),
        ),
        governance=GovernanceConfig(
            enabled=_as_bool(governance.get("enabled", True)),
            results_path=Path(governance.get("results_path") or default_governance_path),
            result_cnotes_path=Path(governance.get("result_cnotes_path") or default_result_cnotes_path),
            summary_path=Path(governance.get("summary_path") or default_summary_path),
            execution_mode=str(governance.get("execution_mode", "clickhouse")).strip().lower(),
            results_table=governance.get("results_table", "governance_results"),
            result_cnotes_table=governance.get("result_cnotes_table", "governance_result_cnotes"),
            summary_table=governance.get("summary_table", "governance_rule_summary"),
            build_document_links=_as_bool(governance.get("build_document_links", True)),
            document_links_table=governance.get("document_links_table", "document_cnote_links"),
            build_dashboard_table=_as_bool(governance.get("build_dashboard_table", False)),
            dashboard_table=governance.get("dashboard_table", "governance_results_dashboard"),
            result_cnotes_statuses=tuple(
                str(status).strip().upper()
                for status in governance.get("result_cnotes_statuses", ["FAIL"])
                if str(status).strip()
            ),
        ),
        unified_mart=UnifiedMartConfig(
            enabled=_as_bool(unified_mart.get("enabled", False)),
            schema=unified_mart.get("schema", "mart"),
            table=unified_mart.get("table", "unified_shipments"),
            sql_path=Path(unified_mart.get("sql_path", "loader/sql/unified_shipments.sql")),
        ),
        skip_stages=tuple(str(stage).lower() for stage in mart.get("skip_stages", [])),
        reuse_existing_stages=tuple(str(stage).lower() for stage in mart.get("reuse_existing_stages", [])),
        load_mode=mart.get("load_mode", "latest_snapshot"),
    )
    if config.load_mode != "latest_snapshot":
        raise ValueError(f"Unsupported mart.load_mode: {config.load_mode}")
    if config.governance.execution_mode not in {"clickhouse", "csv"}:
        raise ValueError("governance.execution_mode must be one of: clickhouse, csv")
    if not config.bronze.run_prefix:
        raise ValueError("bronze.run_prefix is required")
    return config


def _connect_clickhouse(config: MartClickHouseConfig):
    try:
        import clickhouse_connect
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "clickhouse-connect is required for ClickHouse mart loading. Install "
            "dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    return clickhouse_connect.get_client(
        host=config.clickhouse.host,
        port=config.clickhouse.port,
        username=config.clickhouse.user,
        password=config.clickhouse.password,
        secure=config.clickhouse.secure,
    )


def _minio_client(config: MartClickHouseConfig):
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "minio is required to read the bronze run manifest. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    return Minio(
        config.minio.endpoint,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=config.minio.secure,
    )


def _log(message: str) -> None:
    print(message, flush=True)


def _quote_ident(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _quote_sql(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _read_manifest(client: Any, config: MartClickHouseConfig) -> dict[str, Any]:
    response = client.get_object(
        config.bronze.bucket,
        f"{config.bronze.run_prefix}/run_manifest.json",
    )
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def _table_object_prefix(config: MartClickHouseConfig, table_info: dict[str, Any], default_parent: str | None = None) -> str:
    if table_info.get("source_prefix"):
        return str(table_info["source_prefix"]).rstrip("/") + "/"
    if table_info.get("output_prefix"):
        return str(table_info["output_prefix"]).rstrip("/") + "/"
    output_name = table_info["output_name"]
    if default_parent:
        return f"{config.bronze.run_prefix}/{default_parent.strip('/')}/{output_name}/"
    return f"{config.bronze.run_prefix}/{output_name}/"


def _mart_table_name(table_info: dict[str, Any]) -> str:
    if table_info.get("output_name") == "cms_cnote":
        return "cms_cnote_raw"
    return table_info["output_name"]


def _should_skip_bronze_table(table_info: dict[str, Any], config: MartClickHouseConfig) -> bool:
    stage = str(table_info.get("stage", "")).lower()
    return stage in config.skip_stages


def _s3_url(config: MartClickHouseConfig, prefix: str, pattern: str = "part-*.parquet") -> str:
    protocol = "https" if config.minio.secure else "http"
    endpoint = config.minio.endpoint.rstrip("/")
    object_prefix = prefix.strip("/")
    return f"{protocol}://{endpoint}/{config.bronze.bucket}/{object_prefix}/{pattern}"


def _s3_table_expr(config: MartClickHouseConfig, prefix: str, fmt: str = "Parquet") -> str:
    return (
        "s3("
        f"{_quote_sql(_s3_url(config, prefix))}, "
        f"{_quote_sql(config.minio.access_key)}, "
        f"{_quote_sql(config.minio.secret_key)}, "
        f"{_quote_sql(fmt)}"
        ")"
    )


def _command(client: Any, sql: str) -> Any:
    return client.command(sql)


def _query_scalar(client: Any, sql: str) -> Any:
    result = client.query(sql)
    return result.result_rows[0][0]


def _create_database(client: Any, schema: str) -> None:
    _command(client, f"CREATE DATABASE IF NOT EXISTS {_quote_ident(schema)}")


def _drop_database(client: Any, schema: str) -> None:
    _command(client, f"DROP DATABASE IF EXISTS {_quote_ident(schema)}")


def _staging_tables(client: Any, schema: str) -> list[str]:
    result = client.query(
        "SELECT name FROM system.tables WHERE database = {database:String} ORDER BY name",
        parameters={"database": schema},
    )
    return [row[0] for row in result.result_rows]


def _table_exists(client: Any, schema: str, table: str) -> bool:
    result = client.query(
        """
        SELECT count()
        FROM system.tables
        WHERE database = {database:String}
          AND name = {table:String}
        """,
        parameters={"database": schema, "table": table},
    )
    return bool(result.result_rows and int(result.result_rows[0][0]) > 0)


def _table_columns(client: Any, schema: str, table: str) -> set[str]:
    if not _table_exists(client, schema, table):
        return set()
    result = client.query(f"DESCRIBE TABLE {_qualified(schema, table)}")
    return {str(row[0]) for row in result.result_rows}


def _create_empty_table_from_s3(client: Any, schema: str, table: str, s3_expr: str) -> None:
    _command(
        client,
        f"CREATE TABLE {_qualified(schema, table)} "
        "ENGINE = MergeTree ORDER BY tuple() AS "
        f"SELECT * FROM {s3_expr} LIMIT 0",
    )


def _load_s3_table(
    client: Any,
    config: MartClickHouseConfig,
    schema: str,
    table_info: dict[str, Any],
    label: str,
    default_parent: str | None = None,
) -> int:
    source_name = table_info["output_name"]
    table_name = _mart_table_name(table_info)
    prefix = _table_object_prefix(config, table_info, default_parent=default_parent)
    s3_expr = _s3_table_expr(config, prefix)
    started = time.monotonic()
    _log(f"Loading ClickHouse {label}.{table_name} from {source_name} at {_s3_url(config, prefix)}")
    _create_empty_table_from_s3(client, schema, table_name, s3_expr)
    _command(client, f"INSERT INTO {_qualified(schema, table_name)} SELECT * FROM {s3_expr}")
    row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(schema, table_name)}"))
    expected = table_info.get("row_count")
    if expected is not None and int(expected) != row_count:
        raise RuntimeError(f"{label}.{table_name} row count mismatch: expected {expected:,}, got {row_count:,}")
    _log(f"Loaded ClickHouse {label}.{table_name}: {row_count:,} rows in {time.monotonic() - started:.1f}s")
    return row_count


def _load_table_entries(
    client: Any,
    config: MartClickHouseConfig,
    entries: Iterable[dict[str, Any]],
    staging_schema: str,
    label: str,
    default_parent: str | None = None,
    target_schema: str | None = None,
) -> dict[str, int]:
    loaded = {}
    for table_info in entries:
        source_name = table_info["output_name"]
        table_name = _mart_table_name(table_info)
        stage = str(table_info.get("stage", "")).lower()
        if label == "bronze" and _should_skip_bronze_table(table_info, config):
            _log(f"Skipping ClickHouse bronze.{source_name}; stage is configured to skip")
            continue
        if (
            label == "bronze"
            and target_schema
            and stage in config.reuse_existing_stages
            and _table_exists(client, target_schema, table_name)
        ):
            _log(f"Reusing existing ClickHouse {target_schema}.{table_name}; stage={stage} is configured for reuse")
            continue
        loaded[table_name] = _load_s3_table(
            client,
            config,
            staging_schema,
            table_info,
            label,
            default_parent=default_parent,
        )
    return loaded


def _publish_schema(client: Any, staging_schema: str, target_schema: str) -> None:
    tables = _staging_tables(client, staging_schema)
    _create_database(client, target_schema)
    for table_name in tables:
        _command(client, f"DROP TABLE IF EXISTS {_qualified(target_schema, table_name)}")
        _command(client, f"RENAME TABLE {_qualified(staging_schema, table_name)} TO {_qualified(target_schema, table_name)}")
    _drop_database(client, staging_schema)


def _ch_text(alias: str, column: str) -> str:
    return f"trimBoth(toString({alias}.{_quote_ident(column)}))"


def _ch_not_blank(alias: str, column: str) -> str:
    return f"isNotNull({alias}.{_quote_ident(column)}) AND {_ch_text(alias, column)} != ''"


def _document_link_select(
    source_table: str,
    document_expr: str,
    cnote_expr: str,
    method: str,
    from_sql: str,
    where: Iterable[str],
) -> str:
    document_type = source_table.removeprefix("CMS_")
    return f"""
        SELECT
            {_quote_sql(source_table)} AS source_table,
            {_quote_sql(document_type)} AS document_type,
            {document_expr} AS document_id,
            {cnote_expr} AS cnote_no,
            {_quote_sql(method)} AS link_method,
            'safe' AS link_confidence
        {from_sql}
        WHERE {' AND '.join(where)}
    """


def _empty_document_links_table(client: Any, schema: str, table: str) -> int:
    _command(
        client,
        f"""
        CREATE TABLE {_qualified(schema, table)} (
            source_table String,
            document_type String,
            document_id String,
            cnote_no String,
            link_method String,
            link_confidence String
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )
    return 0


def _build_document_cnote_links(client: Any, config: MartClickHouseConfig) -> int:
    governance = config.schemas.governance
    target_table = config.governance.document_links_table
    if not config.governance.build_document_links:
        _create_database(client, governance)
        _command(client, f"DROP TABLE IF EXISTS {_qualified(governance, target_table)}")
        _log("Skipping ClickHouse document-to-CNOTE link build because governance.build_document_links=false")
        return 0
    if not _table_exists(client, config.schemas.bronze, "cms_cnote"):
        _log("Skipping ClickHouse document-to-CNOTE link build because bronze.cms_cnote is missing")
        return 0

    started = time.monotonic()
    bronze = config.schemas.bronze
    has_tables: dict[str, bool] = {}

    def has(*tables: str) -> bool:
        missing = [table for table in tables if not has_tables.setdefault(table, _table_exists(client, bronze, table))]
        return not missing

    def q(table: str) -> str:
        return _qualified(bronze, table)

    selects: list[str] = []
    mf = q("cms_mfcnote")
    if has("cms_mfcnote"):
        selects.append(_document_link_select(
            "CMS_MFCNOTE",
            _ch_text("mf", "MFCNOTE_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "direct_mfcnote",
            f"FROM {mf} mf",
            [_ch_not_blank("mf", "MFCNOTE_NO")],
        ))
        selects.append(_document_link_select(
            "CMS_MFBAG",
            _ch_text("mf", "MFCNOTE_BAG_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "mfbag_to_mfcnote",
            f"FROM {mf} mf",
            [_ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))
        selects.append(_document_link_select(
            "CMS_DMBAG",
            _ch_text("mf", "MFCNOTE_BAG_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "dmbag_to_mfbag_mfcnote",
            f"FROM {mf} mf",
            [_ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))
        selects.append(_document_link_select(
            "CMS_MANIFEST",
            _ch_text("mf", "MFCNOTE_MAN_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "manifest_to_mfcnote",
            f"FROM {mf} mf",
            [_ch_not_blank("mf", "MFCNOTE_MAN_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))

    if has("cms_mfcnote", "cms_dmbag"):
        selects.append(_document_link_select(
            "CMS_DMBAG",
            _ch_text("d", "DMBAG_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "dmbag_to_mfbag_mfcnote",
            f"FROM {q('cms_dmbag')} d INNER JOIN {mf} mf ON {_ch_text('d', 'DMBAG_BAG_NO')} = {_ch_text('mf', 'MFCNOTE_BAG_NO')}",
            [_ch_not_blank("d", "DMBAG_NO"), _ch_not_blank("d", "DMBAG_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))

    if has("cms_mfcnote", "cms_dmbag", "cms_mmbag"):
        selects.append(_document_link_select(
            "CMS_MMBAG",
            _ch_text("m", "MMBAG_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "mmbag_to_dmbag_mfbag_mfcnote",
            f"FROM {q('cms_mmbag')} m INNER JOIN {mf} mf ON {_ch_text('m', 'MMBAG_NO')} = {_ch_text('mf', 'MFCNOTE_BAG_NO')}",
            [_ch_not_blank("m", "MMBAG_NO"), _ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))
        selects.append(_document_link_select(
            "CMS_MMBAG",
            _ch_text("m", "MMBAG_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "mmbag_to_dmbag_mfbag_mfcnote",
            f"FROM {q('cms_mmbag')} m INNER JOIN {q('cms_dmbag')} d ON {_ch_text('m', 'MMBAG_NO')} = {_ch_text('d', 'DMBAG_NO')} INNER JOIN {mf} mf ON {_ch_text('d', 'DMBAG_BAG_NO')} = {_ch_text('mf', 'MFCNOTE_BAG_NO')}",
            [_ch_not_blank("m", "MMBAG_NO"), _ch_not_blank("d", "DMBAG_NO"), _ch_not_blank("d", "DMBAG_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))

    direct_detail_links = [
        ("cms_drsheet", "CMS_MRSHEET", "DRSHEET_NO", "DRSHEET_CNOTE_NO", "mrsheet_to_drsheet"),
        ("cms_dhicnote", "CMS_MHICNOTE", "DHICNOTE_NO", "DHICNOTE_CNOTE_NO", "mhicnote_to_dhicnote"),
        ("cms_dhi_hoc", "CMS_MHI_HOC", "DHI_NO", "DHI_CNOTE_NO", "mhi_hoc_to_dhi_hoc"),
        ("cms_dhocnote", "CMS_MHOCNOTE", "DHOCNOTE_NO", "DHOCNOTE_CNOTE_NO", "mhocnote_to_dhocnote"),
        ("cms_dhoundel_pod", "CMS_MHOUNDEL_POD", "DHOUNDEL_NO", "DHOUNDEL_CNOTE_NO", "mhoundel_to_dhoundel"),
    ]
    for table, source_table, document_column, cnote_column, method in direct_detail_links:
        if has(table):
            selects.append(_document_link_select(
                source_table,
                _ch_text("d", document_column),
                _ch_text("d", cnote_column),
                method,
                f"FROM {q(table)} d",
                [_ch_not_blank("d", document_column), _ch_not_blank("d", cnote_column)],
            ))

    if has("cms_dsmu", "cms_mfcnote"):
        selects.append(_document_link_select(
            "CMS_DSMU",
            _ch_text("dsmu", "DSMU_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "dsmu_to_dmbag_mfcnote",
            f"FROM {q('cms_dsmu')} dsmu INNER JOIN {mf} mf ON {_ch_text('dsmu', 'DSMU_BAG_NO')} = {_ch_text('mf', 'MFCNOTE_BAG_NO')}",
            [_ch_not_blank("dsmu", "DSMU_NO"), _ch_not_blank("dsmu", "DSMU_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))
    if has("cms_dsmu", "cms_dmbag", "cms_mfcnote"):
        selects.append(_document_link_select(
            "CMS_DSMU",
            _ch_text("dsmu", "DSMU_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "dsmu_to_dmbag_mfcnote",
            f"FROM {q('cms_dsmu')} dsmu INNER JOIN {q('cms_dmbag')} d ON {_ch_text('dsmu', 'DSMU_BAG_NO')} = {_ch_text('d', 'DMBAG_NO')} INNER JOIN {mf} mf ON {_ch_text('d', 'DMBAG_BAG_NO')} = {_ch_text('mf', 'MFCNOTE_BAG_NO')}",
            [_ch_not_blank("dsmu", "DSMU_NO"), _ch_not_blank("dsmu", "DSMU_BAG_NO"), _ch_not_blank("d", "DMBAG_NO"), _ch_not_blank("d", "DMBAG_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))
    if has("cms_msmu", "cms_dsmu", "cms_mfcnote"):
        selects.append(_document_link_select(
            "CMS_MSMU",
            _ch_text("msmu", "MSMU_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "msmu_to_dsmu_dmbag_mfcnote",
            f"FROM {q('cms_msmu')} msmu INNER JOIN {q('cms_dsmu')} dsmu ON {_ch_text('msmu', 'MSMU_NO')} = {_ch_text('dsmu', 'DSMU_NO')} INNER JOIN {mf} mf ON {_ch_text('dsmu', 'DSMU_BAG_NO')} = {_ch_text('mf', 'MFCNOTE_BAG_NO')}",
            [_ch_not_blank("msmu", "MSMU_NO"), _ch_not_blank("dsmu", "DSMU_NO"), _ch_not_blank("dsmu", "DSMU_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))
    if has("cms_msmu", "cms_dsmu", "cms_dmbag", "cms_mfcnote"):
        selects.append(_document_link_select(
            "CMS_MSMU",
            _ch_text("msmu", "MSMU_NO"),
            _ch_text("mf", "MFCNOTE_NO"),
            "msmu_to_dsmu_dmbag_mfcnote",
            f"FROM {q('cms_msmu')} msmu INNER JOIN {q('cms_dsmu')} dsmu ON {_ch_text('msmu', 'MSMU_NO')} = {_ch_text('dsmu', 'DSMU_NO')} INNER JOIN {q('cms_dmbag')} d ON {_ch_text('dsmu', 'DSMU_BAG_NO')} = {_ch_text('d', 'DMBAG_NO')} INNER JOIN {mf} mf ON {_ch_text('d', 'DMBAG_BAG_NO')} = {_ch_text('mf', 'MFCNOTE_BAG_NO')}",
            [_ch_not_blank("msmu", "MSMU_NO"), _ch_not_blank("dsmu", "DSMU_NO"), _ch_not_blank("dsmu", "DSMU_BAG_NO"), _ch_not_blank("d", "DMBAG_NO"), _ch_not_blank("d", "DMBAG_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_BAG_NO"), _ch_not_blank("mf", "MFCNOTE_NO")],
        ))

    if has("cms_dsj", "cms_rdsj", "cms_dhicnote"):
        selects.append(_document_link_select(
            "CMS_DSJ",
            _ch_text("dsj", "DSJ_NO"),
            _ch_text("dhic", "DHICNOTE_CNOTE_NO"),
            "dsj_to_rdsj_mhicnote",
            f"FROM {q('cms_dsj')} dsj INNER JOIN {q('cms_rdsj')} r ON {_ch_text('dsj', 'DSJ_HVO_NO')} = {_ch_text('r', 'RDSJ_HVO_NO')} INNER JOIN {q('cms_dhicnote')} dhic ON {_ch_text('r', 'RDSJ_HVI_NO')} = {_ch_text('dhic', 'DHICNOTE_NO')}",
            [_ch_not_blank("dsj", "DSJ_NO"), _ch_not_blank("dsj", "DSJ_HVO_NO"), _ch_not_blank("r", "RDSJ_HVO_NO"), _ch_not_blank("r", "RDSJ_HVI_NO"), _ch_not_blank("dhic", "DHICNOTE_NO"), _ch_not_blank("dhic", "DHICNOTE_CNOTE_NO")],
        ))
    if has("cms_rdsj", "cms_dhocnote"):
        selects.append(_document_link_select(
            "CMS_RDSJ",
            _ch_text("r", "RDSJ_NO"),
            _ch_text("dhoc", "DHOCNOTE_CNOTE_NO"),
            "rdsj_hvo_to_mhocnote",
            f"FROM {q('cms_rdsj')} r INNER JOIN {q('cms_dhocnote')} dhoc ON {_ch_text('r', 'RDSJ_HVO_NO')} = {_ch_text('dhoc', 'DHOCNOTE_NO')}",
            [_ch_not_blank("r", "RDSJ_NO"), _ch_not_blank("r", "RDSJ_HVO_NO"), _ch_not_blank("dhoc", "DHOCNOTE_NO"), _ch_not_blank("dhoc", "DHOCNOTE_CNOTE_NO")],
        ))
    if has("cms_msj", "cms_dsj", "cms_rdsj", "cms_dhicnote"):
        selects.append(_document_link_select(
            "CMS_MSJ",
            _ch_text("msj", "MSJ_NO"),
            _ch_text("dhic", "DHICNOTE_CNOTE_NO"),
            "msj_to_dsj_rdsj_mhicnote",
            f"FROM {q('cms_msj')} msj INNER JOIN {q('cms_dsj')} dsj ON {_ch_text('msj', 'MSJ_NO')} = {_ch_text('dsj', 'DSJ_NO')} INNER JOIN {q('cms_rdsj')} r ON {_ch_text('dsj', 'DSJ_HVO_NO')} = {_ch_text('r', 'RDSJ_HVO_NO')} INNER JOIN {q('cms_dhicnote')} dhic ON {_ch_text('r', 'RDSJ_HVI_NO')} = {_ch_text('dhic', 'DHICNOTE_NO')}",
            [_ch_not_blank("msj", "MSJ_NO"), _ch_not_blank("dsj", "DSJ_NO"), _ch_not_blank("dsj", "DSJ_HVO_NO"), _ch_not_blank("r", "RDSJ_HVO_NO"), _ch_not_blank("r", "RDSJ_HVI_NO"), _ch_not_blank("dhic", "DHICNOTE_NO"), _ch_not_blank("dhic", "DHICNOTE_CNOTE_NO")],
        ))

    _create_database(client, governance)
    _command(client, f"DROP TABLE IF EXISTS {_qualified(governance, target_table)}")
    if not selects:
        _log("No supported ClickHouse document-to-CNOTE link sources are available; creating empty link table")
        return _empty_document_links_table(client, governance, target_table)

    union_sql = "\nUNION ALL\n".join(selects)
    _command(
        client,
        f"""
        CREATE TABLE {_qualified(governance, target_table)}
        ENGINE = MergeTree
        ORDER BY tuple()
        AS
        SELECT DISTINCT l.*
        FROM (
            {union_sql}
        ) l
        INNER JOIN {_qualified(bronze, 'cms_cnote')} c
            ON l.cnote_no = {_ch_text('c', 'CNOTE_NO')}
        WHERE {_ch_not_blank('c', 'CNOTE_NO')}
        """,
    )
    row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(governance, target_table)}"))
    _log(
        f"Built ClickHouse {governance}.{target_table}: "
        f"{row_count:,} rows in {time.monotonic() - started:.1f}s"
    )
    return row_count


def _timestamp_expr(alias: str, column: str) -> str:
    return f"parseDateTimeBestEffortOrNull(toString({alias}.{_quote_ident(column)}))"


def _transform_query_settings() -> str:
    return """
        SETTINGS
            max_threads = 1,
            join_algorithm = 'grace_hash',
            max_bytes_before_external_group_by = 1073741824,
            max_bytes_before_external_sort = 1073741824
    """


def _create_cnote_feature_table(
    client: Any,
    schema: str,
    table: str,
    required_tables: Iterable[str],
    select_sql: str,
    empty_columns: str,
) -> None:
    target = _qualified(schema, table)
    _command(client, f"DROP TABLE IF EXISTS {target}")
    missing = [source for source in required_tables if not _table_exists(client, schema, source)]
    if missing:
        _log(
            "ClickHouse CNOTE transform: "
            f"{table} uses empty input because missing table(s): {', '.join(missing)}"
        )
        select_sql = f"SELECT {empty_columns} WHERE 0"

    _command(
        client,
        f"""
        CREATE TABLE {target}
        ENGINE = MergeTree
        ORDER BY tuple()
        AS
        {select_sql}
        {_transform_query_settings()}
        """,
    )
    row_count = int(_query_scalar(client, f"SELECT count() FROM {target}"))
    _log(f"Built ClickHouse transform helper {schema}.{table}: {row_count:,} rows")


def _build_clickhouse_cnote_transform(client: Any, config: MartClickHouseConfig) -> int:
    bronze = config.schemas.bronze
    if not _table_exists(client, bronze, "cms_cnote_raw"):
        raise RuntimeError("Cannot build transformed bronze.cms_cnote because bronze.cms_cnote_raw is missing")

    started = time.monotonic()
    q = lambda table: _qualified(bronze, table)
    text = _ch_text
    ts = _timestamp_expr

    helper_tables = (
        "__cnote_transform_transit",
        "__cnote_transform_pickup",
        "__cnote_transform_handover_in",
        "__cnote_transform_handover_out",
        "__cnote_transform_runsheet",
        "__cnote_transform_pod",
    )
    for helper_table in helper_tables:
        _command(client, f"DROP TABLE IF EXISTS {_qualified(bronze, helper_table)}")

    try:
        _create_cnote_feature_table(
            client,
            bronze,
            "__cnote_transform_transit",
            ("cms_mfcnote", "cms_manifest"),
        f"""
            SELECT
                {text('mf', 'MFCNOTE_NO')} AS cnote_no,
                count() AS transit_leg_count,
                min({ts('mf', 'MFCNOTE_CRDATE')}) AS first_manifest_ts
            FROM {q('cms_mfcnote')} mf
            INNER JOIN {q('cms_manifest')} m
                ON {text('mf', 'MFCNOTE_MAN_NO')} = {text('m', 'MANIFEST_NO')}
            WHERE toInt32OrNull(toString(m.{_quote_ident('MANIFEST_CODE')})) = 3
              AND {text('mf', 'MFCNOTE_NO')} != ''
            GROUP BY cnote_no
        """,
            "CAST('' AS String) AS cnote_no, CAST(0 AS UInt64) AS transit_leg_count, CAST(NULL AS Nullable(DateTime64(3))) AS first_manifest_ts",
        )
        _create_cnote_feature_table(
            client,
            bronze,
            "__cnote_transform_pickup",
            ("cms_drcnote", "cms_mrcnote"),
        f"""
            SELECT
                {text('d', 'DRCNOTE_CNOTE_NO')} AS cnote_no,
                min({ts('r', 'MRCNOTE_DATE')}) AS pickup_ts
            FROM {q('cms_drcnote')} d
            INNER JOIN {q('cms_mrcnote')} r
                ON {text('d', 'DRCNOTE_NO')} = {text('r', 'MRCNOTE_NO')}
            WHERE {text('d', 'DRCNOTE_CNOTE_NO')} != ''
            GROUP BY cnote_no
        """,
            "CAST('' AS String) AS cnote_no, CAST(NULL AS Nullable(DateTime64(3))) AS pickup_ts",
        )
        _create_cnote_feature_table(
            client,
            bronze,
            "__cnote_transform_handover_in",
            ("cms_dhicnote",),
        f"""
            SELECT
                {text('dhi', 'DHICNOTE_CNOTE_NO')} AS cnote_no,
                min({ts('dhi', 'DHICNOTE_TDATE')}) AS handover_in_ts
            FROM {q('cms_dhicnote')} dhi
            WHERE {text('dhi', 'DHICNOTE_CNOTE_NO')} != ''
            GROUP BY cnote_no
        """,
            "CAST('' AS String) AS cnote_no, CAST(NULL AS Nullable(DateTime64(3))) AS handover_in_ts",
        )
        _create_cnote_feature_table(
            client,
            bronze,
            "__cnote_transform_handover_out",
            ("cms_dhocnote",),
        f"""
            SELECT
                {text('dho', 'DHOCNOTE_CNOTE_NO')} AS cnote_no,
                min({ts('dho', 'DHOCNOTE_TDATE')}) AS handover_out_ts
            FROM {q('cms_dhocnote')} dho
            WHERE {text('dho', 'DHOCNOTE_CNOTE_NO')} != ''
            GROUP BY cnote_no
        """,
            "CAST('' AS String) AS cnote_no, CAST(NULL AS Nullable(DateTime64(3))) AS handover_out_ts",
        )
        _create_cnote_feature_table(
            client,
            bronze,
            "__cnote_transform_runsheet",
            ("cms_drsheet",),
        f"""
            SELECT
                {text('drs', 'DRSHEET_CNOTE_NO')} AS cnote_no,
                min({ts('drs', 'DRSHEET_DATE')}) AS runsheet_ts
            FROM {q('cms_drsheet')} drs
            WHERE {text('drs', 'DRSHEET_CNOTE_NO')} != ''
            GROUP BY cnote_no
        """,
            "CAST('' AS String) AS cnote_no, CAST(NULL AS Nullable(DateTime64(3))) AS runsheet_ts",
        )
        _create_cnote_feature_table(
            client,
            bronze,
            "__cnote_transform_pod",
            ("cms_cnote_pod",),
        f"""
            SELECT
                {text('pod', 'CNOTE_POD_NO')} AS cnote_no,
                min({ts('pod', 'CNOTE_POD_DATE')}) AS pod_ts
            FROM {q('cms_cnote_pod')} pod
            WHERE {text('pod', 'CNOTE_POD_NO')} != ''
            GROUP BY cnote_no
        """,
            "CAST('' AS String) AS cnote_no, CAST(NULL AS Nullable(DateTime64(3))) AS pod_ts",
        )

        _command(client, f"DROP TABLE IF EXISTS {_qualified(bronze, 'cms_cnote')}")
        _command(
            client,
            f"""
            CREATE TABLE {_qualified(bronze, 'cms_cnote')}
            ENGINE = MergeTree
            ORDER BY tuple()
            AS
            WITH
        parts AS (
            SELECT
                c.*,
                regexpExtract(upper(trimBoth(toString(c.{_quote_ident('CNOTE_ORIGIN')}))), '^([A-Z]{{3}})', 1) AS o_code,
                regexpExtract(upper(trimBoth(toString(c.{_quote_ident('CNOTE_DESTINATION')}))), '^([A-Z]{{3}})', 1) AS d_code,
                regexpExtract(upper(trimBoth(toString(c.{_quote_ident('CNOTE_ORIGIN')}))), '^[A-Z]{{3}}([0-9])', 1) AS o_digit,
                regexpExtract(upper(trimBoth(toString(c.{_quote_ident('CNOTE_DESTINATION')}))), '^[A-Z]{{3}}([0-9])', 1) AS d_digit,
                isNotNull(t.cnote_no) AND t.cnote_no != '' AS has_transit,
                ifNull(t.transit_leg_count, 0) AS transit_leg_count,
                t.first_manifest_ts AS first_manifest_ts,
                p.pickup_ts AS pickup_ts,
                hi.handover_in_ts AS handover_in_ts,
                ho.handover_out_ts AS handover_out_ts,
                rs.runsheet_ts AS runsheet_ts,
                pod.pod_ts AS pod_ts
            FROM {q('cms_cnote_raw')} c
            LEFT JOIN {q('__cnote_transform_transit')} t ON {text('c', 'CNOTE_NO')} = t.cnote_no
            LEFT JOIN {q('__cnote_transform_pickup')} p ON {text('c', 'CNOTE_NO')} = p.cnote_no
            LEFT JOIN {q('__cnote_transform_handover_in')} hi ON {text('c', 'CNOTE_NO')} = hi.cnote_no
            LEFT JOIN {q('__cnote_transform_handover_out')} ho ON {text('c', 'CNOTE_NO')} = ho.cnote_no
            LEFT JOIN {q('__cnote_transform_runsheet')} rs ON {text('c', 'CNOTE_NO')} = rs.cnote_no
            LEFT JOIN {q('__cnote_transform_pod')} pod ON {text('c', 'CNOTE_NO')} = pod.cnote_no
        ),
        classified AS (
            SELECT
                *,
                if(has_transit, 'Transit', 'Direct') AS delivery_type,
                multiIf(
                    o_code = '' OR d_code = '' OR o_digit = '' OR d_digit = '', 'Unknown',
                    o_code = d_code AND o_digit = d_digit, 'Intracity',
                    o_code = d_code, 'Intercity',
                    'Domestic'
                ) AS shipment_scope,
                transit_leg_count AS transit_manifest_count,
                transit_leg_count
                    + if(isNotNull(pickup_ts), 1, 0)
                    + if(isNotNull(handover_in_ts), 1, 0)
                    + if(isNotNull(handover_out_ts), 1, 0)
                    + if(isNotNull(runsheet_ts), 1, 0) AS handover_count,
                if(
                    isNotNull(pickup_ts) AND isNotNull(pod_ts),
                    dateDiff('second', pickup_ts, pod_ts) / 3600.0,
                    NULL
                ) AS sla_total_hours,
                if(
                    isNotNull(pickup_ts) AND isNotNull(first_manifest_ts),
                    dateDiff('second', pickup_ts, first_manifest_ts) / 3600.0,
                    NULL
                ) AS sla_pickup_to_firstmanifest_hours,
                toJSONString(map(
                    'pickup', ifNull(toString(pickup_ts), ''),
                    'first_manifest', ifNull(toString(first_manifest_ts), ''),
                    'handover_in', ifNull(toString(handover_in_ts), ''),
                    'handover_out', ifNull(toString(handover_out_ts), ''),
                    'delivery', ifNull(toString(runsheet_ts), ''),
                    'pod', ifNull(toString(pod_ts), '')
                )) AS sla_per_step
            FROM parts
        )
        SELECT
            * EXCEPT (
                o_code, d_code, o_digit, d_digit, has_transit, transit_leg_count,
                first_manifest_ts, pickup_ts, handover_in_ts, handover_out_ts,
                runsheet_ts, pod_ts
            ),
            multiIf(
                shipment_scope IN ('Intracity', 'Intercity'), shipment_scope,
                shipment_scope = 'Domestic' AND delivery_type = 'Transit', 'Transit Domestic',
                shipment_scope = 'Domestic' AND delivery_type = 'Direct', 'Direct Domestic',
                shipment_scope
            ) AS delivery_category
        FROM classified
            {_transform_query_settings()}
            """,
        )
        row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(bronze, 'cms_cnote')}"))
        raw_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(bronze, 'cms_cnote_raw')}"))
        if row_count != raw_count:
            raise RuntimeError(f"bronze.cms_cnote row count mismatch: raw={raw_count:,}, transformed={row_count:,}")
        _log(f"Built ClickHouse {bronze}.cms_cnote from cms_cnote_raw: {row_count:,} rows in {time.monotonic() - started:.1f}s")
        return row_count
    finally:
        for helper_table in helper_tables:
            _command(client, f"DROP TABLE IF EXISTS {_qualified(bronze, helper_table)}")


def _read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            columns = next(reader)
        except StopIteration:
            columns = []
    columns = [column.strip() for column in columns if column.strip()]
    if not columns:
        raise ValueError(f"Governance results CSV has no header: {path}")
    return columns


def _load_governance_csv(
    client: Any,
    schema: str,
    table: str,
    path: Path,
    batch_size: int = 100_000,
) -> int:
    columns = _read_csv_header(path)
    column_defs = ", ".join(f"{_quote_ident(column)} Nullable(String)" for column in columns)
    _command(
        client,
        f"CREATE TABLE {_qualified(schema, table)} "
        f"({column_defs}) ENGINE = MergeTree ORDER BY tuple()",
    )

    row_count = 0
    batch = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for line_number, row in enumerate(reader, start=2):
            if len(row) != len(columns):
                preview = ",".join(row[: min(len(row), 5)])
                raise ValueError(
                    f"Governance CSV row has {len(row)} columns but header has {len(columns)} "
                    f"at {path}:{line_number}. First values: {preview!r}"
                )
            batch.append([value if value != "" else None for value in row])
            if len(batch) >= batch_size:
                client.insert(
                    table,
                    batch,
                    column_names=columns,
                    database=schema,
                )
                row_count += len(batch)
                _log(f"Loaded ClickHouse {schema}.{table}: {row_count:,} rows")
                batch = []
    if batch:
        client.insert(
            table,
            batch,
            column_names=columns,
            database=schema,
        )
        row_count += len(batch)

    _log(f"Loaded ClickHouse {schema}.{table}: {row_count:,} rows from {path}")
    return row_count


def _load_governance_results(client: Any, config: MartClickHouseConfig, batch_size: int = 100_000) -> int:
    if not config.governance.enabled:
        _log("Skipping ClickHouse governance results load because governance.enabled=false")
        return 0

    path = config.governance.results_path
    if not path.exists():
        raise FileNotFoundError(f"Governance results file not found: {path}")

    _create_database(client, config.schemas.governance)
    for table in (
        config.governance.results_table,
        config.governance.result_cnotes_table,
        config.governance.summary_table,
    ):
        _command(client, f"DROP TABLE IF EXISTS {_qualified(config.schemas.governance, table)}")
    row_count = _load_governance_csv(
        client,
        config.schemas.governance,
        config.governance.results_table,
        path,
        batch_size=batch_size,
    )
    if config.governance.result_cnotes_path.exists():
        row_count += _load_governance_csv(
            client,
            config.schemas.governance,
            config.governance.result_cnotes_table,
            config.governance.result_cnotes_path,
            batch_size=batch_size,
        )
    else:
        _log(f"Governance result CNOTE bridge file not found, skipping: {config.governance.result_cnotes_path}")
    if config.governance.summary_path.exists():
        row_count += _load_governance_csv(
            client,
            config.schemas.governance,
            config.governance.summary_table,
            config.governance.summary_path,
            batch_size=batch_size,
        )
    else:
        _log(f"Governance rule summary file not found, skipping: {config.governance.summary_path}")
    return row_count


RESULT_COLUMNS = [
    "shipment_type",
    "result_id",
    "document_id",
    "cnote_no",
    "document_no",
    "cnote_origin",
    "cnote_destination",
    "origin_region",
    "destination_region",
    "cnote_service_code",
    "index_code",
    "element",
    "logic_description",
    "table_name",
    "level",
    "stage",
    "variable_1",
    "variable_2",
    "column_name",
    "main_indicator",
    "main_impact",
    "impact_details",
    "issue_description",
    "status",
]

RESULT_CNOTE_COLUMNS = [
    "result_id",
    "cnote_no",
    "link_method",
    "link_confidence",
    "cnote_origin",
    "cnote_destination",
    "cnote_service_code",
    "delivery_type",
    "shipment_scope",
    "delivery_category",
]

RULE_SUMMARY_COLUMNS = [
    "index_code",
    "element",
    "main_indicator",
    "rule_family",
    "table_name",
    "status",
    "total_checked",
    "total_failed",
    "result_rows",
    "skip_reason",
    "error_message",
]

CLICKHOUSE_RULE_FAMILIES = {
    "completeness",
    "conditional_completeness",
    "reference_conditional_completeness",
    "validity_regex",
    "validity_integer",
    "validity_datetime",
    "validity_in_set",
    "reference_format",
    "value_in_reference",
    "non_negative",
    "non_negative_not_in_reference",
    "uniqueness",
    "pair_consistency",
    "rounded_pair_consistency",
    "prefix_match",
    "suffix_after_prefix_match",
    "timeliness",
    "count_consistency",
    "aggregate_sum_consistency",
    "aggregate_count_consistency",
    "bridged_pair_consistency",
    "bridged_substring_match",
    "bridged_timeliness",
    "cnote_im_manifest_before_msj",
    "duplicate_aware_weight_consistency",
    "manifest_code_sequence",
    "transit_manifest_required_for_origin_mismatch",
}

SIMPLE_CLICKHOUSE_RULE_FAMILIES = {
    "completeness",
    "conditional_completeness",
    "validity_regex",
    "validity_integer",
    "validity_datetime",
    "validity_in_set",
    "non_negative",
    "uniqueness",
}


def _create_empty_governance_tables(client: Any, config: MartClickHouseConfig) -> None:
    governance = config.schemas.governance
    _create_database(client, governance)
    for table in (
        config.governance.results_table,
        config.governance.result_cnotes_table,
        config.governance.summary_table,
    ):
        _command(client, f"DROP TABLE IF EXISTS {_qualified(governance, table)}")
    _command(
        client,
        f"""
        CREATE TABLE {_qualified(governance, config.governance.results_table)}
        ({", ".join(f"{_quote_ident(column)} Nullable(String)" for column in RESULT_COLUMNS)})
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )
    _command(
        client,
        f"""
        CREATE TABLE {_qualified(governance, config.governance.result_cnotes_table)}
        ({", ".join(f"{_quote_ident(column)} Nullable(String)" for column in RESULT_CNOTE_COLUMNS)})
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )
    _command(
        client,
        f"""
        CREATE TABLE {_qualified(governance, config.governance.summary_table)}
        (
            {_quote_ident("index_code")} String,
            {_quote_ident("element")} String,
            {_quote_ident("main_indicator")} String,
            {_quote_ident("rule_family")} String,
            {_quote_ident("table_name")} String,
            {_quote_ident("status")} String,
            {_quote_ident("total_checked")} UInt64,
            {_quote_ident("total_failed")} UInt64,
            {_quote_ident("result_rows")} UInt64,
            {_quote_ident("skip_reason")} String,
            {_quote_ident("error_message")} String
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,
    )


def _document_type(table_name: str) -> str:
    return table_name.removeprefix("CMS_")


def _document_level(entry: dict[str, Any]) -> str:
    table_name = str(entry.get("table", "")).upper()
    if table_name == "CMS_CNOTE":
        return "CNOTE"
    if any(token in table_name for token in ("BAG", "MANIFEST", "SHEET", "SMU", "SJ", "HOC", "HIC")):
        return "Operational Document"
    return "Document"


def _document_stage(entry: dict[str, Any]) -> str:
    table_name = str(entry.get("table", "")).upper()
    if table_name == "CMS_CNOTE":
        return "Booking"
    if "BAG" in table_name:
        return "Bagging"
    if "MANIFEST" in table_name or "SMU" in table_name:
        return "Manifest"
    if "SHEET" in table_name or "POD" in table_name:
        return "Delivery"
    if "HOC" in table_name or "HIC" in table_name:
        return "Handover"
    return ""


def _entry_column_name(entry: dict[str, Any]) -> str:
    params = entry.get("params", {})
    for key in ("column", "left_column", "right_column", "start_column", "end_column"):
        if params.get(key):
            return str(params[key])
    if params.get("columns"):
        return ", ".join(str(column) for column in params["columns"])
    return ""


def _entry_document_column(entry: dict[str, Any]) -> str:
    params = entry.get("params", {})
    if params.get("cnote_column"):
        return str(params["cnote_column"])
    if params.get("column"):
        return str(params["column"])
    if params.get("columns"):
        return str(params["columns"][0])
    return "CNOTE_NO"


def _present_expr(alias: str, column: str) -> str:
    return f"isNotNull({alias}.{_quote_ident(column)}) AND trimBoth(toString({alias}.{_quote_ident(column)})) != ''"


def _normalized_expr(alias: str, column: str) -> str:
    return f"replaceRegexpOne(trimBoth(toString({alias}.{_quote_ident(column)})), '\\\\.0+$', '')"


def _reference_component_expr(alias: str, params: dict[str, Any]) -> str:
    column = str(params["reference_column"])
    component = str(params.get("reference_component", ""))
    value = _normalized_expr(alias, column)
    if str(params.get("reference_table", "")).upper() == "CMS_DROURATE":
        if component == "origin":
            return f"regexpExtract({value}, '^([A-Z]{{3}}[0-9]{{5}})', 1)"
        if component == "destination":
            return f"regexpExtract({value}, '^[A-Z]{{3}}[0-9]{{5}}([A-Z]{{3}}[0-9]{{5}})$', 1)"
    return value


def _column_prefix(table_name: str) -> str:
    return table_name.upper().removeprefix("CMS_")


def _alias_for_column(column: str, aliases: list[tuple[str, str]], fallback_alias: str) -> str:
    column_upper = column.upper()
    for alias, table_name in aliases:
        if column_upper.startswith(f"{_column_prefix(table_name)}_"):
            return alias
    return fallback_alias


def _alias_for_bridge_column(
    column: str,
    aliases: list[tuple[str, str]],
    joins: list[dict[str, Any]],
    fallback_alias: str,
) -> str:
    prefix_alias = _alias_for_column(column, aliases, "")
    if prefix_alias:
        return prefix_alias
    column_upper = column.upper()
    current_alias = aliases[0][0]
    for idx, join in enumerate(joins, start=1):
        join_alias = aliases[idx][0]
        if str(join["left_on"]).upper() == column_upper:
            return current_alias
        if str(join["right_on"]).upper() == column_upper:
            return join_alias
        current_alias = join_alias
    return fallback_alias


def _joined_detail_source(
    config: MartClickHouseConfig,
    detail_table: str,
    joins: list[dict[str, Any]],
) -> tuple[str, list[tuple[str, str]]]:
    aliases = [("d0", detail_table)]
    source = _qualified(config.schemas.bronze, detail_table.lower()) + " d0"
    current_alias = "d0"
    for idx, join in enumerate(joins, start=1):
        join_alias = f"d{idx}"
        join_table = str(join["table"])
        source += (
            f" INNER JOIN {_qualified(config.schemas.bronze, join_table.lower())} {join_alias} "
            f"ON {_normalized_expr(current_alias, str(join['left_on']))} = "
            f"{_normalized_expr(join_alias, str(join['right_on']))}"
        )
        aliases.append((join_alias, join_table))
        current_alias = join_alias
    return source, aliases


def _joined_bridge_source(
    config: MartClickHouseConfig,
    left_table: str,
    joins: list[dict[str, Any]],
) -> tuple[str, list[tuple[str, str]]]:
    aliases = [("b0", left_table)]
    source = _qualified(config.schemas.bronze, left_table.lower()) + " b0"
    current_alias = "b0"
    for idx, join in enumerate(joins, start=1):
        join_alias = f"b{idx}"
        join_table = str(join["table"])
        source += (
            f" INNER JOIN {_qualified(config.schemas.bronze, join_table.lower())} {join_alias} "
            f"ON {_normalized_expr(current_alias, str(join['left_on']))} = "
            f"{_normalized_expr(join_alias, str(join['right_on']))}"
        )
        aliases.append((join_alias, join_table))
        current_alias = join_alias
    return source, aliases


def _insert_summary_sql(
    config: MartClickHouseConfig,
    entry: dict[str, Any],
    status: str,
    total_checked: str = "0",
    total_failed: str = "0",
    result_rows: str = "0",
    skip_reason: str = "",
    error_message: str = "",
) -> str:
    values = [
        _quote_sql(str(entry.get("index_code", ""))),
        _quote_sql(str(entry.get("element", ""))),
        _quote_sql(str(entry.get("indicator", ""))),
        _quote_sql(str(entry.get("rule_family", ""))),
        _quote_sql(str(entry.get("table", ""))),
        status,
        total_checked,
        total_failed,
        result_rows,
        _quote_sql(skip_reason),
        _quote_sql(error_message),
    ]
    return (
        f"INSERT INTO {_qualified(config.schemas.governance, config.governance.summary_table)} "
        f"({', '.join(_quote_ident(column) for column in RULE_SUMMARY_COLUMNS)}) "
        f"SELECT {', '.join(values)}"
    )


def _rule_conditions(entry: dict[str, Any], columns: set[str]) -> tuple[str, str, str, str, set[str]]:
    params = entry.get("params", {})
    family = str(entry.get("rule_family", ""))
    required: set[str] = {_entry_document_column(entry)}
    if family == "completeness":
        column = str(params["column"])
        required.add(column)
        return "1", f"NOT ({_present_expr('t', column)})", f"toString(t.{_quote_ident(column)})", "''", required
    if family == "conditional_completeness":
        column = str(params["column"])
        condition_column = str(params["condition_column"])
        required.update((column, condition_column))
        if params.get("condition_present"):
            condition = _present_expr("t", condition_column)
            condition_label = "'filled'"
        else:
            condition_value = str(params.get("condition_value", "Y")).strip().upper()
            condition = f"upper(trimBoth(toString(t.{_quote_ident(condition_column)}))) = {_quote_sql(condition_value)}"
            condition_label = _quote_sql(condition_value)
        return condition, f"NOT ({_present_expr('t', column)})", f"toString(t.{_quote_ident(column)})", condition_label, required
    if family == "validity_regex":
        column = str(params["column"])
        pattern = str(params["pattern"])
        required.add(column)
        present = _present_expr("t", column)
        return present, f"NOT match(toString(t.{_quote_ident(column)}), {_quote_sql(pattern)})", f"toString(t.{_quote_ident(column)})", _quote_sql(pattern), required
    if family == "validity_integer":
        column = str(params["column"])
        required.add(column)
        present = _present_expr("t", column)
        return present, f"isNull(toInt64OrNull(toString(t.{_quote_ident(column)})))", f"toString(t.{_quote_ident(column)})", "''", required
    if family == "validity_datetime":
        column = str(params["column"])
        required.add(column)
        present = _present_expr("t", column)
        return present, f"isNull(parseDateTimeBestEffortOrNull(toString(t.{_quote_ident(column)})))", f"toString(t.{_quote_ident(column)})", "''", required
    if family == "validity_in_set":
        column = str(params["column"])
        values = [str(value) for value in params.get("allowed_values", params.get("allowed", []))]
        required.add(column)
        present = _present_expr("t", column)
        allowed = "[" + ", ".join(_quote_sql(value) for value in values) + "]"
        return present, f"NOT has({allowed}, trimBoth(toString(t.{_quote_ident(column)})))", f"toString(t.{_quote_ident(column)})", "''", required
    if family == "non_negative":
        column = str(params["column"])
        required.add(column)
        present = _present_expr("t", column)
        numeric = f"toFloat64OrNull(toString(t.{_quote_ident(column)}))"
        return present, f"isNull({numeric}) OR {numeric} < 0", f"toString(t.{_quote_ident(column)})", "''", required
    if family == "uniqueness":
        rule_columns = [str(column) for column in params["columns"]]
        required.update(rule_columns)
        present = " AND ".join(_present_expr("t", column) for column in rule_columns)
        value_expr = "concat(" + ", '|', ".join(f"toString(t.{_quote_ident(column)})" for column in rule_columns) + ")"
        return present, "__duplicate_count > 1", value_expr, "''", required
    raise ValueError(f"Unsupported ClickHouse governance rule family: {family}")


def _pair_document_expr(params: dict[str, Any]) -> str:
    left_join_key = str(params.get("left_join_key", params.get("join_key", "")))
    right_join_key = str(params.get("right_join_key", params.get("join_key", "")))
    candidates = []
    if left_join_key:
        candidates.append(f"nullIf({_normalized_expr('l', left_join_key)}, '')")
    if right_join_key:
        candidates.append(f"nullIf({_normalized_expr('r', right_join_key)}, '')")
    candidates.append("''")
    return f"coalesce({', '.join(candidates)})"


def _reference_rule_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    table = str(entry["table"]).lower()
    reference_table = str(params["reference_table"]).lower()
    column = str(params["column"])
    document_column = _entry_document_column(entry)
    reference_value = _reference_component_expr("ref", params)
    source_value = _normalized_expr("t", column)
    ref_source = (
        f"(SELECT DISTINCT {reference_value} AS __ref_value "
        f"FROM {_qualified(config.schemas.bronze, reference_table)} ref "
        f"WHERE {reference_value} != '')"
    )
    source = (
        f"{_qualified(config.schemas.bronze, table)} t "
        f"LEFT JOIN {ref_source} r ON {source_value} = r.__ref_value"
    )
    checked_where = _present_expr("t", column)
    if entry["rule_family"] == "reference_format":
        failed_expr = f"NOT match({source_value}, '^[A-Za-z0-9]+$') OR isNull(r.__ref_value)"
    else:
        failed_expr = "isNull(r.__ref_value)"
    return source, checked_where, failed_expr, f"toString(t.{_quote_ident(column)})", _quote_sql(str(params["reference_table"])), f"trimBoth(toString(t.{_quote_ident(document_column)}))"


def _pair_rule_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    family = str(entry["rule_family"])
    left_table = str(params["left_table"]).lower()
    right_table = str(params["right_table"]).lower()
    left_key = str(params.get("left_join_key", params.get("join_key")))
    right_key = str(params.get("right_join_key", params.get("join_key")))
    left_column = str(params["left_column"])
    right_column = str(params["right_column"])
    source = (
        f"{_qualified(config.schemas.bronze, left_table)} l "
        f"INNER JOIN {_qualified(config.schemas.bronze, right_table)} r "
        f"ON {_normalized_expr('l', left_key)} = {_normalized_expr('r', right_key)}"
    )
    left_value = f"toString(l.{_quote_ident(left_column)})"
    right_value = f"toString(r.{_quote_ident(right_column)})"
    checked_where = f"{_present_expr('l', left_column)} AND {_present_expr('r', right_column)}"
    if family == "rounded_pair_consistency":
        decimals = int(params.get("decimals", 0))
        left_compare = f"round(toFloat64OrNull({left_value}), {decimals})"
        right_compare = f"round(toFloat64OrNull({right_value}), {decimals})"
        checked_where = (
            f"{checked_where} AND isNotNull(toFloat64OrNull({left_value})) "
            f"AND isNotNull(toFloat64OrNull({right_value}))"
        )
        failed_expr = f"{left_compare} != {right_compare}"
    elif family == "prefix_match":
        length = int(params.get("prefix_length", 3))
        failed_expr = f"substring({left_value}, 1, {length}) != substring({right_value}, 1, {length})"
    elif family == "suffix_after_prefix_match":
        start = int(params.get("prefix_length", 3)) + 1
        failed_expr = f"substring({left_value}, {start}) != substring({right_value}, {start})"
    else:
        failed_expr = f"{left_value} != {right_value}"
    return source, checked_where, failed_expr, left_value, right_value, _pair_document_expr(params)


def _timeliness_rule_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    start_table = str(params["start_table"]).lower()
    end_table = str(params["end_table"]).lower()
    start_key = str(params.get("start_join_key", params.get("join_key")))
    end_key = str(params.get("end_join_key", params.get("join_key")))
    start_column = str(params["start_column"])
    end_column = str(params["end_column"])
    source = (
        f"{_qualified(config.schemas.bronze, start_table)} s "
        f"INNER JOIN {_qualified(config.schemas.bronze, end_table)} e "
        f"ON {_normalized_expr('s', start_key)} = {_normalized_expr('e', end_key)}"
    )
    start_value = f"parseDateTimeBestEffortOrNull(toString(s.{_quote_ident(start_column)}))"
    end_value = f"parseDateTimeBestEffortOrNull(toString(e.{_quote_ident(end_column)}))"
    checked_where = f"isNotNull({start_value}) AND isNotNull({end_value})"
    failed_expr = f"{start_value} > {end_value}"
    document_column = str(params.get("cnote_column", start_key))
    document_expr = f"trimBoth(toString(s.{_quote_ident(document_column)}))"
    return source, checked_where, failed_expr, f"toString({start_value})", f"toString({end_value})", document_expr


def _non_negative_not_reference_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    table = str(entry["table"]).lower()
    reference_table = str(params["reference_table"]).lower()
    column = str(params["column"])
    cnote_column = str(params.get("cnote_column", "CNOTE_NO"))
    reference_column = str(params["reference_column"])
    ref_source = (
        f"(SELECT DISTINCT {_normalized_expr('ref', reference_column)} AS __ref_value "
        f"FROM {_qualified(config.schemas.bronze, reference_table)} ref "
        f"WHERE {_normalized_expr('ref', reference_column)} != '')"
    )
    source = (
        f"{_qualified(config.schemas.bronze, table)} t "
        f"LEFT JOIN {ref_source} r ON {_normalized_expr('t', cnote_column)} = r.__ref_value"
    )
    numeric = f"toFloat64OrNull(toString(t.{_quote_ident(column)}))"
    checked_where = f"isNotNull({numeric})"
    failed_expr = f"{numeric} < 0 OR isNotNull(r.__ref_value)"
    return source, checked_where, failed_expr, f"toString(t.{_quote_ident(column)})", _quote_sql(str(params["reference_table"])), f"trimBoth(toString(t.{_quote_ident(cnote_column)}))"


def _reference_conditional_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    table = str(entry["table"]).lower()
    column = str(params["column"])
    condition_column = str(params["condition_column"])
    document_column = _entry_document_column(entry)
    references = params.get("references") or [{"table": params["reference_table"], "column": params["reference_column"]}]
    selects = []
    for ref in references:
        ref_table = str(ref["table"]).lower()
        ref_column = str(ref["column"])
        selects.append(
            f"SELECT DISTINCT {_normalized_expr('ref', ref_column)} AS __ref_value "
            f"FROM {_qualified(config.schemas.bronze, ref_table)} ref "
            f"WHERE {_normalized_expr('ref', ref_column)} != ''"
        )
    ref_source = "(" + " UNION DISTINCT ".join(selects) + ")"
    source = (
        f"{_qualified(config.schemas.bronze, table)} t "
        f"INNER JOIN {ref_source} r ON {_normalized_expr('t', condition_column)} = r.__ref_value"
    )
    checked_where = "1"
    failed_expr = f"NOT ({_present_expr('t', column)})"
    return source, checked_where, failed_expr, f"toString(t.{_quote_ident(column)})", f"toString(t.{_quote_ident(condition_column)})", f"trimBoth(toString(t.{_quote_ident(document_column)}))"


def _count_consistency_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    master_table = str(params["master_table"]).lower()
    child_table = str(params["child_table"]).lower()
    master_key = str(params["master_key"])
    child_key = str(params["child_key"])
    count_column = str(params["count_column"])
    master_column = str(params["master_column"])
    document_column = str(params.get("cnote_column", master_key))
    child_counts = (
        f"(SELECT {_normalized_expr('c', child_key)} AS __join_key, "
        f"uniqExact({_normalized_expr('c', count_column)}) AS __child_count "
        f"FROM {_qualified(config.schemas.bronze, child_table)} c "
        f"WHERE {_normalized_expr('c', count_column)} != '' "
        f"GROUP BY __join_key)"
    )
    source = (
        f"{_qualified(config.schemas.bronze, master_table)} t "
        f"INNER JOIN {child_counts} c ON {_normalized_expr('t', master_key)} = c.__join_key"
    )
    master_value = f"toFloat64OrNull(toString(t.{_quote_ident(master_column)}))"
    checked_where = f"isNotNull({master_value})"
    failed_expr = f"{master_value} != c.__child_count"
    return source, checked_where, failed_expr, f"toString(t.{_quote_ident(master_column)})", "toString(c.__child_count)", f"trimBoth(toString(t.{_quote_ident(document_column)}))"


def _aggregate_sum_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    master_table = str(params["master_table"]).lower()
    detail_table = str(params["detail_table"]).lower()
    master_key = str(params["master_key"])
    detail_key = str(params["detail_key"])
    master_value_column = str(params["master_value_column"])
    detail_value_column = str(params["detail_value_column"])
    document_column = str(params.get("cnote_column", master_key))
    decimals = int(params.get("decimals", 0))
    detail_source, detail_aliases = _joined_detail_source(config, detail_table, params.get("joins", []))
    detail_key_alias = _alias_for_column(detail_key, detail_aliases, detail_aliases[-1][0])
    detail_value_alias = _alias_for_column(detail_value_column, detail_aliases, "d0")
    detail_totals = (
        f"(SELECT {_normalized_expr(detail_key_alias, detail_key)} AS __join_key, "
        f"sum(toFloat64OrNull(toString({detail_value_alias}.{_quote_ident(detail_value_column)}))) AS __detail_total "
        f"FROM {detail_source} "
        f"WHERE isNotNull(toFloat64OrNull(toString({detail_value_alias}.{_quote_ident(detail_value_column)}))) "
        f"GROUP BY __join_key)"
    )
    source = (
        f"{_qualified(config.schemas.bronze, master_table)} t "
        f"INNER JOIN {detail_totals} d ON {_normalized_expr('t', master_key)} = d.__join_key"
    )
    master_value = f"toFloat64OrNull(toString(t.{_quote_ident(master_value_column)}))"
    checked_where = f"isNotNull({master_value}) AND isNotNull(d.__detail_total)"
    failed_expr = f"round({master_value}, {decimals}) != round(d.__detail_total, {decimals})"
    return source, checked_where, failed_expr, f"toString(t.{_quote_ident(master_value_column)})", "toString(d.__detail_total)", f"trimBoth(toString(t.{_quote_ident(document_column)}))"


def _aggregate_count_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    master_table = str(params["master_table"]).lower()
    detail_table = str(params["detail_table"]).lower()
    master_key = str(params["master_key"])
    detail_key = str(params["detail_key"])
    master_count_column = str(params["master_count_column"])
    detail_count_column = str(params["detail_count_column"])
    document_column = str(params.get("cnote_column", master_key))
    detail_source, detail_aliases = _joined_detail_source(config, detail_table, params.get("joins", []))
    detail_key_alias = _alias_for_column(detail_key, detail_aliases, detail_aliases[-1][0])
    detail_count_alias = _alias_for_column(detail_count_column, detail_aliases, "d0")
    detail_counts = (
        f"(SELECT {_normalized_expr(detail_key_alias, detail_key)} AS __join_key, "
        f"uniqExact({_normalized_expr(detail_count_alias, detail_count_column)}) AS __detail_count "
        f"FROM {detail_source} "
        f"WHERE {_normalized_expr(detail_count_alias, detail_count_column)} != '' "
        f"GROUP BY __join_key)"
    )
    source = (
        f"{_qualified(config.schemas.bronze, master_table)} t "
        f"INNER JOIN {detail_counts} d ON {_normalized_expr('t', master_key)} = d.__join_key"
    )
    master_value = f"toFloat64OrNull(toString(t.{_quote_ident(master_count_column)}))"
    checked_where = f"isNotNull({master_value})"
    failed_expr = f"{master_value} != d.__detail_count"
    return source, checked_where, failed_expr, f"toString(t.{_quote_ident(master_count_column)})", "toString(d.__detail_count)", f"trimBoth(toString(t.{_quote_ident(document_column)}))"


def _bridged_pair_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    family = str(entry["rule_family"])
    joins = params.get("joins", [])
    source, aliases = _joined_bridge_source(config, str(params["left_table"]), joins)
    left_column = str(params["left_column"])
    right_column = str(params["right_column"])
    cnote_column = str(params.get("cnote_column", left_column))
    left_alias = _alias_for_bridge_column(left_column, aliases, joins, "b0")
    right_alias = _alias_for_bridge_column(right_column, aliases, joins, aliases[-1][0])
    cnote_alias = _alias_for_bridge_column(cnote_column, aliases, joins, "b0")
    left_value = f"toString({left_alias}.{_quote_ident(left_column)})"
    right_value = f"toString({right_alias}.{_quote_ident(right_column)})"
    checked_where = f"{_present_expr(left_alias, left_column)} AND {_present_expr(right_alias, right_column)}"
    if family == "bridged_substring_match":
        start = int(params["substring_start"]) + 1
        length = int(params["substring_length"])
        left_compare = f"substring({left_value}, {start}, {length})"
        failed_expr = f"{left_compare} != {right_value}"
        variable_1 = f"concat({left_value}, ' -> ', {left_compare})"
    elif "decimals" in params:
        decimals = int(params.get("decimals", 0))
        left_compare = f"round(toFloat64OrNull({left_value}), {decimals})"
        right_compare = f"round(toFloat64OrNull({right_value}), {decimals})"
        checked_where = (
            f"{checked_where} AND isNotNull(toFloat64OrNull({left_value})) "
            f"AND isNotNull(toFloat64OrNull({right_value}))"
        )
        failed_expr = f"{left_compare} != {right_compare}"
        variable_1 = left_value
    else:
        failed_expr = f"{left_value} != {right_value}"
        variable_1 = left_value
    document_expr = f"trimBoth(toString({cnote_alias}.{_quote_ident(cnote_column)}))"
    return source, checked_where, failed_expr, variable_1, right_value, document_expr


def _bridged_timeliness_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    joins = params.get("joins", [])
    source, aliases = _joined_bridge_source(config, str(params["left_table"]), joins)
    start_column = str(params["start_column"])
    end_column = str(params["end_column"])
    cnote_column = str(params.get("cnote_column", start_column))
    start_alias = _alias_for_bridge_column(start_column, aliases, joins, "b0")
    end_alias = _alias_for_bridge_column(end_column, aliases, joins, aliases[-1][0])
    cnote_alias = _alias_for_bridge_column(cnote_column, aliases, joins, "b0")
    start_time = f"parseDateTimeBestEffortOrNull(toString({start_alias}.{_quote_ident(start_column)}))"
    end_time = f"parseDateTimeBestEffortOrNull(toString({end_alias}.{_quote_ident(end_column)}))"
    document_expr = f"trimBoth(toString({cnote_alias}.{_quote_ident(cnote_column)}))"
    if params.get("first_start_group"):
        group_column = str(params["first_start_group"])
        group_alias = _alias_for_bridge_column(group_column, aliases, joins, "b0")
        source = (
            f"(SELECT *, row_number() OVER (PARTITION BY {_normalized_expr(group_alias, group_column)} "
            f"ORDER BY {start_time} ASC) AS __rn FROM {source} "
            f"WHERE isNotNull({start_time}) AND isNotNull({end_time})) b"
        )
        checked_where = "__rn = 1"
        start_time = f"parseDateTimeBestEffortOrNull(toString(b.{_quote_ident(start_column)}))"
        end_time = f"parseDateTimeBestEffortOrNull(toString(b.{_quote_ident(end_column)}))"
        document_expr = f"trimBoth(toString(b.{_quote_ident(cnote_column)}))"
    else:
        checked_where = f"isNotNull({start_time}) AND isNotNull({end_time})"
    failed_expr = f"{start_time} > {end_time}"
    return source, checked_where, failed_expr, f"toString({start_time})", f"toString({end_time})", document_expr


def _transit_manifest_required_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    source = (
        f"{_qualified(config.schemas.bronze, 'cms_dsmu')} d "
        f"INNER JOIN {_qualified(config.schemas.bronze, 'cms_msmu')} m "
        f"ON {_normalized_expr('d', 'DSMU_NO')} = {_normalized_expr('m', 'MSMU_NO')} "
        f"INNER JOIN {_qualified(config.schemas.bronze, 'cms_mfbag')} b "
        f"ON {_normalized_expr('d', 'DSMU_BAG_NO')} = {_normalized_expr('b', 'MFBAG_NO')}"
    )
    dsmu_origin = "trimBoth(toString(d.`DSMU_BAG_ORIGIN`))"
    msmu_origin = "trimBoth(toString(m.`MSMU_ORIGIN`))"
    manifest_no = "trimBoth(toString(b.`MFBAG_MAN_NO`))"
    checked_where = f"{dsmu_origin} != '' AND {msmu_origin} != '' AND substring({dsmu_origin}, 1, 3) != substring({msmu_origin}, 1, 3)"
    failed_expr = f"positionCaseInsensitive({manifest_no}, 'TM') = 0"
    return source, checked_where, failed_expr, f"concat({dsmu_origin}, ' / ', {msmu_origin})", manifest_no, "trimBoth(toString(b.`MFBAG_NO`))"


def _manifest_code_sequence_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    mode = str(params["mode"])
    manifest_code_column = str(params.get("manifest_code_column", "MANIFEST_CODE"))
    date_column = str(params.get("date_column", "MANIFEST_CRDATE"))
    event_time = f"parseDateTimeBestEffortOrNull(toString(man.{_quote_ident(date_column)}))"
    code = f"trimBoth(toString(man.{_quote_ident(manifest_code_column)}))"
    source = (
        "(SELECT "
        f"{_normalized_expr('mfc', 'MFCNOTE_NO')} AS document_id, "
        f"maxIf({event_time}, {code} = '1') AS om_max, "
        f"minIf({event_time}, {code} = '2') AS tm_min, "
        f"maxIf({event_time}, {code} = '2') AS tm_max, "
        f"minIf({event_time}, {code} = '3') AS im_min, "
        f"countIf({code} = '2' AND isNotNull({event_time})) AS tm_count, "
        f"uniqExactIf(toString({event_time}), {code} = '2' AND isNotNull({event_time})) AS tm_unique "
        f"FROM {_qualified(config.schemas.bronze, 'cms_mfcnote')} mfc "
        f"INNER JOIN {_qualified(config.schemas.bronze, 'cms_manifest')} man "
        f"ON {_normalized_expr('mfc', 'MFCNOTE_MAN_NO')} = {_normalized_expr('man', 'MANIFEST_NO')} "
        f"WHERE {_normalized_expr('mfc', 'MFCNOTE_NO')} != '' "
        "GROUP BY document_id) t"
    )
    if mode == "om_before_tm":
        checked_where = "isNotNull(om_max) AND isNotNull(tm_min)"
        failed_expr = "om_max > tm_min"
        variable_1 = "toString(om_max)"
        variable_2 = "toString(tm_min)"
    elif mode == "tm_sequence_before_im":
        checked_where = "tm_count > 1 OR (isNotNull(tm_max) AND isNotNull(im_min))"
        failed_expr = "(tm_count > 1 AND tm_unique < tm_count) OR (isNotNull(tm_max) AND isNotNull(im_min) AND tm_max > im_min)"
        variable_1 = "toString(tm_max)"
        variable_2 = "toString(im_min)"
    elif mode == "im_after_tm":
        checked_where = "isNotNull(tm_max) AND isNotNull(im_min)"
        failed_expr = "im_min < tm_max"
        variable_1 = "toString(tm_max)"
        variable_2 = "toString(im_min)"
    else:
        raise ValueError(f"Unsupported manifest sequence mode: {mode}")
    return source, checked_where, failed_expr, variable_1, variable_2, "document_id"


def _cnote_im_manifest_before_msj_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    manifest_code = str(params.get("manifest_code", "3"))
    manifest_code_column = str(params.get("manifest_code_column", "MANIFEST_CODE"))
    manifest_date_column = str(params.get("manifest_date_column", "MANIFEST_DATE"))
    msj_date_column = str(params.get("msj_date_column", "MSJ_SIGNDATE"))
    im_source = (
        f"(SELECT {_normalized_expr('mfc', 'MFCNOTE_NO')} AS cnote_no, "
        f"min(parseDateTimeBestEffortOrNull(toString(man.{_quote_ident(manifest_date_column)}))) AS im_time "
        f"FROM {_qualified(config.schemas.bronze, 'cms_mfcnote')} mfc "
        f"INNER JOIN {_qualified(config.schemas.bronze, 'cms_manifest')} man "
        f"ON {_normalized_expr('mfc', 'MFCNOTE_MAN_NO')} = {_normalized_expr('man', 'MANIFEST_NO')} "
        f"WHERE trimBoth(toString(man.{_quote_ident(manifest_code_column)})) = {_quote_sql(manifest_code)} "
        f"GROUP BY cnote_no)"
    )
    msj_source = (
        f"(SELECT {_normalized_expr('dh', 'DHICNOTE_CNOTE_NO')} AS cnote_no, "
        f"min(parseDateTimeBestEffortOrNull(toString(msj.{_quote_ident(msj_date_column)}))) AS msj_time "
        f"FROM {_qualified(config.schemas.bronze, 'cms_dhicnote')} dh "
        f"INNER JOIN {_qualified(config.schemas.bronze, 'cms_rdsj')} r "
        f"ON {_normalized_expr('dh', 'DHICNOTE_NO')} = {_normalized_expr('r', 'RDSJ_HVI_NO')} "
        f"INNER JOIN {_qualified(config.schemas.bronze, 'cms_dsj')} dsj "
        f"ON {_normalized_expr('r', 'RDSJ_HVO_NO')} = {_normalized_expr('dsj', 'DSJ_HVO_NO')} "
        f"INNER JOIN {_qualified(config.schemas.bronze, 'cms_msj')} msj "
        f"ON {_normalized_expr('dsj', 'DSJ_NO')} = {_normalized_expr('msj', 'MSJ_NO')} "
        f"GROUP BY cnote_no)"
    )
    source = f"{im_source} i INNER JOIN {msj_source} m ON i.cnote_no = m.cnote_no"
    checked_where = "isNotNull(i.im_time) AND isNotNull(m.msj_time)"
    failed_expr = "i.im_time > m.msj_time"
    return source, checked_where, failed_expr, "toString(i.im_time)", "toString(m.msj_time)", "i.cnote_no"


def _duplicate_aware_weight_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    params = entry.get("params", {})
    left_table = str(params["left_table"]).lower()
    right_table = str(params["right_table"]).lower()
    left_key = str(params["left_join_key"])
    right_key = str(params["right_join_key"])
    duplicate_key = str(params["duplicate_key"])
    left_column = str(params["left_column"])
    right_column = str(params["right_column"])
    decimals = int(params.get("decimals", 0))
    source = (
        f"(SELECT {_normalized_expr('l', duplicate_key)} AS document_id, "
        f"sum(toFloat64OrNull(toString(l.{_quote_ident(left_column)}))) AS left_total, "
        f"sum(toFloat64OrNull(toString(r.{_quote_ident(right_column)}))) AS right_total "
        f"FROM {_qualified(config.schemas.bronze, left_table)} l "
        f"INNER JOIN {_qualified(config.schemas.bronze, right_table)} r "
        f"ON {_normalized_expr('l', left_key)} = {_normalized_expr('r', right_key)} "
        f"WHERE {_normalized_expr('l', duplicate_key)} != '' "
        f"AND isNotNull(toFloat64OrNull(toString(l.{_quote_ident(left_column)}))) "
        f"AND isNotNull(toFloat64OrNull(toString(r.{_quote_ident(right_column)}))) "
        "GROUP BY document_id) t"
    )
    checked_where = "1"
    failed_expr = f"round(left_total, {decimals}) != round(right_total, {decimals})"
    return source, checked_where, failed_expr, "toString(left_total)", "toString(right_total)", "document_id"


def _insert_rule_results_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str]:
    table_name = str(entry["table"]).upper()
    table = table_name.lower()
    params = entry.get("params", {})
    family = str(entry.get("rule_family", ""))
    if family in {"reference_format", "value_in_reference"}:
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _reference_rule_sql(config, entry)
    elif family == "non_negative_not_in_reference":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _non_negative_not_reference_sql(config, entry)
    elif family == "reference_conditional_completeness":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _reference_conditional_sql(config, entry)
    elif family in {"pair_consistency", "rounded_pair_consistency", "prefix_match", "suffix_after_prefix_match"}:
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _pair_rule_sql(config, entry)
    elif family == "timeliness":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _timeliness_rule_sql(config, entry)
    elif family == "count_consistency":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _count_consistency_sql(config, entry)
    elif family == "aggregate_sum_consistency":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _aggregate_sum_sql(config, entry)
    elif family == "aggregate_count_consistency":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _aggregate_count_sql(config, entry)
    elif family in {"bridged_pair_consistency", "bridged_substring_match"}:
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _bridged_pair_sql(config, entry)
    elif family == "bridged_timeliness":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _bridged_timeliness_sql(config, entry)
    elif family == "transit_manifest_required_for_origin_mismatch":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _transit_manifest_required_sql(config, entry)
    elif family == "manifest_code_sequence":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _manifest_code_sequence_sql(config, entry)
    elif family == "cnote_im_manifest_before_msj":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _cnote_im_manifest_before_msj_sql(config, entry)
    elif family == "duplicate_aware_weight_consistency":
        source, checked_where, failed_expr, variable_1, variable_2, document_id_expr = _duplicate_aware_weight_sql(config, entry)
    else:
        document_column = _entry_document_column(entry)
        checked_where, failed_expr, variable_1, variable_2, _required = _rule_conditions(entry, set())
        document_id_expr = f"trimBoth(toString(t.{_quote_ident(document_column)}))"
        source = f"{_qualified(config.schemas.bronze, table)} t"
        if family == "uniqueness":
            rule_columns = [str(column) for column in params["columns"]]
            partitions = ", ".join(_quote_ident(column) for column in rule_columns)
            source = f"(SELECT *, count() OVER (PARTITION BY {partitions}) AS __duplicate_count FROM {_qualified(config.schemas.bronze, table)}) t"
    status_expr = f"if({failed_expr}, 'FAIL', 'PASS')"
    result_id_expr = (
        "concat("
        f"{_quote_sql(str(entry['index_code']))}, ':', "
        "toString(rowNumberInAllBlocks()), ':', "
        "hex(sipHash64("
        f"{document_id_expr}, {status_expr}, {variable_1}, {variable_2}"
        ")))"
    )

    values = [
        "''",
        result_id_expr,
        document_id_expr,
        f"if({_quote_sql(table_name)} = 'CMS_CNOTE', {document_id_expr}, '')",
        f"if({_quote_sql(table_name)} = 'CMS_CNOTE', '', {document_id_expr})",
        "''",
        "''",
        "''",
        "''",
        "''",
        _quote_sql(str(entry.get("index_code", ""))),
        _quote_sql(str(entry.get("element", ""))),
        _quote_sql(str(entry.get("description", ""))),
        _quote_sql(table_name),
        _quote_sql(_document_level(entry)),
        _quote_sql(_document_stage(entry)),
        variable_1,
        variable_2,
        _quote_sql(_entry_column_name(entry)),
        _quote_sql(str(entry.get("indicator", ""))),
        _quote_sql(str(entry.get("main_impact", ""))),
        _quote_sql(str(entry.get("impact_details", ""))),
        _quote_sql(str(entry.get("issue_description", ""))),
        status_expr,
    ]
    insert_sql = (
        f"INSERT INTO {_qualified(config.schemas.governance, config.governance.results_table)} "
        f"({', '.join(_quote_ident(column) for column in RESULT_COLUMNS)}) "
        f"SELECT {', '.join(values)} FROM {source} WHERE {checked_where}"
    )
    summary_sql = _insert_summary_sql(
        config,
        entry,
        "multiIf(count() = 0, 'NO_ROWS', countIf(status = 'FAIL') > 0, 'FAIL', 'PASS')",
        "toUInt64(count())",
        "toUInt64(countIf(status = 'FAIL'))",
        "toUInt64(count())",
    ) + (
        f" FROM {_qualified(config.schemas.governance, config.governance.results_table)} "
        f"WHERE index_code = {_quote_sql(str(entry.get('index_code', '')))}"
    )
    return insert_sql, summary_sql, checked_where


def _build_clickhouse_governance_results(client: Any, config: MartClickHouseConfig) -> int:
    if not config.governance.enabled:
        _log("Skipping ClickHouse governance because governance.enabled=false")
        return 0
    if config.governance.execution_mode == "csv":
        return _load_governance_results(client, config)

    from governance.catalog import CATALOG

    started = time.monotonic()
    _create_empty_governance_tables(client, config)
    active_entries = [entry for entry in CATALOG if entry.get("active", True)]
    supported = 0
    skipped = 0
    for entry in active_entries:
        table_name = str(entry.get("table", "")).upper()
        table = table_name.lower()
        family = str(entry.get("rule_family", ""))
        if family not in CLICKHOUSE_RULE_FAMILIES:
            skipped += 1
            _command(
                client,
                _insert_summary_sql(
                    config,
                    entry,
                    _quote_sql("SKIPPED"),
                    skip_reason=f"ClickHouse governance does not yet implement rule family {family}",
                ),
            )
            continue
        if not _table_exists(client, config.schemas.bronze, table):
            skipped += 1
            _command(
                client,
                _insert_summary_sql(
                    config,
                    entry,
                    _quote_sql("SKIPPED"),
                    skip_reason=f"missing ClickHouse table: {config.schemas.bronze}.{table}",
                ),
            )
            continue
        columns = _table_columns(client, config.schemas.bronze, table)
        if family in SIMPLE_CLICKHOUSE_RULE_FAMILIES:
            try:
                _checked, _failed, _v1, _v2, required = _rule_conditions(entry, columns)
            except Exception as exc:
                skipped += 1
                _command(
                    client,
                    _insert_summary_sql(config, entry, _quote_sql("ERROR"), error_message=str(exc)),
                )
                continue
            missing = sorted(column for column in required if column not in columns)
            if missing:
                skipped += 1
                _command(
                    client,
                    _insert_summary_sql(
                        config,
                        entry,
                        _quote_sql("SKIPPED"),
                        skip_reason=f"missing column(s): {', '.join(missing)}",
                    ),
                )
                continue
        try:
            insert_sql, summary_sql, _ = _insert_rule_results_sql(config, entry)
            _command(client, insert_sql)
            _command(client, summary_sql)
            supported += 1
        except Exception as exc:
            skipped += 1
            _log(f"Governance rule {entry.get('index_code', '')} failed: {exc}")
            _command(
                client,
                _insert_summary_sql(config, entry, _quote_sql("ERROR"), error_message=str(exc)),
            )

    row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(config.schemas.governance, config.governance.results_table)}"))
    _log(
        f"Built ClickHouse {config.schemas.governance}.{config.governance.results_table}: "
        f"{row_count:,} rows from {supported:,} supported rule(s), {skipped:,} skipped rule(s) "
        f"in {time.monotonic() - started:.1f}s"
    )
    return row_count


def _build_governance_result_cnotes(client: Any, config: MartClickHouseConfig) -> int:
    if not config.governance.enabled:
        return 0
    governance = config.schemas.governance
    bronze = config.schemas.bronze
    results_table = config.governance.results_table
    result_cnotes_table = config.governance.result_cnotes_table
    links_table = config.governance.document_links_table
    if not _table_exists(client, governance, results_table) or not _table_exists(client, bronze, "cms_cnote"):
        return 0

    started = time.monotonic()
    _command(client, f"DROP TABLE IF EXISTS {_qualified(governance, result_cnotes_table)}")
    results = _qualified(governance, results_table)
    cnote = _qualified(bronze, "cms_cnote")
    status_filter = ", ".join(_quote_sql(status) for status in config.governance.result_cnotes_statuses)
    status_predicate = "1" if not status_filter else f"upper(ifNull(r.`status`, '')) IN ({status_filter})"
    _log(
        f"Building ClickHouse {governance}.{result_cnotes_table} "
        f"for statuses={list(config.governance.result_cnotes_statuses) or ['ALL']}"
    )
    link_select = ""
    if config.governance.build_document_links and _table_exists(client, governance, links_table):
        link_select = f"""
            UNION ALL
            SELECT
                r.`result_id` AS result_id,
                l.`cnote_no` AS cnote_no,
                l.`link_method` AS link_method,
                l.`link_confidence` AS link_confidence
            FROM {results} r
            INNER JOIN {_qualified(governance, links_table)} l
                ON upper(trimBoth(ifNull(r.`table_name`, ''))) = l.`source_table`
               AND trimBoth(ifNull(r.`document_id`, '')) = l.`document_id`
            WHERE {status_predicate}
        """
    _command(
        client,
        f"""
        CREATE TABLE {_qualified(governance, result_cnotes_table)}
        ENGINE = MergeTree
        ORDER BY tuple()
        AS
        WITH candidates AS (
            SELECT
                r.`result_id` AS result_id,
                trimBoth(ifNull(r.`document_id`, '')) AS cnote_no,
                'direct_cnote' AS link_method,
                'safe' AS link_confidence
            FROM {results} r
            WHERE upper(trimBoth(ifNull(r.`table_name`, ''))) = 'CMS_CNOTE'
              AND trimBoth(ifNull(r.`document_id`, '')) != ''
              AND {status_predicate}
            {link_select}
        )
        SELECT DISTINCT
            cnd.result_id,
            cnd.cnote_no,
            cnd.link_method,
            cnd.link_confidence,
            ifNull(c.`CNOTE_ORIGIN`, '') AS cnote_origin,
            ifNull(c.`CNOTE_DESTINATION`, '') AS cnote_destination,
            ifNull(c.`CNOTE_SERVICES_CODE`, '') AS cnote_service_code,
            ifNull(c.`delivery_type`, '') AS delivery_type,
            ifNull(c.`shipment_scope`, '') AS shipment_scope,
            ifNull(c.`delivery_category`, '') AS delivery_category
        FROM candidates cnd
        INNER JOIN {cnote} c
            ON cnd.cnote_no = trimBoth(ifNull(c.`CNOTE_NO`, ''))
        WHERE cnd.cnote_no != ''
        """,
    )
    row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(governance, result_cnotes_table)}"))
    _log(
        f"Built ClickHouse {governance}.{result_cnotes_table}: "
        f"{row_count:,} rows in {time.monotonic() - started:.1f}s"
    )
    return row_count


def _build_governance_dashboard_table(client: Any, config: MartClickHouseConfig) -> int:
    if not config.governance.enabled or not config.governance.build_dashboard_table:
        _log("Skipping ClickHouse governance dashboard table build")
        return 0

    governance = config.schemas.governance
    bronze = config.schemas.bronze
    results_table = config.governance.results_table
    links_table = config.governance.document_links_table
    result_cnotes_table = config.governance.result_cnotes_table
    target_table = config.governance.dashboard_table
    if not _table_exists(client, governance, results_table):
        _log(f"Skipping ClickHouse governance dashboard table build because {governance}.{results_table} is missing")
        return 0
    if not _table_exists(client, bronze, "cms_cnote"):
        _log(f"Skipping ClickHouse governance dashboard table build because {bronze}.cms_cnote is missing")
        return 0

    started = time.monotonic()
    _command(client, f"DROP TABLE IF EXISTS {_qualified(governance, target_table)}")
    results = _qualified(governance, results_table)
    cnote_table = _qualified(bronze, "cms_cnote")
    if _table_exists(client, governance, result_cnotes_table):
        result_cnotes = _qualified(governance, result_cnotes_table)
        _command(
            client,
            f"""
            CREATE TABLE {_qualified(governance, target_table)}
            ENGINE = MergeTree
            ORDER BY tuple()
            AS
            SELECT
                r.*,
                ifNull(rc.`cnote_no`, '') AS linked_cnote_no,
                ifNull(rc.`link_method`, '') AS link_method,
                ifNull(rc.`link_confidence`, '') AS link_confidence,
                ifNull(rc.`cnote_origin`, '') AS linked_cnote_origin,
                ifNull(rc.`cnote_destination`, '') AS linked_cnote_destination,
                ifNull(rc.`cnote_service_code`, '') AS linked_cnote_service_code,
                ifNull(rc.`delivery_type`, '') AS linked_delivery_type,
                ifNull(rc.`shipment_scope`, '') AS linked_shipment_scope,
                ifNull(rc.`delivery_category`, '') AS linked_delivery_category
            FROM {results} r
            LEFT JOIN {result_cnotes} rc
                ON r.`result_id` = rc.`result_id`
            """,
        )
        row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(governance, target_table)}"))
        _log(
            f"Built ClickHouse {governance}.{target_table}: "
            f"{row_count:,} rows in {time.monotonic() - started:.1f}s"
        )
        return row_count

    link_join = ""
    link_cnote_expr = "CAST(NULL, 'Nullable(String)')"
    link_method_expr = "CAST(NULL, 'Nullable(String)')"
    link_confidence_expr = "CAST(NULL, 'Nullable(String)')"
    if _table_exists(client, governance, links_table):
        link_join = (
            f"LEFT JOIN {_qualified(governance, links_table)} l "
            "ON upper(trimBoth(ifNull(r.`table_name`, ''))) = l.`source_table` "
            "AND trimBoth(ifNull(r.`document_id`, '')) = l.`document_id`"
        )
        link_cnote_expr = "l.`cnote_no`"
        link_method_expr = "l.`link_method`"
        link_confidence_expr = "l.`link_confidence`"

    _command(
        client,
        f"""
        CREATE TABLE {_qualified(governance, target_table)}
        ENGINE = MergeTree
        ORDER BY tuple()
        AS
        WITH linked AS (
            SELECT
                r.*,
                multiIf(
                    nullIf(trimBoth(ifNull({link_cnote_expr}, '')), '') IS NOT NULL,
                        trimBoth(ifNull({link_cnote_expr}, '')),
                    upper(trimBoth(ifNull(r.`table_name`, ''))) = 'CMS_CNOTE'
                        AND nullIf(trimBoth(ifNull(r.`document_id`, '')), '') IS NOT NULL,
                        trimBoth(ifNull(r.`document_id`, '')),
                    nullIf(trimBoth(ifNull(r.`cnote_no`, '')), '') IS NOT NULL,
                        trimBoth(ifNull(r.`cnote_no`, '')),
                    nullIf(trimBoth(ifNull(direct.`CNOTE_NO`, '')), '') IS NOT NULL,
                        trimBoth(ifNull(r.`document_id`, '')),
                    ''
                ) AS linked_cnote_no,
                multiIf(
                    nullIf(trimBoth(ifNull({link_method_expr}, '')), '') IS NOT NULL,
                        trimBoth(ifNull({link_method_expr}, '')),
                    nullIf(trimBoth(ifNull(direct.`CNOTE_NO`, '')), '') IS NOT NULL
                        OR upper(trimBoth(ifNull(r.`table_name`, ''))) = 'CMS_CNOTE',
                        'direct_cnote',
                    ''
                ) AS link_method,
                multiIf(
                    nullIf(trimBoth(ifNull({link_confidence_expr}, '')), '') IS NOT NULL,
                        trimBoth(ifNull({link_confidence_expr}, '')),
                    nullIf(trimBoth(ifNull(direct.`CNOTE_NO`, '')), '') IS NOT NULL
                        OR upper(trimBoth(ifNull(r.`table_name`, ''))) = 'CMS_CNOTE',
                        'safe',
                    ''
                ) AS link_confidence
            FROM {results} r
            {link_join}
            LEFT JOIN {cnote_table} direct
                ON trimBoth(ifNull(r.`document_id`, '')) = trimBoth(ifNull(direct.`CNOTE_NO`, ''))
        )
        SELECT
            linked.*,
            ifNull(c.`CNOTE_ORIGIN`, '') AS linked_cnote_origin,
            ifNull(c.`CNOTE_DESTINATION`, '') AS linked_cnote_destination,
            ifNull(c.`CNOTE_SERVICES_CODE`, '') AS linked_cnote_service_code,
            ifNull(c.`delivery_type`, '') AS linked_delivery_type,
            ifNull(c.`shipment_scope`, '') AS linked_shipment_scope,
            ifNull(c.`delivery_category`, '') AS linked_delivery_category
        FROM linked
        LEFT JOIN {cnote_table} c
            ON linked.linked_cnote_no = trimBoth(ifNull(c.`CNOTE_NO`, ''))
        """,
    )
    row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(governance, target_table)}"))
    _log(
        f"Built ClickHouse {governance}.{target_table}: "
        f"{row_count:,} rows in {time.monotonic() - started:.1f}s"
    )
    return row_count


def _render_unified_sql(config: MartClickHouseConfig) -> str:
    if not config.unified_mart.sql_path.exists():
        raise FileNotFoundError(f"Unified mart SQL file not found: {config.unified_mart.sql_path}")
    template = config.unified_mart.sql_path.read_text(encoding="utf-8")
    return template.format(
        bronze_schema=_quote_ident(config.schemas.bronze),
        target_table=_qualified(config.unified_mart.schema, config.unified_mart.table),
    )


def _validate_unified_sources(client: Any, config: MartClickHouseConfig) -> None:
    missing = [
        table
        for table in UNIFIED_REQUIRED_TABLES
        if not _table_exists(client, config.schemas.bronze, table)
    ]
    if missing:
        formatted = ", ".join(f"{config.schemas.bronze}.{table}" for table in missing)
        raise RuntimeError(f"Cannot build unified mart table; missing required ClickHouse table(s): {formatted}")


def _load_unified_mart(client: Any, config: MartClickHouseConfig) -> int:
    if not config.unified_mart.enabled:
        _log("Skipping unified ClickHouse mart table because unified_mart.enabled=false")
        return 0

    started = time.monotonic()
    _validate_unified_sources(client, config)
    _create_database(client, config.unified_mart.schema)
    _command(client, f"DROP TABLE IF EXISTS {_qualified(config.unified_mart.schema, config.unified_mart.table)}")
    _command(client, _render_unified_sql(config))
    row_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(config.unified_mart.schema, config.unified_mart.table)}"))
    bronze_cnote_count = int(_query_scalar(client, f"SELECT count() FROM {_qualified(config.schemas.bronze, 'cms_cnote')}"))
    if row_count != bronze_cnote_count:
        raise RuntimeError(
            f"Unified mart row count mismatch: expected {bronze_cnote_count:,} rows from "
            f"{config.schemas.bronze}.cms_cnote, got {row_count:,}"
        )
    _log(
        f"Loaded ClickHouse {config.unified_mart.schema}.{config.unified_mart.table}: "
        f"{row_count:,} rows in {time.monotonic() - started:.1f}s"
    )
    return row_count


def _ensure_metadata_table(client: Any, config: MartClickHouseConfig) -> None:
    _create_database(client, config.clickhouse.database)
    _command(
        client,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified(config.clickhouse.database, 'mart_load_runs')} (
            run_id Nullable(String),
            window_start Nullable(Date32),
            window_end Nullable(Date32),
            bronze_bucket String,
            bronze_prefix String,
            governance_prefix String,
            table_count UInt32,
            row_count UInt64,
            status String,
            loaded_at DateTime64(3),
            error_message Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY loaded_at
        """,
    )


def _date_or_none(value: Any) -> date | None:
    if not value:
        return None
    return date.fromisoformat(str(value))


def _insert_load_run(
    client: Any,
    config: MartClickHouseConfig,
    manifest: dict[str, Any],
    table_count: int,
    row_count: int,
    status: str,
    error_message: str | None = None,
) -> None:
    _ensure_metadata_table(client, config)
    client.insert(
        "mart_load_runs",
        [[
            manifest.get("run_id"),
            _date_or_none(manifest.get("window_start")),
            _date_or_none(manifest.get("window_end")),
            config.bronze.bucket,
            config.bronze.run_prefix,
            str(config.governance.results_path) if config.governance.enabled else "",
            int(table_count),
            int(row_count),
            status,
            datetime.now(),
            error_message,
        ]],
        column_names=[
            "run_id",
            "window_start",
            "window_end",
            "bronze_bucket",
            "bronze_prefix",
            "governance_prefix",
            "table_count",
            "row_count",
            "status",
            "loaded_at",
            "error_message",
        ],
        database=config.clickhouse.database,
    )


def run(config_path: str = "config/mart_clickhouse.yaml", stage: str = "all") -> None:
    stage = stage.strip().lower()
    if stage not in {"all", "load", "transform", "governance"}:
        raise ValueError("stage must be one of: all, load, transform, governance")
    config = load_config(config_path)
    _log(
        "Starting ClickHouse mart load: "
        f"bronze=s3://{config.bronze.bucket}/{config.bronze.run_prefix}, "
        f"stage={stage}, "
        f"skip_stages={list(config.skip_stages)}, "
        f"reuse_existing_stages={list(config.reuse_existing_stages)}"
    )
    minio_client = _minio_client(config)
    manifest = _read_manifest(minio_client, config)
    _log(
        f"Read run manifest for run_id={manifest.get('run_id')} "
        f"with {len(manifest.get('tables', []))} bronze table(s), "
        f"{len(manifest.get('derived', []))} derived table(s)"
    )
    client = _connect_clickhouse(config)
    loaded_tables: dict[str, int] = {}
    transformed_rows = 0
    unified_rows = 0
    governance_rows = 0
    result_cnote_rows = 0
    document_link_rows = 0
    dashboard_rows = 0
    try:
        if stage in {"all", "load"}:
            _drop_database(client, config.schemas.bronze_staging)
            _create_database(client, config.schemas.bronze_staging)
            loaded_tables = _load_table_entries(
                client,
                config,
                manifest.get("tables", []),
                config.schemas.bronze_staging,
                "bronze",
                target_schema=config.schemas.bronze,
            )
            _publish_schema(client, config.schemas.bronze_staging, config.schemas.bronze)

        if stage in {"all", "transform"}:
            transformed_rows = _build_clickhouse_cnote_transform(client, config)
            unified_rows = _load_unified_mart(client, config)

        if stage in {"all", "governance"}:
            document_link_rows = _build_document_cnote_links(client, config)
            governance_rows = _build_clickhouse_governance_results(client, config)
            result_cnote_rows = _build_governance_result_cnotes(client, config)
            dashboard_rows = _build_governance_dashboard_table(client, config)

        total_rows = (
            sum(loaded_tables.values())
            + transformed_rows
            + document_link_rows
            + unified_rows
            + governance_rows
            + result_cnote_rows
            + dashboard_rows
        )
        _insert_load_run(
            client,
            config,
            manifest,
            table_count=(
                len(loaded_tables)
                + (1 if transformed_rows else 0)
                + (1 if document_link_rows else 0)
                + (1 if unified_rows else 0)
                + (1 if config.governance.enabled else 0)
                + (1 if result_cnote_rows else 0)
                + (1 if dashboard_rows else 0)
            ),
            row_count=total_rows,
            status="SUCCESS",
        )
    except Exception as exc:
        _drop_database(client, config.schemas.bronze_staging)
        try:
            _insert_load_run(
                client,
                config,
                manifest,
                table_count=(
                    len(loaded_tables)
                    + (1 if transformed_rows else 0)
                    + (1 if document_link_rows else 0)
                    + (1 if unified_rows else 0)
                    + (1 if governance_rows else 0)
                    + (1 if result_cnote_rows else 0)
                    + (1 if dashboard_rows else 0)
                ),
                row_count=(
                    sum(loaded_tables.values())
                    + transformed_rows
                    + document_link_rows
                    + unified_rows
                    + governance_rows
                    + result_cnote_rows
                    + dashboard_rows
                ),
                status="FAILED",
                error_message=str(exc),
            )
        except Exception:
            pass
        raise

    _log("Loaded ClickHouse mart snapshot:")
    for table, rows in sorted(loaded_tables.items()):
        _log(f"  {config.schemas.bronze}.{table}: {rows} rows")
    if transformed_rows:
        _log(f"  {config.schemas.bronze}.cms_cnote: {transformed_rows} rows")
    if config.governance.build_document_links:
        _log(f"  {config.schemas.governance}.{config.governance.document_links_table}: {document_link_rows} rows")
    if config.unified_mart.enabled:
        _log(f"  {config.unified_mart.schema}.{config.unified_mart.table}: {unified_rows} rows")
    if config.governance.enabled:
        _log(f"  {config.schemas.governance}.{config.governance.results_table}: {governance_rows} rows")
        _log(f"  {config.schemas.governance}.{config.governance.result_cnotes_table}: {result_cnote_rows} rows")
    if config.governance.build_dashboard_table:
        _log(f"  {config.schemas.governance}.{config.governance.dashboard_table}: {dashboard_rows} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load bronze Parquet into ClickHouse.")
    parser.add_argument("--config", default="config/mart_clickhouse.yaml")
    parser.add_argument("--stage", choices=["all", "load", "transform", "governance"], default="all")
    args = parser.parse_args()
    run(args.config, stage=args.stage)


if __name__ == "__main__":
    main()
