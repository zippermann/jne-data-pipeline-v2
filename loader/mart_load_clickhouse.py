"""Load bronze, derived, and governance outputs into a ClickHouse mart."""

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
    results_table: str = "governance_results_2"
    result_cnotes_table: str = "governance_result_cnotes_2"
    summary_table: str = "governance_rule_summary_2"
    build_document_links: bool = True
    document_links_table: str = "document_cnote_links_2"
    build_dashboard_table: bool = True
    dashboard_table: str = "governance_results_dashboard_2"
    execution_mode: str = "clickhouse"


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
            results_table=governance.get("results_table", "governance_results_2"),
            result_cnotes_table=governance.get("result_cnotes_table", "governance_result_cnotes_2"),
            summary_table=governance.get("summary_table", "governance_rule_summary_2"),
            build_document_links=_as_bool(governance.get("build_document_links", True)),
            document_links_table=governance.get("document_links_table", "document_cnote_links_2"),
            build_dashboard_table=_as_bool(governance.get("build_dashboard_table", True)),
            dashboard_table=governance.get("dashboard_table", "governance_results_dashboard_2"),
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
    if table_info.get("output_name") == "cms_cnote_transformed":
        return "cms_cnote"
    return table_info["output_name"]


def _should_skip_bronze_table(table_info: dict[str, Any], config: MartClickHouseConfig) -> bool:
    stage = str(table_info.get("stage", "")).lower()
    return stage in config.skip_stages or table_info.get("output_name") == "cms_cnote"


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
            _log(f"Skipping ClickHouse bronze.{source_name}; mart uses transformed cms_cnote and skips configured stages")
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
    if not config.governance.build_document_links:
        _log("Skipping ClickHouse document-to-CNOTE link build because governance.build_document_links=false")
        return 0
    if not _table_exists(client, config.schemas.bronze, "cms_cnote"):
        _log("Skipping ClickHouse document-to-CNOTE link build because bronze.cms_cnote is missing")
        return 0

    started = time.monotonic()
    bronze = config.schemas.bronze
    governance = config.schemas.governance
    target_table = config.governance.document_links_table
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


def _insert_rule_results_sql(config: MartClickHouseConfig, entry: dict[str, Any]) -> tuple[str, str, str]:
    table_name = str(entry["table"]).upper()
    table = table_name.lower()
    params = entry.get("params", {})
    family = str(entry.get("rule_family", ""))
    document_column = _entry_document_column(entry)
    checked_where, failed_expr, variable_1, variable_2, _required = _rule_conditions(entry, set())
    document_id_expr = f"trimBoth(toString(t.{_quote_ident(document_column)}))"
    status_expr = f"if({failed_expr}, 'FAIL', 'PASS')"
    result_id_expr = (
        "concat("
        f"{_quote_sql(str(entry['index_code']))}, ':', "
        "toString(rowNumberInAllBlocks()), ':', "
        "hex(sipHash64("
        f"{document_id_expr}, {status_expr}, {variable_1}, {variable_2}"
        ")))"
    )

    source = _qualified(config.schemas.bronze, table)
    if family == "uniqueness":
        rule_columns = [str(column) for column in params["columns"]]
        partitions = ", ".join(_quote_ident(column) for column in rule_columns)
        source = f"(SELECT *, count() OVER (PARTITION BY {partitions}) AS __duplicate_count FROM {source})"

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
        f"SELECT {', '.join(values)} FROM {source} t WHERE {checked_where}"
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
        insert_sql, summary_sql, _ = _insert_rule_results_sql(config, entry)
        _command(client, insert_sql)
        _command(client, summary_sql)
        supported += 1

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
    link_select = ""
    if _table_exists(client, governance, links_table):
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
    if stage not in {"all", "load", "governance"}:
        raise ValueError("stage must be one of: all, load, governance")
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
    loaded_derived: dict[str, int] = {}
    unified_rows = 0
    governance_rows = 0
    result_cnote_rows = 0
    document_link_rows = 0
    dashboard_rows = 0
    try:
        if stage in {"all", "load"}:
            _drop_database(client, config.schemas.bronze_staging)
            _drop_database(client, config.schemas.derived_staging)
            _create_database(client, config.schemas.bronze_staging)
            loaded_tables = _load_table_entries(
                client,
                config,
                manifest.get("tables", []),
                config.schemas.bronze_staging,
                "bronze",
                target_schema=config.schemas.bronze,
            )
            if manifest.get("derived"):
                _log("Loading transformed CNOTE into ClickHouse bronze staging")
                loaded_derived = _load_table_entries(
                    client,
                    config,
                    manifest.get("derived", []),
                    config.schemas.bronze_staging,
                    "bronze",
                    default_parent="derived",
                )
            _publish_schema(client, config.schemas.bronze_staging, config.schemas.bronze)
            _drop_database(client, config.schemas.derived)
            unified_rows = _load_unified_mart(client, config)

        if stage in {"all", "governance"}:
            document_link_rows = _build_document_cnote_links(client, config)
            governance_rows = _build_clickhouse_governance_results(client, config)
            result_cnote_rows = _build_governance_result_cnotes(client, config)
            dashboard_rows = _build_governance_dashboard_table(client, config)

        total_rows = (
            sum(loaded_tables.values())
            + sum(loaded_derived.values())
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
                + len(loaded_derived)
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
        _drop_database(client, config.schemas.derived_staging)
        try:
            _insert_load_run(
                client,
                config,
                manifest,
                table_count=(
                    len(loaded_tables)
                    + len(loaded_derived)
                    + (1 if document_link_rows else 0)
                    + (1 if unified_rows else 0)
                    + (1 if governance_rows else 0)
                    + (1 if result_cnote_rows else 0)
                    + (1 if dashboard_rows else 0)
                ),
                row_count=(
                    sum(loaded_tables.values())
                    + sum(loaded_derived.values())
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
    for table, rows in sorted(loaded_derived.items()):
        _log(f"  {config.schemas.derived}.{table}: {rows} rows")
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
    parser.add_argument("--stage", choices=["all", "load", "governance"], default="all")
    args = parser.parse_args()
    run(args.config, stage=args.stage)


if __name__ == "__main__":
    main()
