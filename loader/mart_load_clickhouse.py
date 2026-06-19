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
    summary_path: Path


@dataclass(frozen=True)
class MartClickHouseConfig:
    minio: MinioConfig
    bronze: BronzeConfig
    clickhouse: ClickHouseConfig
    schemas: SchemaConfig
    governance: GovernanceConfig
    skip_stages: tuple[str, ...] = ()
    reuse_existing_stages: tuple[str, ...] = ()
    load_mode: str = "latest_snapshot"


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
    mart = raw.get("mart", {})
    run_id = os.getenv("RUN_ID", "")
    default_governance_path = Path("governance/outputs") / run_id / "governance_results.csv"
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
            database=clickhouse.get("database", "jne_mart"),
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
            summary_path=Path(governance.get("summary_path") or default_summary_path),
        ),
        skip_stages=tuple(str(stage).lower() for stage in mart.get("skip_stages", [])),
        reuse_existing_stages=tuple(str(stage).lower() for stage in mart.get("reuse_existing_stages", [])),
        load_mode=mart.get("load_mode", "latest_snapshot"),
    )
    if config.load_mode != "latest_snapshot":
        raise ValueError(f"Unsupported mart.load_mode: {config.load_mode}")
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

    _drop_database(client, config.schemas.governance)
    _create_database(client, config.schemas.governance)
    row_count = _load_governance_csv(
        client,
        config.schemas.governance,
        "governance_results",
        path,
        batch_size=batch_size,
    )
    if config.governance.summary_path.exists():
        row_count += _load_governance_csv(
            client,
            config.schemas.governance,
            "governance_rule_summary",
            config.governance.summary_path,
            batch_size=batch_size,
        )
    else:
        _log(f"Governance rule summary file not found, skipping: {config.governance.summary_path}")
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


def run(config_path: str = "config/mart_clickhouse.yaml") -> None:
    config = load_config(config_path)
    _log(
        "Starting ClickHouse mart load: "
        f"bronze=s3://{config.bronze.bucket}/{config.bronze.run_prefix}, "
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
    governance_rows = 0
    try:
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

        governance_rows = _load_governance_results(client, config)
        total_rows = sum(loaded_tables.values()) + sum(loaded_derived.values()) + governance_rows
        _insert_load_run(
            client,
            config,
            manifest,
            table_count=len(loaded_tables) + len(loaded_derived) + (1 if config.governance.enabled else 0),
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
                table_count=len(loaded_tables) + len(loaded_derived) + (1 if governance_rows else 0),
                row_count=sum(loaded_tables.values()) + sum(loaded_derived.values()) + governance_rows,
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
    if config.governance.enabled:
        _log(f"  {config.schemas.governance}.governance_results: {governance_rows} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load bronze Parquet into ClickHouse.")
    parser.add_argument("--config", default="config/mart_clickhouse.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
