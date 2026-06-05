"""Load governed bronze Parquet runs from MinIO into a ClickHouse serving layer."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq


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
class GovernanceConfig:
    output_bucket: str
    output_prefix: str


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
    governance: str
    governance_staging: str


@dataclass(frozen=True)
class MartConfig:
    minio: MinioConfig
    bronze: BronzeConfig
    governance: GovernanceConfig
    clickhouse: ClickHouseConfig
    schemas: SchemaConfig
    parquet_batch_rows: int = 10000
    load_mode: str = "latest_snapshot"


def load_config(path: str | Path = "config/mart.yaml") -> MartConfig:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyYAML is required to load mart config files. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    with Path(path).open("r", encoding="utf-8") as handle:
        raw = _expand_env(yaml.safe_load(handle) or {})

    minio = raw.get("minio", {})
    bronze = raw.get("bronze", {})
    governance = raw.get("governance", {})
    clickhouse = raw.get("clickhouse", {})
    schemas = raw.get("schemas", {})
    mart = raw.get("mart", {})

    config = MartConfig(
        minio=MinioConfig(
            endpoint=minio.get("endpoint", "localhost:9000"),
            access_key=minio.get("access_key", "minioadmin"),
            secret_key=minio.get("secret_key", "minioadmin"),
            secure=_as_bool(minio.get("secure", False)),
        ),
        bronze=BronzeConfig(
            bucket=bronze["bucket"],
            run_prefix=bronze["run_prefix"].strip("/"),
        ),
        governance=GovernanceConfig(
            output_bucket=governance.get("output_bucket", bronze["bucket"]),
            output_prefix=governance["output_prefix"].strip("/"),
        ),
        clickhouse=ClickHouseConfig(
            host=clickhouse.get("host", "clickhouse"),
            port=int(clickhouse.get("port", 8123)),
            database=clickhouse.get("database", "jne_mart"),
            user=clickhouse.get("user", "default"),
            password=clickhouse.get("password", ""),
            secure=_as_bool(clickhouse.get("secure", False)),
        ),
        schemas=SchemaConfig(
            bronze=schemas.get("bronze", "bronze"),
            bronze_staging=schemas.get("bronze_staging", "bronze_staging"),
            governance=schemas.get("governance", "governance"),
            governance_staging=schemas.get("governance_staging", "governance_staging"),
        ),
        parquet_batch_rows=int(mart.get("parquet_batch_rows", 10000)),
        load_mode=mart.get("load_mode", "latest_snapshot"),
    )
    if config.load_mode != "latest_snapshot":
        raise ValueError(f"Unsupported mart.load_mode: {config.load_mode}")
    if not config.bronze.run_prefix:
        raise ValueError("bronze.run_prefix is required")
    if config.parquet_batch_rows <= 0:
        raise ValueError("mart.parquet_batch_rows must be greater than zero")
    return config


def _minio_client(config: MartConfig):
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "minio is required to load mart data from bronze objects. Install "
            "dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    return Minio(
        config.minio.endpoint,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=config.minio.secure,
    )


def _connect_clickhouse(config: MartConfig):
    try:
        import clickhouse_connect
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "clickhouse-connect is required for mart loading. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    return clickhouse_connect.get_client(
        host=config.clickhouse.host,
        port=config.clickhouse.port,
        username=config.clickhouse.user,
        password=config.clickhouse.password,
        database=config.clickhouse.database,
        secure=config.clickhouse.secure,
    )


def _log(message: str) -> None:
    print(message, flush=True)


def _format_count(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value:,}"


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _progress(row_count: int, expected_rows: int | None, started_at: float) -> str:
    elapsed = max(time.monotonic() - started_at, 0.001)
    rows_per_second = row_count / elapsed
    if expected_rows:
        percent = min((row_count / expected_rows) * 100, 100)
        return (
            f"{row_count:,}/{expected_rows:,} rows ({percent:.1f}%), "
            f"{rows_per_second:,.0f} rows/sec"
        )
    return f"{row_count:,} rows, {rows_per_second:,.0f} rows/sec"


def _quote_ident(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _quote_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _qualified(database: str, table: str) -> str:
    return f"{_quote_ident(database)}.{_quote_ident(table)}"


def clickhouse_type(arrow_type: pa.DataType, nullable: bool = True) -> str:
    if pa.types.is_boolean(arrow_type):
        type_name = "Bool"
    elif pa.types.is_int8(arrow_type):
        type_name = "Int8"
    elif pa.types.is_int16(arrow_type):
        type_name = "Int16"
    elif pa.types.is_int32(arrow_type):
        type_name = "Int32"
    elif pa.types.is_int64(arrow_type):
        type_name = "Int64"
    elif pa.types.is_uint8(arrow_type):
        type_name = "UInt8"
    elif pa.types.is_uint16(arrow_type):
        type_name = "UInt16"
    elif pa.types.is_uint32(arrow_type):
        type_name = "UInt32"
    elif pa.types.is_uint64(arrow_type):
        type_name = "UInt64"
    elif pa.types.is_float32(arrow_type):
        type_name = "Float32"
    elif pa.types.is_float64(arrow_type):
        type_name = "Float64"
    elif pa.types.is_decimal(arrow_type):
        type_name = f"Decimal({arrow_type.precision}, {arrow_type.scale})"
    elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        type_name = "Date"
    elif pa.types.is_timestamp(arrow_type):
        scale = {"s": 0, "ms": 3, "us": 6, "ns": 9}.get(arrow_type.unit, 6)
        type_name = f"DateTime64({scale})"
    else:
        type_name = "String"

    if nullable:
        return f"Nullable({type_name})"
    return type_name


def _create_database(ch: Any, database: str) -> None:
    ch.command(f"CREATE DATABASE IF NOT EXISTS {_quote_ident(database)}")


def _drop_database(ch: Any, database: str) -> None:
    ch.command(f"DROP DATABASE IF EXISTS {_quote_ident(database)} SYNC")


def _create_table(ch: Any, database: str, table: str, arrow_schema: pa.Schema) -> None:
    columns = [
        f"{_quote_ident(field.name)} {clickhouse_type(field.type, field.nullable)}"
        for field in arrow_schema
    ]
    if not columns:
        raise ValueError(f"Cannot create {database}.{table} with no columns")
    ch.command(
        f"CREATE TABLE {_qualified(database, table)} "
        f"({', '.join(columns)}) ENGINE = MergeTree ORDER BY tuple()"
    )


def _ensure_metadata_table(ch: Any, config: MartConfig) -> None:
    _create_database(ch, config.clickhouse.database)
    ch.command(f"""
        CREATE TABLE IF NOT EXISTS {_qualified(config.clickhouse.database, "mart_load_runs")} (
            run_id Nullable(String),
            window_start Nullable(Date),
            window_end Nullable(Date),
            bronze_bucket String,
            bronze_prefix String,
            governance_bucket String,
            governance_prefix String,
            table_count Int32,
            row_count Int64,
            status String,
            loaded_at DateTime DEFAULT now(),
            error_message Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY (loaded_at, status)
    """)


def _read_manifest(client: Any, config: MartConfig) -> dict[str, Any]:
    response = client.get_object(
        config.bronze.bucket,
        f"{config.bronze.run_prefix}/run_manifest.json",
    )
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def _list_parquet_objects(client: Any, bucket: str, prefix: str) -> list[str]:
    objects = []
    for item in client.list_objects(bucket, prefix=prefix, recursive=True):
        object_name = item.object_name
        if object_name.endswith(".parquet"):
            objects.append(object_name)
    return sorted(objects)


def _download_object(client: Any, bucket: str, object_name: str, target_dir: Path) -> Path:
    local_path = target_dir / Path(object_name).name
    client.fget_object(bucket, object_name, str(local_path))
    return local_path


def _insert_batch(ch: Any, database: str, table: str, batch: pa.RecordBatch) -> int:
    if batch.num_rows == 0:
        return 0
    ch.insert_arrow(table, pa.Table.from_batches([batch]), database=database)
    return batch.num_rows


def _load_parquet_table(
    ch: Any,
    client: Any,
    bucket: str,
    objects: Iterable[str],
    database: str,
    table: str,
    batch_rows: int,
    tmpdir: Path,
    expected_rows: int | None = None,
) -> int:
    started_at = time.monotonic()
    object_list = list(objects)
    row_count = 0
    created = False
    for object_index, object_name in enumerate(object_list, start=1):
        _log(
            f"{database}.{table}: downloading part {object_index}/{len(object_list)} "
            f"from s3://{bucket}/{object_name}"
        )
        download_start = time.monotonic()
        local_path = _download_object(client, bucket, object_name, tmpdir)
        _log(
            f"{database}.{table}: downloaded part {object_index}/{len(object_list)} "
            f"({_format_bytes(local_path.stat().st_size)}) in {time.monotonic() - download_start:.1f}s"
        )
        parquet_file = pq.ParquetFile(local_path)
        if not created:
            _create_table(ch, database, table, parquet_file.schema_arrow)
            created = True
            _log(
                f"{database}.{table}: created staging table with "
                f"{len(parquet_file.schema_arrow)} column(s)"
            )
        for batch_index, batch in enumerate(parquet_file.iter_batches(batch_size=batch_rows), start=1):
            next_total = row_count + batch.num_rows
            _log(
                f"{database}.{table}: inserting part {object_index}/{len(object_list)} "
                f"batch {batch_index} ({batch.num_rows:,} rows; "
                f"next total {_format_count(next_total)}/{_format_count(expected_rows)})"
            )
            row_count += _insert_batch(ch, database, table, batch)
            _log(f"{database}.{table}: inserted {_progress(row_count, expected_rows, started_at)}")
        _log(
            f"{database}.{table}: flushed part {object_index}/{len(object_list)} "
            f"({row_count:,} rows staged so far)"
        )
        local_path.unlink(missing_ok=True)
    if not created:
        raise RuntimeError(f"No parquet objects found for {database}.{table}")
    _log(f"{database}.{table}: finished {_progress(row_count, expected_rows, started_at)}")
    return row_count


def _load_manifest_tables(
    ch: Any,
    client: Any,
    config: MartConfig,
    manifest: dict[str, Any],
    tmpdir: Path,
) -> dict[str, int]:
    loaded = {}
    for table_info in manifest.get("tables", []):
        table_name = table_info["output_name"]
        prefix = f"{config.bronze.run_prefix}/{table_name}/"
        objects = _list_parquet_objects(client, config.bronze.bucket, prefix)
        expected_rows = table_info.get("row_count")
        _log(
            f"Loading bronze.{table_name}: {len(objects)} parquet object(s), "
            f"expected {_format_count(expected_rows)} rows"
        )
        loaded[table_name] = _load_parquet_table(
            ch,
            client,
            config.bronze.bucket,
            objects,
            config.schemas.bronze_staging,
            table_name,
            config.parquet_batch_rows,
            tmpdir,
            expected_rows=expected_rows,
        )
    return loaded


def _load_governance_outputs(
    ch: Any,
    client: Any,
    config: MartConfig,
    tmpdir: Path,
) -> dict[str, int]:
    outputs = {
        "scorecard": f"{config.governance.output_prefix}/scorecard.parquet",
        "failures": f"{config.governance.output_prefix}/failures.parquet",
        "cnote_index_status": f"{config.governance.output_prefix}/cnote_index_status.parquet",
    }
    loaded = {}
    for table_name, object_name in outputs.items():
        _log(f"Loading governance.{table_name}: {object_name}")
        loaded[table_name] = _load_parquet_table(
            ch,
            client,
            config.governance.output_bucket,
            [object_name],
            config.schemas.governance_staging,
            table_name,
            config.parquet_batch_rows,
            tmpdir,
        )
    return loaded


def _staging_tables(ch: Any, database: str) -> list[str]:
    result = ch.query(
        "SELECT name FROM system.tables "
        f"WHERE database = {_quote_literal(database)} AND is_temporary = 0 "
        "ORDER BY name"
    )
    return [row[0] for row in result.result_rows]


def _publish_database(ch: Any, staging_database: str, target_database: str) -> None:
    _create_database(ch, target_database)
    for table_name in _staging_tables(ch, staging_database):
        ch.command(f"DROP TABLE IF EXISTS {_qualified(target_database, table_name)} SYNC")
        ch.command(
            f"RENAME TABLE {_qualified(staging_database, table_name)} "
            f"TO {_qualified(target_database, table_name)}"
        )


def _insert_load_run(
    ch: Any,
    config: MartConfig,
    manifest: dict[str, Any],
    table_count: int,
    row_count: int,
    status: str,
    error_message: str | None = None,
) -> None:
    _ensure_metadata_table(ch, config)
    ch.insert(
        "mart_load_runs",
        [[
            manifest.get("run_id"),
            manifest.get("window_start"),
            manifest.get("window_end"),
            config.bronze.bucket,
            config.bronze.run_prefix,
            config.governance.output_bucket,
            config.governance.output_prefix,
            table_count,
            row_count,
            status,
            error_message,
        ]],
        column_names=[
            "run_id",
            "window_start",
            "window_end",
            "bronze_bucket",
            "bronze_prefix",
            "governance_bucket",
            "governance_prefix",
            "table_count",
            "row_count",
            "status",
            "error_message",
        ],
        database=config.clickhouse.database,
    )


def run(config_path: str = "config/mart.yaml") -> None:
    config = load_config(config_path)
    _log(
        "Starting ClickHouse mart load: "
        f"bronze=s3://{config.bronze.bucket}/{config.bronze.run_prefix}, "
        f"governance=s3://{config.governance.output_bucket}/{config.governance.output_prefix}, "
        f"batch_rows={config.parquet_batch_rows:,}"
    )
    client = _minio_client(config)
    manifest = _read_manifest(client, config)
    _log(
        f"Read run manifest for run_id={manifest.get('run_id')} "
        f"with {len(manifest.get('tables', []))} table(s)"
    )
    ch = _connect_clickhouse(config)
    loaded_tables: dict[str, int] = {}
    loaded_governance: dict[str, int] = {}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            _ensure_metadata_table(ch, config)
            _drop_database(ch, config.schemas.bronze_staging)
            _drop_database(ch, config.schemas.governance_staging)
            _create_database(ch, config.schemas.bronze_staging)
            _create_database(ch, config.schemas.governance_staging)
            _log("Prepared staging databases")

            loaded_governance = _load_governance_outputs(
                ch,
                client,
                config,
                tmpdir,
            )
            loaded_tables = _load_manifest_tables(
                ch,
                client,
                config,
                manifest,
                tmpdir,
            )

            _log("Publishing staging tables to visible databases")
            _publish_database(ch, config.schemas.governance_staging, config.schemas.governance)
            _publish_database(ch, config.schemas.bronze_staging, config.schemas.bronze)
            _drop_database(ch, config.schemas.governance_staging)
            _drop_database(ch, config.schemas.bronze_staging)
            total_rows = sum(loaded_tables.values()) + sum(loaded_governance.values())
            _insert_load_run(
                ch,
                config,
                manifest,
                table_count=len(loaded_tables) + len(loaded_governance),
                row_count=total_rows,
                status="SUCCESS",
            )
            _log("Committed ClickHouse mart snapshot")
    except Exception as exc:
        try:
            _insert_load_run(
                ch,
                config,
                manifest,
                table_count=len(loaded_tables) + len(loaded_governance),
                row_count=sum(loaded_tables.values()) + sum(loaded_governance.values()),
                status="FAILED",
                error_message=str(exc),
            )
        except Exception:
            pass
        raise
    finally:
        close = getattr(ch, "close", None)
        if close is not None:
            close()

    _log("Loaded ClickHouse mart snapshot:")
    for table, rows in sorted(loaded_governance.items()):
        _log(f"  governance.{table}: {rows} rows")
    for table, rows in sorted(loaded_tables.items()):
        _log(f"  bronze.{table}: {rows} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load governed bronze Parquet into ClickHouse.")
    parser.add_argument("--config", default="config/mart.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
