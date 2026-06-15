"""Load bronze Parquet runs from MinIO into a Postgres serving layer."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Iterable

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
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


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
class MartConfig:
    minio: MinioConfig
    bronze: BronzeConfig
    postgres: PostgresConfig
    schemas: SchemaConfig
    governance: GovernanceConfig
    skip_stages: tuple[str, ...] = ()
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
    postgres = raw.get("postgres", {})
    schemas = raw.get("schemas", {})
    mart = raw.get("mart", {})
    governance = raw.get("governance", {})
    run_id = os.getenv("RUN_ID", "")
    default_governance_path = Path("governance/outputs") / run_id / "governance_results.csv"
    default_summary_path = Path("governance/outputs") / run_id / "governance_rule_summary.csv"

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
        postgres=PostgresConfig(
            host=postgres.get("host", "mart-postgres"),
            port=int(postgres.get("port", 5432)),
            database=postgres.get("database", "jne_mart"),
            user=postgres.get("user", "jne_mart"),
            password=postgres.get("password", "jne_mart"),
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


def _connect_postgres(config: MartConfig):
    try:
        import psycopg2
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg2-binary is required for mart loading. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    return psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        dbname=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password,
        application_name="jne_mart_load",
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
    return '"' + value.replace('"', '""') + '"'


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def postgres_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
        return "INTEGER"
    if pa.types.is_int64(arrow_type) or pa.types.is_uint32(arrow_type):
        return "BIGINT"
    if pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type):
        return "INTEGER"
    if pa.types.is_uint64(arrow_type):
        return "NUMERIC(20,0)"
    if pa.types.is_float32(arrow_type):
        return "REAL"
    if pa.types.is_float64(arrow_type):
        return "DOUBLE PRECISION"
    if pa.types.is_decimal(arrow_type):
        return f"NUMERIC({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        return "TIME"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "BYTEA"
    return "TEXT"


def _create_schema(cursor: Any, schema: str) -> None:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}")


def _drop_schema(cursor: Any, schema: str) -> None:
    cursor.execute(f"DROP SCHEMA IF EXISTS {_quote_ident(schema)} CASCADE")


def _create_table(cursor: Any, schema: str, table: str, arrow_schema: pa.Schema) -> None:
    columns = [
        f"{_quote_ident(field.name)} {postgres_type(field.type)}"
        for field in arrow_schema
    ]
    if not columns:
        raise ValueError(f"Cannot create {schema}.{table} with no columns")
    cursor.execute(f"CREATE TABLE {_qualified(schema, table)} ({', '.join(columns)})")


def _ensure_metadata_table(cursor: Any) -> None:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mart_load_runs (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT,
            window_start DATE,
            window_end DATE,
            bronze_bucket TEXT NOT NULL,
            bronze_prefix TEXT NOT NULL,
            governance_bucket TEXT NOT NULL,
            governance_prefix TEXT NOT NULL,
            table_count INTEGER NOT NULL,
            row_count BIGINT NOT NULL,
            status TEXT NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        )
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


def _copy_value(value: Any) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, bytes):
        text = r"\x" + value.hex()
    elif isinstance(value, (datetime, date, Decimal)):
        text = value.isoformat()
    else:
        text = str(value)
    return (
        text
        .replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _batch_to_copy_buffer(batch: pa.RecordBatch) -> StringIO:
    columns = [column.to_pylist() for column in batch.columns]
    buffer = StringIO()
    for row in zip(*columns):
        buffer.write("\t".join(_copy_value(value) for value in row))
        buffer.write("\n")
    buffer.seek(0)
    return buffer


def _copy_batch(cursor: Any, schema: str, table: str, batch: pa.RecordBatch) -> int:
    if batch.num_rows == 0:
        return 0
    column_sql = ", ".join(_quote_ident(name) for name in batch.schema.names)
    sql = (
        f"COPY {_qualified(schema, table)} ({column_sql}) "
        "FROM STDIN WITH (FORMAT text, NULL '\\N')"
    )
    cursor.copy_expert(sql, _batch_to_copy_buffer(batch))
    return batch.num_rows


def _load_parquet_table(
    cursor: Any,
    client: Any,
    bucket: str,
    objects: Iterable[str],
    schema: str,
    table: str,
    batch_rows: int,
    tmpdir: Path,
    expected_rows: int | None = None,
    commit_callback: Callable[[], None] | None = None,
) -> int:
    started_at = time.monotonic()
    object_list = list(objects)
    row_count = 0
    created = False
    for object_index, object_name in enumerate(object_list, start=1):
        _log(
            f"{schema}.{table}: downloading part {object_index}/{len(object_list)} "
            f"from s3://{bucket}/{object_name}"
        )
        download_start = time.monotonic()
        local_path = _download_object(client, bucket, object_name, tmpdir)
        _log(
            f"{schema}.{table}: downloaded part {object_index}/{len(object_list)} "
            f"({_format_bytes(local_path.stat().st_size)}) in {time.monotonic() - download_start:.1f}s"
        )
        parquet_file = pq.ParquetFile(local_path)
        if not created:
            _create_table(cursor, schema, table, parquet_file.schema_arrow)
            created = True
            _log(
                f"{schema}.{table}: created staging table with "
                f"{len(parquet_file.schema_arrow)} column(s)"
            )
        for batch_index, batch in enumerate(parquet_file.iter_batches(batch_size=batch_rows), start=1):
            next_total = row_count + batch.num_rows
            _log(
                f"{schema}.{table}: copying part {object_index}/{len(object_list)} "
                f"batch {batch_index} ({batch.num_rows:,} rows; "
                f"next total {_format_count(next_total)}/{_format_count(expected_rows)})"
            )
            row_count += _copy_batch(cursor, schema, table, batch)
            _log(f"{schema}.{table}: copied {_progress(row_count, expected_rows, started_at)}")
        if commit_callback is not None:
            commit_callback()
            _log(
                f"{schema}.{table}: committed part {object_index}/{len(object_list)} "
                f"({row_count:,} rows staged so far)"
            )
        local_path.unlink(missing_ok=True)
    if not created:
        raise RuntimeError(f"No parquet objects found for {schema}.{table}")
    _log(f"{schema}.{table}: finished {_progress(row_count, expected_rows, started_at)}")
    return row_count


def _table_object_prefix(config: MartConfig, table_info: dict[str, Any], default_parent: str | None = None) -> str:
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


def _should_skip_bronze_table(table_info: dict[str, Any], config: MartConfig) -> bool:
    stage = str(table_info.get("stage", "")).lower()
    return stage in config.skip_stages or table_info.get("output_name") == "cms_cnote"


def _load_table_entries(
    cursor: Any,
    client: Any,
    config: MartConfig,
    table_entries: Iterable[dict[str, Any]],
    tmpdir: Path,
    staging_schema: str,
    label: str,
    default_parent: str | None = None,
    commit_callback: Callable[[], None] | None = None,
) -> dict[str, int]:
    loaded = {}
    for table_info in table_entries:
        source_name = table_info["output_name"]
        table_name = _mart_table_name(table_info)
        if label == "bronze" and _should_skip_bronze_table(table_info, config):
            _log(f"Skipping bronze.{source_name}; mart uses transformed cms_cnote and skips configured stages")
            continue
        if _can_skip_reused_reference(cursor, config, table_info):
            _log(f"Skipping reused reference table bronze.{table_name}; target table already exists")
            loaded[table_name] = int(table_info.get("row_count") or 0)
            continue
        prefix = _table_object_prefix(config, table_info, default_parent=default_parent)
        objects = _list_parquet_objects(client, config.bronze.bucket, prefix)
        expected_rows = table_info.get("row_count")
        _log(
            f"Loading {label}.{table_name}: {len(objects)} parquet object(s), "
            f"expected {_format_count(expected_rows)} rows"
        )
        loaded[table_name] = _load_parquet_table(
            cursor,
            client,
            config.bronze.bucket,
            objects,
            staging_schema,
            table_name,
            config.parquet_batch_rows,
            tmpdir,
            expected_rows=expected_rows,
            commit_callback=commit_callback,
        )
    return loaded


def _load_manifest_tables(
    cursor: Any,
    client: Any,
    config: MartConfig,
    manifest: dict[str, Any],
    tmpdir: Path,
    commit_callback: Callable[[], None] | None = None,
) -> dict[str, int]:
    return _load_table_entries(
        cursor,
        client,
        config,
        manifest.get("tables", []),
        tmpdir,
        config.schemas.bronze_staging,
        "bronze",
        commit_callback=commit_callback,
    )


def _load_derived_tables(
    cursor: Any,
    client: Any,
    config: MartConfig,
    manifest: dict[str, Any],
    tmpdir: Path,
    commit_callback: Callable[[], None] | None = None,
) -> dict[str, int]:
    return _load_table_entries(
        cursor,
        client,
        config,
        manifest.get("derived", []),
        tmpdir,
        config.schemas.bronze_staging,
        "bronze",
        default_parent="derived",
        commit_callback=commit_callback,
    )


def _table_exists(cursor: Any, schema: str, table: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
        LIMIT 1
        """,
        (schema, table),
    )
    return cursor.fetchone() is not None


def _can_skip_reused_reference(cursor: Any, config: MartConfig, table_info: dict[str, Any]) -> bool:
    return (
        table_info.get("stage") == "reference"
        and table_info.get("reused") is True
        and _table_exists(cursor, config.schemas.bronze, table_info["output_name"])
    )


def _staging_tables(cursor: Any, schema: str) -> list[str]:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    return [row[0] for row in cursor.fetchall()]


def _publish_schema(cursor: Any, staging_schema: str, target_schema: str) -> None:
    _create_schema(cursor, target_schema)
    for table_name in _staging_tables(cursor, staging_schema):
        cursor.execute(f"DROP TABLE IF EXISTS {_qualified(target_schema, table_name)} CASCADE")
        cursor.execute(
            f"ALTER TABLE {_qualified(staging_schema, table_name)} "
            f"SET SCHEMA {_quote_ident(target_schema)}"
        )


def _read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        header = handle.readline().rstrip("\n").rstrip("\r")
    columns = [column.strip() for column in header.split(",") if column.strip()]
    if not columns:
        raise ValueError(f"Governance results CSV has no header: {path}")
    return columns


def _load_governance_csv(cursor: Any, schema: str, table: str, path: Path) -> int:
    columns = _read_csv_header(path)
    column_defs = ", ".join(f"{_quote_ident(column)} TEXT" for column in columns)
    cursor.execute(f"CREATE TABLE {_qualified(schema, table)} ({column_defs})")
    column_sql = ", ".join(_quote_ident(column) for column in columns)
    with path.open("r", encoding="utf-8", newline="") as handle:
        cursor.copy_expert(
            f"COPY {_qualified(schema, table)} ({column_sql}) "
            "FROM STDIN WITH (FORMAT csv, HEADER true)",
            handle,
        )
    cursor.execute(f"SELECT COUNT(*) FROM {_qualified(schema, table)}")
    row_count = int(cursor.fetchone()[0])
    _log(f"Loaded {schema}.{table}: {row_count:,} rows from {path}")
    return row_count


def _load_governance_results(cursor: Any, config: MartConfig) -> int:
    if not config.governance.enabled:
        _log("Skipping governance results load because governance.enabled=false")
        return 0

    path = config.governance.results_path
    if not path.exists():
        raise FileNotFoundError(f"Governance results file not found: {path}")

    _drop_schema(cursor, config.schemas.governance)
    _create_schema(cursor, config.schemas.governance)
    row_count = _load_governance_csv(cursor, config.schemas.governance, "governance_results", path)
    if config.governance.summary_path.exists():
        row_count += _load_governance_csv(
            cursor,
            config.schemas.governance,
            "governance_rule_summary",
            config.governance.summary_path,
        )
    else:
        _log(f"Governance rule summary file not found, skipping: {config.governance.summary_path}")
    return row_count


def _insert_load_run(
    cursor: Any,
    config: MartConfig,
    manifest: dict[str, Any],
    table_count: int,
    row_count: int,
    status: str,
    error_message: str | None = None,
) -> None:
    _ensure_metadata_table(cursor)
    cursor.execute(
        """
        INSERT INTO mart_load_runs (
            run_id,
            window_start,
            window_end,
            bronze_bucket,
            bronze_prefix,
            governance_bucket,
            governance_prefix,
            table_count,
            row_count,
            status,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            manifest.get("run_id"),
            manifest.get("window_start"),
            manifest.get("window_end"),
            config.bronze.bucket,
            config.bronze.run_prefix,
            "",
            str(config.governance.results_path) if config.governance.enabled else "",
            table_count,
            row_count,
            status,
            error_message,
        ),
    )


def run(config_path: str = "config/mart.yaml") -> None:
    config = load_config(config_path)
    _log(
        "Starting Postgres mart load: "
        f"bronze=s3://{config.bronze.bucket}/{config.bronze.run_prefix}, "
        f"batch_rows={config.parquet_batch_rows:,}"
    )
    client = _minio_client(config)
    manifest = _read_manifest(client, config)
    _log(
        f"Read run manifest for run_id={manifest.get('run_id')} "
        f"with {len(manifest.get('tables', []))} table(s)"
    )
    con = _connect_postgres(config)
    con.autocommit = False
    loaded_tables: dict[str, int] = {}
    loaded_derived: dict[str, int] = {}
    governance_rows = 0
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            with con.cursor() as cursor:
                _drop_schema(cursor, config.schemas.bronze_staging)
                _drop_schema(cursor, config.schemas.derived_staging)
                _create_schema(cursor, config.schemas.bronze_staging)
                con.commit()
                _log("Prepared bronze staging schema")

                loaded_tables = _load_manifest_tables(
                    cursor,
                    client,
                    config,
                    manifest,
                    tmpdir,
                    commit_callback=con.commit,
                )
                con.commit()

                if manifest.get("derived"):
                    _log("Loading transformed CNOTE into bronze staging")
                    loaded_derived = _load_derived_tables(
                        cursor,
                        client,
                        config,
                        manifest,
                        tmpdir,
                        commit_callback=con.commit,
                    )
                    con.commit()

                _log("Publishing bronze staging tables")
                _publish_schema(cursor, config.schemas.bronze_staging, config.schemas.bronze)
                _drop_schema(cursor, config.schemas.bronze_staging)
                _drop_schema(cursor, config.schemas.derived)

                _log("Publishing governance results")
                governance_rows = _load_governance_results(cursor, config)
                total_rows = sum(loaded_tables.values()) + sum(loaded_derived.values())
                _insert_load_run(
                    cursor,
                    config,
                    manifest,
                    table_count=len(loaded_tables) + len(loaded_derived),
                    row_count=total_rows,
                    status="SUCCESS",
                )
                con.commit()
                _log("Committed Postgres mart snapshot")
    except Exception as exc:
        con.rollback()
        try:
            with con.cursor() as cursor:
                _insert_load_run(
                    cursor,
                    config,
                    manifest,
                    table_count=len(loaded_tables) + len(loaded_derived),
                    row_count=sum(loaded_tables.values()) + sum(loaded_derived.values()),
                    status="FAILED",
                    error_message=str(exc),
                )
                con.commit()
        except Exception:
            con.rollback()
        raise
    finally:
        con.close()

    _log("Loaded Postgres mart snapshot:")
    for table, rows in sorted(loaded_tables.items()):
        _log(f"  bronze.{table}: {rows} rows")
    for table, rows in sorted(loaded_derived.items()):
        _log(f"  {config.schemas.bronze}.{table}: {rows} rows (transformed)")
    if config.governance.enabled:
        _log(f"  {config.schemas.governance}.governance_results: {governance_rows} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load bronze Parquet into Postgres.")
    parser.add_argument("--config", default="config/mart.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
