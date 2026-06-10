"""Load governed bronze Parquet runs from MinIO into a Postgres serving layer."""

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
class GovernanceConfig:
    output_bucket: str
    output_prefix: str


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
    governance: str
    governance_staging: str


@dataclass(frozen=True)
class MartConfig:
    minio: MinioConfig
    bronze: BronzeConfig
    governance: GovernanceConfig
    postgres: PostgresConfig
    schemas: SchemaConfig
    parquet_batch_rows: int = 10000
    load_mode: str = "latest_snapshot"
    load_governance: bool = True


@dataclass(frozen=True)
class CnoteFailureMapping:
    source_table: str
    bronze_table: str
    failed_key: str
    cnote_key: str
    mapping_method: str
    mapping_confidence: str


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
    postgres = raw.get("postgres", {})
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
            governance=schemas.get("governance", "governance"),
            governance_staging=schemas.get("governance_staging", "governance_staging"),
        ),
        parquet_batch_rows=int(mart.get("parquet_batch_rows", 10000)),
        load_mode=mart.get("load_mode", "latest_snapshot"),
        load_governance=_as_bool(mart.get("load_governance", True)),
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


def _load_manifest_tables(
    cursor: Any,
    client: Any,
    config: MartConfig,
    manifest: dict[str, Any],
    tmpdir: Path,
    commit_callback: Callable[[], None] | None = None,
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
            cursor,
            client,
            config.bronze.bucket,
            objects,
            config.schemas.bronze_staging,
            table_name,
            config.parquet_batch_rows,
            tmpdir,
            expected_rows=expected_rows,
            commit_callback=commit_callback,
        )
    return loaded


def _load_governance_outputs(
    cursor: Any,
    client: Any,
    config: MartConfig,
    tmpdir: Path,
    commit_callback: Callable[[], None] | None = None,
) -> dict[str, int]:
    outputs = {
        "scorecard": f"{config.governance.output_prefix}/scorecard.parquet",
        "failures": f"{config.governance.output_prefix}/failures.parquet",
    }
    loaded = {}
    for table_name, object_name in outputs.items():
        _log(f"Loading governance.{table_name}: {object_name}")
        loaded[table_name] = _load_parquet_table(
            cursor,
            client,
            config.governance.output_bucket,
            [object_name],
            config.schemas.governance_staging,
            table_name,
            config.parquet_batch_rows,
            tmpdir,
            commit_callback=commit_callback,
        )
    return loaded


CNOTE_FAILURE_MAPPINGS: tuple[CnoteFailureMapping, ...] = (
    CnoteFailureMapping("CMS_CNOTE", "cms_cnote", "CNOTE_NO", "CNOTE_NO", "direct_cnote_no", "high"),
    CnoteFailureMapping("CMS_APICUST", "cms_apicust", "APICUST_CNOTE_NO", "APICUST_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_CNOTE_AMO", "cms_cnote_amo", "CNOTE_NO", "CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DRCNOTE", "cms_drcnote", "DRCNOTE_CNOTE_NO", "DRCNOTE_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DRCNOTE", "cms_drcnote", "DRCNOTE_NO", "DRCNOTE_CNOTE_NO", "child_table_record_key", "medium"),
    CnoteFailureMapping("CMS_DHI_HOC", "cms_dhi_hoc", "DHI_CNOTE_NO", "DHI_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DSTATUS", "cms_dstatus", "DSTATUS_CNOTE_NO", "DSTATUS_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_CNOTE_POD", "cms_cnote_pod", "CNOTE_POD_NO", "CNOTE_POD_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DHOV_RSHEET", "cms_dhov_rsheet", "DHOV_RSHEET_CNOTE", "DHOV_RSHEET_CNOTE", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DHOUNDEL_POD", "cms_dhoundel_pod", "DHOUNDEL_CNOTE_NO", "DHOUNDEL_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DRSHEET", "cms_drsheet", "DRSHEET_CNOTE_NO", "DRSHEET_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DRSHEET_PRA", "cms_drsheet_pra", "DRSHEET_CNOTE_NO", "DRSHEET_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DBAG_HO", "cms_dbag_ho", "DBAG_CNOTE_NO", "DBAG_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DHOCNOTE", "cms_dhocnote", "DHOCNOTE_CNOTE_NO", "DHOCNOTE_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DHICNOTE", "cms_dhicnote", "DHICNOTE_CNOTE_NO", "DHICNOTE_CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_COST_DTRANSIT_AGEN", "cms_cost_dtransit_agen", "CNOTE_NO", "CNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_MFCNOTE", "cms_mfcnote", "MFCNOTE_NO", "MFCNOTE_NO", "child_table_cnote_fk", "high"),
    CnoteFailureMapping("CMS_DCORRECT_DEST", "cms_dcorrect_dest", "DCORRECT_CNOTE_NO", "DCORRECT_CNOTE_NO", "child_table_cnote_fk", "high"),
)


def _table_exists(cursor: Any, schema: str, table: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (schema, table),
    )
    return bool(cursor.fetchone()[0])


def _table_columns(cursor: Any, schema: str, table: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {row[0] for row in cursor.fetchall()}


def _cnote_column_expr(column: str, available_columns: set[str], alias: str = "c") -> str:
    if column in available_columns:
        return f"CAST({alias}.{_quote_ident(column)} AS TEXT)"
    return "NULL::TEXT"


def _candidate_mapping_sql(
    config: MartConfig,
    mapping: CnoteFailureMapping,
    cnote_columns: set[str],
) -> str:
    bronze_table = _qualified(config.schemas.bronze_staging, mapping.bronze_table)
    cnote_table = _qualified(config.schemas.bronze_staging, "cms_cnote")
    failures_table = _qualified(config.schemas.governance_staging, "failures")
    scorecard_table = _qualified(config.schemas.governance_staging, "scorecard")
    source_table = mapping.source_table.replace("'", "''")
    failed_key = mapping.failed_key.replace("'", "''")
    mapping_method = mapping.mapping_method.replace("'", "''")
    mapping_confidence = mapping.mapping_confidence.replace("'", "''")
    return f"""
        SELECT DISTINCT
            CAST(src.{_quote_ident(mapping.cnote_key)} AS TEXT) AS cnote_no,
            f.index_code,
            s.element,
            s.rule_family,
            f.table_name AS source_table,
            f.column_names AS source_columns,
            f.failed_value,
            f.failure_reason,
            f.affected_rows,
            '{mapping_method}' AS mapping_method,
            '{mapping_confidence}' AS mapping_confidence,
            f.boundary_suspect,
            {_cnote_column_expr("CNOTE_DATE", cnote_columns)} AS cnote_date,
            {_cnote_column_expr("CNOTE_ORIGIN", cnote_columns)} AS cnote_origin,
            {_cnote_column_expr("CNOTE_DESTINATION", cnote_columns)} AS cnote_destination,
            {_cnote_column_expr("CNOTE_SERVICES_CODE", cnote_columns)} AS cnote_services_code,
            {_cnote_column_expr("CNOTE_BRANCH_ID", cnote_columns)} AS cnote_branch_id
        FROM {failures_table} f
        JOIN {scorecard_table} s
          ON s.index_code = f.index_code
        JOIN {bronze_table} src
          ON TRIM(CAST(f.failed_value AS TEXT)) = TRIM(CAST(src.{_quote_ident(mapping.failed_key)} AS TEXT))
        JOIN {cnote_table} c
          ON TRIM(CAST(src.{_quote_ident(mapping.cnote_key)} AS TEXT)) = TRIM(CAST(c."CNOTE_NO" AS TEXT))
        WHERE f.table_name = '{source_table}'
          AND f.failed_value IS NOT NULL
          AND f.column_names LIKE '%{failed_key}%'
    """


def _create_empty_cnote_failure_candidates(cursor: Any, schema: str) -> None:
    cursor.execute(f"""
        CREATE TABLE {_qualified(schema, "cnote_failure_candidates")} (
            cnote_no TEXT,
            index_code TEXT,
            element TEXT,
            rule_family TEXT,
            source_table TEXT,
            source_columns TEXT,
            failed_value TEXT,
            failure_reason TEXT,
            affected_rows BIGINT,
            mapping_method TEXT,
            mapping_confidence TEXT,
            boundary_suspect BOOLEAN,
            cnote_date TEXT,
            cnote_origin TEXT,
            cnote_destination TEXT,
            cnote_services_code TEXT,
            cnote_branch_id TEXT
        )
    """)


def _create_cnote_failure_candidates(cursor: Any, config: MartConfig) -> int:
    table_name = "cnote_failure_candidates"
    cursor.execute(f"DROP TABLE IF EXISTS {_qualified(config.schemas.governance_staging, table_name)}")

    required_governance = {"scorecard", "failures"}
    if any(
        not _table_exists(cursor, config.schemas.governance_staging, table)
        for table in required_governance
    ):
        _create_empty_cnote_failure_candidates(cursor, config.schemas.governance_staging)
        _log("governance.cnote_failure_candidates: governance scorecard/failures missing; created empty table")
        return 0

    if not _table_exists(cursor, config.schemas.bronze_staging, "cms_cnote"):
        _create_empty_cnote_failure_candidates(cursor, config.schemas.governance_staging)
        _log("governance.cnote_failure_candidates: bronze.cms_cnote missing; created empty table")
        return 0

    cnote_columns = _table_columns(cursor, config.schemas.bronze_staging, "cms_cnote")
    required_cnote_columns = {"CNOTE_NO"}
    if not required_cnote_columns <= cnote_columns:
        missing = ", ".join(sorted(required_cnote_columns - cnote_columns))
        _create_empty_cnote_failure_candidates(cursor, config.schemas.governance_staging)
        _log(f"governance.cnote_failure_candidates: bronze.cms_cnote missing column(s) {missing}; created empty table")
        return 0

    statements = []
    skipped = []
    for mapping in CNOTE_FAILURE_MAPPINGS:
        if not _table_exists(cursor, config.schemas.bronze_staging, mapping.bronze_table):
            skipped.append(f"{mapping.source_table}: table missing")
            continue
        columns = _table_columns(cursor, config.schemas.bronze_staging, mapping.bronze_table)
        required_columns = {mapping.failed_key, mapping.cnote_key}
        if not required_columns <= columns:
            skipped.append(f"{mapping.source_table}: missing {', '.join(sorted(required_columns - columns))}")
            continue
        statements.append(_candidate_mapping_sql(config, mapping, cnote_columns))

    if not statements:
        _create_empty_cnote_failure_candidates(cursor, config.schemas.governance_staging)
        _log("governance.cnote_failure_candidates: no supported mappings available; created empty table")
        return 0

    union_sql = "\nUNION ALL\n".join(statements)
    cursor.execute(f"""
        CREATE TABLE {_qualified(config.schemas.governance_staging, table_name)} AS
        {union_sql}
    """)
    cursor.execute(f"SELECT COUNT(*) FROM {_qualified(config.schemas.governance_staging, table_name)}")
    row_count = int(cursor.fetchone()[0])
    _log(
        "governance.cnote_failure_candidates: "
        f"created {row_count:,} row(s) from {len(statements)} mapping(s)"
    )
    if skipped:
        _log(f"governance.cnote_failure_candidates: skipped {len(skipped)} mapping(s): {'; '.join(skipped)}")
    return row_count


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
            config.governance.output_bucket,
            config.governance.output_prefix,
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
        f"governance=s3://{config.governance.output_bucket}/{config.governance.output_prefix}, "
        f"batch_rows={config.parquet_batch_rows:,}, "
        f"load_governance={config.load_governance}"
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
    loaded_governance: dict[str, int] = {}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            with con.cursor() as cursor:
                _drop_schema(cursor, config.schemas.bronze_staging)
                if config.load_governance:
                    _drop_schema(cursor, config.schemas.governance_staging)
                _create_schema(cursor, config.schemas.bronze_staging)
                if config.load_governance:
                    _create_schema(cursor, config.schemas.governance_staging)
                con.commit()
                _log("Prepared staging schemas")

                loaded_tables = _load_manifest_tables(
                    cursor,
                    client,
                    config,
                    manifest,
                    tmpdir,
                    commit_callback=con.commit,
                )
                if config.load_governance:
                    loaded_governance = _load_governance_outputs(
                        cursor,
                        client,
                        config,
                        tmpdir,
                        commit_callback=con.commit,
                    )
                    loaded_governance["cnote_failure_candidates"] = _create_cnote_failure_candidates(cursor, config)
                else:
                    _log("Skipping governance mart load because mart.load_governance is false")
                con.commit()

                _log("Publishing staging tables to visible schemas")
                _publish_schema(cursor, config.schemas.bronze_staging, config.schemas.bronze)
                if config.load_governance:
                    _publish_schema(cursor, config.schemas.governance_staging, config.schemas.governance)
                _drop_schema(cursor, config.schemas.bronze_staging)
                if config.load_governance:
                    _drop_schema(cursor, config.schemas.governance_staging)
                total_rows = sum(loaded_tables.values()) + sum(loaded_governance.values())
                _insert_load_run(
                    cursor,
                    config,
                    manifest,
                    table_count=len(loaded_tables) + len(loaded_governance),
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
                    table_count=len(loaded_tables) + len(loaded_governance),
                    row_count=sum(loaded_tables.values()) + sum(loaded_governance.values()),
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
    for table, rows in sorted(loaded_governance.items()):
        _log(f"  governance.{table}: {rows} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load governed bronze Parquet into Postgres.")
    parser.add_argument("--config", default="config/mart.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
