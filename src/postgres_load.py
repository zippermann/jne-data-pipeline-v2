"""Load governance Parquet outputs from MinIO into Postgres."""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import GovernanceConfig, load_governance_config


logger = logging.getLogger(__name__)
VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ObjectGroup:
    table_name: str
    object_names: list[str]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def _quote_identifier(name: str) -> str:
    if not VALID_IDENTIFIER.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return f'"{name}"'


def _client(config: GovernanceConfig) -> Any:
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "minio is required to load Parquet objects from MinIO. Install "
            "dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    return Minio(
        config.minio.endpoint,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=config.minio.secure,
    )


def _connect_postgres(config: GovernanceConfig) -> Any:
    try:
        import psycopg2
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg2 is required to load Postgres tables. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the Airflow image."
        ) from exc

    pg = config.postgres
    return psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.database,
        user=pg.user,
        password=pg.password,
    )


def _postgres_type(arrow_type: Any) -> str:
    import pyarrow as pa

    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_integer(arrow_type):
        return "BIGINT"
    if pa.types.is_floating(arrow_type):
        return "DOUBLE PRECISION"
    if pa.types.is_decimal(arrow_type):
        return "NUMERIC"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_date(arrow_type):
        return "DATE"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "BYTEA"
    return "TEXT"


def _create_schema(cursor: Any, schema: str) -> None:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")


def _replace_table(cursor: Any, schema_name: str, table_name: str, arrow_schema: Any) -> None:
    schema_sql = _quote_identifier(schema_name)
    table_sql = _quote_identifier(table_name)
    columns = [
        f"{_quote_identifier(field.name.lower())} {_postgres_type(field.type)}"
        for field in arrow_schema
    ]
    cursor.execute(f"DROP TABLE IF EXISTS {schema_sql}.{table_sql}")
    cursor.execute(f"CREATE TABLE {schema_sql}.{table_sql} ({', '.join(columns)})")


def _copy_batch(cursor: Any, schema_name: str, table_name: str, batch: Any) -> None:
    columns = [_quote_identifier(name.lower()) for name in batch.schema.names]
    destination = (
        f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)} "
        f"({', '.join(columns)})"
    )
    copy_sql = f"COPY {destination} FROM STDIN WITH (FORMAT CSV, NULL '\\N')"

    stream = io.StringIO()
    writer = csv.writer(stream)
    rows = zip(*(column.to_pylist() for column in batch.columns))
    for row in rows:
        writer.writerow(["\\N" if value is None else value for value in row])
    stream.seek(0)
    cursor.copy_expert(copy_sql, stream)


def _download_parquet(client: Any, bucket: str, object_name: str, tmpdir: Path) -> Path:
    local_path = tmpdir / object_name.replace("/", "__")
    client.fget_object(bucket, object_name, str(local_path))
    return local_path


def _load_parquet_group(
    conn: Any,
    client: Any,
    bucket: str,
    schema_name: str,
    group: ObjectGroup,
    tmpdir: Path,
) -> int:
    if not group.object_names:
        raise RuntimeError(f"No Parquet objects found to load for {schema_name}.{group.table_name}")

    import pyarrow.parquet as pq

    row_count = 0
    with conn.cursor() as cursor:
        _create_schema(cursor, schema_name)
        for index, object_name in enumerate(group.object_names):
            local_path = _download_parquet(client, bucket, object_name, tmpdir)
            try:
                parquet_file = pq.ParquetFile(local_path)
                if index == 0:
                    _replace_table(cursor, schema_name, group.table_name, parquet_file.schema_arrow)
                for batch in parquet_file.iter_batches(batch_size=10000):
                    _copy_batch(cursor, schema_name, group.table_name, batch)
                    row_count += batch.num_rows
            finally:
                local_path.unlink(missing_ok=True)
    conn.commit()
    logger.info(
        "Loaded %s.%s: %s rows from %s object(s)",
        schema_name,
        group.table_name,
        row_count,
        len(group.object_names),
    )
    return row_count


def _governance_groups(client: Any, config: GovernanceConfig) -> list[ObjectGroup]:
    bucket = config.governance.output_bucket
    prefix = config.governance.output_prefix.strip("/")
    wanted = {
        f"{prefix}/scorecard.parquet": "scorecard",
        f"{prefix}/failures.parquet": "failures",
    }
    available = {
        item.object_name
        for item in client.list_objects(bucket, prefix=f"{prefix}/", recursive=True)
        if item.object_name in wanted
    }
    return [
        ObjectGroup(table_name=wanted[object_name], object_names=[object_name])
        for object_name in sorted(available)
    ]


def run(config_path: str) -> None:
    config = load_governance_config(config_path)
    client = _client(config)
    governance_groups = _governance_groups(client, config)

    if not governance_groups:
        raise RuntimeError(
            "No governance Parquet objects found under "
            f"s3://{config.governance.output_bucket}/{config.governance.output_prefix}"
        )

    with tempfile.TemporaryDirectory() as tmp, _connect_postgres(config) as conn:
        tmpdir = Path(tmp)
        for group in governance_groups:
            _load_parquet_group(
                conn,
                client,
                config.governance.output_bucket,
                config.postgres.governance_schema,
                group,
                tmpdir,
            )

    logger.info(
        "Postgres governance load complete: %s table(s)",
        len(governance_groups),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Load JNE governance outputs into Postgres.")
    parser.add_argument("--config", default="config/governance.yaml")
    args = parser.parse_args()
    configure_logging()
    run(args.config)


if __name__ == "__main__":
    main()
