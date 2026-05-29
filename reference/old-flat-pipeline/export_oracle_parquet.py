"""
Export the transformed unified PostgreSQL table as a partitioned Parquet dataset.

The exporter fetches rows in small batches and writes multiple Parquet files so
the full unified dataset is never held in memory at once.
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from pipeline_config import (
        DB_CONN,
        LOAD_BATCH_MAX_ROWS,
        SCHEMA_TRANSFORMED,
    )
except ImportError:
    sys.path.insert(0, "/opt/airflow")
    from pipeline_config import (
        DB_CONN,
        LOAD_BATCH_MAX_ROWS,
        SCHEMA_TRANSFORMED,
    )


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_TABLE = f"{SCHEMA_TRANSFORMED}.unified_shipments"
DEFAULT_OUTPUT_DIR = "/opt/airflow/data/transformed_unified_shipments_parquet"


def get_postgres_conn():
    return psycopg2.connect(DB_CONN)


def _postgres_columns_metadata(conn):
    schema, table = SOURCE_TABLE.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, numeric_precision, numeric_scale,
                   datetime_precision, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        metadata = cur.fetchall()
    if not metadata:
        raise RuntimeError(f"Could not read PostgreSQL metadata for {SOURCE_TABLE}")
    return metadata


def _arrow_type(data_type, precision, scale, _datetime_precision, udt_name):
    dtype = (data_type or "").lower()
    udt = (udt_name or "").lower()
    if dtype in {
        "text", "character varying", "character", "json", "jsonb",
        "uuid", "xml",
    }:
        return pa.string()
    if dtype in {"smallint"}:
        return pa.int16()
    if dtype in {"integer"}:
        return pa.int32()
    if dtype in {"bigint"}:
        return pa.int64()
    if dtype in {"numeric", "decimal"}:
        if precision is not None and scale is not None:
            if precision <= 38:
                return pa.decimal128(int(precision), int(scale))
            if precision <= 76:
                return pa.decimal256(int(precision), int(scale))
        # COPY-created NUMERIC columns are commonly unconstrained. psycopg2
        # returns their values as Decimal, so preserve them as strings rather
        # than asking Arrow to coerce Decimal values to floats.
        return pa.string()
    if dtype in {"real", "double precision"}:
        return pa.float64()
    if dtype == "date":
        return pa.date32()
    if dtype == "timestamp without time zone":
        return pa.timestamp("us")
    if dtype == "timestamp with time zone":
        return pa.timestamp("us", tz="UTC")
    if dtype == "boolean":
        return pa.bool_()
    if dtype == "bytea" or udt == "bytea":
        return pa.binary()
    logger.warning("Unknown PostgreSQL type %s (%s); storing it as text", data_type, udt_name)
    return pa.string()


def _arrow_schema(metadata):
    return pa.schema([
        pa.field(name, _arrow_type(dtype, precision, scale, datetime_precision, udt_name))
        for name, dtype, precision, scale, datetime_precision, udt_name in metadata
    ])


def _table_from_rows(rows, schema):
    columns = zip(*rows)
    arrays = [
        pa.array(
            (str(value) if value is not None else None for value in values),
            type=field.type,
        )
        if pa.types.is_string(field.type)
        else pa.array(values, type=field.type)
        for field, values in zip(schema, columns)
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def export_parquet(output_dir, batch_size, rows_per_file, compression, limit=None):
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_parts = list(output_dir.glob("part-*.parquet"))
    if existing_parts:
        raise RuntimeError(
            f"{output_dir} already contains Parquet parts. "
            "Choose a new --output-dir before starting another export."
        )

    conn = get_postgres_conn()
    conn.set_session(readonly=True, autocommit=False)
    writer = None
    part_no = 0
    rows_in_part = 0
    total_rows = 0
    try:
        metadata = _postgres_columns_metadata(conn)
        schema = _arrow_schema(metadata)
        logger.info("Exporting %s with %s columns to %s", SOURCE_TABLE, len(schema), output_dir)

        sql = f"SELECT * FROM {SOURCE_TABLE}"
        params = ()
        if limit is not None:
            sql += " LIMIT %s"
            params = (limit,)

        with conn.cursor(name="transformed_shipments_parquet_export") as cur:
            cur.itersize = batch_size
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                if writer is None or rows_in_part >= rows_per_file:
                    if writer is not None:
                        writer.close()
                    part_no += 1
                    rows_in_part = 0
                    part_path = output_dir / f"part-{part_no:05d}.parquet"
                    writer = pq.ParquetWriter(
                        part_path,
                        schema,
                        compression=compression,
                        use_dictionary=True,
                    )
                table = _table_from_rows(rows, schema)
                writer.write_table(table)
                batch_rows = len(rows)
                rows_in_part += batch_rows
                total_rows += batch_rows
                logger.info("Exported %s rows (%s files started)", f"{total_rows:,}", part_no)
    finally:
        if writer is not None:
            writer.close()
        conn.close()

    (output_dir / "_SUCCESS").write_text(f"{total_rows}\n", encoding="ascii")
    logger.info("Parquet export complete: %s rows in %s files", f"{total_rows:,}", part_no)
    return total_rows


def main():
    parser = argparse.ArgumentParser(description="Export unified PostgreSQL shipments to Parquet.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--batch-size", type=int, default=LOAD_BATCH_MAX_ROWS)
    parser.add_argument("--rows-per-file", type=int, default=250_000)
    parser.add_argument("--compression", choices=("snappy", "gzip", "zstd"), default="zstd")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for a test export.")
    args = parser.parse_args()
    export_parquet(
        Path(args.output_dir),
        args.batch_size,
        args.rows_per_file,
        args.compression,
        args.limit,
    )


if __name__ == "__main__":
    main()
