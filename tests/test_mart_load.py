from datetime import datetime

import pyarrow as pa

from src.mart_load import (
    _batch_to_copy_buffer,
    _list_parquet_objects,
    load_config,
    postgres_type,
)


def test_load_config_expands_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("BRONZE_RUN_PREFIX", "bronze/jne/run_id=R_TEST")
    monkeypatch.setenv("MART_POSTGRES_PASSWORD", "secret")
    config_path = tmp_path / "mart.yaml"
    config_path.write_text(
        """
minio:
  endpoint: "${MINIO_ENDPOINT:-localhost:9000}"
  access_key: "${MINIO_ACCESS_KEY:-minioadmin}"
  secret_key: "${MINIO_SECRET_KEY:-minioadmin123}"
  secure: "${MINIO_SECURE:-false}"
bronze:
  bucket: "${MINIO_BUCKET:-jne-bronze}"
  run_prefix: "${BRONZE_RUN_PREFIX}"
governance:
  output_bucket: "${GOVERNANCE_OUTPUT_BUCKET:-jne-bronze}"
  output_prefix: "${GOVERNANCE_OUTPUT_PREFIX:-governance/jne/run_id=local}"
postgres:
  host: "${MART_POSTGRES_HOST:-mart-postgres}"
  port: "${MART_POSTGRES_PORT:-5432}"
  database: "${MART_POSTGRES_DB:-jne_mart}"
  user: "${MART_POSTGRES_USER:-jne_mart}"
  password: "${MART_POSTGRES_PASSWORD:-jne_mart}"
schemas:
  bronze: "bronze"
  bronze_staging: "bronze_staging"
  governance: "governance"
  governance_staging: "governance_staging"
mart:
  load_mode: "latest_snapshot"
  parquet_batch_rows: 123
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.bronze.run_prefix == "bronze/jne/run_id=R_TEST"
    assert config.postgres.password == "secret"
    assert config.parquet_batch_rows == 123


def test_postgres_type_maps_arrow_types():
    assert postgres_type(pa.bool_()) == "BOOLEAN"
    assert postgres_type(pa.int32()) == "INTEGER"
    assert postgres_type(pa.int64()) == "BIGINT"
    assert postgres_type(pa.uint64()) == "NUMERIC(20,0)"
    assert postgres_type(pa.float64()) == "DOUBLE PRECISION"
    assert postgres_type(pa.decimal128(10, 2)) == "NUMERIC(10,2)"
    assert postgres_type(pa.timestamp("us")) == "TIMESTAMP"
    assert postgres_type(pa.date32()) == "DATE"
    assert postgres_type(pa.string()) == "TEXT"


def test_list_parquet_objects_filters_and_sorts():
    class Item:
        def __init__(self, object_name):
            self.object_name = object_name

    class Client:
        def list_objects(self, bucket, prefix, recursive):
            assert bucket == "jne-bronze"
            assert prefix == "bronze/run/table/"
            assert recursive is True
            return [
                Item("bronze/run/table/_SUCCESS"),
                Item("bronze/run/table/part-00002.parquet"),
                Item("bronze/run/table/part-00001.parquet"),
            ]

    assert _list_parquet_objects(Client(), "jne-bronze", "bronze/run/table/") == [
        "bronze/run/table/part-00001.parquet",
        "bronze/run/table/part-00002.parquet",
    ]


def test_batch_to_copy_buffer_escapes_text_and_nulls():
    batch = pa.record_batch(
        [
            pa.array(["plain", "line\nbreak", None]),
            pa.array([datetime(2026, 6, 5, 1, 2, 3), None, datetime(2026, 6, 6)]),
        ],
        names=["text_col", "ts_col"],
    )

    assert _batch_to_copy_buffer(batch).read() == (
        "plain\t2026-06-05T01:02:03\n"
        "line\\nbreak\t\\N\n"
        "\\N\t2026-06-06T00:00:00\n"
    )
