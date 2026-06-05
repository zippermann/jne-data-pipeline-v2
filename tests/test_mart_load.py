import time

import pyarrow as pa

import src.mart_load as mart_load
from src.mart_load import (
    BronzeConfig,
    ClickHouseConfig,
    GovernanceConfig,
    MartConfig,
    MinioConfig,
    SchemaConfig,
    _format_bytes,
    _insert_batch,
    _list_parquet_objects,
    _progress,
    clickhouse_type,
    load_config,
)


def test_load_config_expands_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("BRONZE_RUN_PREFIX", "bronze/jne/run_id=R_TEST")
    monkeypatch.setenv("MART_CLICKHOUSE_PASSWORD", "secret")
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
clickhouse:
  host: "${MART_CLICKHOUSE_HOST:-clickhouse}"
  port: "${MART_CLICKHOUSE_PORT:-8123}"
  database: "${MART_CLICKHOUSE_DB:-jne_mart}"
  user: "${MART_CLICKHOUSE_USER:-default}"
  password: "${MART_CLICKHOUSE_PASSWORD:-}"
  secure: "${MART_CLICKHOUSE_SECURE:-false}"
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
    assert config.clickhouse.password == "secret"
    assert config.clickhouse.host == "clickhouse"
    assert config.clickhouse.port == 8123
    assert config.parquet_batch_rows == 123


def test_clickhouse_type_maps_arrow_types():
    assert clickhouse_type(pa.bool_()) == "Nullable(Bool)"
    assert clickhouse_type(pa.int8()) == "Nullable(Int8)"
    assert clickhouse_type(pa.int16()) == "Nullable(Int16)"
    assert clickhouse_type(pa.int32()) == "Nullable(Int32)"
    assert clickhouse_type(pa.int64()) == "Nullable(Int64)"
    assert clickhouse_type(pa.uint8()) == "Nullable(UInt8)"
    assert clickhouse_type(pa.uint16()) == "Nullable(UInt16)"
    assert clickhouse_type(pa.uint32()) == "Nullable(UInt32)"
    assert clickhouse_type(pa.uint64()) == "Nullable(UInt64)"
    assert clickhouse_type(pa.float32()) == "Nullable(Float32)"
    assert clickhouse_type(pa.float64()) == "Nullable(Float64)"
    assert clickhouse_type(pa.decimal128(10, 2)) == "Nullable(Decimal(10, 2))"
    assert clickhouse_type(pa.timestamp("us")) == "Nullable(DateTime64(6))"
    assert clickhouse_type(pa.timestamp("ns")) == "Nullable(DateTime64(9))"
    assert clickhouse_type(pa.date32()) == "Nullable(Date)"
    assert clickhouse_type(pa.string()) == "Nullable(String)"
    assert clickhouse_type(pa.binary()) == "Nullable(String)"
    assert clickhouse_type(pa.int32(), nullable=False) == "Int32"


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


def test_insert_batch_uses_clickhouse_arrow_insert():
    batch = pa.record_batch([pa.array([1, 2])], names=["id"])

    class Client:
        def __init__(self):
            self.calls = []

        def insert_arrow(self, table, arrow_table, database):
            self.calls.append((table, arrow_table, database))

    client = Client()

    assert _insert_batch(client, "bronze_staging", "shipments", batch) == 2
    assert client.calls[0][0] == "shipments"
    assert client.calls[0][1].num_rows == 2
    assert client.calls[0][2] == "bronze_staging"


def test_progress_formatting_for_known_and_unknown_totals():
    started_at = time.monotonic() - 1

    assert "50/100 rows (50.0%)" in _progress(50, 100, started_at)
    assert "50 rows" in _progress(50, None, started_at)
    assert _format_bytes(1536) == "1.5 KB"


def test_run_loads_governance_before_bronze(monkeypatch):
    events = []
    config = MartConfig(
        minio=MinioConfig("minio:9000", "minioadmin", "minioadmin123", False),
        bronze=BronzeConfig("jne-bronze", "bronze/jne/run_id=R_TEST"),
        governance=GovernanceConfig("jne-bronze", "governance/jne/run_id=R_TEST"),
        clickhouse=ClickHouseConfig("clickhouse", 8123, "jne_mart", "default", "", False),
        schemas=SchemaConfig("bronze", "bronze_staging", "governance", "governance_staging"),
        parquet_batch_rows=100,
        load_mode="latest_snapshot",
    )

    class ClickHouse:
        def close(self):
            events.append("close")

    monkeypatch.setattr(mart_load, "load_config", lambda path: config)
    monkeypatch.setattr(mart_load, "_minio_client", lambda cfg: object())
    monkeypatch.setattr(
        mart_load,
        "_read_manifest",
        lambda client, cfg: {"run_id": "R_TEST", "tables": [{"output_name": "shipments", "row_count": 2}]},
    )
    monkeypatch.setattr(mart_load, "_connect_clickhouse", lambda cfg: ClickHouse())
    monkeypatch.setattr(mart_load, "_ensure_metadata_table", lambda ch, cfg: events.append("metadata"))
    monkeypatch.setattr(mart_load, "_drop_database", lambda ch, db: events.append(f"drop:{db}"))
    monkeypatch.setattr(mart_load, "_create_database", lambda ch, db: events.append(f"create:{db}"))
    monkeypatch.setattr(
        mart_load,
        "_load_governance_outputs",
        lambda ch, client, cfg, tmpdir: events.append("load_governance") or {"scorecard": 1},
    )
    monkeypatch.setattr(
        mart_load,
        "_load_manifest_tables",
        lambda ch, client, cfg, manifest, tmpdir: events.append("load_bronze") or {"shipments": 2},
    )
    monkeypatch.setattr(
        mart_load,
        "_publish_database",
        lambda ch, staging, target: events.append(f"publish:{staging}->{target}"),
    )
    monkeypatch.setattr(
        mart_load,
        "_insert_load_run",
        lambda ch, cfg, manifest, table_count, row_count, status, error_message=None: events.append(
            f"status:{status}:{table_count}:{row_count}"
        ),
    )

    mart_load.run("unused.yaml")

    assert events.index("load_governance") < events.index("load_bronze")
    assert events.index("publish:governance_staging->governance") < events.index("publish:bronze_staging->bronze")
    assert "status:SUCCESS:2:3" in events
