"""Governance output writers."""

from __future__ import annotations

import csv
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from src.config import GovernanceConfig
from src.rules.executors import RuleResult


SCORECARD_COLUMNS = [
    "index_code",
    "element",
    "rule_family",
    "child_table",
    "child_fk",
    "parent_table",
    "parent_pk",
    "total_checked",
    "orphan_key_count",
    "orphan_row_count",
    "orphan_rate",
    "status",
    "needs_confirmation",
    "skipped_reason",
    "run_at",
]


def _client(config: GovernanceConfig) -> Minio:
    return Minio(
        config.minio.endpoint,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=config.minio.secure,
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def _upload(client: Minio, bucket: str, object_name: str, path: Path) -> None:
    content_type = "application/octet-stream"
    if path.suffix == ".csv":
        content_type = "text/csv"
    elif path.suffix == ".parquet":
        content_type = "application/vnd.apache.parquet"
    client.fput_object(bucket, object_name, str(path), content_type=content_type)


def write_outputs(
    config: GovernanceConfig,
    con: Any,
    results: list[RuleResult],
    failures_table: str,
) -> tuple[str, str, str]:
    """Write scorecard CSV, scorecard Parquet, and failure Parquet to MinIO."""
    client = _client(config)
    bucket = config.governance.output_bucket
    prefix = config.governance.output_prefix.strip("/")
    _ensure_bucket(client, bucket)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        scorecard_csv = tmp / "scorecard.csv"
        scorecard_parquet = tmp / "scorecard.parquet"
        failures_parquet = tmp / "failures.parquet"

        rows = [asdict(result) for result in results]
        with scorecard_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=SCORECARD_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

        table = pa.Table.from_pylist(rows, schema=pa.schema([
            pa.field("index_code", pa.string()),
            pa.field("element", pa.string()),
            pa.field("rule_family", pa.string()),
            pa.field("child_table", pa.string()),
            pa.field("child_fk", pa.string()),
            pa.field("parent_table", pa.string()),
            pa.field("parent_pk", pa.string()),
            pa.field("total_checked", pa.int64()),
            pa.field("orphan_key_count", pa.int64()),
            pa.field("orphan_row_count", pa.int64()),
            pa.field("orphan_rate", pa.float64()),
            pa.field("status", pa.string()),
            pa.field("needs_confirmation", pa.bool_()),
            pa.field("skipped_reason", pa.string()),
            pa.field("run_at", pa.string()),
        ]))
        pq.write_table(table, scorecard_parquet)

        failures_path = str(failures_parquet).replace("'", "''")
        con.execute(f"COPY (SELECT * FROM {failures_table}) TO '{failures_path}' (FORMAT PARQUET)")

        csv_object = f"{prefix}/scorecard.csv"
        scorecard_object = f"{prefix}/scorecard.parquet"
        failures_object = f"{prefix}/failures.parquet"
        _upload(client, bucket, csv_object, scorecard_csv)
        _upload(client, bucket, scorecard_object, scorecard_parquet)
        _upload(client, bucket, failures_object, failures_parquet)

    return (
        f"s3://{bucket}/{csv_object}",
        f"s3://{bucket}/{scorecard_object}",
        f"s3://{bucket}/{failures_object}",
    )
