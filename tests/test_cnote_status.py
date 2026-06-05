from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.cnote_status import write_cnote_index_status
from src.config import (
    BronzeConfig,
    DuckDBConfig,
    GovernanceConfig,
    GovernanceOutputConfig,
    MinioConfig,
)
from src.rules.executors import RuleResult


duckdb = __import__("pytest").importorskip("duckdb")


def _write_table(root: Path, name: str, data: dict) -> str:
    path = root / name
    path.mkdir()
    pq.write_table(pa.table(data), path / "part-00000.parquet")
    return str(path / "*.parquet")


def _config() -> GovernanceConfig:
    return GovernanceConfig(
        minio=MinioConfig("localhost:9000", "minioadmin", "minioadmin123", False),
        bronze=BronzeConfig("unused", "bronze/jne/run_id=R_TEST"),
        governance=GovernanceOutputConfig("unused", "unused", 10000),
        duckdb=DuckDBConfig(),
        extraction_window={},
    )


def _result(code: str, status: str = "PASS", skipped_reason: str | None = None) -> RuleResult:
    return RuleResult(
        index_code=code,
        element="COMP",
        rule_family="COMP",
        table_name="CMS_CNOTE",
        column_names="CNOTE_DATE",
        compared_table=None,
        compared_columns=None,
        total_checked=2,
        failed_key_count=0,
        failed_row_count=0,
        failure_rate=0.0,
        status=status,
        needs_confirmation=False,
        skipped_reason=skipped_reason,
        run_at="2026-06-05T00:00:00+00:00",
    )


def test_write_cnote_index_status_assigns_full_matrix_statuses(tmp_path):
    cnote_path = _write_table(
        tmp_path,
        "cms_cnote",
        {
            "CNOTE_NO": ["A", "B"],
            "CNOTE_DATE": ["2026-06-01", None],
        },
    )

    con = duckdb.connect()
    output = tmp_path / "cnote_index_status.parquet"
    write_cnote_index_status(
        con,
        _config(),
        {"CMS_CNOTE": cnote_path},
        [_result("COMP1B2")],
        output,
    )

    rows = pq.read_table(output).to_pylist()
    statuses = {
        (row["cnote_no"], row["index_code"]): row["status"]
        for row in rows
        if row["index_code"] == "COMP1B2"
    }

    assert statuses[("A", "COMP1B2")] == "PASS"
    assert statuses[("B", "COMP1B2")] == "FAIL"
