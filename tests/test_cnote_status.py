from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.cnote_status import write_top_index_cnote_examples
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


def _result(
    code: str,
    element: str = "CONS",
    rule_family: str = "CONS1",
    table_name: str = "CMS_CNOTE",
    column_names: str = "CNOTE_BRANCH_ID",
    failed_row_count: int = 0,
    failure_rate: float = 0.0,
    total_checked: int = 10,
) -> RuleResult:
    return RuleResult(
        index_code=code,
        element=element,
        rule_family=rule_family,
        table_name=table_name,
        column_names=column_names,
        compared_table=None,
        compared_columns=None,
        total_checked=total_checked,
        failed_key_count=0,
        failed_row_count=failed_row_count,
        failure_rate=failure_rate,
        status="FAIL" if failed_row_count else "PASS",
        needs_confirmation=False,
        skipped_reason=None,
        run_at="2026-06-05T00:00:00+00:00",
    )


def test_write_top_index_cnote_examples_pivots_worst_indexes(tmp_path):
    cnote_path = _write_table(
        tmp_path,
        "cms_cnote",
        {
            "CNOTE_NO": ["A", "B", "C"],
            "CNOTE_BRANCH_ID": ["BR1", "BR2", "BR3"],
        },
    )
    apicust_path = _write_table(
        tmp_path,
        "cms_apicust",
        {
            "APICUST_CNOTE_NO": ["A", "A", "A", "A", "B", "C"],
            "APICUST_BRANCH": ["X1", "X2", "X3", "X4", "Y1", "BR3"],
        },
    )

    con = duckdb.connect()
    output = tmp_path / "top_index_cnote_examples.parquet"
    write_top_index_cnote_examples(
        con,
        _config(),
        {"CMS_CNOTE": cnote_path, "CMS_APICUST": apicust_path},
        [
            _result("CONS1B3", failed_row_count=5, failure_rate=0.5),
            _result("CONS1B10", failed_row_count=1, failure_rate=0.1),
            _result("ACCU4B15", element="ACCU", rule_family="ACCU4", failed_row_count=10, failure_rate=1.0),
        ],
        output,
        top_n=1,
        example_limit=3,
    )

    rows = pq.read_table(output).to_pylist()
    assert [row["cnote_no"] for row in rows] == ["A", "B"]
    assert rows[0]["selected_failed_index_count"] == 1
    assert rows[0]["selected_total_failed_rows"] == 4
    assert "CONS1B3" in rows[0]
    assert "CONS1B10" not in rows[0]
    assert "ACCU4B15" not in rows[0]

    examples = rows[0]["CONS1B3"].split("; ")
    assert len(examples) == 3
    assert "CNOTE_BRANCH_ID does not match APICUST_BRANCH: BR1 <> X1" in examples
