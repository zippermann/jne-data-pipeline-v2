from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.config import (
    BronzeConfig,
    DuckDBConfig,
    GovernanceConfig,
    GovernanceOutputConfig,
    MinioConfig,
    PostgresConfig,
)
from src.rules.executors import run_intg1
from src.rules.registry import RuleSpec


duckdb = pytest.importorskip("duckdb")


def _write_table(root: Path, name: str, data: dict) -> str:
    table_dir = root / name
    table_dir.mkdir()
    pq.write_table(pa.table(data), table_dir / "part-00000.parquet")
    return str(table_dir / "*.parquet")


def _config() -> GovernanceConfig:
    return GovernanceConfig(
        minio=MinioConfig("localhost:9000", "minioadmin", "minioadmin123", False),
        bronze=BronzeConfig("unused", "unused"),
        governance=GovernanceOutputConfig("unused", "unused", 10000),
        duckdb=DuckDBConfig(),
        postgres=PostgresConfig("localhost", 5432, "jne", "jne", "jne"),
        extraction_window={},
    )


def test_run_intg1_finds_known_orphan(tmp_path):
    child_path = _write_table(
        tmp_path,
        "child",
        {"CHILD_FK": ["A", "B", "B", None]},
    )
    parent_path = _write_table(
        tmp_path,
        "parent",
        {"PARENT_PK": ["A"]},
    )
    spec = RuleSpec(
        code="INTG1TEST",
        element="INTG",
        rule_family="INTG1",
        child_table="CHILD",
        child_fk="CHILD_FK",
        parent_table="PARENT",
        parent_pk="PARENT_PK",
        description="Test referential rule.",
    )
    con = duckdb.connect()
    con.execute("""
        CREATE TEMP TABLE failures (
            index_code VARCHAR,
            child_table VARCHAR,
            child_fk VARCHAR,
            child_fk_value VARCHAR,
            parent_table VARCHAR,
            parent_pk VARCHAR,
            affected_child_rows BIGINT,
            boundary_suspect BOOLEAN,
            run_at VARCHAR
        )
    """)

    result = run_intg1(
        spec,
        con,
        _config(),
        {"CHILD": child_path, "PARENT": parent_path},
        "failures",
    )

    failures = con.execute("SELECT child_fk_value, affected_child_rows FROM failures").fetchall()
    assert result.total_checked == 3
    assert result.orphan_key_count == 1
    assert result.orphan_row_count == 2
    assert result.status == "FAIL"
    assert failures == [("B", 2)]
