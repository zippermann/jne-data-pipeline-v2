import tempfile
from pathlib import Path

import pandas as pd

from governance.catalog import CATALOG
from governance.runner import (
    _entry_tables,
    _missing_entry_columns,
    _read_parquet_files,
    _required_tables,
    _scan_drourate_path,
)


def test_required_tables_include_reference_and_master_tables():
    required = _required_tables()

    assert "CMS_DROURATE" in required
    assert "T_CORRECT_AWB" in required
    assert "CMS_MSMU" in required
    assert "CMS_DSMU" in required


def test_entry_tables_collect_all_param_table_roles():
    entry = {
        "table": "BASE_TABLE",
        "params": {
            "left_table": "LEFT_TABLE",
            "right_table": "RIGHT_TABLE",
            "start_table": "START_TABLE",
            "end_table": "END_TABLE",
            "child_table": "CHILD_TABLE",
            "parent_table": "PARENT_TABLE",
            "reference_table": "REFERENCE_TABLE",
            "master_table": "MASTER_TABLE",
        },
    }

    assert _entry_tables(entry) == {
        "BASE_TABLE",
        "LEFT_TABLE",
        "RIGHT_TABLE",
        "START_TABLE",
        "END_TABLE",
        "CHILD_TABLE",
        "PARENT_TABLE",
        "REFERENCE_TABLE",
        "MASTER_TABLE",
    }


def test_catalog_entries_include_analysis_metadata():
    by_code = {entry["index_code"]: entry for entry in CATALOG}

    assert all("indicator" in entry for entry in CATALOG)
    assert all("impact_billing" in entry for entry in CATALOG)
    assert all("impact_operational" in entry for entry in CATALOG)
    assert all("impact" not in entry for entry in CATALOG)
    assert all("impact_none" not in entry for entry in CATALOG)
    assert by_code["COMP1V19"]["indicator"] == "Timestamp"
    assert by_code["COMP1E11"]["indicator"] == "Zone Code"
    assert by_code["COMP1J9"]["indicator"] == "Unique Identifier"
    assert by_code["COMP2P6"]["indicator"] == "Flag"
    assert by_code["COMP1V19"]["impact_operational"] == "Y"
    assert by_code["ACCU1A25"]["impact_billing"] == "Y"
    assert by_code["ACCU1A25"]["impact_operational"] == "Y"


def test_deleted_and_disabled_index_checks_are_not_active():
    by_code = {entry["index_code"]: entry for entry in CATALOG}

    assert "UNIQ1D1" not in by_code
    assert "TIME1P3" not in by_code
    assert by_code["INTG1D1"]["enabled"] is False
    assert by_code["INTG1I1"]["enabled"] is False
    assert by_code["INTG1J"]["enabled"] is False


def test_parquet_loader_ignores_missing_requested_columns_for_later_skip():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError:
        return

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "part-00000.parquet"
        table = pa.Table.from_pandas(pd.DataFrame({"KNOWN": ["A"], "CNOTE_NO": ["C1"]}))
        pq.write_table(table, path)

        frame = _read_parquet_files([path], ["KNOWN", "MISSING", "CNOTE_NO"])

    assert list(frame.columns) == ["KNOWN", "CNOTE_NO"]
    assert _missing_entry_columns(
        {"table": "BASE", "params": {"column": "MISSING", "cnote_column": "CNOTE_NO"}},
        {"BASE": frame},
    ) == ["BASE.MISSING"]


def test_parquet_loader_can_stream_distinct_reference_values():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError:
        return

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "part-00000.parquet"
        table = pa.Table.from_pandas(pd.DataFrame({
            "DROURATE_CODE": ["CGK", "CGK", "SUB"],
            "DROURATE_SERVICE": ["REG", "REG", "YES"],
        }))
        pq.write_table(table, path)

        frame = _read_parquet_files([path], ["DROURATE_CODE", "DROURATE_SERVICE"], distinct=True)

    assert len(frame) == 2
    assert set(frame["DROURATE_CODE"]) == {"CGK", "SUB"}


def test_drourate_stream_matches_candidate_components_and_counts_malformed():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError:
        return

    candidates = {
        "DROURATE_CODE": {"AAA10000BBB20000", "NOT_FOUND"},
        "DROURATE_SERVICE": {"REG", "NOPE"},
        "__origin_component": {"AAA10000", "MISS0000"},
        "__destination_component": {"BBB20000", "MISS9999"},
    }
    matched = {column: set() for column in candidates}

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "part-00000.parquet"
        table = pa.Table.from_pandas(pd.DataFrame({
            "DROURATE_CODE": ["AAA10000BBB20000", "BADCODE", "CCC30000DDD40000"],
            "DROURATE_SERVICE": ["REG", "YES", "YES"],
        }))
        pq.write_table(table, path)

        batches, malformed = _scan_drourate_path(path, candidates, matched)

    assert batches == 1
    assert malformed == 1
    assert matched["DROURATE_CODE"] == {"AAA10000BBB20000"}
    assert matched["DROURATE_SERVICE"] == {"REG"}
    assert matched["__origin_component"] == {"AAA10000"}
    assert matched["__destination_component"] == {"BBB20000"}
    assert "NOT_FOUND" not in matched["DROURATE_CODE"]


def test_drourate_catalog_components_and_branch_reference_are_configured():
    by_code = {entry["index_code"]: entry for entry in CATALOG}
    origin_codes = {"VALD1A3", "VALD1Y3", "VALD1K3", "VALD1M6", "VALD1H5", "VALD1H9", "VALD1L3", "VALD1Z12", "VALD1N3", "VALD1B12"}
    destination_codes = {"VALD1A7", "VALD1B13", "VALD1Y4", "VALD1X4", "VALD1AE6", "VALD1K4", "VALD1M7", "VALD1H6", "VALD1L4", "VALD1Z11", "VALD1N4"}

    for code in origin_codes:
        assert by_code[code]["params"]["reference_component"] == "origin"
    for code in destination_codes:
        assert by_code[code]["params"]["reference_component"] == "destination"
    for code in ("VALD1A6", "VALD1Y6", "VALD1AE8", "ACCU5A6", "ACCU6B6"):
        assert "reference_component" not in by_code[code]["params"]

    assert by_code["VALD1A4"]["params"]["reference_table"] == "ORA_BRANCH"
    assert by_code["VALD1A4"]["params"]["reference_column"] == "BRANCH_CODE"
    assert by_code["VALD1B12"]["rule_family"] == "reference_format"
    assert by_code["VALD1B12"]["params"]["reference_component"] == "origin"
    assert "pattern" not in by_code["VALD1B12"]["params"]
