import tempfile
from pathlib import Path

import pandas as pd

import extractor.bronze as bronze
from governance.catalog import CATALOG
from governance.output import GovernanceResultWriter, RESULT_COLUMNS
import governance.runner as runner
from governance.rules import RuleOutcome
from governance.runner import (
    GovernanceSource,
    BronzeTable,
    _entry_tables,
    _check_rows_frame,
    _cnote_contexts,
    _document_level,
    _document_stage,
    _document_bridges,
    _result_cnote_rows,
    _missing_entry_columns,
    _list_minio_parquet_objects,
    _read_parquet_files,
    _required_tables,
    _run_entries,
    _scan_drourate_path,
    _upload_governance_outputs_to_minio,
)


def test_required_tables_include_reference_and_master_tables():
    required = _required_tables()

    assert "CMS_DROURATE" in required
    assert "T_CORRECT_AWB" in required
    assert "CMS_MSMU" in required
    assert "CMS_DSMU" in required


def test_extractor_inventory_covers_governance_required_tables():
    inventory = {spec.table.upper() for spec in bronze.TABLE_SPECS}

    assert _required_tables() <= inventory


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


def test_pair_consistency_does_not_require_right_cnote_column_on_left_table():
    entry = {
        "index_code": "CONS_TEST",
        "table": "CMS_APICUST",
        "params": {
            "left_table": "CMS_APICUST",
            "left_column": "APICUST_WEIGHT",
            "right_table": "CMS_CNOTE",
            "right_column": "CNOTE_WEIGHT",
            "left_join_key": "APICUST_CNOTE_NO",
            "right_join_key": "CNOTE_NO",
            "cnote_column": "CNOTE_NO",
        },
    }
    data = {
        "CMS_APICUST": pd.DataFrame({
            "APICUST_CNOTE_NO": ["C1"],
            "APICUST_WEIGHT": [1.0],
        }),
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["C1"],
            "CNOTE_WEIGHT": [1.0],
        }),
    }

    assert _missing_entry_columns(entry, data) == []


def test_minio_listing_uses_manifest_source_prefix_for_reused_tables():
    class Item:
        def __init__(self, object_name):
            self.object_name = object_name

    class Client:
        def list_objects(self, bucket, prefix, recursive):
            assert bucket == "jne-bronze"
            assert prefix == "bronze/jne/old_run/ora_zone/"
            assert recursive is True
            return [
                Item("bronze/jne/old_run/ora_zone/_SUCCESS"),
                Item("bronze/jne/old_run/ora_zone/part-00001.parquet"),
            ]

    source = GovernanceSource(
        manifest={},
        tables={},
        client=Client(),
        bucket="jne-bronze",
        prefix="bronze/jne/current_run",
    )
    table = BronzeTable("ORA_ZONE", "ora_zone", source_prefix="bronze/jne/old_run/ora_zone/")

    assert _list_minio_parquet_objects(source, table) == ["bronze/jne/old_run/ora_zone/part-00001.parquet"]


def test_governance_output_uploads_under_run_prefix(tmp_path):
    class Client:
        def __init__(self):
            self.uploads = []

        def fput_object(self, bucket, object_name, file_path):
            self.uploads.append((bucket, object_name, Path(file_path).name))

    client = Client()
    source = GovernanceSource(
        manifest={},
        tables={},
        client=client,
        bucket="jne-bronze",
        prefix="bronze/jne/run_id=R_TEST",
    )
    output = tmp_path / "governance_results.parquet"
    output.write_bytes(b"parquet")

    uploaded = _upload_governance_outputs_to_minio(source, [output])

    assert client.uploads == [
        ("jne-bronze", "bronze/jne/run_id=R_TEST/governance/governance_results.parquet", "governance_results.parquet")
    ]
    assert uploaded == ["s3://jne-bronze/bronze/jne/run_id=R_TEST/governance/governance_results.parquet"]


def test_governance_writer_replaces_existing_output_files(tmp_path):
    try:
        import pyarrow  # noqa: F401
    except ModuleNotFoundError:
        return

    csv_path = tmp_path / "governance_results.csv"
    parquet_path = tmp_path / "governance_results.parquet"
    csv_path.write_text("old_header\nold_row\n", encoding="utf-8")
    parquet_path.write_bytes(b"old parquet")

    with GovernanceResultWriter(csv_path, parquet_path) as writer:
        writer.write(pd.DataFrame({
            "cnote_no": ["C1"],
            "document_type": ["CNOTE"],
            "document_id": ["C1"],
            "level": ["index"],
            "stage": [""],
            "index_code": ["COMP_TEST"],
            "main_indicator": ["Completeness"],
            "column_name": ["CNOTE_NO"],
            "table_name": ["CMS_CNOTE"],
            "status": ["PASS"],
            "variable_1": ["C1"],
            "variable_2": [""],
            "impact_billing": ["Y"],
            "impact_operational": ["Y"],
        }))

    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == ",".join(RESULT_COLUMNS)
    assert "old_header" not in lines
    assert "old_row" not in lines


def test_catalog_entries_include_analysis_metadata():
    by_code = {entry["index_code"]: entry for entry in CATALOG}

    assert all("indicator" in entry for entry in CATALOG)
    assert all("impact_billing" in entry for entry in CATALOG)
    assert all("impact_operational" in entry for entry in CATALOG)
    assert all("main_impact" in entry for entry in CATALOG)
    assert all("impact_details" in entry for entry in CATALOG)
    assert all("impact" not in entry for entry in CATALOG)
    assert all("impact_none" not in entry for entry in CATALOG)
    assert by_code["COMP1V19"]["indicator"] == "Timestamp"
    assert by_code["COMP1E11"]["indicator"] == "Zone Code"
    assert by_code["COMP1J9"]["indicator"] == "Unique Identifier"
    assert by_code["COMP2P6"]["indicator"] == "Flag"
    assert by_code["COMP1V19"]["impact_operational"] == "Y"
    assert by_code["ACCU1A25"]["impact_billing"] == "Y"
    assert by_code["ACCU1A25"]["impact_operational"] == "Y"
    assert by_code["ACCU4B15"]["main_impact"] == "Billing"
    assert by_code["ACCU4B15"]["impact_details"] == "Potential Revenue Loss"
    assert by_code["ACCU3B13B"]["main_impact"] == "Billing"
    assert by_code["ACCU3B13B"]["impact_details"] == "Over-billing/Under-billing"
    assert by_code["COMP1B1"]["main_impact"] == "TBD"
    assert by_code["COMP1B1"]["impact_details"] == "TBD"


def test_governance_results_enrich_pass_rows_with_dashboard_context():
    data = {
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["CNOTE1"],
            "CNOTE_ORIGIN": ["CGK10000"],
            "CNOTE_DESTINATION": ["BDO10000"],
            "CNOTE_SERVICES_CODE": ["REG"],
        }),
    }
    entry = {
        "index_code": "COMP_TEST",
        "element": "Completeness",
        "indicator": "Service Code",
        "table": "CMS_CNOTE",
        "params": {"column": "CNOTE_SERVICES_CODE", "cnote_column": "CNOTE_NO"},
        "description": "Service code should be present",
        "main_impact": "Operational",
        "impact_details": "Routing visibility",
        "impact_billing": "",
        "impact_operational": "Y",
    }
    outcome = RuleOutcome(
        total_checked=1,
        total_failed=0,
        failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
        checks=pd.DataFrame({
            "cnote_no": ["CNOTE1"],
            "status": ["PASS"],
            "variable_1": ["REG"],
            "variable_2": [""],
        }),
    )

    rows = _check_rows_frame(entry, outcome, {}, {"CNOTE1"}, _cnote_contexts(data))

    assert rows.loc[0, "status"] == "PASS"
    assert rows.loc[0, "element"] == "Completeness"
    assert rows.loc[0, "main_impact"] == "Operational"
    assert rows.loc[0, "impact_details"] == "Routing visibility"
    assert rows.loc[0, "issue_description"] == "Service code should be present"
    assert rows.loc[0, "package_journey"] == "Shipper"
    assert rows.loc[0, "service_type"] == "REG"
    assert rows.loc[0, "shipment_type"] == "Domestic"
    assert rows.loc[0, "origin_region"] == ""
    assert rows.loc[0, "destination_region"] == ""
    assert rows.loc[0, "origin_destination_region"] == ""


def test_drsheet_pra_uses_existing_cnote_column_as_document_key():
    for entry in CATALOG:
        if entry.get("table") == "CMS_DRSHEET_PRA":
            assert entry["params"].get("cnote_column") == "DRSHEET_CNOTE_NO"


def test_mhi_hoc_uses_mhi_no_as_document_key():
    for entry in CATALOG:
        if entry.get("table") == "CMS_MHI_HOC" and entry.get("rule_family") != "bridged_timeliness":
            assert entry["params"].get("cnote_column") == "MHI_NO"


def test_bag_governance_rows_keep_document_id_and_links_cnotes_separately():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["CNOTE1", "CNOTE2"],
            "MFCNOTE_BAG_NO": ["BAG1", "BAG1"],
        }),
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_NO": ["DMBAG1"],
            "DMBAG_BAG_NO": ["BAG1"],
        }),
    }
    entry = {
        "index_code": "COMP_TEST",
        "indicator": "Unique Identifier",
        "table": "CMS_DMBAG",
        "params": {"column": "DMBAG_BAG_NO", "cnote_column": "DMBAG_BAG_NO"},
    }
    outcome = RuleOutcome(
        total_checked=1,
        total_failed=0,
        failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
        checks=pd.DataFrame({
            "cnote_no": ["BAG1"],
            "status": ["PASS"],
            "variable_1": ["BAG1"],
            "variable_2": [""],
        }),
    )

    rows = _check_rows_frame(entry, outcome, _document_bridges(data), {"CNOTE1", "CNOTE2"})
    rows.insert(0, "result_id", ["R000000000001"])
    link_rows = _result_cnote_rows(rows)

    assert rows["cnote_no"].tolist() == [""]
    assert rows["document_id"].tolist() == ["BAG1"]
    assert rows["document_type"].tolist() == ["DMBAG"]
    assert rows["level"].tolist() == ["bag"]
    assert rows["stage"].tolist() == ["manifest"]
    assert link_rows["result_id"].tolist() == ["R000000000001", "R000000000001"]
    assert link_rows["cnote_no"].tolist() == ["CNOTE1", "CNOTE2"]
    assert link_rows.columns.tolist() == ["result_id", "cnote_no", "link_method"]


def test_bag_bridge_uses_mfcnote_bag_not_manifest_context():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["CNOTE1"],
            "MFCNOTE_BAG_NO": ["OTHER_BAG"],
            "MFCNOTE_MAN_NO": ["MAN1"],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["BAG1"],
            "MFBAG_MAN_NO": ["MAN1"],
        }),
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_NO": ["DMBAG1"],
            "DMBAG_BAG_NO": ["BAG1"],
        }),
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["MAN1"],
        }),
    }

    bridges = _document_bridges(data)

    assert bridges["CMS_MANIFEST"]["MAN1"] == ["CNOTE1"]
    assert "BAG1" not in bridges["CMS_MFBAG"]
    assert "BAG1" not in bridges["CMS_DMBAG"]


def test_mmbag_links_to_cnotes_through_dmbag():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["CNOTE1", "CNOTE2"],
            "MFCNOTE_BAG_NO": ["BAG1", "BAG1"],
        }),
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_NO": ["MMBAG1"],
            "DMBAG_BAG_NO": ["BAG1"],
        }),
        "CMS_MMBAG": pd.DataFrame({
            "MMBAG_NO": ["MMBAG1"],
        }),
    }
    entry = {
        "index_code": "COMP_TEST",
        "indicator": "Unique Identifier",
        "table": "CMS_MMBAG",
        "params": {"column": "MMBAG_NO", "cnote_column": "MMBAG_NO"},
    }
    outcome = RuleOutcome(
        total_checked=1,
        total_failed=0,
        failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
        checks=pd.DataFrame({
            "cnote_no": ["MMBAG1"],
            "status": ["PASS"],
            "variable_1": ["MMBAG1"],
            "variable_2": [""],
        }),
    )

    rows = _check_rows_frame(entry, outcome, _document_bridges(data), {"CNOTE1", "CNOTE2"})
    rows.insert(0, "result_id", ["R000000000001"])
    link_rows = _result_cnote_rows(rows)

    assert rows.loc[0, "document_id"] == "MMBAG1"
    assert rows.loc[0, "cnote_no"] == ""
    assert rows.loc[0, "level"] == "bag"
    assert rows.loc[0, "stage"] == "manifest"
    assert link_rows["cnote_no"].tolist() == ["CNOTE1", "CNOTE2"]


def test_confirmed_operational_documents_bridge_to_cnotes():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["CNOTE1"],
            "MFCNOTE_BAG_NO": ["BAG1"],
            "MFCNOTE_MAN_NO": ["MAN1"],
        }),
        "CMS_DRSHEET": pd.DataFrame({
            "DRSHEET_NO": ["RS1"],
            "DRSHEET_CNOTE_NO": ["CNOTE2"],
        }),
        "CMS_DHICNOTE": pd.DataFrame({
            "DHICNOTE_NO": ["HIC1"],
            "DHICNOTE_CNOTE_NO": ["CNOTE3"],
        }),
        "CMS_DHI_HOC": pd.DataFrame({
            "DHI_NO": ["MHI1"],
            "DHI_CNOTE_NO": ["CNOTE4"],
        }),
        "CMS_DHOCNOTE": pd.DataFrame({
            "DHOCNOTE_NO": ["HOC1"],
            "DHOCNOTE_CNOTE_NO": ["CNOTE5"],
        }),
        "CMS_DHOUNDEL_POD": pd.DataFrame({
            "DHOUNDEL_NO": ["UND1"],
            "DHOUNDEL_CNOTE_NO": ["CNOTE6"],
        }),
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_NO": ["MMBAG1"],
            "DMBAG_BAG_NO": ["BAG1"],
        }),
        "CMS_DSMU": pd.DataFrame({
            "DSMU_NO": ["SMU1"],
            "DSMU_BAG_NO": ["BAG1"],
        }),
        "CMS_MSMU": pd.DataFrame({
            "MSMU_NO": ["SMU1"],
        }),
        "CMS_RDSJ": pd.DataFrame({
            "RDSJ_NO": ["RDSJ1"],
            "RDSJ_HVO_NO": ["HOC1"],
            "RDSJ_HVI_NO": ["HIC1"],
        }),
        "CMS_DSJ": pd.DataFrame({
            "DSJ_NO": ["MSJ1"],
            "DSJ_HVO_NO": ["HOC1"],
        }),
        "CMS_MSJ": pd.DataFrame({
            "MSJ_NO": ["MSJ1"],
        }),
    }

    bridges = _document_bridges(data)

    assert bridges["CMS_MANIFEST"]["MAN1"] == ["CNOTE1"]
    assert bridges["CMS_MRSHEET"]["RS1"] == ["CNOTE2"]
    assert bridges["CMS_MHICNOTE"]["HIC1"] == ["CNOTE3"]
    assert bridges["CMS_MHI_HOC"]["MHI1"] == ["CNOTE4"]
    assert bridges["CMS_MHOCNOTE"]["HOC1"] == ["CNOTE5"]
    assert bridges["CMS_MHOUNDEL_POD"]["UND1"] == ["CNOTE6"]
    assert bridges["CMS_DSMU"]["SMU1"] == ["CNOTE1"]
    assert bridges["CMS_MSMU"]["SMU1"] == ["CNOTE1"]
    assert bridges["CMS_DSJ"]["MSJ1"] == ["CNOTE3"]
    assert bridges["CMS_RDSJ"]["RDSJ1"] == ["CNOTE5"]
    assert bridges["CMS_MSJ"]["MSJ1"] == ["CNOTE3"]


def test_unmapped_bag_document_does_not_masquerade_as_cnote():
    entry = {
        "index_code": "COMP_TEST",
        "indicator": "Unique Identifier",
        "table": "CMS_MFBAG",
        "params": {"column": "MFBAG_NO", "cnote_column": "MFBAG_NO"},
    }
    outcome = RuleOutcome(
        total_checked=1,
        total_failed=0,
        failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
        checks=pd.DataFrame({
            "cnote_no": ["BAG_ONLY"],
            "status": ["PASS"],
            "variable_1": ["BAG_ONLY"],
            "variable_2": [""],
        }),
    )

    rows = _check_rows_frame(entry, outcome, {"CMS_MFBAG": {}})

    assert rows.loc[0, "cnote_no"] == ""
    assert rows.loc[0, "document_id"] == "BAG_ONLY"
    assert rows.loc[0, "document_type"] == "MFBAG"


def test_non_cnote_document_only_populates_cnote_when_in_sample():
    entry = {
        "index_code": "COMP_TEST",
        "indicator": "Completeness",
        "table": "CMS_MANIFEST",
        "params": {"column": "MANIFEST_NO", "cnote_column": "MANIFEST_NO"},
    }
    outcome = RuleOutcome(
        total_checked=2,
        total_failed=0,
        failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
        checks=pd.DataFrame({
            "cnote_no": ["MANIFEST1", "CNOTE1"],
            "status": ["PASS", "PASS"],
            "variable_1": ["MANIFEST1", "CNOTE1"],
            "variable_2": ["", ""],
        }),
    )

    rows = _check_rows_frame(entry, outcome, {}, {"CNOTE1"})

    assert rows["document_id"].tolist() == ["MANIFEST1", "CNOTE1"]
    assert rows["cnote_no"].tolist() == ["", "CNOTE1"]
    assert rows["level"].tolist() == ["bag", "bag"]
    assert rows["stage"].tolist() == ["manifest", "manifest"]


def test_regular_cnote_governance_rows_are_index_level_without_stage():
    entry = {
        "index_code": "COMP_TEST",
        "indicator": "Completeness",
        "table": "CMS_CNOTE",
        "params": {"column": "CNOTE_NO", "cnote_column": "CNOTE_NO"},
    }
    outcome = RuleOutcome(
        total_checked=1,
        total_failed=0,
        failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
        checks=pd.DataFrame({
            "cnote_no": ["CNOTE1"],
            "status": ["PASS"],
            "variable_1": ["CNOTE1"],
            "variable_2": [""],
        }),
    )

    rows = _check_rows_frame(entry, outcome, {}, {"CNOTE1"})

    assert rows.loc[0, "level"] == "index"
    assert rows.loc[0, "stage"] == ""


def test_document_tags_cover_level_and_operational_stage():
    examples = {
        "CMS_DRCNOTE": ("bag", "receival"),
        "CMS_MFCNOTE": ("bag", "manifest"),
        "CMS_DHOCNOTE": ("bag", "handover"),
        "CMS_DRSHEET": ("bag", "runsheet"),
        "CMS_CNOTE": ("index", ""),
    }

    for table_name, (expected_level, expected_stage) in examples.items():
        entry = {"table": table_name}

        assert _document_level(entry) == expected_level
        assert _document_stage(entry) == expected_stage


def test_bridge_cnote_outside_sample_keeps_document_but_blanks_cnote():
    entry = {
        "index_code": "COMP_TEST",
        "indicator": "Completeness",
        "table": "CMS_MFBAG",
        "params": {"column": "MFBAG_NO", "cnote_column": "MFBAG_NO"},
    }
    outcome = RuleOutcome(
        total_checked=1,
        total_failed=0,
        failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
        checks=pd.DataFrame({
            "cnote_no": ["BAG1"],
            "status": ["PASS"],
            "variable_1": ["BAG1"],
            "variable_2": [""],
        }),
    )

    rows = _check_rows_frame(entry, outcome, {"CMS_MFBAG": {"BAG1": ["OUTSIDE_SAMPLE"]}}, {"CNOTE1"})

    assert rows.loc[0, "document_id"] == "BAG1"
    assert rows.loc[0, "cnote_no"] == ""


def test_deleted_and_disabled_index_checks_are_not_active():
    by_code = {entry["index_code"]: entry for entry in CATALOG}

    assert "UNIQ1D1" not in by_code
    assert "TIME1P3" not in by_code
    assert "INTG1D1" not in by_code
    assert "INTG1I1" not in by_code
    assert "INTG1J" not in by_code


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
    origin_codes = {"VALD1A3", "VALD1Y3", "VALD1K3", "VALD1M6", "VALD1H5", "VALD1H9", "VALD1L3", "VALD1B12"}
    destination_codes = {"VALD1A7", "VALD1B13", "VALD1Y4", "VALD1X4", "VALD1AE6", "VALD1K4", "VALD1M7", "VALD1H6", "VALD1L4"}

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
    assert by_code["VALD1Z11"]["params"]["reference_table"] == "ORA_ZONE"
    assert by_code["VALD1Z11"]["params"]["reference_column"] == "ZONE_CODE"
    assert by_code["VALD1Z12"]["params"]["reference_table"] == "ORA_ZONE"
    assert by_code["VALD1Z12"]["params"]["reference_column"] == "ZONE_CODE"
    assert by_code["VALD1N3"]["params"]["reference_table"] == "ORA_BRANCH"
    assert by_code["VALD1N3"]["params"]["reference_column"] == "BRANCH_CODE"
    assert by_code["VALD1N4"]["params"]["reference_table"] == "ORA_BRANCH"
    assert by_code["VALD1N4"]["params"]["reference_column"] == "BRANCH_CODE"


def test_rule_summary_records_skipped_and_no_row_rules(monkeypatch, tmp_path):
    skipped_entry = {
        "index_code": "TIME_TEST_SKIP",
        "element": "Timeliness",
        "indicator": "Timestamp",
        "rule_family": "fake_rule",
        "table": "MISSING_TABLE",
        "params": {},
        "impact_billing": "N",
        "impact_operational": "Y",
    }
    no_rows_entry = {
        "index_code": "CONS_TEST_EMPTY",
        "element": "Consistency",
        "indicator": "Weight",
        "rule_family": "fake_rule",
        "table": "BASE_TABLE",
        "params": {},
        "impact_billing": "Y",
        "impact_operational": "Y",
    }

    def fake_rule(data, params):
        return RuleOutcome(
            total_checked=0,
            total_failed=0,
            failures=pd.DataFrame(columns=["cnote_no", "failed_value", "failure_reason"]),
            checks=pd.DataFrame(columns=["cnote_no", "status", "variable_1", "variable_2"]),
        )

    monkeypatch.setattr(runner, "CATALOG", [skipped_entry, no_rows_entry])
    monkeypatch.setitem(runner.RULE_FUNCTIONS, "fake_rule", fake_rule)

    _run_entries(
        entries=[no_rows_entry],
        data={"BASE_TABLE": pd.DataFrame({"CNOTE_NO": []})},
        skipped={"TIME_TEST_SKIP": "missing bronze table(s): MISSING_TABLE"},
        output_dir=tmp_path,
        strict=False,
    )

    summary = pd.read_csv(tmp_path / "governance_rule_summary.csv")

    by_code = summary.set_index("index_code")
    assert by_code.loc["TIME_TEST_SKIP", "status"] == "SKIPPED"
    assert by_code.loc["TIME_TEST_SKIP", "skip_reason"] == "missing bronze table(s): MISSING_TABLE"
    assert by_code.loc["CONS_TEST_EMPTY", "status"] == "NO_ROWS"
    assert by_code.loc["CONS_TEST_EMPTY", "result_rows"] == 0


def test_skipped_rules_only_fail_when_requested(monkeypatch, tmp_path):
    skipped_entry = {
        "index_code": "TIME_TEST_SKIP",
        "element": "Timeliness",
        "indicator": "Timestamp",
        "rule_family": "fake_rule",
        "table": "MISSING_TABLE",
        "params": {},
        "impact_billing": "N",
        "impact_operational": "Y",
    }
    monkeypatch.setattr(runner, "CATALOG", [skipped_entry])

    _run_entries(
        entries=[],
        data={},
        skipped={"TIME_TEST_SKIP": "missing bronze table(s): MISSING_TABLE"},
        output_dir=tmp_path,
        strict=True,
    )

    try:
        _run_entries(
            entries=[],
            data={},
            skipped={"TIME_TEST_SKIP": "missing bronze table(s): MISSING_TABLE"},
            output_dir=tmp_path,
            strict=True,
            fail_on_skipped=True,
        )
    except RuntimeError as exc:
        assert "Governance skipped 1 active rule" in str(exc)
    else:
        raise AssertionError("fail_on_skipped=True should fail skipped active rules")
