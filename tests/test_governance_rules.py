import pandas as pd

from governance.rules import (
    check_aggregate_count_consistency,
    check_aggregate_sum_consistency,
    check_bridged_pair_consistency,
    check_bridged_timeliness,
    check_uniqueness,
    check_conditional_completeness,
    check_prefix_match,
    check_rounded_pair_consistency,
    check_suffix_after_prefix_match,
    check_timeliness,
)


def test_prefix_match_compares_first_three_characters():
    data = {
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["C1", "C2"],
            "CNOTE_ORIGIN": ["CGK001", "SUB001"],
        }),
        "CMS_DCORRECT_DEST": pd.DataFrame({
            "DCORRECT_CNOTE_NO": ["C1", "C2"],
            "DCORRECT_ORIGIN": ["CGK999", "BDO001"],
        }),
    }

    outcome = check_prefix_match(data, {
        "left_table": "CMS_CNOTE",
        "left_column": "CNOTE_ORIGIN",
        "right_table": "CMS_DCORRECT_DEST",
        "right_column": "DCORRECT_ORIGIN",
        "left_join_key": "CNOTE_NO",
        "right_join_key": "DCORRECT_CNOTE_NO",
        "cnote_column": "CNOTE_NO",
        "prefix_length": 3,
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


def test_suffix_after_prefix_match_compares_remaining_characters():
    data = {
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["C1", "C2"],
            "CNOTE_ORIGIN": ["CGK001", "SUB777"],
        }),
        "CMS_DCORRECT_DEST": pd.DataFrame({
            "DCORRECT_CNOTE_NO": ["C1", "C2"],
            "DCORRECT_ORIGIN": ["BDO001", "SUB001"],
        }),
    }

    outcome = check_suffix_after_prefix_match(data, {
        "left_table": "CMS_CNOTE",
        "left_column": "CNOTE_ORIGIN",
        "right_table": "CMS_DCORRECT_DEST",
        "right_column": "DCORRECT_ORIGIN",
        "left_join_key": "CNOTE_NO",
        "right_join_key": "DCORRECT_CNOTE_NO",
        "cnote_column": "CNOTE_NO",
        "prefix_length": 3,
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


def test_conditional_completeness_only_checks_matching_condition():
    data = {
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["M1", "M2", "M3"],
            "MANIFEST_APPROVED": ["Y", "N", "Y"],
            "MANIFEST_ROUTE": ["CGK-SUB", None, None],
        })
    }

    outcome = check_conditional_completeness(data, {
        "table": "CMS_MANIFEST",
        "column": "MANIFEST_ROUTE",
        "condition_column": "MANIFEST_APPROVED",
        "condition_value": "Y",
        "cnote_column": "MANIFEST_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "M3"


def test_rounded_pair_consistency_compares_rounded_values():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["C1", "C2"],
            "MFCNOTE_WEIGHT": [10.4, 11.6],
        }),
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["C1", "C2"],
            "CNOTE_WEIGHT": [10.49, 10.4],
        }),
    }

    outcome = check_rounded_pair_consistency(data, {
        "left_table": "CMS_MFCNOTE",
        "left_column": "MFCNOTE_WEIGHT",
        "right_table": "CMS_CNOTE",
        "right_column": "CNOTE_WEIGHT",
        "left_join_key": "MFCNOTE_NO",
        "right_join_key": "CNOTE_NO",
        "cnote_column": "MFCNOTE_NO",
        "decimals": 0,
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


def test_timeliness_uses_explicit_join_keys_and_direction():
    data = {
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["M1", "M2"],
            "MANIFEST_CRDATE": ["2026-06-01 08:00:00", "2026-06-01 10:00:00"],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_MAN_NO": ["M1", "M2"],
            "MFBAG_CRDATE": ["2026-06-01 09:00:00", "2026-06-01 09:30:00"],
        }),
    }

    outcome = check_timeliness(data, {
        "start_table": "CMS_MANIFEST",
        "start_column": "MANIFEST_CRDATE",
        "end_table": "CMS_MFBAG",
        "end_column": "MFBAG_CRDATE",
        "start_join_key": "MANIFEST_NO",
        "end_join_key": "MFBAG_MAN_NO",
        "cnote_column": "MANIFEST_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "M2"


def test_uniqueness_handles_clean_single_column_rule():
    data = {
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["C1", "C2"],
        })
    }

    outcome = check_uniqueness(data, {
        "table": "CMS_CNOTE",
        "columns": ["CNOTE_NO"],
        "cnote_column": "CNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 0
    assert outcome.failures.empty


def test_bridged_pair_consistency_compares_across_intermediate_table():
    data = {
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_BAG_NO": ["B1", "B2"],
            "DMBAG_ORIGIN": ["CGK", "SUB"],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["B1", "B2"],
            "MFBAG_MAN_NO": ["M1", "M2"],
        }),
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["M1", "M2"],
            "MANIFEST_FROM": ["CGK", "BDO"],
        }),
    }

    outcome = check_bridged_pair_consistency(data, {
        "left_table": "CMS_DMBAG",
        "left_column": "DMBAG_ORIGIN",
        "right_column": "MANIFEST_FROM",
        "joins": [
            {"table": "CMS_MFBAG", "left_on": "DMBAG_BAG_NO", "right_on": "MFBAG_NO"},
            {"table": "CMS_MANIFEST", "left_on": "MFBAG_MAN_NO", "right_on": "MANIFEST_NO"},
        ],
        "cnote_column": "DMBAG_BAG_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "B2"


def test_bridged_timeliness_compares_across_path():
    data = {
        "CMS_MHI_HOC": pd.DataFrame({
            "MHI_CNOTE_NO": ["C1", "C2"],
            "MHI_APPROVE_DATE": ["2026-06-01 08:00:00", "2026-06-01 10:00:00"],
        }),
        "CMS_DRCNOTE": pd.DataFrame({
            "DRCNOTE_NO": ["D1", "D2"],
            "DRCNOTE_CNOTE_NO": ["C1", "C2"],
        }),
        "CMS_MRCNOTE": pd.DataFrame({
            "MRCNOTE_NO": ["D1", "D2"],
            "MRCNOTE_SIGNDATE": ["2026-06-01 09:00:00", "2026-06-01 09:30:00"],
        }),
    }

    outcome = check_bridged_timeliness(data, {
        "left_table": "CMS_MHI_HOC",
        "start_column": "MHI_APPROVE_DATE",
        "end_column": "MRCNOTE_SIGNDATE",
        "joins": [
            {"table": "CMS_DRCNOTE", "left_on": "MHI_CNOTE_NO", "right_on": "DRCNOTE_CNOTE_NO"},
            {"table": "CMS_MRCNOTE", "left_on": "DRCNOTE_NO", "right_on": "MRCNOTE_NO"},
        ],
        "cnote_column": "DRCNOTE_CNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


def test_aggregate_sum_consistency_groups_detail_rows():
    data = {
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["B1", "B2"],
            "MFBAG_ACT_WEIGHT": [15, 9],
        }),
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_BAG_NO": ["B1", "B1", "B2"],
            "MFCNOTE_WEIGHT": [10, 5, 7],
        }),
    }

    outcome = check_aggregate_sum_consistency(data, {
        "master_table": "CMS_MFBAG",
        "master_key": "MFBAG_NO",
        "master_value_column": "MFBAG_ACT_WEIGHT",
        "detail_table": "CMS_MFCNOTE",
        "detail_key": "MFCNOTE_BAG_NO",
        "detail_value_column": "MFCNOTE_WEIGHT",
        "cnote_column": "MFBAG_NO",
        "decimals": 0,
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "B2"


def test_aggregate_count_consistency_counts_distinct_detail_rows_across_bridge():
    data = {
        "CMS_MMBAG": pd.DataFrame({
            "MMBAG_NO": ["MB1"],
            "MMBAG_QTY": [2],
        }),
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["C1", "C2", "C3"],
            "MFCNOTE_BAG_NO": ["B1", "B1", "B2"],
        }),
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_NO": ["MB1", "MB1"],
            "DMBAG_BAG_NO": ["B1", "B2"],
        }),
    }

    outcome = check_aggregate_count_consistency(data, {
        "master_table": "CMS_MMBAG",
        "master_key": "MMBAG_NO",
        "master_count_column": "MMBAG_QTY",
        "detail_table": "CMS_MFCNOTE",
        "detail_key": "DMBAG_NO",
        "detail_count_column": "MFCNOTE_NO",
        "joins": [
            {"table": "CMS_DMBAG", "left_on": "MFCNOTE_BAG_NO", "right_on": "DMBAG_BAG_NO"},
        ],
        "cnote_column": "MMBAG_NO",
    })

    assert outcome.total_checked == 1
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "MB1"
