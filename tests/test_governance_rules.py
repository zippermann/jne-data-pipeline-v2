import pandas as pd

from governance.rules import (
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
