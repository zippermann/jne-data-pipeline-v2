import pandas as pd

from governance.rules import (
    check_aggregate_count_consistency,
    check_aggregate_sum_consistency,
    check_bridged_pair_consistency,
    check_bridged_substring_match,
    check_bridged_timeliness,
    check_cnote_im_manifest_before_msj,
    check_duplicate_aware_weight_consistency,
    check_manifest_code_sequence,
    check_pair_consistency,
    check_reference_conditional_completeness,
    check_uniqueness,
    check_conditional_completeness,
    check_prefix_match,
    check_rounded_pair_consistency,
    check_suffix_after_prefix_match,
    check_timeliness,
    check_transit_manifest_required_for_origin_mismatch,
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


def test_pair_consistency_handles_same_named_columns_after_merge():
    data = {
        "CMS_COST_DTRANSIT_AGEN": pd.DataFrame({
            "CNOTE_NO": ["C1", "C2"],
            "CNOTE_QTY": [1, 2],
        }),
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["C1", "C2"],
            "CNOTE_QTY": [1, 3],
        }),
    }

    outcome = check_pair_consistency(data, {
        "left_table": "CMS_COST_DTRANSIT_AGEN",
        "left_column": "CNOTE_QTY",
        "right_table": "CMS_CNOTE",
        "right_column": "CNOTE_QTY",
        "left_join_key": "CNOTE_NO",
        "right_join_key": "CNOTE_NO",
        "cnote_column": "CNOTE_NO",
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


def test_conditional_completeness_can_use_regex_condition():
    data = {
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["B1", "B2", "B3"],
            "MFBAG_MAN_REF": ["CGK/IM/001", "CGK/TM/002", None],
        })
    }

    outcome = check_conditional_completeness(data, {
        "table": "CMS_MFBAG",
        "column": "MFBAG_MAN_REF",
        "condition_column": "MFBAG_MAN_REF",
        "condition_regex": r"(?:^|/)IM(?:/|$)",
        "cnote_column": "MFBAG_NO",
    })

    assert outcome.total_checked == 1
    assert outcome.total_failed == 0
    assert outcome.failures.empty


def test_reference_conditional_completeness_checks_only_reference_matches():
    data = {
        "CMS_DRSHEET": pd.DataFrame({
            "DRSHEET_CNOTE_NO": ["C1", "C2", "C3"],
            "DRSHEET_FLAG": ["Y", None, None],
        }),
        "CMS_CNOTE_POD": pd.DataFrame({
            "CNOTE_POD_NO": ["C1", "C2"],
        }),
    }

    outcome = check_reference_conditional_completeness(data, {
        "table": "CMS_DRSHEET",
        "column": "DRSHEET_FLAG",
        "condition_column": "DRSHEET_CNOTE_NO",
        "reference_table": "CMS_CNOTE_POD",
        "reference_column": "CNOTE_POD_NO",
        "cnote_column": "DRSHEET_CNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


def test_reference_conditional_completeness_accepts_multiple_references():
    data = {
        "CMS_DHOV_RSHEET": pd.DataFrame({
            "DHOV_RSHEET_CNOTE": ["C1", "C2", "C3"],
            "DHOV_RSHEET_UNDEL": [None, None, "Y"],
        }),
        "CMS_DHOUNDEL_POD": pd.DataFrame({
            "DHOUNDEL_CNOTE_NO": ["C2"],
        }),
        "CMS_MHOUNDEL_POD": pd.DataFrame({
            "MHOUNDEL_NO": ["C3"],
        }),
    }

    outcome = check_reference_conditional_completeness(data, {
        "table": "CMS_DHOV_RSHEET",
        "column": "DHOV_RSHEET_UNDEL",
        "condition_column": "DHOV_RSHEET_CNOTE",
        "references": [
            {"table": "CMS_DHOUNDEL_POD", "column": "DHOUNDEL_CNOTE_NO"},
            {"table": "CMS_MHOUNDEL_POD", "column": "MHOUNDEL_NO"},
        ],
        "cnote_column": "DHOV_RSHEET_CNOTE",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


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


def test_transit_manifest_required_for_origin_mismatch_requires_tm_manifest():
    data = {
        "CMS_DSMU": pd.DataFrame({
            "DSMU_NO": ["S1", "S2", "S3"],
            "DSMU_BAG_NO": ["B1", "B2", "B3"],
            "DSMU_BAG_ORIGIN": ["CGK001", "SUB001", "BDO001"],
        }),
        "CMS_MSMU": pd.DataFrame({
            "MSMU_NO": ["S1", "S2", "S3"],
            "MSMU_ORIGIN": ["CGK999", "CGK001", "SUB001"],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["B1", "B2", "B3"],
            "MFBAG_MAN_NO": ["OM001", "TM001", "OM003"],
        }),
    }

    outcome = check_transit_manifest_required_for_origin_mismatch(data, {})

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "B3"


def test_duplicate_aware_weight_consistency_uses_direct_check_for_single_dmbag_no():
    data = {
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_NO": ["D1", "D2"],
            "DMBAG_BAG_NO": ["B1", "B2"],
            "DMBAG_WEIGHT": [25, 30],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["B1", "B2"],
            "MFBAG_CTC_WEIGHT": [25, 28],
        }),
    }

    outcome = check_duplicate_aware_weight_consistency(data, {
        "left_table": "CMS_DMBAG",
        "left_column": "DMBAG_WEIGHT",
        "right_table": "CMS_MFBAG",
        "right_column": "MFBAG_CTC_WEIGHT",
        "left_join_key": "DMBAG_BAG_NO",
        "right_join_key": "MFBAG_NO",
        "duplicate_key": "DMBAG_NO",
        "cnote_column": "DMBAG_NO",
        "duplicate_threshold": 50,
        "decimals": 0,
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "D2"


def test_duplicate_aware_weight_consistency_aggregates_same_duplicate_weights_above_threshold():
    data = {
        "CMS_DMBAG": pd.DataFrame({
            "DMBAG_NO": ["D1", "D1", "D2", "D2"],
            "DMBAG_BAG_NO": ["B1", "B2", "B3", "B4"],
            "DMBAG_WEIGHT": [60, 60, 70, 70],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["B1", "B2", "B3", "B4"],
            "MFBAG_CTC_WEIGHT": [100, 20, 70, 60],
        }),
    }

    outcome = check_duplicate_aware_weight_consistency(data, {
        "left_table": "CMS_DMBAG",
        "left_column": "DMBAG_WEIGHT",
        "right_table": "CMS_MFBAG",
        "right_column": "MFBAG_CTC_WEIGHT",
        "left_join_key": "DMBAG_BAG_NO",
        "right_join_key": "MFBAG_NO",
        "duplicate_key": "DMBAG_NO",
        "cnote_column": "DMBAG_NO",
        "duplicate_threshold": 50,
        "decimals": 0,
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.checks.loc[outcome.checks["cnote_no"].eq("D1"), "status"].iloc[0] == "PASS"
    assert outcome.failures.iloc[0]["cnote_no"] == "D2"


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


def test_bridged_substring_match_compares_full_route_destination_component():
    data = {
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["M1", "M2"],
            "MANIFEST_ROUTE": ["CGK10000MES10000", "CGK10000BOO10000"],
        }),
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["C1", "C2"],
            "MFCNOTE_MAN_NO": ["M1", "M2"],
        }),
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": ["C1", "C2"],
            "CNOTE_DESTINATION": ["MES10000", "BOO10026"],
        }),
    }

    outcome = check_bridged_substring_match(data, {
        "left_table": "CMS_MANIFEST",
        "left_column": "MANIFEST_ROUTE",
        "right_column": "CNOTE_DESTINATION",
        "joins": [
            {"table": "CMS_MFCNOTE", "left_on": "MANIFEST_NO", "right_on": "MFCNOTE_MAN_NO"},
            {"table": "CMS_CNOTE", "left_on": "MFCNOTE_NO", "right_on": "CNOTE_NO"},
        ],
        "substring_start": 8,
        "substring_length": 8,
        "cnote_column": "CNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.checks.loc[outcome.checks["cnote_no"].eq("C1"), "status"].iloc[0] == "PASS"
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


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


def test_bridged_timeliness_uses_first_start_per_group():
    data = {
        "CMS_MHOCNOTE": pd.DataFrame({
            "MHOCNOTE_NO": ["HVO1", "HVO1"],
            "MHOCNOTE_SIGNDATE": ["2026-06-01 08:00:00", "2026-06-01 12:00:00"],
        }),
        "CMS_DSJ": pd.DataFrame({
            "DSJ_HVO_NO": ["HVO1"],
            "DSJ_NO": ["MSJ1"],
        }),
        "CMS_MSJ": pd.DataFrame({
            "MSJ_NO": ["MSJ1"],
            "MSJ_SIGNDATE": ["2026-06-01 09:00:00"],
        }),
    }

    outcome = check_bridged_timeliness(data, {
        "left_table": "CMS_MHOCNOTE",
        "start_column": "MHOCNOTE_SIGNDATE",
        "end_column": "MSJ_SIGNDATE",
        "joins": [
            {"table": "CMS_DSJ", "left_on": "MHOCNOTE_NO", "right_on": "DSJ_HVO_NO"},
            {"table": "CMS_MSJ", "left_on": "DSJ_NO", "right_on": "MSJ_NO"},
        ],
        "first_start_group": "MSJ_NO",
        "cnote_column": "MHOCNOTE_NO",
    })

    assert outcome.total_checked == 1
    assert outcome.total_failed == 0
    assert outcome.failures.empty


def test_bridged_timeliness_can_use_earliest_times_per_cnote():
    data = {
        "CMS_MRCNOTE": pd.DataFrame({
            "MRCNOTE_NO": ["R1"],
            "MRCNOTE_SIGNDATE": ["2026-05-01 11:19:22"],
        }),
        "CMS_DRCNOTE": pd.DataFrame({
            "DRCNOTE_NO": ["R1"],
            "DRCNOTE_CNOTE_NO": ["C1"],
        }),
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["C1", "C1"],
            "MFCNOTE_BAG_NO": ["B1", "B2"],
        }),
        "CMS_MFBAG": pd.DataFrame({
            "MFBAG_NO": ["B1", "B2"],
            "MFBAG_CRDATE": ["2026-05-02 09:39:13", "2026-05-01 11:59:02"],
        }),
    }

    outcome = check_bridged_timeliness(data, {
        "left_table": "CMS_MRCNOTE",
        "start_column": "MRCNOTE_SIGNDATE",
        "end_column": "MFBAG_CRDATE",
        "joins": [
            {"table": "CMS_DRCNOTE", "left_on": "MRCNOTE_NO", "right_on": "DRCNOTE_NO"},
            {"table": "CMS_MFCNOTE", "left_on": "DRCNOTE_CNOTE_NO", "right_on": "MFCNOTE_NO"},
            {"table": "CMS_MFBAG", "left_on": "MFCNOTE_BAG_NO", "right_on": "MFBAG_NO"},
        ],
        "cnote_column": "DRCNOTE_CNOTE_NO",
        "aggregate_by_cnote": "earliest",
    })

    assert outcome.total_checked == 1
    assert outcome.total_failed == 0
    assert outcome.checks.iloc[0]["cnote_no"] == "C1"
    assert outcome.checks.iloc[0]["variable_1"] == "2026-05-01 11:19:22"
    assert outcome.checks.iloc[0]["variable_2"] == "2026-05-01 11:59:02"


def test_bridged_timeliness_links_msj_to_mhicnote():
    data = {
        "CMS_MSJ": pd.DataFrame({
            "MSJ_NO": ["MSJ1", "MSJ2"],
            "MSJ_SIGNDATE": ["2026-06-01 08:00:00", "2026-06-01 10:00:00"],
        }),
        "CMS_DSJ": pd.DataFrame({
            "DSJ_NO": ["MSJ1", "MSJ2"],
            "DSJ_HVO_NO": ["HVO1", "HVO2"],
        }),
        "CMS_RDSJ": pd.DataFrame({
            "RDSJ_HVO_NO": ["HVO1", "HVO2"],
            "RDSJ_HVI_NO": ["HVI1", "HVI2"],
        }),
        "CMS_MHICNOTE": pd.DataFrame({
            "MHICNOTE_NO": ["HVI1", "HVI2"],
            "MHICNOTE_DATE": ["2026-06-01 09:00:00", "2026-06-01 09:30:00"],
        }),
    }

    outcome = check_bridged_timeliness(data, {
        "left_table": "CMS_MSJ",
        "start_column": "MSJ_SIGNDATE",
        "end_column": "MHICNOTE_DATE",
        "joins": [
            {"table": "CMS_DSJ", "left_on": "MSJ_NO", "right_on": "DSJ_NO"},
            {"table": "CMS_RDSJ", "left_on": "DSJ_HVO_NO", "right_on": "RDSJ_HVO_NO"},
            {"table": "CMS_MHICNOTE", "left_on": "RDSJ_HVI_NO", "right_on": "MHICNOTE_NO"},
        ],
        "cnote_column": "MHICNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "HVI2"


def test_bridged_timeliness_links_mhicnote_to_mrsheet():
    data = {
        "CMS_MHICNOTE": pd.DataFrame({
            "MHICNOTE_NO": ["HVI1", "HVI2"],
            "MHICNOTE_DATE": ["2026-06-01 08:00:00", "2026-06-01 10:00:00"],
        }),
        "CMS_DHICNOTE": pd.DataFrame({
            "DHICNOTE_NO": ["HVI1", "HVI2"],
            "DHICNOTE_CNOTE_NO": ["C1", "C2"],
        }),
        "CMS_DRSHEET": pd.DataFrame({
            "DRSHEET_CNOTE_NO": ["C1", "C2"],
            "DRSHEET_NO": ["RS1", "RS2"],
        }),
        "CMS_MRSHEET": pd.DataFrame({
            "MRSHEET_NO": ["RS1", "RS2"],
            "MRSHEET_DATE": ["2026-06-01 09:00:00", "2026-06-01 09:30:00"],
        }),
    }

    outcome = check_bridged_timeliness(data, {
        "left_table": "CMS_MHICNOTE",
        "start_column": "MHICNOTE_DATE",
        "end_column": "MRSHEET_DATE",
        "joins": [
            {"table": "CMS_DHICNOTE", "left_on": "MHICNOTE_NO", "right_on": "DHICNOTE_NO"},
            {"table": "CMS_DRSHEET", "left_on": "DHICNOTE_CNOTE_NO", "right_on": "DRSHEET_CNOTE_NO"},
            {"table": "CMS_MRSHEET", "left_on": "DRSHEET_NO", "right_on": "MRSHEET_NO"},
        ],
        "cnote_column": "DHICNOTE_CNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C2"


def test_manifest_code_sequence_checks_om_before_tm():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["C1", "C1", "C2", "C2"],
            "MFCNOTE_MAN_NO": ["OM1", "TM1", "OM2", "TM2"],
        }),
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["OM1", "TM1", "OM2", "TM2"],
            "MANIFEST_CODE": [1, 2, 1, 2],
            "MANIFEST_CRDATE": [
                "2026-06-01 10:00:00",
                "2026-06-01 09:00:00",
                "2026-06-01 08:00:00",
                "2026-06-01 09:00:00",
            ],
        }),
    }

    outcome = check_manifest_code_sequence(data, {
        "mode": "om_before_tm",
        "manifest_code_column": "MANIFEST_CODE",
        "date_column": "MANIFEST_CRDATE",
        "cnote_column": "MFCNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C1"


def test_manifest_code_sequence_checks_tm_before_im():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["C1", "C1", "C2", "C2"],
            "MFCNOTE_MAN_NO": ["TM1", "IM1", "TM2", "IM2"],
        }),
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["TM1", "IM1", "TM2", "IM2"],
            "MANIFEST_CODE": [2, 3, 2, 3],
            "MANIFEST_CRDATE": [
                "2026-06-01 10:00:00",
                "2026-06-01 09:00:00",
                "2026-06-01 08:00:00",
                "2026-06-01 09:00:00",
            ],
        }),
    }

    outcome = check_manifest_code_sequence(data, {
        "mode": "tm_sequence_before_im",
        "manifest_code_column": "MANIFEST_CODE",
        "date_column": "MANIFEST_CRDATE",
        "cnote_column": "MFCNOTE_NO",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C1"


def test_cnote_im_manifest_before_msj_uses_manifest_code_three():
    data = {
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": ["C1", "C2"],
            "MFCNOTE_MAN_NO": ["IM1", "IM2"],
        }),
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": ["IM1", "IM2"],
            "MANIFEST_CODE": [3, 3],
            "MANIFEST_DATE": ["2026-06-01 10:00:00", "2026-06-01 08:00:00"],
        }),
        "CMS_DHICNOTE": pd.DataFrame({
            "DHICNOTE_NO": ["HVI1", "HVI2"],
            "DHICNOTE_CNOTE_NO": ["C1", "C2"],
        }),
        "CMS_RDSJ": pd.DataFrame({
            "RDSJ_HVI_NO": ["HVI1", "HVI2"],
            "RDSJ_HVO_NO": ["HVO1", "HVO2"],
        }),
        "CMS_DSJ": pd.DataFrame({
            "DSJ_HVO_NO": ["HVO1", "HVO2"],
            "DSJ_NO": ["MSJ1", "MSJ2"],
        }),
        "CMS_MSJ": pd.DataFrame({
            "MSJ_NO": ["MSJ1", "MSJ2"],
            "MSJ_SIGNDATE": ["2026-06-01 09:00:00", "2026-06-01 09:00:00"],
        }),
    }

    outcome = check_cnote_im_manifest_before_msj(data, {
        "manifest_code": "3",
        "manifest_code_column": "MANIFEST_CODE",
        "manifest_date_column": "MANIFEST_DATE",
        "msj_date_column": "MSJ_SIGNDATE",
    })

    assert outcome.total_checked == 2
    assert outcome.total_failed == 1
    assert outcome.failures.iloc[0]["cnote_no"] == "C1"


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
