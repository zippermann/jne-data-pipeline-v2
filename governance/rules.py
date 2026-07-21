"""Pandas rule functions for the simple governance checker.

Each function handles one rule family and returns the same RuleOutcome shape.
The code favors readability over speed so junior engineers can trace the logic.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable

import pandas as pd


@dataclass
class RuleOutcome:
    total_checked: int
    total_failed: int
    failures: pd.DataFrame
    checks: pd.DataFrame | None = None


FAILURE_COLUMNS = ["cnote_no", "failed_value", "failure_reason"]
CHECK_COLUMNS = ["cnote_no", "status", "variable_1", "variable_2"]
RuleFunction = Callable[[dict[str, pd.DataFrame], dict], RuleOutcome]

# JNE lookup guide:
# - Full index codes such as VALD2Y15 live in governance/catalog.py.
# - The rule index is the leading code group, such as VALD2, COMP1, or TIME1.
# - Each check function below is labeled with the rule indexes it implements.
# - rule_family maps catalog entries to functions through RULE_FUNCTIONS.


def _empty_failures() -> pd.DataFrame:
    return pd.DataFrame(columns=FAILURE_COLUMNS)


def _empty_checks() -> pd.DataFrame:
    return pd.DataFrame(columns=CHECK_COLUMNS)


def _cnote_values(df: pd.DataFrame, params: dict) -> pd.Series:
    column = params.get("cnote_column", "CNOTE_NO")
    if column in df.columns:
        return df[column]
    return pd.Series([""] * len(df), index=df.index)


def _as_failure_frame(cnote_no: pd.Series, failed_value: pd.Series | str, reason: str) -> pd.DataFrame:
    if isinstance(failed_value, str):
        failed_value = pd.Series([failed_value] * len(cnote_no), index=cnote_no.index)
    return pd.DataFrame({
        "cnote_no": cnote_no.fillna("").astype(str),
        "failed_value": failed_value.fillna("").astype(str),
        "failure_reason": reason,
    })


def _as_check_frame(
    cnote_no: pd.Series,
    failed: pd.Series,
    variable_1: pd.Series | str = "",
    variable_2: pd.Series | str = "",
) -> pd.DataFrame:
    if isinstance(variable_1, str):
        variable_1 = pd.Series([variable_1] * len(cnote_no), index=cnote_no.index)
    if isinstance(variable_2, str):
        variable_2 = pd.Series([variable_2] * len(cnote_no), index=cnote_no.index)
    return pd.DataFrame({
        "cnote_no": cnote_no.fillna("").astype(str),
        "status": failed.map({True: "FAIL", False: "PASS"}).astype(str),
        "variable_1": variable_1.fillna("").astype(str),
        "variable_2": variable_2.fillna("").astype(str),
    })


def _present(values: pd.Series) -> pd.Series:
    return values.notna() & values.astype("string").str.strip().ne("")


def _string_values(values: pd.Series) -> pd.Series:
    return values.fillna("").astype("string").str.strip()


def _merge_pair(data: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame:
    left_key = params.get("left_join_key", params.get("join_key"))
    right_key = params.get("right_join_key", params.get("join_key"))
    if left_key is None or right_key is None:
        raise ValueError("pair rule requires left_join_key and right_join_key")
    return data[params["left_table"]].merge(
        data[params["right_table"]],
        left_on=left_key,
        right_on=right_key,
        suffixes=("_left", "_right"),
    )


def _merge_bridge(data: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame:
    merged = data[params["left_table"]].copy()
    for step in params["joins"]:
        merged = merged.merge(
            data[step["table"]],
            left_on=step["left_on"],
            right_on=step["right_on"],
            suffixes=("", f"_{step['table'].lower()}"),
        )
    return merged


def _merged_cnote_values(merged: pd.DataFrame, params: dict) -> pd.Series:
    column = params.get("cnote_column", params.get("left_join_key", "CNOTE_NO"))
    for candidate in (column, f"{column}_left", f"{column}_right", "cnote_no"):
        if candidate in merged.columns:
            return merged[candidate]
    return pd.Series([""] * len(merged), index=merged.index)


def _merged_pair_column(merged: pd.DataFrame, column: str, side: str) -> pd.Series:
    if column in merged.columns:
        return merged[column]
    suffixed = f"{column}_{side}"
    if suffixed in merged.columns:
        return merged[suffixed]
    raise KeyError(column)


# JNE rule indexes here: COMP1, plus COMP2 rows that are simple null checks.
def check_completeness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    failed = values.isna() | values.astype("string").str.strip().eq("")
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is null or empty")
    checks = _as_check_frame(_cnote_values(table, params), failed, values, "")
    return RuleOutcome(len(table), int(failed.sum()), failures, checks)


# JNE rule indexes here: COMP2 conditional rows; current catalog also routes
# COMP3J9 here because it is implemented as a conditional completeness check.
def check_conditional_completeness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    condition_column = params["condition_column"]
    if params.get("condition_regex"):
        condition = _string_values(table[condition_column]).str.contains(params["condition_regex"], regex=True, na=False)
        condition_label = params["condition_regex"]
    elif params.get("condition_mode") == "not_null":
        # Rule applies whenever the condition column is filled (e.g. an approval date exists).
        condition = _present(table[condition_column])
        condition_label = "filled"
    else:
        condition_value = str(params.get("condition_value", "Y")).strip().upper()
        condition = _string_values(table[condition_column]).str.upper().eq(condition_value)
        condition_label = condition_value
    values = table[column]
    failed = condition & ~_present(values)
    failures = _as_failure_frame(
        _cnote_values(table.loc[failed], params),
        values.loc[failed],
        f"{column} is required when {condition_column} is {condition_label}",
    )
    checks = _as_check_frame(_cnote_values(table.loc[condition], params), failed.loc[condition], values.loc[condition], table.loc[condition, condition_column])
    return RuleOutcome(int(condition.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes here: COMP2 rows whose condition comes from a reference table.
def check_reference_conditional_completeness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    references = params.get("references")
    if references is None:
        references = [{"table": params["reference_table"], "column": params["reference_column"]}]
    condition_values: set[str] = set()
    for reference_params in references:
        reference = data[reference_params["table"]]
        condition_values.update(_normalized_strings(reference[reference_params["column"]].dropna()))
    condition_key = _normalized_strings(table[params["condition_column"]])
    condition = condition_key.isin(condition_values)
    values = table[column]
    failed = condition & ~_present(values)
    failures = _as_failure_frame(
        _cnote_values(table.loc[failed], params),
        values.loc[failed],
        f"{column} is required when {params['condition_column']} exists in reference rows",
    )
    checks = _as_check_frame(_cnote_values(table.loc[condition], params), failed.loc[condition], values.loc[condition], table.loc[condition, params["condition_column"]])
    return RuleOutcome(int(condition.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: VALD1, VALD5, VALD7, VALD8, VALD9, VALD11.
def check_validity_regex(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    present = values.notna() & values.astype("string").str.strip().ne("")
    failed = present & ~values.astype("string").str.match(params["pattern"])
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} does not match pattern")
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], values.loc[present], params["pattern"])
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: UNIQ1, UNIQ2.
def check_uniqueness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    columns = params["columns"]
    present = table[columns].notna().all(axis=1)
    duplicates = table.loc[present].duplicated(subset=columns, keep=False)
    failed_rows = table.loc[present].loc[duplicates]
    if failed_rows.empty:
        failed_value = pd.Series([], index=failed_rows.index, dtype="string")
    else:
        failed_value = failed_rows[columns].astype(str).agg("|".join, axis=1)
    failures = _as_failure_frame(_cnote_values(failed_rows, params), failed_value, f"{', '.join(columns)} is duplicated")
    present_rows = table.loc[present]
    present_values = present_rows[columns].astype(str).agg("|".join, axis=1)
    failed_present = present_rows.duplicated(subset=columns, keep=False)
    checks = _as_check_frame(_cnote_values(present_rows, params), failed_present, present_values, "")
    return RuleOutcome(int(present.sum()), len(failed_rows), failures, checks)


# JNE rule indexes: CONS1, CONS2.
def check_pair_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    left_value = _merged_pair_column(merged, params["left_column"], "left")
    right_value = _merged_pair_column(merged, params["right_column"], "right")
    comparable = left_value.notna() & right_value.notna()
    failed = comparable & left_value.ne(right_value)
    # Null comparisons are skipped because some child rows are optional by business path.
    failed_value = left_value.loc[failed].astype(str) + " != " + right_value.loc[failed].astype(str)
    failures = _as_failure_frame(_merged_cnote_values(merged.loc[failed], params), failed_value, "paired values do not match")
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], left_value.loc[comparable], right_value.loc[comparable])
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: ACCU2.
def check_prefix_match(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    prefix_length = int(params.get("prefix_length", 3))
    left_value = _string_values(_merged_pair_column(merged, params["left_column"], "left"))
    right_value = _string_values(_merged_pair_column(merged, params["right_column"], "right"))
    comparable = left_value.ne("") & right_value.ne("")
    failed = comparable & left_value.str[:prefix_length].ne(right_value.str[:prefix_length])
    failed_value = left_value.loc[failed] + " != " + right_value.loc[failed]
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        f"first {prefix_length} characters do not match",
    )
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], left_value.loc[comparable], right_value.loc[comparable])
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: ACCU3.
def check_suffix_after_prefix_match(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    prefix_length = int(params.get("prefix_length", 3))
    left_value = _string_values(_merged_pair_column(merged, params["left_column"], "left"))
    right_value = _string_values(_merged_pair_column(merged, params["right_column"], "right"))
    comparable = left_value.ne("") & right_value.ne("")
    failed = comparable & left_value.str[prefix_length:].ne(right_value.str[prefix_length:])
    failed_value = left_value.loc[failed] + " != " + right_value.loc[failed]
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        f"characters after first {prefix_length} do not match",
    )
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], left_value.loc[comparable], right_value.loc[comparable])
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: CONS2.
def check_rounded_pair_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    decimals = int(params.get("decimals", 0))
    left_raw = pd.to_numeric(_merged_pair_column(merged, params["left_column"], "left"), errors="coerce")
    right_raw = pd.to_numeric(_merged_pair_column(merged, params["right_column"], "right"), errors="coerce")
    comparable = left_raw.notna() & right_raw.notna()
    left_value = left_raw.round(decimals)
    right_value = right_raw.round(decimals)
    failed = comparable & left_value.ne(right_value)
    failed_value = left_raw.loc[failed].astype(str) + " != " + right_raw.loc[failed].astype(str)
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        "rounded numeric values do not match",
    )
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], left_raw.loc[comparable], right_raw.loc[comparable])
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: CONS3.
def check_duplicate_aware_weight_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    group_key = params["duplicate_key"]
    decimals = int(params.get("decimals", 0))
    duplicate_threshold = float(params.get("duplicate_threshold", 50))
    left_raw = pd.to_numeric(_merged_pair_column(merged, params["left_column"], "left"), errors="coerce")
    right_raw = pd.to_numeric(_merged_pair_column(merged, params["right_column"], "right"), errors="coerce")
    comparable = left_raw.notna() & right_raw.notna() & _present(merged[group_key])
    work = merged.loc[comparable].copy()
    if work.empty:
        return RuleOutcome(0, 0, _empty_failures(), _empty_checks())

    work["_left_raw"] = left_raw.loc[comparable]
    work["_right_raw"] = right_raw.loc[comparable]
    work["_left_compare"] = work["_left_raw"].round(decimals)
    work["_right_compare"] = work["_right_raw"].round(decimals)
    grouped = work.groupby(group_key, dropna=False)
    group_size = grouped["_left_compare"].transform("size")
    group_same_weight = grouped["_left_compare"].transform("nunique").eq(1)
    group_weight_above_threshold = grouped["_left_compare"].transform("first").gt(duplicate_threshold)
    aggregate_row = group_size.gt(1) & group_same_weight & group_weight_above_threshold

    direct = work.loc[~aggregate_row].copy()
    direct_failed = direct["_left_compare"].ne(direct["_right_compare"])
    direct_failed_value = direct.loc[direct_failed, "_left_raw"].astype(str) + " != " + direct.loc[direct_failed, "_right_raw"].astype(str)
    direct_failures = _as_failure_frame(
        _merged_cnote_values(direct.loc[direct_failed], params),
        direct_failed_value,
        "paired weights do not match",
    )
    direct_checks = _as_check_frame(
        _merged_cnote_values(direct, params),
        direct_failed,
        direct["_left_raw"],
        direct["_right_raw"],
    )

    aggregate = work.loc[aggregate_row].copy()
    if aggregate.empty:
        aggregate_failures = _empty_failures()
        aggregate_checks = _empty_checks()
        aggregate_checked = 0
    else:
        aggregate_values = aggregate.groupby(group_key, dropna=False).agg(
            left_total=("_left_raw", "sum"),
            right_total=("_right_raw", "sum"),
        )
        aggregate_failed = aggregate_values["left_total"].round(decimals).ne(aggregate_values["right_total"].round(decimals))
        aggregate_failed_value = (
            aggregate_values.loc[aggregate_failed, "left_total"].astype(str)
            + " != "
            + aggregate_values.loc[aggregate_failed, "right_total"].astype(str)
        )
        aggregate_failures = _as_failure_frame(
            pd.Series(aggregate_values.loc[aggregate_failed].index, index=aggregate_values.loc[aggregate_failed].index),
            aggregate_failed_value,
            "duplicate-group weight total does not match",
        )
        aggregate_checks = _as_check_frame(
            pd.Series(aggregate_values.index, index=aggregate_values.index),
            aggregate_failed,
            aggregate_values["left_total"],
            aggregate_values["right_total"],
        )
        aggregate_checked = len(aggregate_values)

    failures = pd.concat([direct_failures, aggregate_failures], ignore_index=True)
    checks = pd.concat([direct_checks, aggregate_checks], ignore_index=True)
    return RuleOutcome(len(direct) + aggregate_checked, len(failures), failures, checks)


# JNE rule indexes: CONS2.
def check_bridged_pair_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_bridge(data, params)
    left_value = merged[params["left_column"]]
    right_value = merged[params["right_column"]]
    comparable = left_value.notna() & right_value.notna()
    if "decimals" in params:
        left_compare = pd.to_numeric(left_value, errors="coerce").round(int(params["decimals"]))
        right_compare = pd.to_numeric(right_value, errors="coerce").round(int(params["decimals"]))
        comparable = left_compare.notna() & right_compare.notna()
    else:
        left_compare = _string_values(left_value)
        right_compare = _string_values(right_value)
        comparable = left_compare.ne("") & right_compare.ne("")
    failed = comparable & left_compare.ne(right_compare)
    failed_value = left_value.loc[failed].astype(str) + " != " + right_value.loc[failed].astype(str)
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        "bridged values do not match",
    )
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], left_value.loc[comparable], right_value.loc[comparable])
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: CONS3.
def check_bridged_substring_match(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_bridge(data, params)
    start = int(params["substring_start"])
    length = int(params["substring_length"])
    left_value = _string_values(merged[params["left_column"]])
    right_value = _string_values(merged[params["right_column"]])
    left_compare = left_value.str[start:start + length]
    comparable = left_value.ne("") & right_value.ne("")
    failed = comparable & left_compare.ne(right_value)
    failed_value = left_value.loc[failed] + " -> " + left_compare.loc[failed] + " != " + right_value.loc[failed]
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        "bridged substring does not match",
    )
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], left_value.loc[comparable], right_value.loc[comparable])
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: CONS3.
def check_aggregate_sum_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    detail_params = {
        "left_table": params["detail_table"],
        "joins": params.get("joins", []),
        "cnote_column": params.get("detail_cnote_column", params["detail_key"]),
    }
    detail = _merge_bridge(data, detail_params) if params.get("joins") else data[params["detail_table"]].copy()
    grouped = (
        pd.to_numeric(detail[params["detail_value_column"]], errors="coerce")
        .groupby(detail[params["detail_key"]])
        .sum(min_count=1)
        .rename("detail_total")
    )
    master = data[params["master_table"]]
    merged = master.merge(grouped, left_on=params["master_key"], right_index=True, how="inner")
    master_value = pd.to_numeric(merged[params["master_value_column"]], errors="coerce")
    detail_value = pd.to_numeric(merged["detail_total"], errors="coerce")
    decimals = int(params.get("decimals", 0))
    comparable = master_value.notna() & detail_value.notna()
    failed = comparable & master_value.round(decimals).ne(detail_value.round(decimals))
    failed_value = master_value.loc[failed].astype(str) + " != " + detail_value.loc[failed].astype(str)
    failures = _as_failure_frame(
        _cnote_values(merged.loc[failed], {"cnote_column": params.get("cnote_column", params["master_key"])}),
        failed_value,
        "master value does not match bridged detail sum",
    )
    checks = _as_check_frame(
        _cnote_values(merged.loc[comparable], {"cnote_column": params.get("cnote_column", params["master_key"])}),
        failed.loc[comparable],
        master_value.loc[comparable],
        detail_value.loc[comparable],
    )
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: CONS4.
def check_aggregate_count_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    detail_params = {
        "left_table": params["detail_table"],
        "joins": params.get("joins", []),
        "cnote_column": params.get("detail_cnote_column", params["detail_count_column"]),
    }
    detail = _merge_bridge(data, detail_params) if params.get("joins") else data[params["detail_table"]].copy()
    grouped = (
        detail.groupby(params["detail_key"])[params["detail_count_column"]]
        .nunique()
        .rename("detail_count")
    )
    master = data[params["master_table"]]
    merged = master.merge(grouped, left_on=params["master_key"], right_index=True, how="inner")
    master_value = pd.to_numeric(merged[params["master_count_column"]], errors="coerce")
    comparable = master_value.notna()
    failed = comparable & master_value.ne(merged["detail_count"])
    failed_value = master_value.loc[failed].astype(str) + " != " + merged.loc[failed, "detail_count"].astype(str)
    failures = _as_failure_frame(
        _cnote_values(merged.loc[failed], {"cnote_column": params.get("cnote_column", params["master_key"])}),
        failed_value,
        "master count does not match bridged detail count",
    )
    checks = _as_check_frame(
        _cnote_values(merged.loc[comparable], {"cnote_column": params.get("cnote_column", params["master_key"])}),
        failed.loc[comparable],
        master_value.loc[comparable],
        merged.loc[comparable, "detail_count"],
    )
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)



# JNE rule indexes: VALD4.
def check_validity_datetime(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    present = _present(values)
    parsed = pd.to_datetime(values.where(present), errors="coerce")
    failed = present & parsed.isna()
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is not a valid timestamp")
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], values.loc[present], "")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: VALD2, VALD3.
def check_validity_integer(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    present = _present(values)
    numeric = pd.to_numeric(values.where(present), errors="coerce")
    # A value passes when it parses as a number and carries no fractional part.
    failed = present & (numeric.isna() | numeric.ne(numeric.round(0)))
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is not a whole number")
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], values.loc[present], "")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


def _normalized_strings(values: pd.Series) -> pd.Series:
    # Strip a trailing .0 so numeric columns read from floats compare as plain digits.
    return _string_values(values).str.replace(r"\.0+$", "", regex=True)


# JNE rule indexes: VALD10.
def check_validity_in_set(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    allowed = {str(v) for v in params["allowed"]}
    values = table[column]
    present = _present(values)
    failed = present & ~_normalized_strings(values).isin(allowed)
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is not one of {sorted(allowed)}")
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], values.loc[present], ",".join(sorted(allowed)))
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


def _reference_values(data: dict[str, pd.DataFrame], params: dict) -> set[str]:
    reference = data[params["reference_table"]]
    component_columns = {
        "origin": "__origin_component",
        "destination": "__destination_component",
    }
    column = component_columns.get(params.get("reference_component"), params["reference_column"])
    return set(_normalized_strings(reference[column].dropna()))


# JNE rule indexes: ACCU5, ACCU6.
def check_value_in_reference(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    reference_values = _reference_values(data, params)
    values = table[column]
    present = _present(values)
    failed = present & ~_normalized_strings(values).isin(reference_values)
    failures = _as_failure_frame(
        _cnote_values(table.loc[failed], params),
        values.loc[failed],
        f"{column} not found in {params['reference_table']}.{params['reference_column']}",
    )
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], values.loc[present], params["reference_table"])
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: VALD1, VALD12, VALD13.
def check_reference_format(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    reference_values = _reference_values(data, params)
    values = table[column]
    present = _present(values)
    strings = _normalized_strings(values)
    # Two conditions from the index list: alphanumeric only, and known to the reference table.
    failed = present & (~strings.str.fullmatch(r"[A-Za-z0-9]+") | ~strings.isin(reference_values))
    failures = _as_failure_frame(
        _cnote_values(table.loc[failed], params),
        values.loc[failed],
        f"{column} is not alphanumeric or not found in {params['reference_table']}.{params['reference_column']}",
    )
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], values.loc[present], params["reference_table"])
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: ACCU1.
def check_non_negative(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = pd.to_numeric(table[column], errors="coerce")
    present = values.notna()
    failed = present & values.lt(0)
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), table[column].loc[failed], f"{column} is negative")
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], table.loc[present, column], "0")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: ACCU4.
def check_non_negative_not_in_reference(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    reference_values = _reference_values(data, params)
    values = pd.to_numeric(table[column], errors="coerce")
    present = values.notna()
    cnotes = _normalized_strings(_cnote_values(table, params))
    failed = present & (values.lt(0) | cnotes.isin(reference_values))
    failures = _as_failure_frame(
        _cnote_values(table.loc[failed], params),
        table[column].loc[failed],
        f"{column} is negative or the record appears in {params['reference_table']}",
    )
    checks = _as_check_frame(_cnote_values(table.loc[present], params), failed.loc[present], table.loc[present, column], params["reference_table"])
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: CONS4.
def check_count_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    master = data[params["master_table"]]
    child = data[params["child_table"]]
    counts = (
        child.groupby(params["child_key"])[params["count_column"]]
        .nunique()
        .rename("child_count")
    )
    merged = master.merge(counts, left_on=params["master_key"], right_index=True, how="inner")
    master_value = pd.to_numeric(merged[params["master_column"]], errors="coerce")
    comparable = master_value.notna()
    failed = comparable & master_value.ne(merged["child_count"])
    failed_value = master_value.loc[failed].astype(str) + " != " + merged.loc[failed, "child_count"].astype(str)
    failures = _as_failure_frame(merged.loc[failed, params["cnote_column"]], failed_value, "master count does not match child count")
    checks = _as_check_frame(merged.loc[comparable, params["cnote_column"]], failed.loc[comparable], master_value.loc[comparable], merged.loc[comparable, "child_count"])
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE special completeness check: the current catalog names this COMP4J1.
# The uploaded "What to fix" sheet has the related row as COMP3J1.
def check_transit_manifest_required_for_origin_mismatch(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = (
        data["CMS_DSMU"]
        .merge(data["CMS_MSMU"], left_on="DSMU_NO", right_on="MSMU_NO")
        .merge(data["CMS_MFBAG"], left_on="DSMU_BAG_NO", right_on="MFBAG_NO")
    )
    dsmu_origin = _string_values(merged["DSMU_BAG_ORIGIN"])
    msmu_origin = _string_values(merged["MSMU_ORIGIN"])
    manifest_no = _string_values(merged["MFBAG_MAN_NO"])
    comparable = dsmu_origin.ne("") & msmu_origin.ne("")
    origin_mismatch = comparable & dsmu_origin.str[:3].ne(msmu_origin.str[:3])
    failed = origin_mismatch & ~manifest_no.str.contains("TM", case=False, regex=False, na=False)
    failed_value = (
        dsmu_origin.loc[failed]
        + " != "
        + msmu_origin.loc[failed]
        + "; MFBAG_MAN_NO="
        + manifest_no.loc[failed]
    )
    failures = _as_failure_frame(
        merged.loc[failed, "MFBAG_NO"],
        failed_value,
        "TM manifest is required when DSMU and MSMU origin prefixes differ",
    )
    checks = _as_check_frame(
        merged.loc[origin_mismatch, "MFBAG_NO"],
        failed.loc[origin_mismatch],
        dsmu_origin.loc[origin_mismatch] + " / " + msmu_origin.loc[origin_mismatch],
        manifest_no.loc[origin_mismatch],
    )
    return RuleOutcome(int(origin_mismatch.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: TIME1.
def check_timeliness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    start = data[params["start_table"]]
    end = data[params["end_table"]]
    start_key = params.get("start_join_key", params.get("join_key"))
    end_key = params.get("end_join_key", params.get("join_key"))
    if start_key is None or end_key is None:
        raise ValueError("timeliness rule requires start_join_key and end_join_key")
    merged = start.merge(end, left_on=start_key, right_on=end_key, suffixes=("_start", "_end"))
    start_time = pd.to_datetime(merged[params["start_column"]], errors="coerce")
    end_time = pd.to_datetime(merged[params["end_column"]], errors="coerce")
    comparable = start_time.notna() & end_time.notna()
    failed = comparable & start_time.gt(end_time)
    # Null dates are skipped here so missingness stays a completeness concern.
    failed_value = start_time.loc[failed].astype(str) + " > " + end_time.loc[failed].astype(str)
    failures = _as_failure_frame(_merged_cnote_values(merged.loc[failed], params), failed_value, "start timestamp is after end timestamp")
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], start_time.loc[comparable].astype(str), end_time.loc[comparable].astype(str))
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: TIME1.
def check_bridged_timeliness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_bridge(data, params)
    start_time = pd.to_datetime(merged[params["start_column"]], errors="coerce")
    end_time = pd.to_datetime(merged[params["end_column"]], errors="coerce")
    if params.get("aggregate_by_cnote") == "earliest":
        cnote_no = _merged_cnote_values(merged, params).astype("string")
        work = pd.DataFrame({
            "cnote_no": cnote_no,
            "_start_time": start_time,
            "_end_time": end_time,
        })
        work = work.loc[work["cnote_no"].notna() & work["cnote_no"].str.strip().ne("")]
        grouped = work.groupby("cnote_no", dropna=True).agg(
            _start_time=("_start_time", "min"),
            _end_time=("_end_time", "min"),
        )
        start_time = grouped["_start_time"]
        end_time = grouped["_end_time"]
        merged = pd.DataFrame({"cnote_no": grouped.index}, index=grouped.index)
    if params.get("first_start_group"):
        merged = merged.assign(_start_time=start_time, _end_time=end_time)
        merged = (
            merged.sort_values("_start_time")
            .drop_duplicates(subset=[params["first_start_group"]], keep="first")
        )
        start_time = merged["_start_time"]
        end_time = merged["_end_time"]
    comparable = start_time.notna() & end_time.notna()
    failed = comparable & start_time.gt(end_time)
    failed_value = start_time.loc[failed].astype(str) + " > " + end_time.loc[failed].astype(str)
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        "start timestamp is after bridged end timestamp",
    )
    checks = _as_check_frame(_merged_cnote_values(merged.loc[comparable], params), failed.loc[comparable], start_time.loc[comparable].astype(str), end_time.loc[comparable].astype(str))
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


def _cnotes_for_failed_groups(groups: list[str]) -> pd.Series:
    return pd.Series(groups, dtype="string")


# JNE rule indexes: TIME1.
def check_manifest_code_sequence(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = data["CMS_MFCNOTE"].merge(
        data["CMS_MANIFEST"],
        left_on="MFCNOTE_MAN_NO",
        right_on="MANIFEST_NO",
        suffixes=("_mfcnote", "_manifest"),
    )
    cnote_column = params.get("cnote_column", "MFCNOTE_NO")
    code = _normalized_strings(merged[params.get("manifest_code_column", "MANIFEST_CODE")])
    event_time = pd.to_datetime(merged[params.get("date_column", "MANIFEST_CRDATE")], errors="coerce")
    mode = params["mode"]
    checked = 0
    failed_cnotes: list[str] = []
    failed_values: list[str] = []
    check_rows: list[dict[str, str]] = []

    frame = merged.assign(_manifest_code=code, _event_time=event_time)
    for cnote_no, group in frame.groupby(cnote_column, dropna=True):
        om_times = group.loc[group["_manifest_code"].eq("1"), "_event_time"].dropna().sort_values()
        tm_times = group.loc[group["_manifest_code"].eq("2"), "_event_time"].dropna().sort_values()
        im_times = group.loc[group["_manifest_code"].eq("3"), "_event_time"].dropna().sort_values()

        if mode == "om_before_tm":
            if om_times.empty or tm_times.empty:
                continue
            checked += 1
            failed_check = om_times.max() > tm_times.min()
            check_rows.append({
                "cnote_no": str(cnote_no),
                "status": "FAIL" if failed_check else "PASS",
                "variable_1": str(om_times.max()),
                "variable_2": str(tm_times.min()),
            })
            if failed_check:
                failed_cnotes.append(str(cnote_no))
                failed_values.append(f"OM {om_times.max()} > TM {tm_times.min()}")
        elif mode == "tm_sequence_before_im":
            if len(tm_times) > 1 or (not tm_times.empty and not im_times.empty):
                checked += 1
            duplicate_tm = len(tm_times) > 1 and tm_times.duplicated().any()
            tm_after_im = not tm_times.empty and not im_times.empty and tm_times.max() > im_times.min()
            if len(tm_times) > 1 or (not tm_times.empty and not im_times.empty):
                check_rows.append({
                    "cnote_no": str(cnote_no),
                    "status": "FAIL" if duplicate_tm or tm_after_im else "PASS",
                    "variable_1": str(tm_times.max()) if not tm_times.empty else "",
                    "variable_2": str(im_times.min()) if not im_times.empty else "",
                })
            if duplicate_tm or tm_after_im:
                failed_cnotes.append(str(cnote_no))
                if duplicate_tm:
                    failed_values.append("TM timestamps are duplicated")
                else:
                    failed_values.append(f"TM {tm_times.max()} > IM {im_times.min()}")
        elif mode == "im_after_tm":
            if tm_times.empty or im_times.empty:
                continue
            checked += 1
            failed_check = im_times.min() < tm_times.max()
            check_rows.append({
                "cnote_no": str(cnote_no),
                "status": "FAIL" if failed_check else "PASS",
                "variable_1": str(tm_times.max()),
                "variable_2": str(im_times.min()),
            })
            if failed_check:
                failed_cnotes.append(str(cnote_no))
                failed_values.append(f"IM {im_times.min()} < TM {tm_times.max()}")
        else:
            raise ValueError(f"Unsupported manifest sequence mode: {mode}")

    failures = _as_failure_frame(
        _cnotes_for_failed_groups(failed_cnotes),
        pd.Series(failed_values, dtype="string"),
        "manifest code sequence is out of order",
    )
    checks = pd.DataFrame(check_rows, columns=CHECK_COLUMNS)
    return RuleOutcome(checked, len(failed_cnotes), failures, checks)


# JNE rule indexes: TIME1.
def check_cnote_im_manifest_before_msj(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    manifest_path = data["CMS_MFCNOTE"].merge(
        data["CMS_MANIFEST"],
        left_on="MFCNOTE_MAN_NO",
        right_on="MANIFEST_NO",
    )
    manifest_code = _normalized_strings(manifest_path[params.get("manifest_code_column", "MANIFEST_CODE")])
    im_path = manifest_path.loc[manifest_code.eq(str(params.get("manifest_code", "3")))].copy()
    im_path["_im_time"] = pd.to_datetime(im_path[params.get("manifest_date_column", "MANIFEST_DATE")], errors="coerce")
    im_by_cnote = im_path.groupby("MFCNOTE_NO")["_im_time"].min()

    msj_path = (
        data["CMS_DHICNOTE"]
        .merge(data["CMS_RDSJ"], left_on="DHICNOTE_NO", right_on="RDSJ_HVI_NO")
        .merge(data["CMS_DSJ"], left_on="RDSJ_HVO_NO", right_on="DSJ_HVO_NO")
        .merge(data["CMS_MSJ"], left_on="DSJ_NO", right_on="MSJ_NO")
    )
    msj_path["_msj_time"] = pd.to_datetime(msj_path[params.get("msj_date_column", "MSJ_SIGNDATE")], errors="coerce")
    msj_by_cnote = msj_path.groupby("DHICNOTE_CNOTE_NO")["_msj_time"].min()

    comparison = pd.concat([im_by_cnote.rename("im_time"), msj_by_cnote.rename("msj_time")], axis=1, join="inner")
    comparable = comparison["im_time"].notna() & comparison["msj_time"].notna()
    failed = comparable & comparison["im_time"].gt(comparison["msj_time"])
    failed_rows = comparison.loc[failed]
    failed_value = failed_rows["im_time"].astype(str) + " > " + failed_rows["msj_time"].astype(str)
    failures = _as_failure_frame(
        pd.Series(failed_rows.index.astype(str), index=failed_rows.index),
        failed_value,
        "CNOTE inbound manifest time is after MSJ sign date",
    )
    comparable_rows = comparison.loc[comparable]
    checks = _as_check_frame(
        pd.Series(comparable_rows.index.astype(str), index=comparable_rows.index),
        failed.loc[comparable],
        comparable_rows["im_time"].astype(str),
        comparable_rows["msj_time"].astype(str),
    )
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures, checks)


# JNE rule indexes: INTG1.
def check_integrity_orphan(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    child = data[params["child_table"]]
    parent = data[params["parent_table"]]
    child_values = child[params["child_column"]]
    parent_values = set(parent[params["parent_column"]].dropna().astype(str))
    present = child_values.notna() & child_values.astype("string").str.strip().ne("")
    failed = present & ~child_values.astype(str).isin(parent_values)
    failures = _as_failure_frame(_cnote_values(child.loc[failed], params), child_values.loc[failed], "parent key is missing")
    checks = _as_check_frame(_cnote_values(child.loc[present], params), failed.loc[present], child_values.loc[present], params["parent_table"])
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures, checks)


def _rule_index(index_code: str) -> str:
    """Return the JNE rule group from a full index code, e.g. VALD2Y15 -> VALD2."""
    match = re.match(r"([A-Z]+\d+)", index_code)
    return match.group(1) if match else index_code


def _run_catalog_rule_family(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    return RULE_FUNCTIONS[params["_rule_family"]](data, params)


# ---------------------------------------------------------------------------
# JNE-facing rule blocks
#
# These functions are intentionally organized by the rule indexes used in JNE's
# workbook. Most of them call a shared implementation above because many index
# groups use the same pandas operation with different catalog parameters.
# ---------------------------------------------------------------------------


def check_COMP1(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """COMP1: required-field completeness; the configured column must be filled."""
    return check_completeness(data, params)


def check_COMP2(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """COMP2: conditional completeness; a field is required only when a condition applies."""
    if params.get("_rule_family"):
        return _run_catalog_rule_family(data, params)
    if params.get("reference_table"):
        return check_reference_conditional_completeness(data, params)
    if params.get("condition_column"):
        return check_conditional_completeness(data, params)
    return check_completeness(data, params)


def check_COMP3(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """COMP3: special conditional completeness row currently modeled as condition-based completeness."""
    return check_conditional_completeness(data, params)


def check_COMP4(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """COMP4: special transit-manifest completeness check in the current catalog."""
    return check_transit_manifest_required_for_origin_mismatch(data, params)


def check_VALD1(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD1: reference/format validity; configured values must be shaped correctly and known."""
    if params.get("_rule_family"):
        return _run_catalog_rule_family(data, params)
    return check_reference_format(data, params)


def check_VALD2(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD2: integer validity; configured values must be whole numbers."""
    return check_validity_integer(data, params)


def check_VALD3(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD3: integer validity; configured values must be whole numbers."""
    return check_validity_integer(data, params)


def check_VALD4(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD4: datetime validity; configured values must parse as timestamps."""
    return check_validity_datetime(data, params)


def check_VALD5(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD5: regex validity; configured values must match the catalog pattern."""
    return check_validity_regex(data, params)


def check_VALD7(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD7: regex validity; configured values must match the catalog pattern."""
    return check_validity_regex(data, params)


def check_VALD8(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD8: regex validity; configured values must match the catalog pattern."""
    return check_validity_regex(data, params)


def check_VALD9(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD9: regex validity; configured values must match the catalog pattern."""
    return check_validity_regex(data, params)


def check_VALD10(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD10: allowed-set validity; configured values must be one of the allowed values."""
    return check_validity_in_set(data, params)


def check_VALD11(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD11: regex validity; configured values must match the catalog pattern."""
    return check_validity_regex(data, params)


def check_VALD12(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD12: reference/format validity; configured values must be known to the reference."""
    return check_reference_format(data, params)


def check_VALD13(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """VALD13: reference/format validity; configured values must be known to the reference."""
    return check_reference_format(data, params)


def check_UNIQ1(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """UNIQ1: uniqueness; configured key columns must not duplicate."""
    return check_uniqueness(data, params)


def check_UNIQ2(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """UNIQ2: uniqueness; configured key columns must not duplicate."""
    return check_uniqueness(data, params)


def check_CONS1(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """CONS1: pair consistency; values from two related sources must match."""
    return check_pair_consistency(data, params)


def check_CONS2(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """CONS2: pair consistency with optional rounding or document bridge logic."""
    return _run_catalog_rule_family(data, params)


def check_CONS3(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """CONS3: aggregate or duplicate-aware consistency, depending on the catalog row."""
    return _run_catalog_rule_family(data, params)


def check_CONS4(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """CONS4: count consistency; master counts must match related detail counts."""
    return _run_catalog_rule_family(data, params)


def check_TIME1(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """TIME1: timeliness/sequence; start events must not happen after end events."""
    return _run_catalog_rule_family(data, params)


def check_INTG1(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """INTG1: integrity; child keys must exist in the parent table."""
    return check_integrity_orphan(data, params)


def check_ACCU1(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """ACCU1: accuracy; configured numeric values must not be negative."""
    return check_non_negative(data, params)


def check_ACCU2(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """ACCU2: accuracy; configured prefixes must match across related rows."""
    return check_prefix_match(data, params)


def check_ACCU3(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """ACCU3: accuracy; configured suffixes must match after the shared prefix."""
    return check_suffix_after_prefix_match(data, params)


def check_ACCU4(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """ACCU4: accuracy; values must be non-negative and excluded from a reference set."""
    return check_non_negative_not_in_reference(data, params)


def check_ACCU5(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """ACCU5: accuracy; configured values must exist in the reference table."""
    return check_value_in_reference(data, params)


def check_ACCU6(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    """ACCU6: accuracy; configured values must exist in the reference table."""
    return check_value_in_reference(data, params)


RULE_FUNCTIONS = {
    "completeness": check_completeness,
    "conditional_completeness": check_conditional_completeness,
    "reference_conditional_completeness": check_reference_conditional_completeness,
    "validity_regex": check_validity_regex,
    "validity_datetime": check_validity_datetime,
    "validity_integer": check_validity_integer,
    "validity_in_set": check_validity_in_set,
    "value_in_reference": check_value_in_reference,
    "reference_format": check_reference_format,
    "non_negative": check_non_negative,
    "non_negative_not_in_reference": check_non_negative_not_in_reference,
    "count_consistency": check_count_consistency,
    "transit_manifest_required_for_origin_mismatch": check_transit_manifest_required_for_origin_mismatch,
    "uniqueness": check_uniqueness,
    "pair_consistency": check_pair_consistency,
    "prefix_match": check_prefix_match,
    "suffix_after_prefix_match": check_suffix_after_prefix_match,
    "rounded_pair_consistency": check_rounded_pair_consistency,
    "duplicate_aware_weight_consistency": check_duplicate_aware_weight_consistency,
    "bridged_pair_consistency": check_bridged_pair_consistency,
    "bridged_substring_match": check_bridged_substring_match,
    "aggregate_sum_consistency": check_aggregate_sum_consistency,
    "aggregate_count_consistency": check_aggregate_count_consistency,
    "timeliness": check_timeliness,
    "bridged_timeliness": check_bridged_timeliness,
    "manifest_code_sequence": check_manifest_code_sequence,
    "cnote_im_manifest_before_msj": check_cnote_im_manifest_before_msj,
    "integrity_orphan": check_integrity_orphan,
}


JNE_RULE_FUNCTIONS: dict[str, RuleFunction] = {
    "COMP1": check_COMP1,
    "COMP2": check_COMP2,
    "COMP3": check_COMP3,
    "COMP4": check_COMP4,
    "VALD1": check_VALD1,
    "VALD2": check_VALD2,
    "VALD3": check_VALD3,
    "VALD4": check_VALD4,
    "VALD5": check_VALD5,
    "VALD7": check_VALD7,
    "VALD8": check_VALD8,
    "VALD9": check_VALD9,
    "VALD10": check_VALD10,
    "VALD11": check_VALD11,
    "VALD12": check_VALD12,
    "VALD13": check_VALD13,
    "UNIQ1": check_UNIQ1,
    "UNIQ2": check_UNIQ2,
    "CONS1": check_CONS1,
    "CONS2": check_CONS2,
    "CONS3": check_CONS3,
    "CONS4": check_CONS4,
    "TIME1": check_TIME1,
    "INTG1": check_INTG1,
    "ACCU1": check_ACCU1,
    "ACCU2": check_ACCU2,
    "ACCU3": check_ACCU3,
    "ACCU4": check_ACCU4,
    "ACCU5": check_ACCU5,
    "ACCU6": check_ACCU6,
}


def rule_function_for_entry(entry: dict) -> RuleFunction:
    """Return the beginner-facing JNE rule block for a catalog entry."""
    index_function = JNE_RULE_FUNCTIONS.get(_rule_index(str(entry.get("index_code", ""))))
    if index_function is not None:
        return index_function
    return RULE_FUNCTIONS[entry["rule_family"]]
