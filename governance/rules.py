"""Pandas rule functions for the simple governance checker.

Each function handles one rule family and returns the same RuleOutcome shape.
The code favors readability over speed so junior engineers can trace the logic.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class RuleOutcome:
    total_checked: int
    total_failed: int
    failures: pd.DataFrame


FAILURE_COLUMNS = ["cnote_no", "failed_value", "failure_reason"]


def _empty_failures() -> pd.DataFrame:
    return pd.DataFrame(columns=FAILURE_COLUMNS)


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


def _merged_cnote_values(merged: pd.DataFrame, params: dict) -> pd.Series:
    column = params.get("cnote_column", params.get("left_join_key", "CNOTE_NO"))
    for candidate in (column, f"{column}_left", f"{column}_right"):
        if candidate in merged.columns:
            return merged[candidate]
    return pd.Series([""] * len(merged), index=merged.index)


def check_completeness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    failed = values.isna() | values.astype("string").str.strip().eq("")
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is null or empty")
    return RuleOutcome(len(table), int(failed.sum()), failures)


def check_conditional_completeness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    condition_column = params["condition_column"]
    if params.get("condition_mode") == "not_null":
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
    return RuleOutcome(int(condition.sum()), int(failed.sum()), failures)


def check_validity_regex(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    present = values.notna() & values.astype("string").str.strip().ne("")
    failed = present & ~values.astype("string").str.match(params["pattern"])
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} does not match pattern")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


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
    return RuleOutcome(int(present.sum()), len(failed_rows), failures)


def check_pair_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    left_value = merged[params["left_column"]]
    right_value = merged[params["right_column"]]
    comparable = left_value.notna() & right_value.notna()
    failed = comparable & left_value.ne(right_value)
    # Null comparisons are skipped because some child rows are optional by business path.
    failed_value = left_value.loc[failed].astype(str) + " != " + right_value.loc[failed].astype(str)
    failures = _as_failure_frame(_merged_cnote_values(merged.loc[failed], params), failed_value, "paired values do not match")
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures)


def check_prefix_match(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    prefix_length = int(params.get("prefix_length", 3))
    left_value = _string_values(merged[params["left_column"]])
    right_value = _string_values(merged[params["right_column"]])
    comparable = left_value.ne("") & right_value.ne("")
    failed = comparable & left_value.str[:prefix_length].ne(right_value.str[:prefix_length])
    failed_value = left_value.loc[failed] + " != " + right_value.loc[failed]
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        f"first {prefix_length} characters do not match",
    )
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures)


def check_suffix_after_prefix_match(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    prefix_length = int(params.get("prefix_length", 3))
    left_value = _string_values(merged[params["left_column"]])
    right_value = _string_values(merged[params["right_column"]])
    comparable = left_value.ne("") & right_value.ne("")
    failed = comparable & left_value.str[prefix_length:].ne(right_value.str[prefix_length:])
    failed_value = left_value.loc[failed] + " != " + right_value.loc[failed]
    failures = _as_failure_frame(
        _merged_cnote_values(merged.loc[failed], params),
        failed_value,
        f"characters after first {prefix_length} do not match",
    )
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures)


def check_rounded_pair_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    merged = _merge_pair(data, params)
    decimals = int(params.get("decimals", 0))
    left_raw = pd.to_numeric(merged[params["left_column"]], errors="coerce")
    right_raw = pd.to_numeric(merged[params["right_column"]], errors="coerce")
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
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures)



def check_validity_datetime(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    present = _present(values)
    parsed = pd.to_datetime(values.where(present), errors="coerce")
    failed = present & parsed.isna()
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is not a valid timestamp")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


def check_validity_integer(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    present = _present(values)
    numeric = pd.to_numeric(values.where(present), errors="coerce")
    # A value passes when it parses as a number and carries no fractional part.
    failed = present & (numeric.isna() | numeric.ne(numeric.round(0)))
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is not a whole number")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


def _normalized_strings(values: pd.Series) -> pd.Series:
    # Strip a trailing .0 so numeric columns read from floats compare as plain digits.
    return _string_values(values).str.replace(r"\.0+$", "", regex=True)


def check_validity_in_set(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    allowed = {str(v) for v in params["allowed"]}
    values = table[column]
    present = _present(values)
    failed = present & ~_normalized_strings(values).isin(allowed)
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is not one of {sorted(allowed)}")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


def _reference_values(data: dict[str, pd.DataFrame], params: dict) -> set[str]:
    reference = data[params["reference_table"]]
    return set(_normalized_strings(reference[params["reference_column"]].dropna()))


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
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


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
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


def check_non_negative(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = pd.to_numeric(table[column], errors="coerce")
    present = values.notna()
    failed = present & values.lt(0)
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), table[column].loc[failed], f"{column} is negative")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


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
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


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
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures)


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
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures)


def check_integrity_orphan(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    child = data[params["child_table"]]
    parent = data[params["parent_table"]]
    child_values = child[params["child_column"]]
    parent_values = set(parent[params["parent_column"]].dropna().astype(str))
    present = child_values.notna() & child_values.astype("string").str.strip().ne("")
    failed = present & ~child_values.astype(str).isin(parent_values)
    failures = _as_failure_frame(_cnote_values(child.loc[failed], params), child_values.loc[failed], "parent key is missing")
    return RuleOutcome(int(present.sum()), int(failed.sum()), failures)


RULE_FUNCTIONS = {
    "completeness": check_completeness,
    "conditional_completeness": check_conditional_completeness,
    "validity_regex": check_validity_regex,
    "validity_datetime": check_validity_datetime,
    "validity_integer": check_validity_integer,
    "validity_in_set": check_validity_in_set,
    "value_in_reference": check_value_in_reference,
    "reference_format": check_reference_format,
    "non_negative": check_non_negative,
    "non_negative_not_in_reference": check_non_negative_not_in_reference,
    "count_consistency": check_count_consistency,
    "uniqueness": check_uniqueness,
    "pair_consistency": check_pair_consistency,
    "prefix_match": check_prefix_match,
    "suffix_after_prefix_match": check_suffix_after_prefix_match,
    "rounded_pair_consistency": check_rounded_pair_consistency,
    "timeliness": check_timeliness,
    "integrity_orphan": check_integrity_orphan,
}
