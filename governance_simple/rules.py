"""Pandas rule functions for the simple governance checker.

Each function handles one rule family and returns the same RuleOutcome shape.
The code favors readability over speed so junior engineers can trace the logic.
Production governance uses DuckDB and batched table scans instead.
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


def check_completeness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    table = data[params["table"]]
    column = params["column"]
    values = table[column]
    failed = values.isna() | values.astype("string").str.strip().eq("")
    failures = _as_failure_frame(_cnote_values(table.loc[failed], params), values.loc[failed], f"{column} is null or empty")
    return RuleOutcome(len(table), int(failed.sum()), failures)


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
    failed_value = failed_rows[columns].astype(str).agg("|".join, axis=1)
    failures = _as_failure_frame(_cnote_values(failed_rows, params), failed_value, f"{', '.join(columns)} is duplicated")
    return RuleOutcome(int(present.sum()), len(failed_rows), failures)


def check_pair_consistency(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    left = data[params["left_table"]]
    right = data[params["right_table"]]
    left_key = params.get("left_join_key", params["join_key"])
    right_key = params.get("right_join_key", params["join_key"])
    merged = left.merge(right, left_on=left_key, right_on=right_key, suffixes=("_left", "_right"))
    left_value = merged[params["left_column"]]
    right_value = merged[params["right_column"]]
    comparable = left_value.notna() & right_value.notna()
    failed = comparable & left_value.ne(right_value)
    # Null comparisons are skipped because some child rows are optional by business path.
    failed_value = left_value.loc[failed].astype(str) + " != " + right_value.loc[failed].astype(str)
    failures = _as_failure_frame(merged.loc[failed, params["cnote_column"]], failed_value, "paired values do not match")
    return RuleOutcome(int(comparable.sum()), int(failed.sum()), failures)


def check_timeliness(data: dict[str, pd.DataFrame], params: dict) -> RuleOutcome:
    start = data[params["start_table"]]
    end = data[params["end_table"]]
    start_key = params.get("start_join_key", params["join_key"])
    end_key = params.get("end_join_key", params["join_key"])
    merged = start.merge(end, left_on=start_key, right_on=end_key, suffixes=("_start", "_end"))
    start_time = pd.to_datetime(merged[params["start_column"]], errors="coerce")
    end_time = pd.to_datetime(merged[params["end_column"]], errors="coerce")
    comparable = start_time.notna() & end_time.notna()
    failed = comparable & start_time.gt(end_time)
    # Null dates are skipped here so missingness stays a completeness concern.
    failed_value = start_time.loc[failed].astype(str) + " > " + end_time.loc[failed].astype(str)
    failures = _as_failure_frame(merged.loc[failed, params["cnote_column"]], failed_value, "start timestamp is after end timestamp")
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
    "validity_regex": check_validity_regex,
    "uniqueness": check_uniqueness,
    "pair_consistency": check_pair_consistency,
    "timeliness": check_timeliness,
    "integrity_orphan": check_integrity_orphan,
}

