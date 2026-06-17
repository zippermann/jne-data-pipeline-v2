"""Writers for governance outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


RESULT_COLUMNS = [
    "cnote_no",
    "index_code",
    "main_indicator",
    "column_name",
    "table_name",
    "status",
    "variable_1",
    "variable_2",
    "impact_billing",
    "impact_operational",
]

RULE_SUMMARY_COLUMNS = [
    "index_code",
    "element",
    "main_indicator",
    "rule_family",
    "table_name",
    "status",
    "total_checked",
    "total_failed",
    "result_rows",
    "skip_reason",
    "error_message",
    "impact_billing",
    "impact_operational",
]


def write_governance_results(results: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if results.empty:
        results = pd.DataFrame(columns=RESULT_COLUMNS)
    results.loc[:, RESULT_COLUMNS].to_csv(output_path, index=False)
    return output_path


def write_governance_results_parquet(results: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if results.empty:
        results = pd.DataFrame(columns=RESULT_COLUMNS)
    results.loc[:, RESULT_COLUMNS].to_parquet(output_path, index=False)
    return output_path


def write_rule_summary(summary: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if summary.empty:
        summary = pd.DataFrame(columns=RULE_SUMMARY_COLUMNS)
    summary.loc[:, RULE_SUMMARY_COLUMNS].to_csv(output_path, index=False)
    return output_path


def write_rule_summary_parquet(summary: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if summary.empty:
        summary = pd.DataFrame(columns=RULE_SUMMARY_COLUMNS)
    summary.loc[:, RULE_SUMMARY_COLUMNS].to_parquet(output_path, index=False)
    return output_path
