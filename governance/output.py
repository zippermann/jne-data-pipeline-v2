"""CSV writers for the simple governance checker.

The demo writes a compact scorecard and long-format failure file.
Only failed rows are written: PASS storage is intentionally left out.
Production governance writes Parquet for larger downstream loads.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_scorecard(results: list[dict], path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for result in results:
        total_checked = result["total_checked"]
        total_failed = result["total_failed"]
        fail_rate = 0 if total_checked == 0 else total_failed / total_checked
        rows.append({
            "index_code": result["index_code"],
            "element": result["element"],
            "rule_family": result["rule_family"],
            "table": result["table"],
            "total_checked": total_checked,
            "total_failed": total_failed,
            "fail_rate": f"{fail_rate:.4f}",
            "run_at": result["run_at"],
        })
    pd.DataFrame(rows).to_csv(output_path, index=False)
    return output_path


def write_failures(failures: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["run_at", "index_code", "element", "cnote_no", "failed_value", "failure_reason"]
    if failures.empty:
        failures = pd.DataFrame(columns=columns)
    failures.loc[:, columns].to_csv(output_path, index=False)
    return output_path

