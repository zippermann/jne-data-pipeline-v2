"""CSV writer for the long CNOTE-level governance result."""

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


def write_governance_results(results: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if results.empty:
        results = pd.DataFrame(columns=RESULT_COLUMNS)
    results.loc[:, RESULT_COLUMNS].to_csv(output_path, index=False)
    return output_path
