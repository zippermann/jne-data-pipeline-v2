from pathlib import Path

import pandas as pd
import pytest

from export_governance_results_parquet import (
    GOVERNANCE_COLUMNS,
    export_governance_results,
    resolve_input_path,
)


def _write_results_csv(path: Path) -> None:
    pd.DataFrame(
        [{
            "cnote_no": "CNOTE001",
            "index_code": "COMP1",
            "main_indicator": "Completeness",
            "column_name": "CNOTE_NO",
            "table_name": "CMS_CNOTE",
            "status": "PASS",
            "variable_1": "",
            "variable_2": "",
            "impact_billing": "Y",
            "impact_operational": "N",
        }]
    ).loc[:, GOVERNANCE_COLUMNS].to_csv(path, index=False)


def test_export_accepts_governance_output_directory(tmp_path):
    pytest.importorskip("pyarrow")
    results_path = tmp_path / "governance_results.csv"
    _write_results_csv(results_path)

    output_path = export_governance_results(tmp_path)

    assert output_path == tmp_path / "governance_results.parquet"
    exported = pd.read_parquet(output_path)
    assert exported.loc[0, "cnote_no"] == "CNOTE001"
    assert list(exported.columns) == GOVERNANCE_COLUMNS


def test_export_refuses_to_overwrite_existing_file(tmp_path):
    pytest.importorskip("pyarrow")
    results_path = tmp_path / "governance_results.csv"
    output_path = tmp_path / "governance_results.parquet"
    _write_results_csv(results_path)
    output_path.write_text("already here")

    with pytest.raises(FileExistsError):
        export_governance_results(results_path)


def test_resolve_input_path_reports_missing_governance_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        resolve_input_path(tmp_path)
