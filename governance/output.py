"""Writers for governance outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


RESULT_COLUMNS = [
    "shipment_type",
    "result_id",
    "document_id",
    "cnote_no",
    "document_no",
    "cnote_origin",
    "cnote_destination",
    "origin_region",
    "destination_region",
    "cnote_service_code",
    "index_code",
    "element",
    "logic_description",
    "table_name",
    "level",
    "stage",
    "variable_1",
    "variable_2",
    "column_name",
    "main_indicator",
    "main_impact",
    "impact_details",
    "issue_description",
    "status",
]

RESULT_CNOTE_COLUMNS = [
    "result_id",
    "cnote_no",
    "link_method",
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
]


def _result_columns(results: pd.DataFrame) -> pd.DataFrame:
    return _select_columns(results, RESULT_COLUMNS)


class GovernanceResultWriter:
    def __init__(
        self,
        csv_path: str | Path,
        parquet_path: str | Path,
        columns: list[str] | None = None,
    ) -> None:
        self.csv_path = Path(csv_path)
        self.parquet_path = Path(parquet_path)
        self.columns = columns or RESULT_COLUMNS
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_path.parent.mkdir(parents=True, exist_ok=True)
        self.csv_path.unlink(missing_ok=True)
        self.parquet_path.unlink(missing_ok=True)
        self._csv_started = False
        self._parquet_writer: Any | None = None
        self._rows_written = 0

    def __enter__(self) -> "GovernanceResultWriter":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def write(self, results: pd.DataFrame) -> int:
        if results.empty:
            return 0
        rows = _select_columns(results, self.columns)
        rows.to_csv(self.csv_path, mode="a", index=False, header=not self._csv_started)
        self._csv_started = True

        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(rows, preserve_index=False)
        if self._parquet_writer is None:
            self._parquet_writer = pq.ParquetWriter(self.parquet_path, table.schema, compression="snappy")
        self._parquet_writer.write_table(table)
        self._rows_written += len(rows)
        return len(rows)

    def close(self) -> None:
        if self._parquet_writer is not None:
            self._parquet_writer.close()
            self._parquet_writer = None
        if self._rows_written:
            return

        empty = pd.DataFrame(columns=self.columns)
        empty.to_csv(self.csv_path, index=False)

        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema([pa.field(column, pa.string()) for column in self.columns])
        arrays = [pa.array([], type=pa.string()) for _ in self.columns]
        pq.write_table(pa.Table.from_arrays(arrays, schema=schema), self.parquet_path, compression="snappy")


def _select_columns(results: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame(columns=columns)
    results = results.copy()
    for column in columns:
        if column not in results.columns:
            results[column] = ""
    return results.loc[:, columns].astype("string").fillna("")


def write_governance_results(results: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _result_columns(results).to_csv(output_path, index=False)
    return output_path


def write_governance_results_parquet(results: pd.DataFrame, path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _result_columns(results).to_parquet(output_path, index=False)
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
