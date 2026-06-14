"""Standalone CSV-to-Parquet exporter for governance results.

This utility is intentionally not wired into Airflow, mart loading, or the
governance runner. It is for copying an existing governance_results.csv into a
local Parquet file that can be shared or analyzed off the VM.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_RESULTS_FILE = "governance_results.csv"
GOVERNANCE_COLUMNS = [
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


def resolve_input_path(path: str | Path) -> Path:
    input_path = Path(path)
    if input_path.is_dir():
        input_path = input_path / DEFAULT_RESULTS_FILE
    if not input_path.exists():
        raise FileNotFoundError(f"Governance results file does not exist: {input_path}")
    if not input_path.is_file():
        raise ValueError(f"Governance results input is not a file: {input_path}")
    return input_path


def default_output_path(input_path: Path) -> Path:
    return input_path.with_suffix(".parquet")


def read_governance_results(input_path: Path) -> pd.DataFrame:
    results = pd.read_csv(input_path, dtype="string", keep_default_na=False)
    missing_columns = [column for column in GOVERNANCE_COLUMNS if column not in results.columns]
    if missing_columns:
        raise ValueError(
            "Governance results file is missing expected column(s): "
            + ", ".join(missing_columns)
        )
    return results.loc[:, GOVERNANCE_COLUMNS]


def export_governance_results(
    input_path: str | Path,
    output_path: str | Path | None = None,
    compression: str | None = "snappy",
    overwrite: bool = False,
) -> Path:
    source_path = resolve_input_path(input_path)
    destination_path = Path(output_path) if output_path else default_output_path(source_path)
    if destination_path.exists() and not overwrite:
        raise FileExistsError(
            f"Output already exists: {destination_path}. Pass --overwrite to replace it."
        )

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    results = read_governance_results(source_path)
    results.to_parquet(destination_path, index=False, compression=compression)
    return destination_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a local governance_results.csv file to Parquet."
    )
    parser.add_argument(
        "input",
        help=(
            "Path to governance_results.csv, or a directory containing "
            "governance_results.csv."
        ),
    )
    parser.add_argument(
        "--output",
        help="Destination Parquet path. Defaults to governance_results.parquet next to the input CSV.",
    )
    parser.add_argument(
        "--compression",
        choices=("snappy", "gzip", "brotli", "zstd", "none"),
        default="snappy",
        help="Parquet compression codec. Use 'none' for an uncompressed file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace the destination file if it already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    compression = None if args.compression == "none" else args.compression
    output_path = export_governance_results(
        input_path=args.input,
        output_path=args.output,
        compression=compression,
        overwrite=args.overwrite,
    )
    print(f"Wrote governance results Parquet to {output_path}")


if __name__ == "__main__":
    main()
