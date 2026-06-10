"""Runnable demo for the simple governance checker.

The runner loads the dummy catalog, builds synthetic JNE-like tables, dispatches
to one pandas function per rule family, and writes CSV outputs. It is designed
for walkthroughs, not Airflow or production MinIO execution.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import argparse
from pathlib import Path
from random import Random

import pandas as pd

from governance.catalog import CATALOG
from governance.output import write_failures, write_scorecard
from governance.rules import FAILURE_COLUMNS, RULE_FUNCTIONS, RuleOutcome


RUN_WINDOW_START = "2026-06-01"
RUN_WINDOW_END = "2026-06-08"
DEFAULT_OUTPUT_DIR = Path("governance/outputs")


def _base_cnotes() -> list[str]:
    return [f"CNOTE{number:04d}" for number in range(1, 51)]


def load_tables(table_names: set[str]) -> dict[str, pd.DataFrame]:
    """Load required tables as DataFrames.

    TODO: replace the synthetic fixtures with a small bronze Parquet reader
    when the simple checker is ready to run against real extraction outputs.
    """
    random = Random(20260610)
    cnotes = _base_cnotes()
    start = datetime.fromisoformat(RUN_WINDOW_START)

    data = {
        "CMS_CNOTE": pd.DataFrame({
            "CNOTE_NO": cnotes,
            "CNOTE_SERVICES_CODE": ["REG"] * 50,
            "CNOTE_ORIGIN": ["CGK"] * 50,
            "CNOTE_WEIGHT": [float(random.randint(1, 20)) for _ in cnotes],
            "CNOTE_CRDATE": [start + timedelta(hours=index) for index in range(50)],
        }),
        "CMS_DRCNOTE": pd.DataFrame({
            "DRCNOTE_NO": [f"DR{number:04d}" for number in range(1, 51)],
            "DRCNOTE_CNOTE_NO": cnotes,
        }),
        "CMS_APICUST": pd.DataFrame({
            "APICUST_CNOTE_NO": cnotes,
            "APICUST_WEIGHT": [0.0] * 50,
            "APICUST_ORIGIN": ["CGK"] * 50,
        }),
        "CMS_MHI_HOC": pd.DataFrame({
            "MHI_CNOTE_NO": cnotes,
            "MHI_APPROVE_DATE": [start + timedelta(hours=index + 1) for index in range(50)],
        }),
        "CMS_DRSHEET": pd.DataFrame({
            "DRSHEET_NO": [f"RS{number:04d}" for number in range(1, 51)],
            "DRSHEET_CNOTE_NO": cnotes,
            "DRSHEET_DATE": [start + timedelta(days=1, hours=index) for index in range(50)],
        }),
        "CMS_CNOTE_POD": pd.DataFrame({
            "CNOTE_POD_NO": cnotes,
            "CNOTE_POD_DATE": [start + timedelta(days=2, hours=index) for index in range(50)],
        }),
        "CMS_MFCNOTE": pd.DataFrame({
            "MFCNOTE_NO": cnotes,
            "MFCNOTE_MAN_NO": [f"MAN{number:04d}" for number in range(1, 51)],
        }),
        "CMS_MANIFEST": pd.DataFrame({
            "MANIFEST_NO": [f"MAN{number:04d}" for number in range(1, 51)],
        }),
    }

    data["CMS_APICUST"]["APICUST_WEIGHT"] = data["CMS_CNOTE"]["CNOTE_WEIGHT"].copy()

    data["CMS_CNOTE"].loc[1, "CNOTE_NO"] = None
    data["CMS_DRCNOTE"].loc[2, "DRCNOTE_CNOTE_NO"] = ""
    data["CMS_CNOTE"].loc[3, "CNOTE_SERVICES_CODE"] = "bad service"
    data["CMS_CNOTE"].loc[4, "CNOTE_ORIGIN"] = "CGK1"
    data["CMS_CNOTE"].loc[6, "CNOTE_NO"] = data["CMS_CNOTE"].loc[5, "CNOTE_NO"]
    data["CMS_DRCNOTE"].loc[8, "DRCNOTE_NO"] = data["CMS_DRCNOTE"].loc[7, "DRCNOTE_NO"]
    data["CMS_APICUST"].loc[10, "APICUST_WEIGHT"] = data["CMS_CNOTE"].loc[10, "CNOTE_WEIGHT"] + 5
    data["CMS_APICUST"].loc[12, "APICUST_ORIGIN"] = "SUB"
    data["CMS_MHI_HOC"].loc[14, "MHI_APPROVE_DATE"] = data["CMS_CNOTE"].loc[14, "CNOTE_CRDATE"] - timedelta(hours=2)
    data["CMS_CNOTE_POD"].loc[16, "CNOTE_POD_DATE"] = data["CMS_DRSHEET"].loc[16, "DRSHEET_DATE"] - timedelta(hours=3)
    data["CMS_DRCNOTE"].loc[18, "DRCNOTE_CNOTE_NO"] = "CNOTE9999"
    data["CMS_MFCNOTE"].loc[20, "MFCNOTE_MAN_NO"] = "MAN9999"
    data["CMS_APICUST"].loc[22, "APICUST_WEIGHT"] = None

    return {table: frame for table, frame in data.items() if table in table_names}


def _required_tables() -> set[str]:
    tables: set[str] = set()
    for entry in CATALOG:
        params = entry["params"]
        tables.add(entry["table"])
        for key in ("left_table", "right_table", "start_table", "end_table", "child_table", "parent_table"):
            if key in params:
                tables.add(params[key])
    return tables


def _error_outcome(message: str) -> RuleOutcome:
    failures = pd.DataFrame([[None, message, "rule raised an exception"]], columns=FAILURE_COLUMNS)
    return RuleOutcome(total_checked=0, total_failed=0, failures=failures)


def run(output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> None:
    run_at = datetime.now(timezone.utc).isoformat()
    output_dir = Path(output_dir)
    data = load_tables(_required_tables())
    results: list[dict] = []
    all_failures: list[pd.DataFrame] = []

    for entry in CATALOG:
        params = dict(entry["params"])
        params.setdefault("table", entry["table"])
        try:
            outcome = RULE_FUNCTIONS[entry["rule_family"]](data, params)
        except Exception as exc:
            print(f"WARNING: {entry['index_code']} failed: {exc}")
            outcome = _error_outcome(str(exc))

        results.append({
            "index_code": entry["index_code"],
            "element": entry["element"],
            "rule_family": entry["rule_family"],
            "table": entry["table"],
            "total_checked": outcome.total_checked,
            "total_failed": outcome.total_failed,
            "run_at": run_at,
        })

        failures = outcome.failures.copy()
        if not failures.empty:
            failures["run_at"] = run_at
            failures["index_code"] = entry["index_code"]
            failures["element"] = entry["element"]
            all_failures.append(failures)

    failure_frame = pd.concat(all_failures, ignore_index=True) if all_failures else pd.DataFrame()
    scorecard_path = write_scorecard(results, output_dir / "scorecard.csv")
    failures_path = write_failures(failure_frame, output_dir / "failures.csv")

    print(f"Indexes run: {len(results)}")
    print(f"Total failures: {int(sum(result['total_failed'] for result in results))}")
    print(f"Outputs: {scorecard_path}, {failures_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the simple JNE governance checker.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    run(args.output_dir)


if __name__ == "__main__":
    main()
