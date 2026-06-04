"""Print derived pipeline context values for Airflow tasks."""

from __future__ import annotations

import argparse

from src.bronze import MinioSettings, lake_prefix, load_config, resolve_window, sanitize_run_id


def bronze_prefix(config_path: str, run_id: str, extract_date: str) -> str:
    config = load_config(config_path)
    window = resolve_window(config)
    settings = MinioSettings.from_config(config)
    return lake_prefix(settings, window, sanitize_run_id(run_id), extract_date)


def governance_prefix(run_id: str) -> str:
    return f"governance/jne/run_id={sanitize_run_id(run_id)}"


def window_label(config_path: str, label: str) -> str:
    window = resolve_window(load_config(config_path))
    if label == "start":
        return window.start_label
    if label == "end":
        return window.end_label
    raise ValueError(f"Unsupported window label: {label}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Print derived JNE pipeline context values.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bronze_parser = subparsers.add_parser("bronze-prefix")
    bronze_parser.add_argument("--config", default="config/config.yaml")
    bronze_parser.add_argument("--run-id", required=True)
    bronze_parser.add_argument("--extract-date", required=True)

    governance_parser = subparsers.add_parser("governance-prefix")
    governance_parser.add_argument("--run-id", required=True)

    window_parser = subparsers.add_parser("window")
    window_parser.add_argument("--config", default="config/config.yaml")
    window_parser.add_argument("label", choices=["start", "end"])

    args = parser.parse_args()
    if args.command == "bronze-prefix":
        print(bronze_prefix(args.config, args.run_id, args.extract_date))
    elif args.command == "governance-prefix":
        print(governance_prefix(args.run_id))
    elif args.command == "window":
        print(window_label(args.config, args.label))


if __name__ == "__main__":
    main()
