"""Governance configuration loading."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def repl(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            return os.getenv(name, default or "")

        return ENV_PATTERN.sub(repl, value)
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    return value


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool


@dataclass(frozen=True)
class BronzeConfig:
    bucket: str
    run_prefix: str
    table_overrides: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class GovernanceOutputConfig:
    output_bucket: str
    output_prefix: str
    orphan_key_limit: int = 10000


@dataclass(frozen=True)
class DuckDBConfig:
    memory_limit: str = "4GB"
    threads: int = 4


@dataclass(frozen=True)
class GovernanceConfig:
    minio: MinioConfig
    bronze: BronzeConfig
    governance: GovernanceOutputConfig
    duckdb: DuckDBConfig
    extraction_window: dict[str, str]


def load_governance_config(path: str | Path = "config/governance.yaml") -> GovernanceConfig:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyYAML is required to load governance config files. Install "
            "dependencies with `pip install -r requirements.txt` or rebuild the image."
        ) from exc

    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        raw = _expand_env(yaml.safe_load(handle) or {})

    minio = raw.get("minio", {})
    bronze = raw.get("bronze", {})
    governance = raw.get("governance", {})
    duckdb = raw.get("duckdb", {})

    return GovernanceConfig(
        minio=MinioConfig(
            endpoint=minio.get("endpoint", "localhost:9000"),
            access_key=minio.get("access_key", "minioadmin"),
            secret_key=minio.get("secret_key", "minioadmin"),
            secure=_as_bool(minio.get("secure", False)),
        ),
        bronze=BronzeConfig(
            bucket=bronze["bucket"],
            run_prefix=bronze["run_prefix"].strip("/"),
            table_overrides={key.upper(): value for key, value in (bronze.get("table_overrides") or {}).items()},
        ),
        governance=GovernanceOutputConfig(
            output_bucket=governance.get("output_bucket", bronze["bucket"]),
            output_prefix=governance["output_prefix"].strip("/"),
            orphan_key_limit=int(governance.get("orphan_key_limit", 10000)),
        ),
        duckdb=DuckDBConfig(
            memory_limit=duckdb.get("memory_limit", "4GB"),
            threads=int(duckdb.get("threads", 4)),
        ),
        extraction_window=raw.get("extraction_window", {}),
    )


def table_folder(table_name: str) -> str:
    return table_name.lower()


def table_path(config: GovernanceConfig, table_name: str) -> str:
    override = config.bronze.table_overrides.get(table_name.upper())
    if override:
        return override
    folder = table_folder(table_name)
    return f"s3://{config.bronze.bucket}/{config.bronze.run_prefix}/{folder}/*.parquet"
