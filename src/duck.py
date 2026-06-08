"""DuckDB connection helpers for direct Parquet governance checks."""

from __future__ import annotations

from typing import Any

from src.config import GovernanceConfig


def connect_duckdb(config: GovernanceConfig) -> Any:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required for governance checks. Install dependencies with "
            "`pip install -r requirements.txt` or rebuild the image."
        ) from exc

    con = duckdb.connect()
    httpfs_loaded = False
    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    except Exception as exc:
        print(f"Warning: DuckDB httpfs extension was not loaded: {exc}")
    else:
        httpfs_loaded = True

    memory_limit = config.duckdb.memory_limit.replace("'", "''")

    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET threads = {int(config.duckdb.threads)}")
    con.execute("SET preserve_insertion_order = false")
    if httpfs_loaded:
        endpoint = config.minio.endpoint.replace("'", "''")
        access_key = config.minio.access_key.replace("'", "''")
        secret_key = config.minio.secret_key.replace("'", "''")
        use_ssl = "true" if config.minio.secure else "false"
        con.execute(f"SET s3_endpoint = '{endpoint}'")
        con.execute(f"SET s3_access_key_id = '{access_key}'")
        con.execute(f"SET s3_secret_access_key = '{secret_key}'")
        con.execute(f"SET s3_use_ssl = '{use_ssl}'")
        con.execute("SET s3_url_style = 'path'")
    return con
