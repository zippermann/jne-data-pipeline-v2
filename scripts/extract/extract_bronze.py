#!/usr/bin/env python3
"""
Bronze extract: Oracle tables → MinIO Parquet landing zone.

One directory of Parquet part files per source table. No joins, no pivots,
no DQ scoring. Bronze is the raw normalized landing zone.

Usage:
    python3 extract_bronze.py --config tables.yaml
    python3 extract_bronze.py --config tables.yaml --only cnote,mfbag
    python3 extract_bronze.py --config tables.yaml --chunksize 500000 --workers 8
"""

import argparse
import io
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import boto3
import oracledb
import pandas as pd
import yaml
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _oracle_conn() -> oracledb.Connection:
    """Open an oracledb thin-mode connection (no Instant Client required).

    DSN resolution order (mirrors v1 pipeline_config.py):
      1. ORACLE_DSN if explicitly set
      2. SID-format if ORACLE_SID is set
      3. EZConnect host:port/service otherwise
    """
    host = os.environ["ORACLE_HOST"]
    port = os.environ.get("ORACLE_PORT", "1521")
    sid = os.environ.get("ORACLE_SID", "").strip()
    service = os.environ.get("ORACLE_SERVICE", "").strip()

    if os.environ.get("ORACLE_DSN", "").strip():
        dsn = os.environ["ORACLE_DSN"].strip()
    elif sid:
        dsn = (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))"
            f"(CONNECT_DATA=(SID={sid})))"
        )
    else:
        dsn = f"{host}:{port}/{service}"

    return oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=dsn,
    )


def _fetch_chunks(conn: oracledb.Connection, query: str, chunksize: int):
    """Yield DataFrames of up to chunksize rows. Columns are lowercased."""
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [col[0].lower() for col in cursor.description]
    while True:
        rows = cursor.fetchmany(chunksize)
        if not rows:
            break
        yield pd.DataFrame(rows, columns=columns)
    cursor.close()


def _s3_client():
    """Build a boto3 S3 client pointed at MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


# ---------------------------------------------------------------------------
# Upload helper
# ---------------------------------------------------------------------------

def _upload_df(s3_client, bucket: str, key: str, df: pd.DataFrame) -> None:
    """Serialize df to Parquet bytes and PUT to S3/MinIO (overwrites existing)."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


# ---------------------------------------------------------------------------
# Per-table extract
# ---------------------------------------------------------------------------

def extract_one(
    entry: dict,
    chunksize: int,
    bucket: str,
) -> tuple[str, int, Optional[str]]:
    """
    Extract one Oracle table and write Parquet parts to MinIO.

    Returns (out_name, total_rows, error_message_or_None).
    """
    out = entry["out"]
    oracle_table = entry["oracle"]
    pk = entry.get("pk") or None

    schema = os.environ.get("ORACLE_SCHEMA", "").strip()
    qualified = f"{schema}.{oracle_table}" if schema else oracle_table
    query = f"SELECT * FROM {qualified}"
    if pk:
        query += f" ORDER BY {pk}"

    try:
        conn = _oracle_conn()
        s3 = _s3_client()
        part = 0
        total = 0

        row_bar = tqdm(
            unit=" rows",
            desc=f"  {out:<28}",
            ascii=True,
            dynamic_ncols=False,
            ncols=88,
            leave=False,
        )
        for chunk in _fetch_chunks(conn, query, chunksize):
            key = f"bronze/{out}/part-{part:04d}.parquet"
            _upload_df(s3, bucket, key, chunk)
            total += len(chunk)
            part += 1
            row_bar.update(len(chunk))
        row_bar.close()

        conn.close()

        if part == 0:
            tqdm.write(f"  [WARN]  {out}: table is empty — no parts written")
        else:
            tqdm.write(
                f"  [OK]    {out}: {total:,} rows → bronze/{out}/ ({part} part{'s' if part > 1 else ''})"
            )

        return out, total, None

    except Exception as exc:
        tqdm.write(f"  [ERROR] {out}: FAILED — {exc}")
        return out, -1, str(exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract Oracle tables to bronze Parquet in MinIO.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="tables.yaml",
        help="Path to the tables manifest YAML",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated `out` names to extract; omit to extract all",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Rows per Parquet part file",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of tables to extract in parallel (I/O-bound thread pool)",
    )
    args = parser.parse_args()

    # Load manifest
    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    tables = cfg["tables"]

    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        tables = [t for t in tables if t["out"] in wanted]
        if not tables:
            log.error(f"--only filter matched no tables: {args.only}")
            return 1

    bucket = os.environ["MINIO_BUCKET"]

    log.info(
        f"Starting bronze extract: {len(tables)} tables, "
        f"{args.workers} workers, chunksize={args.chunksize:,}"
    )

    # Parallel extract — one thread per table, chunks within a table are serial
    results: list[tuple[str, int, Optional[str]]] = []

    table_bar = tqdm(
        total=len(tables),
        desc="Tables",
        unit=" table",
        ascii=True,
        dynamic_ncols=False,
        ncols=88,
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(extract_one, entry, args.chunksize, bucket): entry["out"]
            for entry in tables
        }
        for fut in as_completed(futures):
            result = fut.result()
            results.append(result)
            out, rows, err = result
            table_bar.set_postfix_str(f"last: {out}")
            table_bar.update(1)
    table_bar.close()

    # Summary
    results.sort(key=lambda r: r[0])
    failures = [out for out, _, err in results if err is not None]

    col_w = max((len(r[0]) for r in results), default=12) + 2
    print(f"\n{'=' * (col_w + 30)}")
    print("Bronze Extract Summary")
    print(f"{'=' * (col_w + 30)}")
    print(f"{'Table':<{col_w}}  {'Rows':>12}  Status")
    print(f"{'-' * col_w}  {'-' * 12}  {'-' * 30}")
    for out, rows, err in results:
        if err is not None:
            row_cell = "ERROR"
            status = err[:50]
        else:
            row_cell = f"{rows:,}" if rows >= 0 else "0 (empty)"
            status = "OK"
        print(f"{out:<{col_w}}  {row_cell:>12}  {status}")
    print(f"{'-' * col_w}  {'-' * 12}  {'-' * 30}")
    print(f"Total: {len(results)} tables | OK: {len(results) - len(failures)} | Failed: {len(failures)}")

    if failures:
        log.error(f"Failed tables: {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
