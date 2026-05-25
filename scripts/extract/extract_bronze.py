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
import pandas as pd
import yaml
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _oracle_engine():
    """Build a SQLAlchemy engine for Oracle using oracledb in thin mode."""
    return create_engine(
        "oracle+oracledb://",
        connect_args={
            "user": os.environ["ORACLE_USER"],
            "password": os.environ["ORACLE_PASSWORD"],
            "dsn": os.environ["ORACLE_DSN"],
        },
        pool_pre_ping=True,
    )


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

    query = f"SELECT * FROM {oracle_table}"
    if pk:
        query += f" ORDER BY {pk}"

    try:
        engine = _oracle_engine()
        s3 = _s3_client()
        part = 0
        total = 0

        with engine.connect() as conn:
            result = pd.read_sql(text(query), conn, chunksize=chunksize)

            # pd.read_sql returns a generator when chunksize is set, but guard
            # against versions that return a plain DataFrame.
            chunks = [result] if isinstance(result, pd.DataFrame) else result

            for chunk in chunks:
                key = f"bronze/{out}/part-{part:04d}.parquet"
                _upload_df(s3, bucket, key, chunk)
                total += len(chunk)
                part += 1
                log.debug(f"  {out}: wrote part {part:04d} ({len(chunk):,} rows)")

        if part == 0:
            log.warning(f"{out}: table is empty — no parts written")
        else:
            log.info(
                f"{out}: {total:,} rows → s3://{bucket}/bronze/{out}/ ({part} part{'s' if part > 1 else ''})"
            )

        return out, total, None

    except Exception as exc:
        log.error(f"{out}: FAILED — {exc}")
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

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(extract_one, entry, args.chunksize, bucket): entry["out"]
            for entry in tables
        }
        for fut in as_completed(futures):
            results.append(fut.result())

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
