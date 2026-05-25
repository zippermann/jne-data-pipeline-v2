# Bronze Extract Layer

Raw Oracle table dumps → MinIO Parquet.

## What it does

`extract_bronze.py` reads each Oracle source table in full and writes it as
Parquet part files to MinIO under `bronze/<table>/part-NNNN.parquet`.

**Bronze is the raw, normalized landing zone.** No joins, no column pivots,
no business exclusions, no DQ scoring. Each table lands at its own source
grain — including tables that were previously excluded for business reasons
(those exclusions happen in silver). The script is idempotent: re-running
overwrites existing objects.

## Environment variables

| Variable             | Example                          | Description                            |
|----------------------|----------------------------------|----------------------------------------|
| `ORACLE_DSN`         | `host:1521/SERVICE`              | Oracle EZConnect string                |
| `ORACLE_USER`        | `jne_read`                       | Oracle username                        |
| `ORACLE_PASSWORD`    | `secret`                         | Oracle password                        |
| `MINIO_ENDPOINT`     | `http://localhost:9000`          | MinIO S3-compatible endpoint URL       |
| `MINIO_ACCESS_KEY`   | `minioadmin`                     | MinIO access key                       |
| `MINIO_SECRET_KEY`   | `minioadmin`                     | MinIO secret key                       |
| `MINIO_BUCKET`       | `jne-pipeline`                   | Target bucket (must exist beforehand)  |

## Example runs

```bash
# Extract all 38 tables (default: 4 workers, 100 000 rows/part)
python3 extract_bronze.py --config tables.yaml

# Extract only two tables (useful for testing)
python3 extract_bronze.py --config tables.yaml --only cnote,mfbag

# Larger chunks, more parallelism
python3 extract_bronze.py --config tables.yaml --chunksize 500000 --workers 8

# Run from project root
python3 scripts/extract/extract_bronze.py --config scripts/extract/tables.yaml
```

## Output layout

```
s3://<MINIO_BUCKET>/
  bronze/
    cnote/
      part-0000.parquet
      part-0001.parquet
      ...
    mfbag/
      part-0000.parquet
    mfcnote/           ← one raw table; NOT split into OM/TM1/TM2/IM/HM
      part-0000.parquet
      ...
    ...
```

## Dependencies

```bash
pip install oracledb sqlalchemy pandas pyarrow boto3 pyyaml
```

> `oracledb` runs in **thin mode** — no Oracle Instant Client required.

## Table manifest (`tables.yaml`)

38 tables total (31 from the CMS letter map + 7 additional reference/excluded
tables). Tables with `pk: null` need primary key confirmation — they still
extract correctly with a full `SELECT *`, but will lack deterministic row
ordering until a PK is set. See inline `# TODO confirm PK` comments.

## Bronze vs silver

| Layer  | What happens                                        |
|--------|-----------------------------------------------------|
| Bronze | Raw dump, one Parquet dir per Oracle table          |
| Silver | Normalization, type coercion, business exclusions   |
| Gold   | `transport_unified` narrow join (bag/manifest/SMU)  |
