# JNE Data Pipeline v2

This version starts the pipeline at a relational bronze layer. Instead of
joining 30+ Oracle tables into one flat shipment table during extraction, it
extracts each source table separately to partitioned Parquet, stores the run in
MinIO, then loads the same source-shaped datasets into Postgres.

The first usable path is:

```bash
python3 -m src.bronze --config config/config.yaml --run-id local_test
```

Local working Parquet lands under:

```text
data/bronze/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id>/
```

The durable bronze lake copy lands in MinIO under:

```text
s3://jne-bronze/bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id>/
```

Each source table gets its own folder with `part-*.parquet`, `_SUCCESS`, and the
run writes a top-level `run_manifest.json` with row counts, file counts, sizes,
timings, and Oracle scope-table counts.

The old flat-pipeline files are still present under `reference/old-flat-pipeline/`:

- `unify_jne_oracle.sql`
- `unify_oracle.py`
- `export_oracle_parquet.py`
- `jne_etl_pipeline.py`

## Configuration

Edit `config/config.yaml` for:

- Oracle connection and source schema
- Extraction window
- Output compression and row group sizing
- Scope schema/table prefix
- PII exclusion mode
- Reference table behavior
- MinIO bucket and object prefix
- Postgres connection settings for later bronze loading/governance stages

Environment placeholders like `${ORACLE_USER}` are expanded at runtime.

## MinIO And Postgres

Docker Compose includes `jne-minio` as the bronze data lake and `jne-postgres`
as the query/governance landing database. On first startup Postgres creates:

- `bronze`
- `governance`
- `audit`

The stage 1 pipeline writes Parquet locally as scratch, uploads the run to
MinIO, then loads the same source-shaped datasets from the MinIO copy into
`bronze.<table_name>` in Postgres. The MinIO objects are the durable bronze
artifact; Postgres is the query/governance landing zone.

Each Postgres bronze table also gets `_bronze_run_id`, `_window_start`,
`_window_end`, and `_loaded_at` columns. A retry with the same `run_id` deletes
that run's rows before reloading, so repeated attempts do not duplicate data.

## Airflow

The new DAG is `jne_bronze_extract`. It runs:

```bash
python -m src.bronze --config config/config.yaml --run-id {{ ts_nodash }}
```

Pass `{"keep_scope": true}` in `dag_run.conf` to leave Oracle scope tables in
place for manual inspection.
