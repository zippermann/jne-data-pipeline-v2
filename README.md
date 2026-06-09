# JNE Data Pipeline v2

This version starts the pipeline at a relational bronze layer. Instead of
joining 30+ Oracle tables into one flat shipment table during extraction, it
extracts each source table separately to partitioned Parquet and stores the run
in MinIO.

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
- Optional table subset for smoke tests
- Output compression and row group sizing
- Scope schema/table prefix
- Date guardrails for scoped operational tables
- PII exclusion mode
- Reference table behavior
- MinIO bucket and object prefix

Environment placeholders like `${ORACLE_USER}` are expanded at runtime.

For a one-table smoke test, set:

```yaml
extraction:
  tables: ["CMS_CNOTE"]
```

An empty list extracts every table. The extractor logs row progress every
`output.progress_rows` rows.

Scoped operational tables also use table-specific date guardrails by default.
For example, `CMS_CNOTE_POD` must match the in-window CNOTE scope and have
`CNOTE_POD_DATE` inside the extraction window plus the configured lookahead.
The default guardrail is `window_start - 0 days` through `window_end + 30 days`
so end-of-window shipments can still include after-window delivery events.
Reference tables are not date-guarded.

## MinIO

Docker Compose includes `jne-minio` as the bronze data lake. The stage 1
pipeline writes Parquet locally as scratch, uploads the run to MinIO, and stops
there. The MinIO objects are the durable bronze artifact for later
warehouse/database loading.

Airflow still uses its own internal Postgres metadata database; that is separate
from the JNE data path.

## Airflow

The DAG is `jne_bronze_extract`. It runs two tasks in order:

- `extract_bronze`: Oracle tables to partitioned Parquet in MinIO
- `run_governance`: workbook-indexed governance checks over the MinIO bronze run

```bash
python -m src.bronze --config config/config.yaml --run-id {{ ts_nodash }}
```

Pass `{"keep_scope": true}` in `dag_run.conf` to leave Oracle scope tables in
place for manual inspection.

The DAG derives the exact `BRONZE_RUN_PREFIX`, governance output prefix, and
window labels from the same config for all tasks, so governance points at the
bronze objects produced by that run.

## Governance Outputs

Governance outputs are written to MinIO under:

```text
s3://jne-bronze/governance/jne/run_id=<run_id>/
```

Each governance run writes `scorecard.csv`, `scorecard.parquet`, and
`failures.parquet`. Bronze source data also stays in MinIO as Parquet, so the
pipeline does not duplicate data into a database.

## Postgres Mart Loading

The first Tableau-serving layer is a separate Postgres database, not Airflow's
metadata database. Docker Compose includes `mart-postgres` for this purpose.

The mart loader copies the latest governed bronze run from MinIO into Postgres:

```bash
python -m src.mart_load --config config/mart.yaml
```

Airflow runs this as the third task:

```text
extract_bronze -> run_governance -> load_data_mart
```

This v1 is a latest-snapshot serving copy. It loads the same bronze tables
produced by extraction into the `bronze` schema and governance outputs into the
`governance` schema. MinIO remains the durable bronze archive.

During mart loading, the loader also derives
`governance.cnote_failure_candidates` from `governance.failures`,
`governance.scorecard`, and the loaded bronze tables. This table only contains
failures that can be safely mapped back to a `CNOTE_NO`, with
`mapping_method` and `mapping_confidence` fields for Tableau drilldowns.
Unmapped failures remain available in `governance.failures`.
