# JNE Data Pipeline v2

This version starts the pipeline at a relational bronze layer. Instead of
joining 30+ Oracle tables into one flat shipment table during extraction, it
extracts each source table separately to partitioned Parquet and stores the run
in MinIO.

The first usable path is:

```bash
python3 -m extractor.bronze --config config/config.yaml --run-id local_test
```

## Source Layout

The runtime packages are grouped by pipeline stage:

- `extractor/`: Oracle to relational bronze Parquet extraction
- `loader/`: MinIO bronze outputs into the Postgres mart
- `governance/`: lightweight pandas governance checks and workbook reference
- `pipeline_context.py`: shared Airflow run-prefix/window helpers

`governance/` contains the lightweight pandas governance checker and the source
workbook used as reference material.

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

The DAG is `jne_data_pipeline`. It runs three tasks in order:

- `extract_oracle`: Oracle tables to partitioned Parquet in MinIO
- `run_governance`: simple pandas governance checks with CSV output
- `load_data_mart`: bronze outputs into the Postgres mart

```bash
python -m extractor.bronze --config config/config.yaml --run-id {{ ts_nodash }}
```

Pass `{"keep_scope": true}` in `dag_run.conf` to leave Oracle scope tables in
place for manual inspection.

The DAG derives the exact `BRONZE_RUN_PREFIX` and window labels from the same
config for all tasks.

## Governance Outputs

The simple governance runner writes local CSVs:

```bash
python -m governance.runner --output-dir governance/outputs/local_test
```

Each run writes `scorecard.csv` and `failures.csv`. The current simple runner
uses synthetic fixtures as a readable rule harness; the extraction and mart
paths stay focused on relational bronze Parquet.

## Postgres Mart Loading

The first Tableau-serving layer is a separate Postgres database, not Airflow's
metadata database. Docker Compose includes `mart-postgres` for this purpose.

The mart loader copies a bronze run from MinIO into Postgres:

```bash
python -m loader.mart_load --config config/mart.yaml
```

Airflow runs this as the third task:

```text
extract_oracle -> run_governance -> load_data_mart
```

This v1 is a latest-snapshot serving copy. It loads the same bronze tables
produced by extraction into the `bronze` schema. MinIO remains the durable
bronze archive.
