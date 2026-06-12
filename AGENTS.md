# JNE Data Pipeline v2

This repo is intentionally stripped down to the core relational bronze pipeline.
It does not build the old flat shipment table. The main path extracts Oracle
source tables separately, writes partitioned Parquet to MinIO, and can load that
bronze run into a Postgres mart for Tableau or inspection.

## Current Shape

- `extractor/`: Oracle to relational bronze Parquet extraction.
- `loader/`: MinIO bronze Parquet to Postgres mart loading.
- `governance/`: pandas governance runner, executable catalog, and index workbook.
- `transform/`: derived CNOTE-level enrichment built from bronze Parquet.
- `pipeline_context.py`: Airflow helper for run prefixes and window labels.
- `airflow/dags/jne_data_pipeline_dag.py`: Airflow DAG definition.
- `config/`: extraction, mart, and PII exclusion config.
- `tests/`: regression checks for extraction scoping, windows, and mart loading.

Deleted legacy areas include the old `src/` package shell, old DuckDB governance
runtime, old flat-pipeline references, one-off scripts, and unused Postgres init
scaffolding.

## Main Commands

Run extraction:

```bash
python -m extractor.bronze --config config/config.yaml --run-id local_test
```

Run governance:

```bash
BRONZE_RUN_PREFIX=bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id> \
python -m governance.runner --source minio --config config/config.yaml --output-dir governance/outputs/<run_id>
```

For a local run directory under `data/bronze/.../run_id=<run_id>/`:

```bash
python -m governance.runner --source local --bronze-run-path data/bronze/.../run_id=<run_id> --output-dir governance/outputs/<run_id>
```

Build the first derived CNOTE enrichment:

```bash
BRONZE_RUN_PREFIX=bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id> \
python -m transform.build_derived --source minio --config config/config.yaml
```

For a local run directory:

```bash
python -m transform.build_derived --source local --bronze-run-path data/bronze/.../run_id=<run_id>
```

Load a bronze run and governance results into Postgres:

```bash
python -m loader.mart_load --config config/mart.yaml
```

Derive Airflow context values:

```bash
python -m pipeline_context bronze-prefix --config config/config.yaml --run-id local_test --extract-date 2026-06-10
python -m pipeline_context window --config config/config.yaml start
python -m pipeline_context window --config config/config.yaml end
```

## Airflow

The DAG id is `jne_data_pipeline`.

Task order:

```text
extract_oracle -> run_governance -> build_derived -> load_data_mart
```

`extract_oracle` runs `extractor.bronze`.
`run_governance` runs `governance.runner` against the bronze run manifest.
`build_derived` runs `transform.build_derived` and appends `derived` metadata to
the same run manifest.
`load_data_mart` runs `loader.mart_load`.

Pass `{"keep_scope": true}` in `dag_run.conf` to keep Oracle scope tables for
manual inspection after extraction.

## Data Flow

Local scratch output lands under:

```text
data/bronze/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id>/
```

The durable bronze copy lands in MinIO under:

```text
s3://jne-bronze/bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id>/
```

Each extracted source table gets its own folder with `part-*.parquet`,
`_SUCCESS`, and the run writes a top-level `run_manifest.json`.
Reference tables are reusable across runs. When a completed reference table
already exists in MinIO, extraction records `reused: true` and `source_prefix`
in the manifest instead of pulling the table from Oracle again.
The derived step writes `derived/cnote_enriched/part-*.parquet` and records it in
the manifest's `derived` section.

## Configuration

Use `config/config.yaml` for Oracle extraction settings, source schema, window,
table subset, output sizing, scope naming, date guardrails, PII exclusions, and
MinIO output.

Oracle extraction tuning knobs:

- `oracle.fetch_arraysize`: fetch batch size during table extraction.
- `oracle.prefetch_rows`: Oracle client prefetch rows; keep near
  `fetch_arraysize + 1`.
- `scoping.ctas_parallel_degree`: Oracle `PARALLEL(n)` hint for scope CTAS.
- `scoping.scope_workers`: number of independent scope CTAS jobs to run at once.
- `scoping.date_guardrail_*`: date guardrails applied to extraction SQL and the
  high-volume DRSHEET/MANIFEST scope queries.

Use `config/mart.yaml` for MinIO input, governance result input, and Postgres
mart connection settings. The mart loader publishes bronze tables into the
`bronze` schema, derived tables into the `derived` schema, and replaces the
`governance` schema with the single `governance_results` table.
Reused reference tables are not reloaded into Postgres when the target
`bronze.<table>` already exists.

Environment placeholders like `${ORACLE_USER}` are expanded at runtime.

For a one-table smoke extraction:

```yaml
extraction:
  tables: ["CMS_CNOTE"]
```

An empty list extracts every configured table.

## Governance

`governance/runner.py` reads the bronze `run_manifest.json`, loads the required
Parquet columns for active catalog rules, and writes one long CNOTE-level file:

- `governance_results.csv`

Result rows use explicit statuses:

- `PASS`: rule ran and found no failed rows.
- `FAIL`: rule ran and found failed rows.
- `SKIPPED`: the bronze manifest did not include a required table.
- `ERROR`: the table existed, but the rule could not run because of an
  implementation issue such as a missing column or malformed rule.

By default, governance exits non-zero when any rule has `ERROR`. Use
`--no-strict` only for inspection runs.

`governance/catalog_skipped_rows.csv` records workbook rows that were not mapped
into executable catalog rules.

## Tests

Run tests when dependencies are installed:

```bash
python -m pytest tests
```

Useful lightweight checks:

```bash
python -m compileall extractor loader governance airflow/dags tests pipeline_context.py
python -m governance.runner --source synthetic --no-strict --output-dir /tmp/jne-governance-smoke
```

## Notes For Future Agents

- Keep this repo bare bones unless the user explicitly asks for richer tooling.
- Prefer top-level packages (`extractor`, `loader`, `governance`) over adding a
  new `src` package.
- Preserve relational bronze semantics: no dedupe, no joins, no pivoting during
  extraction.
- Treat MinIO bronze Parquet as the durable data lake artifact.
- Keep Postgres mart loading bronze-only until governance output loading is
  intentionally redesigned.
