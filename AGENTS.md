# JNE Data Pipeline v2

This repo is intentionally stripped down to the core relational bronze pipeline.
It does not build the old flat shipment table. The main path extracts Oracle
source tables separately, writes partitioned Parquet to MinIO, and can load that
bronze run into a Postgres mart for Tableau or inspection.

## Current Shape

- `extractor/`: Oracle to relational bronze Parquet extraction.
- `loader/`: MinIO bronze Parquet to Postgres mart loading.
- `governance/`: small pandas governance runner plus the relational index workbook.
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
python -m governance.runner --output-dir governance/outputs/local_test
```

Load a bronze run into Postgres:

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
extract_oracle -> run_governance -> load_data_mart
```

`extract_oracle` runs `extractor.bronze`.
`run_governance` runs `governance.runner`.
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

Use `config/mart.yaml` for MinIO input and Postgres mart connection settings.
The mart loader is bronze-only; it no longer loads governance parquet artifacts
or builds `governance.cnote_failure_candidates`.

Environment placeholders like `${ORACLE_USER}` are expanded at runtime.

For a one-table smoke extraction:

```yaml
extraction:
  tables: ["CMS_CNOTE"]
```

An empty list extracts every configured table.

## Governance

`governance/runner.py` is deliberately simple. It currently uses synthetic
fixtures as a readable rule harness and writes:

- `scorecard.csv`
- `failures.csv`

The workbook `governance/JNE Index List Relational.xlsx` is reference material.
Do not assume the simple runner covers the full workbook yet.

## Tests

Run tests when dependencies are installed:

```bash
python -m pytest tests
```

Useful lightweight checks:

```bash
python -m compileall extractor loader governance airflow/dags tests pipeline_context.py
python -m governance.runner --output-dir /tmp/jne-governance-smoke
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
