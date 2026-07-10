# JNE Data Pipeline v2

This repo is intentionally stripped down to the core relational bronze pipeline.
It does not build the old flat shipment table during extraction. The main path
extracts Oracle source tables separately, writes partitioned Parquet to MinIO,
transforms the CNOTE table, evaluates governance pass/fail rows, and loads the
run into a ClickHouse mart for Tableau or inspection. Production stress-test
governance now runs in ClickHouse after bronze/transformed CNOTE has been loaded;
the pandas governance runner remains a fallback/smoke-test tool.

## Current Shape

- `extractor/`: Oracle to relational bronze Parquet extraction.
- `loader/`: MinIO Parquet to ClickHouse mart loading, ClickHouse-native
  governance execution, CNOTE link builds, and dashboard-ready enrichment.
- `governance/`: executable catalog, pandas fallback runner, and index workbook.
- `transform/`: derived CNOTE-level enrichment built from bronze Parquet.
- `pipeline_context.py`: Airflow helper for run prefixes and window labels.
- `airflow/dags/jne_data_pipeline_dag.py`: Airflow DAG definition.
- `config/`: extraction, mart, and PII exclusion config.
- `tests/`: regression checks for extraction scoping, windows, and mart loading.

Deleted legacy areas include the old `src` package shell, old DuckDB governance
runtime, old flat-pipeline references, one-off scripts, the retired Postgres
mart loader, and unused Postgres init scaffolding.

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

Transform the CNOTE table:

```bash
BRONZE_RUN_PREFIX=bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id> \
python -m transform.transform_data --source minio --config config/config.yaml
```

For a local run directory:

```bash
python -m transform.transform_data --source local --bronze-run-path data/bronze/.../run_id=<run_id>
```

Load a bronze run into ClickHouse:

```bash
python -m loader.mart_load_clickhouse --config config/mart_clickhouse.yaml --stage load
```

Run ClickHouse-native governance and dashboard enrichment after the load:

```bash
python -m loader.mart_load_clickhouse --config config/mart_clickhouse.yaml --stage governance
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
extract_oracle -> transform_data -> load_data_mart_clickhouse -> run_governance
```

`extract_oracle` runs `extractor.bronze`.
`transform_data` runs `transform.transform_data` and appends `derived` metadata to
the same run manifest.
`load_data_mart_clickhouse` runs `loader.mart_load_clickhouse --stage load` and
publishes raw bronze tables plus transformed CNOTE into ClickHouse.
`run_governance` runs `loader.mart_load_clickhouse --stage governance`. It builds
document-to-CNOTE links, executes supported catalog rules in ClickHouse, writes
raw document-level results and rule summaries, writes the CNOTE bridge/enrichment
table, and builds dashboard-ready governance tables.

Pass `{"keep_scope": true}` in `dag_run.conf` to keep Oracle scope tables for
manual inspection after extraction.
Pass `{"load_clickhouse": false}` to disable the mart load for a single DAG run.

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
The transform step writes derived Parquet outputs and records them in the
manifest's `derived` section:

- `derived/cms_cnote_transformed/part-*.parquet`: CNOTE enrichment used as the
  mart's `bronze.cms_cnote`.

`transform.document_links_mode` is set to `clickhouse` in `config/config.yaml`.
This means `transform.transform_data` intentionally skips the old pandas
`derived/document_cnote_links` build. The ClickHouse mart loader builds
document-to-CNOTE links from loaded bronze tables instead.

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

Use `config/mart_clickhouse.yaml` for the ClickHouse mart. The mart loader
publishes tables into the `bronze` database and replaces raw `bronze.cms_cnote`
with the transformed `derived/cms_cnote_transformed` data. With
`governance.execution_mode: "clickhouse"`, governance output is generated inside
ClickHouse and lands in the `governance` database. Current stress-test table
names use `_2` suffixes so existing dashboard tables from earlier 100k runs are
not overwritten:

- `governance.governance_results_2`: one raw document-level result row per rule
  check.
- `governance.governance_result_cnotes_2`: one row per result-to-CNOTE link,
  including CNOTE-native attributes such as origin, destination, service code,
  delivery type, shipment scope, and delivery category.
- `governance.governance_rule_summary_2`: one audit row per rule.
- `governance.document_cnote_links_2`: ClickHouse-built source document to CNOTE
  bridge.
- `governance.governance_results_dashboard_2`: ClickHouse-enriched dashboard
  table joining raw governance results to `governance_result_cnotes_2`.

Reference tables such as `cms_drourate` are loaded into the mart when missing,
then reused on later mart loads instead of being reloaded every run.

Environment placeholders like `${ORACLE_USER}` are expanded at runtime.

For a one-table smoke extraction:

```yaml
extraction:
  tables: ["CMS_CNOTE"]
```

An empty list extracts every configured table.

## Governance

Production stress-test governance is ClickHouse-native. The Airflow
`run_governance` task calls `loader.mart_load_clickhouse --stage governance`
after the ClickHouse load. It reads `governance/catalog.py`, inserts raw
document-level pass/fail rows into `governance.governance_results_2`, and records
one audit row per catalog rule in `governance.governance_rule_summary_2`.

The initial ClickHouse executor supports the high-volume simple rule families:
`completeness`, `uniqueness`, `validity_regex`, `validity_integer`,
`validity_datetime`, `validity_in_set`, and `non_negative`. Unsupported
join/reference rule families are recorded as `SKIPPED` in the rule summary until
their SQL translations are added.

`governance/runner.py` remains available as a pandas fallback/smoke-test runner.
It reads the bronze `run_manifest.json`, loads required Parquet columns, and
writes CSV/Parquet outputs under `governance/outputs/<run_id>`, but it is no
longer the Airflow stress-test path.

The primary output is one row per checked document/index:

- `governance_results.csv`

It also writes an output-structure-compatible CNOTE bridge file and one
rule-level audit file:

- `governance_result_cnotes.csv`
- `governance_rule_summary.csv`

`governance.governance_results_2` includes:

- `result_id`: stable row id for the result row.
- `document_type`: the source document type, usually the source table name without
  the `CMS_` prefix.
- `document_id`: the exact source document checked, such as a CNOTE number, bag
  number, manifest number, sheet number, or process document number.
- `cnote_no`: populated only for direct `CMS_CNOTE` result rows in the raw
  governance output. Non-CNOTE document rows intentionally keep this blank.
- CNOTE context columns such as `shipment_type`, `cnote_origin`, and
  `origin_region`: kept in the raw output schema for compatibility but left
  blank by ClickHouse-native governance. CNOTE-native values belong in
  `governance.governance_result_cnotes_2`.

`governance.governance_result_cnotes_2` is the CNOTE rollup/enrichment source:

- `result_id`
- `cnote_no`
- `link_method`
- `link_confidence`
- `cnote_origin`
- `cnote_destination`
- `cnote_service_code`
- `delivery_type`
- `shipment_scope`
- `delivery_category`

Dashboard rule of thumb:

- Document-level views should use `governance_results.document_id`.
- CNOTE-level/dashboard views should use
  `governance.governance_result_cnotes_2` or
  `governance.governance_results_dashboard_2`.
- Do not group non-CNOTE tables only by `governance_results.cnote_no`; that
  column is nullable by design.

Reusable bridge maps should only represent confirmed source relationships.
The confirmed relationship definitions still live in `transform/document_links.py`
for shared vocabulary/tests, but production-scale bridge construction belongs in
`loader/mart_load_clickhouse.py` after bronze tables are loaded into ClickHouse.
Current confirmed examples include:

- `CMS_MFCNOTE.MFCNOTE_NO` directly to CNOTE.
- `CMS_MFBAG.MFBAG_NO -> CMS_MFCNOTE.MFCNOTE_BAG_NO -> CNOTE`.
- `CMS_DMBAG.DMBAG_BAG_NO/DMBAG_NO -> MFBAG/MFCNOTE -> CNOTE`.
- `CMS_MMBAG.MMBAG_NO -> CMS_DMBAG.DMBAG_NO -> CNOTE`.
- `CMS_MANIFEST.MANIFEST_NO -> CMS_MFCNOTE.MFCNOTE_MAN_NO -> CNOTE`.
- `CMS_MRSHEET.MRSHEET_NO -> CMS_DRSHEET.DRSHEET_NO -> CNOTE`.
- `CMS_MHICNOTE.MHICNOTE_NO -> CMS_DHICNOTE.DHICNOTE_NO -> CNOTE`.
- `CMS_MHI_HOC.MHI_NO -> CMS_DHI_HOC.DHI_NO -> CNOTE`.
- `CMS_MHOCNOTE.MHOCNOTE_NO -> CMS_DHOCNOTE.DHOCNOTE_NO -> CNOTE`.
- `CMS_MHOUNDEL_POD.MHOUNDEL_NO -> CMS_DHOUNDEL_POD.DHOUNDEL_NO -> CNOTE`.
- `CMS_DSMU.DSMU_NO/DSMU_BAG_NO -> DMBAG/MFCNOTE -> CNOTE`.
- `CMS_MSMU.MSMU_NO -> CMS_DSMU.DSMU_NO -> CNOTE`.
- `CMS_DSJ.DSJ_NO -> CMS_RDSJ/MHICNOTE/DHICNOTE -> CNOTE`.
- `CMS_RDSJ.RDSJ_NO -> RDSJ_HVO_NO -> CMS_MHOCNOTE/CMS_DHOCNOTE -> CNOTE`.
- `CMS_MSJ.MSJ_NO -> CMS_DSJ.DSJ_NO -> CNOTE`.

Direct CNOTE-bearing tables do not need custom bridges when their configured
`cnote_column` already holds a sampled CNOTE value. Examples include
`CMS_APICUST`, `CMS_CNOTE_POD`, `CMS_DRSHEET`, `CMS_DRSHEET_PRA`,
`CMS_DRCNOTE`, `CMS_DHI_HOC`, `CMS_DHICNOTE`, `CMS_DHOCNOTE`,
`CMS_DHOUNDEL_POD`, and cost/detail tables with true `*_CNOTE_NO` columns.

Result rows use explicit statuses:

- `PASS`: rule ran and found no failed rows.
- `FAIL`: rule ran and found failed rows.
- `SKIPPED`: the ClickHouse table/column was unavailable or the rule family has
  not yet been translated to ClickHouse SQL.
- `ERROR`: the table existed, but the rule could not run because of an
  implementation issue such as a missing column or malformed rule.

`SKIPPED`, `ERROR`, and `NO_ROWS` rules are recorded in
`governance.governance_rule_summary_2` for audit.

`governance/catalog_skipped_rows.csv` records workbook rows that were not mapped
into executable catalog rules.

## Stress-Test Notes

For one-week stress tests in the VM, the extraction window must stay consistent
across all task reruns because Airflow recomputes `BRONZE_RUN_PREFIX` from the
current `config/config.yaml` for every task. If extraction ran with:

```yaml
extraction:
  cnote_limit:
  window:
    start_date: "2026-05-01"
    end_date: "2026-05-07"
```

then rerunning `load_data_mart_clickhouse` or `run_governance` must use the same
config. Restoring the default `cnote_limit: 100000` or
`end_date: "2026-06-01"` will point the task at a different MinIO prefix and
cause `NoSuchKey` for `run_manifest.json`.

When testing code changes after extraction and transform have already succeeded,
rerun `load_data_mart_clickhouse` first if bronze/transformed ClickHouse tables
need to be refreshed, then rerun `run_governance`. If the ClickHouse load already
finished successfully and only governance SQL changed, rerun `run_governance`
only. Do not rerun `extract_oracle` or `transform_data` unless the extraction
window, source data, or CNOTE transform changed.

Docker images may not include `git`. To verify the VM container has this branch's
code, inspect files instead of using `git` inside the container. For example,
check that `governance/output.py` contains `document_id` in `RESULT_COLUMNS` and
that `loader/mart_load_clickhouse.py` contains
`--stage governance` and `governance_results_dashboard_2`.

## Tests

Run tests when dependencies are installed:

```bash
python -m pytest tests
```

Useful lightweight checks:

```bash
python -m compileall extractor loader governance transform airflow/dags tests pipeline_context.py
python -m governance.runner --source synthetic --no-strict --output-dir /tmp/jne-governance-smoke
```

The synthetic governance smoke check requires Parquet dependencies such as
`pyarrow`. If the local laptop environment is missing them, use compile checks
and run full validation in the Airflow/Docker environment instead.

## Notes For Future Agents

- Keep this repo bare bones unless the user explicitly asks for richer tooling.
- Prefer top-level packages (`extractor`, `loader`, `governance`) over adding a
  new `src` package.
- Preserve relational bronze semantics: no dedupe, no joins, no pivoting during
  extraction.
- Treat MinIO bronze Parquet as the durable data lake artifact.
- Keep mart loaders manifest-driven and reuse bulky reference tables by default
  once they already exist in ClickHouse.
