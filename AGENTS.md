# JNE Data Pipeline v2

This repo is intentionally stripped down to the core relational bronze pipeline.
It does not build the old flat shipment table. The main path extracts Oracle
source tables separately, writes partitioned Parquet to MinIO, transforms the
CNOTE table, and can load the run into a ClickHouse mart for Tableau or
inspection.

## Current Shape

- `extractor/`: Oracle to relational bronze Parquet extraction.
- `loader/`: MinIO Parquet to ClickHouse mart loading.
- `governance/`: pandas governance runner, executable catalog, and index workbook.
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

Load a bronze run and governance results into ClickHouse:

```bash
python -m loader.mart_load_clickhouse --config config/mart_clickhouse.yaml
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
extract_oracle -> transform_data -> run_governance -> load_data_mart_clickhouse
```

`extract_oracle` runs `extractor.bronze`.
`transform_data` runs `transform.transform_data` and appends `derived` metadata to
the same run manifest.
`run_governance` runs `governance.runner` against the bronze run manifest after
transform has published reusable document-to-CNOTE links.
`load_data_mart_clickhouse` runs `loader.mart_load_clickhouse`.

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
- `derived/document_cnote_links/part-*.parquet`: reusable non-CNOTE document links
  for governance and inspection.

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
with the transformed `derived/cms_cnote_transformed` data. Governance output
lands in the `governance` database:

- `governance.governance_results`: one document-level result row per rule check.
- `governance.governance_result_cnotes`: zero-to-many CNOTE links per
  `result_id`.
- `governance.governance_rule_summary`: one audit row per rule.

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

`governance/runner.py` reads the bronze `run_manifest.json`, loads the required
Parquet columns for active catalog rules, and writes document-level governance
results. The primary output is one row per checked document/index:

- `governance_results.csv`

It also writes a CNOTE bridge file and one rule-level audit file:

- `governance_result_cnotes.csv`
- `governance_rule_summary.csv`

Dashboard-oriented outputs include:

- `tableau_dashboard.csv`: one failed CNOTE/index row for dashboard tools.
- `html_dashboard_summary.csv`: affected CNOTE counts by route, shipment,
  journey stage, DQ element, index, and impact.
- `html_dashboard_denominators.csv`: total CNOTE denominators by origin,
  destination, shipment type, and delivery service, including `ALL` rollup rows.
- `html_dashboard_rule_summary.csv`: dashboard-friendly rule/index totals with
  checked rows, failed rows, rule failure rate, and affected CNOTE count.

`governance_results.csv` includes:

- `result_id`: stable row id for joining to the CNOTE bridge in the same run.
- `document_type`: the source document type, usually the source table name without
  the `CMS_` prefix.
- `document_id`: the exact source document checked, such as a CNOTE number, bag
  number, manifest number, sheet number, or process document number.
- `cnote_no`: a convenience shortcut only when exactly one safe sampled CNOTE is
  linked. It is intentionally blank for zero-link and many-link documents.

`governance_result_cnotes.csv` is the source of truth for CNOTE rollups. It
contains one row per safe CNOTE link:

- `result_id`
- `cnote_no`
- `link_method`
- `link_confidence`

Dashboard rule of thumb:

- Document-level views should use `governance_results.document_id`.
- CNOTE-level views should join `governance_results` to
  `governance_result_cnotes` on `result_id` and use
  `governance_result_cnotes.cnote_no`.
- Do not group non-CNOTE tables only by `governance_results.cnote_no`; that
  column is nullable by design.

Reusable bridge maps should only represent confirmed source relationships.
Bridge construction belongs in `transform/document_links.py`; governance consumes
the derived `DOCUMENT_CNOTE_LINKS` table when it exists. Current confirmed
examples include:

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
- `SKIPPED`: the bronze manifest did not include a required table.
- `ERROR`: the table existed, but the rule could not run because of an
  implementation issue such as a missing column or malformed rule.

By default, governance exits non-zero when any active rule has `ERROR`.
`SKIPPED` and `NO_ROWS` rules are recorded in `governance_rule_summary.csv` for
audit. Use `--fail-on-skipped` when a run should fail on skipped active rules,
and use `--no-strict` only for inspection runs.

`governance/catalog_skipped_rows.csv` records workbook rows that were not mapped
into executable catalog rules.

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

## Notes For Future Agents

- Keep this repo bare bones unless the user explicitly asks for richer tooling.
- Prefer top-level packages (`extractor`, `loader`, `governance`) over adding a
  new `src` package.
- Preserve relational bronze semantics: no dedupe, no joins, no pivoting during
  extraction.
- Treat MinIO bronze Parquet as the durable data lake artifact.
- Keep mart loaders manifest-driven and reuse bulky reference tables by default
  once they already exist in ClickHouse.
