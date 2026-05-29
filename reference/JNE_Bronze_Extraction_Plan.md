# JNE Relational Pipeline — Bronze Layer Extraction Plan

**Scope of this document:** Layer 1 (bronze) extraction only. Governance is deliberately out of scope. The goal is to produce per-table, month-scoped Parquet extracts from Oracle and then measure performance and disk footprint before any governance is built on top. First attempt should pull full table columns, including PII columns, so the relational extract can be tested with minimal moving parts; PII exclusion can be enabled later if extraction fails, storage blows up, or policy requires it.

**Key departure from the current pipeline:** the existing pipeline unifies at extraction time (`unify_jne_oracle.sql` joins 30+ tables and pivots manifests into one flat table). This pipeline keeps data **relational throughout**. Bronze extracts each source table separately. No join, no manifest pivot at this layer.

---

## 1. Architecture Overview

```
Oracle (read replica, IP 220)
        │   per-table SELECT, window predicate pushed down
        ▼
Bronze (Parquet, zstd, one dataset per table, partitioned by window + run)
        │   optional: COPY into Postgres bronze schema (one table per source)
        ▼
[Silver / Gold / Governance — future layers, not in this document]
```

Tables stay in their normalized form. Relationships (cnote → bag → manifest → runsheet → POD) are preserved as foreign keys across separate Parquet datasets, not collapsed into columns. Reconstructing relationships (joins, manifest pivot) is deferred to silver/gold where a specific consumer needs it.

Bronze semantics: scoped, source-shaped, non-deduplicated, non-joined, non-pivoted. Do not reproduce the current pipeline's `ROW_NUMBER()` dedupe CTEs in bronze. If a downstream layer wants latest-row semantics, build that in silver/gold.

---

## 2. Software Reused vs New

| Component | Status | Notes |
|---|---|---|
| Python 3 (`python3`) | reused | Same runtime as current pipeline |
| `oracledb` (Oracle driver) | reused | Connection scaffold lifted from `unify_oracle.py` |
| `pyarrow` | **new** | Native Parquet writer + Arrow batch streaming (replaces pandas-to-CSV) |
| `pyyaml` | **new** | Reads `config.yaml` (replaces hardcoded SQL params) |
| PostgreSQL | reused | Now landing **one table per source**, not one flat table |
| Apache Airflow | reused | New DAG; orchestrates per-table extraction instead of one unify task |
| Docker / docker-compose | reused | Same containerization pattern, new service definitions |
| DBeaver | reused | Manual inspection / validation of bronze output |
| `unify_jne_oracle.sql` | **reference only** | Source of the join chain (which tables, which keys link them). Do **not** reproduce the join. |

`pandas` is intentionally minimized at this layer. The million-row tables (`CMS_APICUST`, `CMS_DROURATE`) will OOM under `read_sql`. Use Arrow batch cursors and write Parquet incrementally.

---

## 3. Codebase File Structure

```
jne-bronze/
├── README.md
├── CLAUDE.md                          # context handoff for the coding agent
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
│
├── config/
│   ├── config.yaml                    # single source of truth for the run
│   └── pii_exclude.yaml               # optional PII columns to drop, per table
│
├── sql/
│   └── reference/
│       └── unify_jne_oracle.sql       # REFERENCE ONLY — the join/scope map
│
├── src/
│   ├── __init__.py
│   ├── oracle_client.py               # connection mgmt (reused scaffold)
│   ├── window.py                      # resolves the adjustable extraction window → (start, end)
│   ├── scoping.py                     # builds/materializes run scope keys from the window
│   ├── table_specs.py                 # per-table: columns, scope key, scope template
│   ├── extractor.py                   # one table → Arrow batches → Parquet (zstd)
│   ├── runner.py                      # orchestrates the full extraction by stage
│   ├── load_postgres.py               # optional: per-table COPY into bronze schema
│   └── utils/
│       ├── logging.py
│       ├── parquet_writer.py          # adapted from current export_oracle_parquet.py writer pattern
│       └── run_manifest.py            # writes an audit record per run (row counts, sizes, timings)
│
├── airflow/
│   └── dags/
│       └── jne_bronze_extract_dag.py
│
├── data/
│   └── bronze/                        # Parquet output root
│       └── window_start=YYYY-MM-DD/
│           └── window_end=YYYY-MM-DD/
│               └── extract_date=YYYY-MM-DD/
│                   └── run_id=<airflow_ts_or_uuid>/
│                       ├── cms_cnote/
│                       ├── cms_apicust/
│                       └── ...
│
└── tests/
    ├── test_window.py
    └── test_scoping.py
```

---

## 4. config.yaml (draft)

```yaml
oracle:
  host: "10.0.0.220"          # read replica
  port: 1521
  service_name: "JNECMS"
  user: "${ORACLE_USER}"      # injected from env, never committed
  password: "${ORACLE_PASSWORD}"
  fetch_arraysize: 50000      # Arrow batch size off the cursor

# --- Adjustable extraction window (mirrors the old pipeline's date filter) ---
extraction:
  anchor_table: "CMS_CNOTE"
  anchor_date_column: "CNOTE_DATE"
  window:
    mode: "relative"          # "relative" | "explicit"
    # relative mode:
    anchor_month: "2026-03"   # YYYY-MM
    num_months: 1             # 1, 2, or 3 — bump up if disk allows
    # explicit mode (used only when mode == "explicit"):
    start_date: "2026-03-01"
    end_date:   "2026-04-01"  # half-open [start, end)

output:
  root: "data/bronze"
  format: "parquet"
  compression: "zstd"
  zstd_level: 9
  partition_by: ["window_start", "window_end", "extract_date", "run_id"]
  write_run_manifest: true

scoping:
  # How non-anchor tables are restricted to the window. See section 6.
  # Each table declares which stage it belongs to in table_specs.py.
  push_down: true             # resolve scope via Oracle subquery, not client-side IN-lists
  materialize_run_scope: true # create run-scoped Oracle scratch/scope tables for reusable keys
  scope_schema: "HOA"
  scope_table_prefix: "BRONZE_SCOPE_"
  reference_tables_mode: "full"   # "full" | "skip" — small lookup tables

columns:
  mode: "all"                # "all" | "exclude_pii"; first attempt should use "all"
  pii_exclude_file: "config/pii_exclude.yaml"

postgres:                     # optional sink for this layer
  enabled: false
  host: "localhost"           # "jne-postgres:5432" from inside the container
  database: "jne_bronze"
  schema: "bronze"
  load_batch_max_rows: 50000  # relational tables are narrow; OOM risk much lower than flat table
```

`config/pii_exclude.yaml` holds the ~35 PII columns grouped by table (`CMS_CNOTE`, `CMS_APICUST`, `CMS_CNOTE_POD`, `CMS_DRSHEET`, `ORA_USER`, `ORA_ZONE`, `LASTMILE_COURIER`). The authoritative column list comes from `JNE_Column_Business_Metadata_Validated.xlsx`. Illustrative entries: shipper/receiver name, all address lines, contact, phone; courier personal fields; user NIK. Keep this file ready, but do not enable it for the first extraction attempt unless full-column extraction fails or policy requires PII to be excluded before data leaves Oracle.

---

## 5. Adjustable Window Logic (`window.py`)

The old pipeline's window was a date filter baked into the SQL. Here it is config-driven and resolves to a half-open `[start, end)` date range. Use `CMS_CNOTE.CNOTE_DATE` as the authoritative anchor date for this relational bronze pipeline, even though older unification comments/scripts may reference `CNOTE_CRDATE`.

- **relative mode:** `start = first day of anchor_month`, `end = start + num_months`. Set `num_months: 1` now, raise to `2` or `3` if disk holds.
- **explicit mode:** uses `start_date` / `end_date` verbatim.

The resolved `(start, end)` becomes two bind variables (`:start`, `:end`) applied to `CMS_CNOTE.CNOTE_DATE`. Every downstream table inherits the same window through its scope predicate, so the window stays adjustable from one place.

---

## 6. Scoping Logic (`scoping.py` + `table_specs.py`)

This is the core design problem for a relational extract. Most child tables have no date column, so the window must propagate from `CMS_CNOTE` outward through the foreign keys shown in the CNOTE journey. Scope is resolved **on Oracle via materialized scope tables and subquery / EXISTS**, never as a client-side `IN` list (avoids Oracle's 1000-item limit and avoids pulling key sets across the wire).

Before extracting child tables, materialize reusable run scope tables in Oracle under `scoping.scope_schema`:

```sql
CREATE TABLE HOA.BRONZE_SCOPE_CNOTE_<run_id> NOLOGGING AS
SELECT CNOTE_NO
FROM JNE.CMS_CNOTE
WHERE CNOTE_DATE >= :start AND CNOTE_DATE < :end;
```

Additional scope tables can be created for bag, manifest, runsheet, DO, HVI, and SMU keys as needed. This keeps predicates push-downable, avoids repeated evaluation of the same anchor window in every table extract, and gives validation a concrete shipment population to count. Drop these scope tables at the end of a successful run, and also on retry cleanup.

Sanitize `run_id` before embedding it in Oracle table names. Airflow timestamps contain characters that are not valid in unquoted Oracle identifiers; prefer uppercase alphanumeric/underscore suffixes only.

Extraction runs in stages, each scoped by the prior:

**Stage 0 — anchor.** Extract `CMS_CNOTE` where `CNOTE_DATE >= :start AND CNOTE_DATE < :end`. This defines the shipment population for the run.

**Stage 1 — cnote-keyed tables.** Every table that carries a `*_CNOTE_NO` is scoped by:
```sql
WHERE <table>.<cnote_col> IN (
  SELECT CNOTE_NO FROM HOA.BRONZE_SCOPE_CNOTE_<run_id>
)
```
Tables: `CMS_APICUST`, `CMS_CNOTE_AMO`, `CMS_MRCNOTE`, `CMS_DRCNOTE`, `CMS_MHI_HOC`, `CMS_DHI_HOC`, `CMS_DSTATUS`, `CMS_CNOTE_POD`, `CMS_DHOV_RSHEET`, `CMS_MHOUNDEL_POD`, `CMS_DHOUNDEL_POD`, `CMS_DRSHEET`, `CMS_DRSHEET_PRA`, `CMS_DBAG_HO`, `CMS_DHOCNOTE`, `CMS_DHICNOTE`, `CMS_COST_DTRANSIT_AGEN`, `CMS_MFCNOTE`, `CMS_DCORRECT_DEST`.

**Stage 2 — bag / manifest / SMU layer.** Keyed by `BAG_NO` / `MAN_NO`, not cnote. Scope resolves through the cnote-linked bridge tables (`CMS_MFCNOTE`, `CMS_DBAG_HO`), e.g.:
```sql
WHERE <table>.<bag_col> IN (
  SELECT DBAG_NO FROM CMS_DBAG_HO
  WHERE DBAG_CNOTE_NO IN (SELECT CNOTE_NO FROM HOA.BRONZE_SCOPE_CNOTE_<run_id>)
)
```
Tables: `CMS_MANIFEST`, `CMS_MFBAG`, `CMS_DMBAG`, `CMS_MMBAG`, `CMS_DSMU`, `CMS_MSMU`, `CMS_COST_MTRANSIT_AGEN`.
Mind the corrected join keys from `CLAUDE.md`: `MFBAG_NO = DMBAG_BAG_NO`, `DMBAG_NO = DSMU_BAG_NO`, `DSMU_NO = MSMU_NO`, `DMBAG_BAG_NO = MMBAG_NO`.

**Stage 3 — runsheet / DO / HVI layer.** Keyed by runsheet/DO/SMU numbers, resolved through `CMS_MRSHEET`, `CMS_MSJ`, `CMS_RDSJ`, `CMS_MHICNOTE`, `CMS_MHOCNOTE`, and `CMS_DSJ`. Scope these masters by the keys collected from their stage-1/stage-2 detail tables.

**Stage 4 — reference / lookup tables.** No window. Extracted whole when `scoping.reference_tables_mode: full` or skipped when set to `skip`: `CMS_DROURATE`, `ORA_ZONE`, `ORA_USER`, `T_MDT_CITY_ORIGIN`, `LASTMILE_COURIER`. `CMS_DROURATE` is still a reference lookup table, but it is tens of millions of rows, so stream it with the same batch Parquet writer and do not load it into memory.

`table_specs.py` declares per table: source name, output name, scope stage, scope key column, and the column projection. In `columns.mode: all`, projection is every table column in source order. In `columns.mode: exclude_pii`, projection is all columns minus `pii_exclude.yaml`. The full table inventory and column lists are derived from `unify_jne_oracle.sql` (table set + join keys) and the validated metadata file (columns + PII flags).

---

## 7. Per-Table Extraction (`extractor.py`)

For each table:
1. Build the `SELECT` from `table_specs` (explicit column list from Oracle metadata, never `SELECT *`; first run uses full columns including PII).
2. Append the scope predicate for the table's stage with `:start` / `:end` binds.
3. Stream results as Arrow `RecordBatch`es off the cursor (`fetch_arraysize`).
4. Write incrementally to `data/bronze/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id>/<table>/` as Parquet, `zstd` level 9.
5. Record row count, file size, and elapsed time to the run manifest.

The Parquet writer can reuse the current `scripts/etl/export_oracle_parquet.py` pattern: stream in batches, create multiple `part-00001.parquet` files, use dictionary encoding, write `_SUCCESS`, and fail if the output directory already contains parts. Adapt it to Oracle cursors and table-specific schemas instead of the current transformed Postgres table.

---

## 8. Airflow DAG (`jne_bronze_extract_dag.py`)

```
resolve_window
      │
materialize_scope_keys       (Oracle scratch/scope tables for this run)
      │
extract_cms_cnote            (stage 0, anchor)
      │
extract_stage1_cnote_tables  (parallel task group, one task per table)
      │
extract_stage2_bag_manifest  (parallel task group)
      │
extract_stage3_runsheet_do   (parallel task group)
      │
extract_stage4_reference     (parallel task group)
      │
write_run_manifest
      │
cleanup_scope_keys
      │
[load_postgres]              (optional, gated by config.postgres.enabled)
```

Stage groups run sequentially (each depends on the prior stage's scope); tables **within** a group run in parallel. The window is resolved once at the top and passed down via XCom so the whole run shares one `[start, end)`. Scope key materialization should use the same `run_id`, so every extract points at the same key population.

---

## 9. Validation — Measuring the First Layer

Since the point is to evaluate bronze before committing to governance, the run manifest (`run_manifest.py`) should capture, per table and in total:

- **Row count** per table (sanity-check against expected month volume).
- **On-disk size** (compressed Parquet) per table and total — the headline disk-effectiveness number.
- **Extraction wall-clock time** per table and total.
- **Null density** per table (compare against the unified flat CSV's null density to quantify the storage saved by staying relational).
- **Scope integrity:** every child row's foreign key resolves to a stage-0 cnote (no orphans introduced by the scoping subqueries).

A short comparison of total bronze size and total extraction time against the current unified-CSV approach for the same month is the cleanest way to show the senior whether relational bronze actually wins on disk and speed.

---

## 10. Open Decisions (need sign-off before build)

1. **Window default:** start at `num_months: 1`, confirm disk headroom before raising to 2–3.
2. **Postgres landing this layer:** keep `postgres.enabled: false` and evaluate Parquet first, or land to Postgres immediately for inspection?
3. **Post-first-run PII behavior:** first extraction uses `columns.mode: all`; decide after the first run whether to switch to `exclude_pii`.
4. **`CMS_DROURATE` handling:** include as a full streamed reference extract or exclude from bronze if it dominates disk/time.
5. **Reference tables:** `full` vs `skip` — full makes bronze self-contained but adds the excluded-five tables back into storage.

---

## Appendix — Files to Seed the Coding Agent

Give the agent the **real source files**, not just this document (a prose context doc alone has already proven to miss detail):

- `unify_jne_oracle.sql` — reference for table set + join/scope keys (do not reproduce the join).
- `unify_oracle.py` — Oracle connection scaffold to refactor into `oracle_client.py`.
- `export_oracle_parquet.py` — Parquet writer pattern to adapt for Oracle table extracts.
- `docker-compose.yml` + Airflow DAG from the current repo — patterns to adapt.
- `CLAUDE.md` — current repo context and corrected join-key notes.
- `JNE_Column_Business_Metadata_Validated.xlsx` — column lists + PII flags.
- This document + the CNOTE journey diagram — the scoping map.

---

## Appendix — Canonical Source Table Inventory

Derived from `scripts/transformations/unify_jne_oracle.sql` in the current repo:

- `CMS_APICUST`
- `CMS_CNOTE`
- `CMS_CNOTE_AMO`
- `CMS_CNOTE_POD`
- `CMS_COST_DTRANSIT_AGEN`
- `CMS_COST_MTRANSIT_AGEN`
- `CMS_DBAG_HO`
- `CMS_DCORRECT_DEST`
- `CMS_DHICNOTE`
- `CMS_DHI_HOC`
- `CMS_DHOCNOTE`
- `CMS_DHOUNDEL_POD`
- `CMS_DHOV_RSHEET`
- `CMS_DMBAG`
- `CMS_DRCNOTE`
- `CMS_DROURATE`
- `CMS_DRSHEET`
- `CMS_DRSHEET_PRA`
- `CMS_DSJ`
- `CMS_DSMU`
- `CMS_DSTATUS`
- `CMS_MANIFEST`
- `CMS_MFBAG`
- `CMS_MFCNOTE`
- `CMS_MHICNOTE`
- `CMS_MHI_HOC`
- `CMS_MHOCNOTE`
- `CMS_MHOUNDEL_POD`
- `CMS_MMBAG`
- `CMS_MRCNOTE`
- `CMS_MRSHEET`
- `CMS_MSJ`
- `CMS_MSMU`
- `CMS_RDSJ`
- `LASTMILE_COURIER`
- `ORA_USER`
- `ORA_ZONE`
- `T_MDT_CITY_ORIGIN`
