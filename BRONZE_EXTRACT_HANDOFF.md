# Claude Code Handoff: Oracle → Parquet Bronze Extract Layer

## Context for the agent

You are working in the JNE data pipeline repo. Read `AGENTS.md` in the repo root
first — it has the source-table letter map, join chains, and project conventions.
Python executable on this machine is `python3`.

**What is changing and why.** The current pipeline unifies ~37 Oracle tables into
one flat 534-column table (`unify_jne_oracle.sql`) keyed on `cnote_no`, then runs
DQ governance on it. We are abandoning that as the backbone. The flat table is
~31% fully-null columns and ~47% >90%-null columns — denormalization tax we pay
and then discard. New direction:

```
Oracle (37 tables)
  → MinIO bronze: one Parquet per source table, NORMALIZED, no joins   ← YOU BUILD THIS
  → Postgres silver: 37 normalized tables at native grain (later)
  → transport_unified: NARROW join, transport tables only (later)
```

Unification is no longer the spine. It becomes one downstream consumer used only
for transport (bag/manifest/SMU relationships). **This task is only the bronze
extract layer — do not build any joins, pivots, or unification.**

---

## Deliverable

A single config-driven extract script that:
1. Reads a table manifest (list of Oracle tables + their PK/order column).
2. For each table, runs `SELECT * FROM <table>` against Oracle.
3. Writes the result to `bronze/<table>.parquet` in MinIO (S3-compatible).
4. Is idempotent (re-running overwrites the object), parallelizable across tables,
   and logs row counts written per table.

Do NOT recreate the flat table. Each Parquet stays at its source table's native
grain. No `cnote_no` join. No MFCNOTE pivot.

---

## Critical: prefix → source table mapping (read before coding)

The flat CSV uses column prefixes that mostly map 1:1 to source tables, but FOUR
groups are entangled. Direct extraction is correct precisely because it undoes
this entanglement automatically — but you must know it so the table manifest is
right.

| Flat prefix(es)              | Source table(s)            | Note                                                                 |
|------------------------------|----------------------------|----------------------------------------------------------------------|
| `om_ tm1_ tm2_ im_ hm_`      | **CMS_MFCNOTE** (one table)| These 5 prefixes are PIVOTED from ONE table by manifest type. Direct extract pulls CMS_MFCNOTE as-is (one row per manifest line). The OM/TM1/TM2/IM/HM split is a pivot we are dropping. Do NOT recreate the pivot. |
| `cost_d_`                    | CMS_COST_DTRANSIT_AGEN     | `cost_` prefix splits into two tables                                |
| `cost_m_`                    | CMS_COST_MTRANSIT_AGEN     |                                                                      |
| `dhi_`                       | CMS_DHIHOC                 | `dhi*` prefixes split — careful                                      |
| `dhicnote_`                  | CMS_DHICNOTE               |                                                                      |
| `mhi_`                       | CMS_MHIHOC                 | `mhi*` prefixes split                                                 |
| `mhicnote_`                  | CMS_MHICNOTE               |                                                                      |

All other prefixes map 1:1 (e.g. `cnote_`→CMS_CNOTE, `mfbag_`→CMS_MFBAG,
`dsmu_`→CMS_DSMU). Use the letter→table map in `AGENTS.md` as the authority for
full table names. Five tables were business-excluded previously
(LASTMILE_COURIER, T_MDT_CITY_ORIGIN, ORA_ZONE, ORA_USER, CMS_CNOTE_AMO) —
confirm with the user whether they stay excluded from bronze or are extracted
raw and excluded later. Default: extract them raw (bronze is the unfiltered
landing zone; exclusions belong in silver).

---

## File structure to create

```
scripts/extract/
  tables.yaml          — the manifest: one entry per Oracle table
  extract_bronze.py    — the runner
  README.md            — how to run
```

### `tables.yaml` schema

```yaml
# Each entry: oracle table name → output parquet name + extraction hints.
# pk is used only for deterministic ordering and (later) incremental extracts.
# If unsure of pk, leave null — full SELECT * with no ORDER BY still works.
tables:
  - oracle: CMS_CNOTE
    out: cnote
    pk: CNOTE_NO
  - oracle: CMS_MFCNOTE
    out: mfcnote
    pk: null          # multi-row per manifest; confirm PK with user
  - oracle: CMS_MFBAG
    out: mfbag
    pk: MFBAG_NO
  # ... agent: generate the full list from AGENTS.md letter map.
  #     Leave pk: null where AGENTS.md does not state it; do NOT guess PKs.
```

**Agent instruction:** generate the complete `tables.yaml` from the AGENTS.md
letter→table map (all ~37 tables). For every `pk` you cannot source from
AGENTS.md, set `pk: null` and add a `# TODO confirm PK` comment. Do not invent
primary keys.

### `extract_bronze.py` requirements

- CLI: `python3 extract_bronze.py --config tables.yaml --only cnote,mfbag`
  (`--only` optional, comma-separated `out` names; default = all).
- Oracle connection via `oracledb` (thin mode preferred, no Instant Client).
  Read connection params from env vars: `ORACLE_DSN`, `ORACLE_USER`,
  `ORACLE_PASSWORD`. Never hardcode credentials.
- MinIO/S3 write via `boto3` or `s3fs`. Read `MINIO_ENDPOINT`,
  `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` from env. Write to
  `s3://<bucket>/bronze/<out>.parquet`.
- Use pandas + pyarrow: `df = pd.read_sql(f"SELECT * FROM {tbl}", con)` then
  `df.to_parquet(...)`. For large tables, chunk with `pd.read_sql(...,
  chunksize=N)` and write a partitioned dataset
  (`bronze/<out>/part-000.parquet`) rather than one object — make chunk size a
  CLI flag `--chunksize` defaulting to 100000.
- Parallelize across tables with a thread pool (I/O-bound). Flag
  `--workers` default 4. Do not parallelize chunks within one table.
- After each table: log `<out>: <n_rows> rows → <path>`. At the end, print a
  summary table of all tables + row counts. Exit non-zero if any table failed,
  but continue extracting the others first (collect errors, report at end).
- Idempotent: overwrite existing objects. No append.

### `README.md`

Document the env vars, an example run, and a one-paragraph note that bronze is
the raw normalized landing zone — no joins, no exclusions, no DQ.

---

## Things NOT to do (guardrails)

- Do NOT join tables or recreate `unify_jne_oracle.sql` logic.
- Do NOT pivot CMS_MFCNOTE into OM/TM1/TM2/IM/HM.
- Do NOT apply the 5-table business exclusion at bronze (default: extract raw).
- Do NOT guess Oracle PKs — use `null` + TODO where unknown.
- Do NOT hardcode any credentials.
- Do NOT add DQ scoring here — bronze is extract-only.

---

## When done

Report back: (1) the generated `tables.yaml` table count vs. the ~37 expected,
(2) which tables have `pk: null` needing user confirmation, (3) a dry-run plan
(don't actually hit Oracle unless env vars are set) showing what would be
extracted. Then stop and wait for the user to confirm the manifest before any
real extraction run.
