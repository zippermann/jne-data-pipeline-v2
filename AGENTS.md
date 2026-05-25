# JNE Data Pipeline — Codex Context

## Project Overview

End-to-end logistics data pipeline for JNE (Indonesian courier). Raw data is extracted from an Oracle database, unified into a single flat CSV, then assessed by a 6-element data quality (DQ) governance framework.

**Python executable on this machine: `python3` (not `py` or `python`)**

---

## Directory Structure

```
scripts/
  transformations/unify_jne_oracle.sql   — Oracle SQL that produces the unified flat table
  etl/unify_oracle.py                    — Python runner for the SQL
  governance/
    config.py     — All DQ rules (the only file that needs editing for rule changes)
    scorer.py     — Scoring logic; one function per DQ element
    main.py       — Entry point: load → prepare helpers → score → write reports
    JNE Index List.xlsx  — Master index of 618 test codes
data/
  unified_shipments_*.csv               — extracted sample files (gitignored — may contain PII)
  dq_output.csv / _inline.csv / _summary.csv / _integrity.csv  — DQ outputs (gitignored)
```

---

## Running Governance Locally

Always output to `data/`. Replace the filename with whichever sample CSV you have locally:

```bash
cd scripts/governance

python3 main.py \
  --input  ../../data/<your_sample>.csv \
  --output ../../data/dq_output.csv \
  --threshold 85.0
```

This produces four files: `dq_output.csv`, `dq_output_inline.csv`,
`dq_output_summary.csv`, and `dq_output_integrity.csv`.

---

## Pipeline Execution (Docker)

The full Oracle → Postgres pipeline runs via:

```bash
docker compose --profile init up data-init --build
```

The `jne-data-init` container runs `unify_oracle.py` then `copy_to_postgres.py`. With 3.7M rows and 500+ columns, the Postgres copy step is memory-intensive. `LOAD_BATCH_MAX_ROWS` is set to `10000` in `docker-compose.yml` to prevent OOM kills (the default of 50000 is too large for a fully-populated flat table).

---

## Governance Scaling for the 60M-Row Dataset

The governance scorer is being prepared for a dataset of approximately 60
million rows. The implemented production path is a resumable, checkpointed
batch run over PostgreSQL input. Daily incremental assessment plus periodic
full revalidation remains a future design step. Production output now omits
the full inline artifact and uses a compact summary containing counters and
failed-check JSON instead of hundreds of PASS/FAIL columns.

### Current implementation verified on `main`

- `scripts/governance/main.py` already supports PostgreSQL streaming through
  `--from-postgres --batch-size`.
- `docker-compose.yml` and `airflow/dags/jne_etl_pipeline.py` currently run DQ
  with `DQ_BATCH_SIZE` / `dq_batch_size`, defaulting to `2000` rows per pandas
  scoring batch.
- Production invocation uses `--resumable --resume --compact-summary`,
  records run/batch checkpoints in PostgreSQL, and fetches only unfinished
  deterministic row partitions when a run resumes with the same `run_id`.
- Cross-batch correctness already requires database-wide helper context.
  `main.py::_prepare_postgres_scoring_query()` prepares aggregate helpers for
  `mfbag_calculated_weight`, `mmbag_calculated_qty`,
  `msmu_calculated_weight`, and `msmu_calculated_qty`, plus helpers for global
  uniqueness checks before row batches are scored.

### Verified scaling pressure points

- On the current 665-column local sample, governance detects 946 active index
  rule columns and prepares 77 uniqueness helper specifications plus the four
  aggregate helpers.
- The 1,000-row sample outputs extrapolate to about `3.6 GB` for
  `dq_output.csv` and about `135 GB` for the current 946-column
  `dq_output_summary.csv` at 60 million rows, before database/index overhead.
- The implemented compact summary reduces the same dirty-sample projection to
  about `92 GB`; retained per-shipment failed-value detail remains a material
  storage cost.
- `dq_output_inline.csv` is intentionally out of scope for the
  production-scale output path because it includes source data interleaved
  with check columns and is substantially larger.

### Planning constraints for the refactor

- Do not evaluate aggregate or uniqueness rules on isolated chunks without
  full-run or affected-key context; that would produce incorrect results at
  batch boundaries.
- Incremental runs must account for prior rows that share changed uniqueness,
  bag, or SMU keys, or explicitly defer those corrections to a full
  revalidation.
- Preserve compact row-level scores and integrity reporting; decide how to
  replace or store the wide per-shipment summary before implementing the
  production output contract.

---

## Index Code System

Every rule in `config.py` carries a trailing comment with its Excel code.

**Format:** `ELEMENT + RULE_TYPE + TABLE_LETTER + FIELD_NUMBER`

| Element prefix | Meaning       |
|----------------|---------------|
| COMP           | Completeness  |
| CONS           | Consistency   |
| VALD           | Validity      |
| TIME           | Timeliness    |
| UNIQ           | Uniqueness    |
| ACCU           | Accuracy      |

**Table letter → source table mapping:**

| Letter | Table                   | Letter | Table                    |
|--------|-------------------------|--------|--------------------------|
| A      | CMS_APICUST             | B      | CMS_CNOTE                |
| C      | CMS_MRCNOTE             | D      | CMS_DRCNOTE              |
| E      | CMS_MHIHOC              | F      | CMS_DHIHOC               |
| G      | CMS_DSTATUS             | H      | CMS_MANIFEST             |
| I      | CMS_MFCNOTE             | J      | CMS_MFBAG                |
| K      | CMS_DMBAG               | L      | CMS_MMBAG                |
| M      | CMS_DSMU                | N      | CMS_MSMU                 |
| O      | CMS_DRSHEET_PRA         | P      | CMS_DRSHEET              |
| Q      | CMS_MRSHEET             | R      | CMS_CNOTE_POD            |
| S      | CMS_DHOV_RSHEET         | T      | CMS_MHOUNDEL_POD         |
| U      | CMS_DHOUNDEL_POD        | V      | CMS_MHOCNOTE             |
| W      | CMS_DHOCNOTE            | X      | CMS_COST_MTRANSIT_AGEN   |
| Y      | CMS_COST_DTRANSIT_AGEN  | Z      | CMS_MSJ                  |
| AA     | CMS_DSJ                 | AB     | CMS_MHICNOTE             |
| AC     | CMS_DHICNOTE            | AD     | CMS_RDSJ                 |
| AE     | CMS_DBAG_HO             |        |                          |

**Rule type numbers** (within each element):

- COMP: 1 = unconditional mandatory, 2 = conditional/gate-triggered
- CONS: 1 = cross-table field agreement, 2 = value equality, 3/4 = aggregate check
- VALD: 1 = ALNUM format, 2 = numeric, 3 = numeric range, 4 = datetime, 5 = enum, 6 = currency, 7 = payment code, 8 = Y/N flag, 9 = ZIP, 11 = status code, 12 = branch ID, 13 = zone code
- TIME: 1 = within-stage ordering, cross-stage = BACKDATE_CHAIN
- UNIQ: 1 = single-column key, 2 = composite key
- ACCU: 1 = field equality, 2/3 = reference lookup, 4 = range check, 6 = cross-reference

---

## Architecture: How the Flat Table Works

`unify_jne_oracle.sql` joins 30+ CMS source tables and pivots manifest types into columns. The result is one row per `cnote_no` (consignment note), with all table fields prefixed by a short alias (e.g., `mhi_`, `dsmu_`, `om_`, `tm1_`).

**Manifest columns** are dynamically pivoted via `mfcnote_pivoted` CTE for five types:
- `OM` — Outbound Manifest
- `TM1`, `TM2` — Transit Manifests (sequenced by date)
- `IM` — Inbound Manifest
- `HM` — House Manifest

Each produces columns: `{pfx}_man_no`, `{pfx}_mfc_no`, `{pfx}_manifest_date`, `{pfx}_manifest_crdate`, `{pfx}_manifest_route`, etc.

`main.py::load_and_prepare()` pre-computes four aggregate columns before scoring:
- `mfbag_calculated_weight` — sum of `mfcnote_weight` per `mfbag_no`
- `mmbag_calculated_qty` — count of `cnote_no` per `mmbag_no`
- `msmu_calculated_weight` — sum of `dsmu_weight` per `msmu_no` (used in CONS3N10)
- `msmu_calculated_qty` — nunique `dsmu_bag_no` per `msmu_no` (used in CONS4N9)

**Bag/SMU join chain** (corrected — these were previously wrong and produced empty columns):
- `CMS_MFBAG` → `CMS_DMBAG`: `mfbag.MFBAG_NO = dmbag.DMBAG_BAG_NO` (not `DMBAG_NO`)
- `CMS_DMBAG` → `CMS_DSMU`: `dmbag.DMBAG_NO = dsmu.DSMU_BAG_NO`
- `CMS_DSMU` → `CMS_MSMU`: `dsmu.DSMU_NO = msmu.MSMU_NO` (shared SMU number, not a FK column)
- `CMS_DMBAG` → `CMS_MMBAG`: `dmbag.DMBAG_BAG_NO = mmbag.MMBAG_NO`

**Computed columns** added by the `data-quality-scoring` branch merge:
- `HANDOVER_COUNT` — count of non-null handover events (MRCNOTE, DHI, DHOCNOTE, DRSHEET, TM1, TM2)
- `DELIVERY_TYPE` — `'Direct'` if no transit manifests, `'Transit'` otherwise
- `TRANSIT_MANIFEST_COUNT` — number of transit manifests (0, 1, or 2)

---

## Governance Framework: config.py

All rule changes happen in `config.py`. Structure:

| Section                        | Purpose                                            |
|--------------------------------|----------------------------------------------------|
| `PRIMARY_KEYS`                 | Which column gates row eligibility per table       |
| `COMPLETENESS_FIELDS`          | COMP1 — unconditional mandatory fields             |
| `CONDITIONAL_COMPLETENESS`     | COMP2 — gate-triggered (non-empty gate → field required) |
| `VALUE_CONDITIONAL_COMPLETENESS` | COMP2 — value-triggered (gate == value → field required) |
| `CONSISTENCY_PAIRS`            | CONS — pairs that must match                       |
| `VALIDITY_REGEX`               | VALD — regex format checks                         |
| `VALIDITY_DATETIMES`           | VALD4 — fields that must be parseable as dates     |
| `TIMELINESS_RULES`             | TIME1 — within-stage start ≤ end checks            |
| `BACKDATE_CHAIN`               | TIME — cross-stage lifecycle ordering (12 steps)   |
| `UNIQUENESS_KEYS`              | UNIQ — fields/composite keys that must be unique   |
| `generate_manifest_config()`   | Dynamic COMP1H/I, VALD1H/I, TIME, UNIQ for manifests |
| `generate_value_conditionals()`| Dynamic COMP2H17 — canceled manifest requires UID  |

**Accuracy checks** have no declarative config — all logic is hardcoded in `scorer.py::check_accuracy()`.

---

## Known Gaps and Deferred Checks

### Deferred — reference table not yet joined into unified table
| Code     | Field                      | Check                                                              |
|----------|----------------------------|--------------------------------------------------------------------|
| ACCU2B12 | `cnote_origin`             | Must exist in `CMS_DCORRECT_DESTINATION`                          |
| ACCU3B13 | `cnote_destination`        | Must exist in `CMS_DCORRECT_DESTINATION`                          |

### Deferred — manifest prefix not resolved in flat table joins
| Code     | Check                                               |
|----------|-----------------------------------------------------|
| CONS2J10 | `mfbag_route` vs `{pfx}_manifest_route`             |
| CONS2K3  | `dmbag_origin` vs `{pfx}_manifest_from`             |
| CONS2K4  | `dmbag_destination` vs `{pfx}_manifest_thru`        |

### Not yet implemented — conditional completeness (COMP2)
Gate conditions unclear — no resolvable gate column in the flat table:

| Code       | Table              | Field                  | Gate (from index)                        |
|------------|--------------------|------------------------|------------------------------------------|
| COMP2S3    | CMS_DHOV_RSHEET    | `dhov_rsheet_do`       | If CNOTE is a special request by shipper |
| COMP2S5    | CMS_DHOV_RSHEET    | `dhov_rsheet_undel`    | If CNOTE is Undelivered                  |
| COMP2Y11   | CMS_COST_DTRANSIT  | `dmanifest_doc_ref`    | If MANIFEST_NO is MTI                    |

### Recently implemented checks
- **UNIQ1B74** (`cnote_refno`) and **UNIQ1G2** (`dstatus_cnote_no`) added to `UNIQUENESS_KEYS` in `config.py`.
- **ACCU5A6** (`apicust_services_code`) and **ACCU6B6** (`cnote_services_code`) implemented in `scorer.py::check_accuracy()`. Both compare against `drourate_service` from the pre-joined `CMS_DROURATE` (joined on `cnote_route_code + cnote_services_code`). ACCU5A6 is a proxy check — it flags rows where apicust's service code doesn't match the drourate-validated code for that shipment.

### Excel index errors / artifacts (continued)
Note: **VALD1A4** (`apicust_branch` ALNUM format) appears in the index alongside the already-implemented **VALD12A4** (same field, same regex). The index has two separate entries for the same check under different rule-type numbers; VALD12A4 is implemented and covers it.

### Design decisions — headers repeat in the flat table (intentional exclusion)
These are in the Excel index but deliberately not checked for uniqueness because the header row legitimately repeats once per shipment in the denormalised flat table:

| Code    | Field(s)                                       |
|---------|------------------------------------------------|
| UNIQ1C1 | `mrcnote_no` (receipt header)                  |
| UNIQ1H1 | `{pfx}_man_no` (manifest header)               |
| UNIQ1P1 | `drsheet_no` (runsheet header)                 |
| UNIQ1Z1 | `msj_no` (delivery order header)               |

### Manifest rules — implemented but unannotated
`generate_manifest_config()` implements the COMP1H\*, COMP1I\*, VALD1H\*, TIME1H\*, and UNIQ1I\* families at runtime. The column names follow `{pfx}_*` patterns. No individual index code comments appear in the function body because the specific field numbers vary by prefix.

### Excel index errors / artifacts
- **ACCU1A29**: Index labels field 29 as `apicust_weight`, but field 29 is `apicust_cod_amount`; weight is field 25 (ACCU1A25). The script is correct; the index has the wrong field number.
- **UNIQ2D / UNIQ2J / UNIQ2AA / UNIQ2U**: In the Excel file these appear with large numbers (e.g., UNIQ2D46024) because the field number cell contains a date serial. These are implemented correctly as composite key checks.
- **VALD1A4 + VALD12A4**: The index contains two separate entries for `apicust_branch` (rule types 1 and 12, same ALNUM regex). Only VALD12A4 is implemented; VALD1A4 is a duplicate entry in the index.

---

## Validity Check Policy

As of the last audit, all entries in `VALIDITY_REGEX` and `VALIDITY_DATETIMES` must carry a VALD index code comment. Rules with no code were removed. Do not add new validity checks without a corresponding code from `JNE Index List.xlsx`.

---

## ID Pattern Reference

Named patterns in `ID_PATTERNS` (used across validity rules):

| Key     | Pattern                          | Meaning                    |
|---------|----------------------------------|----------------------------|
| RCC     | `^[A-Z]{3}/RCC/\d{8}$`          | Receipt note number        |
| RHC     | `^[A-Z]{3}/RHC/\d{8}$`          | HO receipt number          |
| HC      | `^[A-Z]{3}/HC/\d{9}$`           | HO consignment number      |
| ZIP     | `^\d{5}$`                        | 5-digit postcode           |
| ALNUM   | `^[A-Z0-9]+$`                    | Alphanumeric code          |
| YESNO   | `^(Y\|N)$`                       | Boolean flag               |
| RIG     | `^[A-Z]{3}/IRG/\d{8}$`          | Irregular shipment number  |
| OM      | `^[A-Z]{3}/OM/\d+$`             | Outbound manifest number   |
| DRI     | `^[A-Z]{3}/DRI/\d{8}$`          | Delivery runsheet number   |
| DO      | `^[A-Z]{3}/DO/\d{8}$`           | Delivery order number      |
| STATUS  | `^[A-Z]{1,2}\d{1,2}$`           | Status code                |
| BAG     | `^[A-Z0-9]+/\d+$`               | Bag number                 |
| NUMERIC | `^\d+(\.\d+)?$`                  | Non-negative number        |
