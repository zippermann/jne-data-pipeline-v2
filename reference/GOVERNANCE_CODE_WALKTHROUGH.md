# Governance Code Walkthrough

This document explains the governance code in plain language so you can walk
someone through `config.py`, `scorer.py`, and `main.py`.

Short version:

```text
config.py = structured rule catalog plus generated runtime rule views
scorer.py = how each rule type is checked
main.py   = loads data, prepares aggregates, runs every check, writes scores
report.py = expands score JSON into inline and summary reporting CSVs
```

The design is intentionally generic. Instead of writing one function for every
index code, the project stores every static rule as a readable `Rule(...)`
entry in `config.py`, then generates the runtime dictionaries used by
`main.py` and `scorer.py`.

For example:

```text
COMP1B1, COMP1B2, COMP1B3, ...
```

all use the same scoring method:

```text
check_completeness()
```

The difference between those rules is the configured field and index metadata,
not the scoring logic.

## Important Data Shape

The governance scripts work on the unified shipment table. Each row represents
one shipment record after the operational source tables have been flattened
together.

The scorer produces one JSON-like dictionary per row per DQ element. Example:

```python
{"cnote_no": 1, "cnote_date": 1, "cnote_weight": 0}
```

This means:

```text
cnote_no passed
cnote_date passed
cnote_weight failed
```

The final output has columns like:

```text
completeness_json
completeness_score
validity_json
validity_score
overall_score
decision
```

## config.py

`config.py` is the rulebook. It answers:

```text
Which table is checked?
Which column is checked?
Which DQ element does it belong to?
Which index code does it map to?
```

The current version uses the structured v2 catalog design directly in the
original `config.py`. Static rules are visible as `Rule(...)` blocks, then the
familiar runtime dictionaries are generated from that catalog near the bottom
of the file:

```text
RULE_CATALOG -> COMPLETENESS_FIELDS
RULE_CATALOG -> CONSISTENCY_PAIRS
RULE_CATALOG -> VALIDITY_REGEX
RULE_CATALOG -> VALIDITY_DATETIMES
RULE_CATALOG -> TIMELINESS_RULES
RULE_CATALOG -> BACKDATE_CHAIN
RULE_CATALOG -> UNIQUENESS_KEYS
```

This gives reviewers one explicit place to inspect index codes while preserving
the old `main.py`/`scorer.py` interface.

### `Rule`

Each static rule is represented by a dataclass:

```python
Rule(
    index_code="COMP1B1",
    element="Completeness",
    rule_family="COMP1",
    table="CMS_CNOTE",
    columns=("cnote_no",),
    description="...",
    implementation="...",
)
```

Important fields:

```text
index_code     Excel governance code, such as COMP1B1
element        Accuracy, Completeness, Consistency, Timeliness, Validity, Uniqueness
rule_family    COMP1, COMP2, VALD4, TIME1, UNIQ2, etc.
table          source table or CROSS_TABLE
columns        unified-data columns used by the rule
description    human-readable rule meaning
implementation how the runtime dictionary/scorer handles it
```

### `RULE_CATALOG`

`RULE_CATALOG` is the review-facing source of truth for static rules. If
someone asks "where is this rule documented?", start there.

The runtime dictionaries are still present, but they are generated views.
That means a rule should be added to `RULE_CATALOG`, not manually duplicated
across several dictionaries.

### `ID_PATTERNS`

This section defines reusable regex patterns.

Example:

```python
"ALNUM": r"^[A-Z0-9]+$"
```

This means the value must contain only uppercase letters and digits.

Used by validity rules such as:

```python
Rule(..., regex_pattern=ID_PATTERNS["ALNUM"])  # VALD1B6
```

### `PRIMARY_KEYS`

This controls eligibility. A table's checks only apply to rows where that
table's primary key is present.

Example:

```python
"CMS_CNOTE": ["cnote_no"]
```

Explanation:

```text
For CMS_CNOTE checks, a row is eligible only if cnote_no is not empty.
```

If the primary key is empty, the row receives `None` for that table's checks
instead of being counted as pass or fail.

### `COMPLETENESS_FIELDS`

This generated dictionary lists mandatory fields for COMP1.

Example:

```python
"CMS_CNOTE": [
    "cnote_no",
    "cnote_date",
]
```

Explanation:

```text
For eligible CMS_CNOTE rows, cnote_no and cnote_date must be present.
```

Where to point for `COMP1B1`:

```text
config.py -> RULE_CATALOG -> Rule(index_code="COMP1B1", columns=("cnote_no",))
```

### `CONDITIONAL_COMPLETENESS`

This handles COMP2 rules where one field becomes required only if another
field exists.

Shape:

```python
(gate_column, required_column, label)
```

Explanation:

```text
If gate_column is non-empty, required_column must also be non-empty.
```

### `VALUE_CONDITIONAL_COMPLETENESS`

This handles COMP2 rules where one field becomes required only when another
field has a specific value.

Shape:

```python
(gate_column, gate_value, required_column, required_value, label)
```

Example meaning:

```text
If manifest_canceled = Y, then manifest_canceled_uid must be present.
```

### `CONSISTENCY_PAIRS`

This generated dictionary lists pairs of fields that must agree.

Example:

```python
("cnote_weight", "apicust_weight")  # CONS1B15
```

Explanation:

```text
If both values are present, they must match after cleanup.
```

This includes:

```text
CONS1: equivalent fields across tables
CONS2: operational cross-table equality
CONS3/CONS4: aggregate checks using helper columns from main.py
```

### `VALIDITY_REGEX`

This generated dictionary contains validity checks based on regex patterns.

Examples:

```python
"cnote_weight": ID_PATTERNS["NUMERIC"],  # VALD2B15
"cnote_shipper_zip": ID_PATTERNS["ZIP"], # VALD9B22
"dhov_rsheet_undel": r"^[01]$",          # VALD10S5
```

Rule families:

```text
VALD1  = alphanumeric/code format
VALD2  = numeric
VALD3  = numeric/range-style numeric
VALD5  = enum/value set
VALD6  = currency code
VALD7  = payment code
VALD8  = Y/N flag
VALD9  = ZIP/postcode
VALD10 = binary 1/0 flag
VALD11 = status code
VALD12 = branch ID
VALD13 = zone/user/location code
```

### `VALIDITY_DATETIMES`

This generated dictionary contains VALD4 checks.

Example:

```python
"CMS_CNOTE": ["cnote_date", "cnote_crdate"]  # VALD4B2, VALD4B99
```

Explanation:

```text
These fields must be parseable as dates or timestamps.
```

### `TIMELINESS_RULES`

These rules check that one date happens before another date.

Example shape:

```python
{"start": "cnote_crdate", "end": "mhi_approve_date", "assign_to": "cnote_crdate"}
```

Explanation:

```text
cnote_crdate must be earlier than or equal to mhi_approve_date.
The result is stored under cnote_crdate in the timeliness JSON.
```

### `BACKDATE_CHAIN`

This checks the cross-stage shipment lifecycle order.

Shape:

```python
(earlier_column, later_column, label)
```

Explanation:

```text
The earlier event must not happen after the later event.
```

### `UNIQUENESS_KEYS`

This defines uniqueness rules.

Examples:

```python
"CMS_CNOTE": [["cnote_no"]]  # UNIQ1B1
```

Single-column keys are UNIQ1. Multi-column keys are UNIQ2.

### Dynamic Manifest Config

`generate_manifest_config()` creates rules for manifest prefixes found in the
unified data, such as:

```text
om_
tm1_
tm2_
im_
hm_
```

This avoids hardcoding every possible transit manifest level.

## scorer.py

`scorer.py` is the execution engine. It answers:

```text
Given a dataframe and a set of configured rules, did each row pass or fail?
```

Each top-level function maps to one DQ element:

```text
check_completeness()
check_consistency()
check_validity()
check_timeliness()
check_uniqueness()
check_accuracy()
```

### Shared Helpers

#### `_clean(series)`

Purpose:

```text
Normalize values before comparing them.
```

It:

```text
converts to string
fills nulls with ""
removes trailing .0
strips whitespace
uppercases text
```

Why it matters:

```text
"abc", " ABC ", and "ABC" should compare the same.
"123.0" and "123" should often compare the same.
```

#### `_eligible_mask(df, pk_cols)`

Purpose:

```text
Decide which rows are eligible for a table's rules.
```

Example:

```python
pk_cols = ["cnote_no"]
```

If `cnote_no` is empty, the row is not eligible for CMS_CNOTE checks.

#### `_dup_mask(df, key_cols, eligible)`

Purpose:

```text
Find duplicate keys for uniqueness checks.
```

For composite keys, it joins values with `|` before checking duplicates.

#### `_safe_float(series)`

Purpose:

```text
Convert a column to numeric values without crashing on bad input.
```

Invalid values become `NaN`.

#### `_safe_dt(series)`

Purpose:

```text
Convert a column to datetime values without crashing on bad input.
```

Invalid values become `NaT`.

#### `_mask_rows(n, masks, eligible)`

Purpose:

```text
Convert boolean masks into the row-by-row JSON dictionaries.
```

Input example:

```python
masks = {
    "cnote_no": [True, True, False],
    "cnote_date": [True, False, False],
}
```

Output example:

```python
[
    {"cnote_no": 1, "cnote_date": 1},
    {"cnote_no": 1, "cnote_date": 0},
    None,
]
```

### `check_completeness()` Line-by-Line

This is the block from your screenshot.

Function signature:

```python
def check_completeness(
    df: pd.DataFrame,
    mandatory: List[str],
    pk_cols: List[str],
    conditional_rules: List[Tuple[str, str, str]],
    value_conditional_rules: List[Tuple],
) -> List[Optional[dict]]:
```

Explanation:

```text
df
```

The full unified dataframe.

```text
mandatory
```

List of fields that must be non-empty for COMP1.

Example:

```python
["cnote_no", "cnote_date", "cnote_weight"]
```

```text
pk_cols
```

Primary key columns used to decide if this row is eligible.

Example:

```python
["cnote_no"]
```

```text
conditional_rules
```

COMP2 rules where a required field depends on another field being present.

Shape:

```python
(gate_col, required_col, label)
```

```text
value_conditional_rules
```

COMP2 rules where a required field depends on another field having a specific
value.

Shape:

```python
(gate_col, gate_value, required_col, required_value, label)
```

```text
-> List[Optional[dict]]
```

The return type. One output item per dataframe row:

```text
dict = row had applicable completeness checks
None = row was not eligible or no checks applied
```

Inside the function:

```python
n = len(df)
```

Stores the number of rows. This is used to create output lists with the same
length as the input dataframe.

```python
eligible = _eligible_mask(df, pk_cols)
```

Creates a boolean mask showing which rows should be checked. For CMS_CNOTE,
this is true when `cnote_no` is present.

```python
mand_masks: Dict[str, pd.Series] = {}
```

Creates an empty dictionary to store one boolean mask per mandatory field.

Example after it is filled:

```python
{
    "cnote_no": Series([True, True, False]),
    "cnote_date": Series([True, False, True]),
}
```

```python
for col in mandatory:
```

Loops through every mandatory field from `config.py`.

```python
if col not in df.columns:
    continue
```

Skips rules whose column is not present in the current data. This prevents the
script from crashing when an optional table/column is absent.

```python
mand_masks[col] = _clean(df[col]).ne("")
```

This is the actual COMP1 check.

Explanation:

```text
clean the column
check that it is not equal to ""
```

So a value passes completeness if it is non-empty after cleanup.

```python
except Exception:
    pass
```

If one column causes an unexpected error, that column is skipped instead of
stopping the whole governance run.

Next block:

```python
cond_masks: Dict[str, Tuple[pd.Series, pd.Series]] = {}
```

Stores COMP2 gate-triggered checks. Each entry stores two masks:

```text
gate fired?
required field present?
```

```python
for gate_col, req_col, _label in conditional_rules:
```

Loops through conditional completeness rules.

```python
cond_masks[req_col] = (
    _clean(df[gate_col]).ne(""),
    _clean(df[req_col]).ne(""),
)
```

Creates two masks:

```text
1. gate_col is non-empty
2. req_col is non-empty
```

The required field is only included in the output for rows where the gate is
true.

Next block:

```python
val_masks: Dict[str, Tuple[pd.Series, pd.Series]] = {}
```

Stores COMP2 value-triggered checks.

```python
gate_clean = _clean(df[gate_col])
```

Cleans the gate column before comparing it.

```python
triggered = (
    gate_clean.ne("") if gate_value is None
    else gate_clean.eq(gate_value.upper())
)
```

If `gate_value` is `None`, the rule triggers when the gate column is simply
present. Otherwise, it triggers only when the gate column equals a specific
value.

```python
req_clean = _clean(df[req_col])
```

Cleans the required column.

```python
req_ok = (
    req_clean.ne("") if req_value is None
    else req_clean.eq(req_value.upper())
)
```

If `req_value` is `None`, the required column only needs to be present.
Otherwise, it must equal a specific required value.

```python
val_masks[label] = (triggered, req_ok)
```

Stores both masks under a readable label.

Final output block:

```python
out: List[Optional[dict]] = [None] * n
```

Creates one empty output slot per row.

```python
for idx in range(n):
```

Loops row by row to build the JSON dictionary for each shipment.

```python
if not bool(eligible.iloc[idx]):
    continue
```

If the row is not eligible, leave the output as `None`.

```python
obj: Dict[str, int] = {}
```

Creates the result dictionary for this row.

```python
for col, mask in mand_masks.items():
    obj[col] = int(bool(mask.iloc[idx]))
```

Adds COMP1 results. `True` becomes `1`, `False` becomes `0`.

```python
for col, (gate, req) in cond_masks.items():
    if bool(gate.iloc[idx]):
        obj[col] = int(bool(req.iloc[idx]))
```

Adds COMP2 gate-triggered results only when the gate fires.

```python
for label, (gate, req) in val_masks.items():
    if bool(gate.iloc[idx]):
        obj[label] = int(bool(req.iloc[idx]))
```

Adds COMP2 value-triggered results only when the value condition fires.

```python
out[idx] = obj if obj else None
```

If the row had any checks, store the result dictionary. If nothing applied,
store `None`.

```python
return out
```

Returns one result per input row.

Plain-English summary:

```text
check_completeness() takes the configured mandatory and conditional fields,
checks which rows are eligible, checks whether each required value is present,
and returns row-level pass/fail dictionaries.
```

### `check_consistency()`

This checks field pairs from `config.CONSISTENCY_PAIRS`.

Before comparing pairs, `scorer.py` can prepare the aggregate helper columns
needed by CONS3 and CONS4 if they are not already present. In the normal
`main.py` workflow those helpers are computed once up front for efficiency, so
the scorer fallback mainly protects direct/unit-style calls to
`check_consistency()`.

Important logic:

```python
app = l.ne("") & r.ne("")
masks[left] = l.eq(r) & app
```

Explanation:

```text
Only evaluate the rule when both fields are present.
Pass if the cleaned left value equals the cleaned right value.
```

If either side is empty, the rule is not applicable for that row.

### `check_validity()`

This checks two types of validity rules:

```text
regex validity
datetime validity
```

Regex example:

```python
masks[col] = _clean(df[col]).str.fullmatch(pattern, na=False)
```

Explanation:

```text
Clean the value and check if the whole value matches the configured pattern.
```

Datetime example:

```python
masks[col] = _safe_dt(df[col]).notna()
```

Explanation:

```text
Try to parse the value as a date.
Pass if parsing succeeds.
```

### `check_timeliness()`

This checks chronological order.

Core logic:

```python
app = ts.notna() & te.notna()
ok = app & (te >= ts)
```

Explanation:

```text
The rule applies only when both dates exist.
It passes when the end date is greater than or equal to the start date.
```

If multiple timeliness checks write to the same output field, the code ANDs
them together. That means the field passes only if all applicable timing checks
for that field pass.

### `check_uniqueness()`

This checks whether keys are duplicated.

Core logic:

```python
masks[label] = ~_dup_mask(df, present, eligible)
```

Explanation:

```text
_dup_mask returns True for duplicates.
The ~ operator flips it, so passing uniqueness means "not duplicated".
```

For composite keys, `_dup_mask()` combines columns with `|` before checking
duplicates.

### `check_accuracy()`

Accuracy is hardcoded because the checks are more custom.

Examples:

```text
ACCU1M5: dsmu_weight equals cnote_weight
ACCU4B15: cnote_weight is greater than or equal to 0
ACCU1A29: apicust_weight is greater than or equal to 0
ACCU6B6: cnote_services_code matches drourate_service
ACCU5A6: apicust_services_code matches drourate_service
```

DCORRECT destination correction data is not scored as Accuracy.  If the
unified input contains `dcorrect_destination`, `main.add_business_flags()` adds
informational flags to the output instead.  This lets analysts see shipments
with destination correction history without lowering the accuracy or overall
score.

Why these are not in `config.py` like the others:

```text
They need custom calculations or reference-join logic.
```

### `compute_scores()`

This converts raw pass/fail dictionaries into percentage scores.

For each element:

```text
json column = the raw per-field results
score column = passed checks / total checks
```

Example:

```python
{"cnote_no": 1, "cnote_date": 1, "cnote_weight": 0}
```

Score:

```text
2 passed / 3 total = 66.67
```

Overall score:

```text
average of all available element scores for the row
```

## main.py

`main.py` orchestrates the whole governance run.

### Imports

```python
import config as cfg
import scorer as sc
```

Explanation:

```text
cfg gives the rule lists.
sc gives the scoring functions.
```

### `prepare_aggregates(df)`

This adds helper columns needed for aggregate consistency checks.

Examples:

```text
mfbag_calculated_weight = sum of mfcnote_weight per mfbag_no
mmbag_calculated_qty = count of cnote_no per mmbag_no
msmu_calculated_weight = sum of dsmu_weight per msmu_no
msmu_calculated_qty = count distinct dsmu_bag_no per msmu_no
```

These columns are then compared against source header totals in consistency
rules.

The weight aggregates convert source values with `pd.to_numeric(...,
errors="coerce")` before summing. This avoids treating numeric-looking object
columns as strings.

### `load_and_prepare(path)`

Reads a CSV file:

```python
pd.read_csv(path, low_memory=False)
```

Then calls:

```python
prepare_aggregates(df)
```

### `load_from_postgres(table)`

Reads the unified table directly from Postgres:

```python
pd.read_sql_query(text(f"SELECT * FROM {table}"), conn)
```

Then calls:

```python
prepare_aggregates(df)
```

SQLAlchemy is imported lazily for this path only. CSV mode does not require the
database dependency to be installed.

### `run_dq(df, threshold)`

This is the main governance process.

Step 1:

```python
n = len(df)
cols = list(df.columns)
```

Get row count and column names.

Step 2:

```python
manifest_meta = cfg.generate_manifest_config(cols)
value_cond_rules = cfg.generate_value_conditionals(cols)
```

Create dynamic rules for manifest columns that exist in the data.

Step 3:

```python
all_tables = {**{t: {} for t in cfg.PRIMARY_KEYS}, **manifest_meta}
```

Build the list of static and dynamic tables to audit.

Step 4:

```python
acc: dict = {e: [None] * n for e in cfg.DQ_ELEMENTS}
```

Create storage for every DQ element.

Example:

```text
Accuracy -> one result slot per row
Completeness -> one result slot per row
...
```

Step 5:

```python
for table_name in all_tables:
```

Loop through every configured table.

For each table, get:

```text
primary key
mandatory fields
consistency pairs
validity regex rules
validity datetime rules
timeliness rules
uniqueness keys
```

Step 6:

```python
_merge_into("Completeness", sc.check_completeness(...))
_merge_into("Consistency", sc.check_consistency(...))
_merge_into("Validity", sc.check_validity(...))
_merge_into("Timeliness", sc.check_timeliness(...))
_merge_into("Uniqueness", sc.check_uniqueness(...))
```

Run every element for the table and merge the results into the accumulator.

Step 7:

```python
_merge_into("Completeness", sc.check_completeness(
    df, [], global_pk,
    cfg.CONDITIONAL_COMPLETENESS,
    value_cond_rules,
))
```

Run global conditional completeness rules.

Step 8:

```python
_merge_into("Timeliness", sc.check_timeliness(
    df, [], cfg.BACKDATE_CHAIN, global_pk,
))
```

Run lifecycle/backdate checks across shipment stages.

Step 9:

```python
_merge_into("Accuracy", sc.check_accuracy(df))
```

Run hardcoded accuracy checks.

Step 10:

```python
df_scores = sc.compute_scores(n, acc)
```

Turn pass/fail dictionaries into score columns.

Step 11:

```python
df_scores["decision"] = (
    df_scores["overall_score"].ge(threshold).map({True: "PASS", False: "FAIL"})
)
```

Create PASS/FAIL using the configured threshold.

### `main()`

This handles command-line arguments.

CSV mode:

```bash
python main.py --input unified_shipments.csv --output dq_output.csv
```

Postgres mode:

```bash
python main.py --from-postgres --output dq_output.csv
```

Then it:

```text
loads data
runs DQ
writes output CSV
prints summary stats
```

## How To Explain One Rule End-to-End

Example: `COMP1B1` for `CMS_CNOTE.cnote_no`.

1. Rule definition:

```text
config.py -> RULE_CATALOG -> Rule(index_code="COMP1B1", table="CMS_CNOTE", columns=("cnote_no",))
```

2. Eligibility:

```text
config.py -> PRIMARY_KEYS -> CMS_CNOTE -> ["cnote_no"]
```

3. Execution:

```text
scorer.py -> check_completeness()
```

4. Logic:

```text
Clean cnote_no and check it is not empty.
```

5. Output:

```python
{"cnote_no": 1}
```

or:

```python
{"cnote_no": 0}
```

6. Scoring:

```text
compute_scores() includes this result in completeness_score.
```

## Why There Are `try/except/pass` Blocks

You may be asked about this pattern:

```python
try:
    ...
except Exception:
    pass
```

Meaning:

```text
If one column or one rule fails unexpectedly, skip that rule and continue the
rest of the audit.
```

This makes the pipeline resilient, especially when some optional columns are
missing. The tradeoff is that errors can be quiet. A future improvement would be
logging skipped rules instead of silently passing.

## How To Defend The Generic Design

If someone asks why every index code does not have its own function:

```text
Most rules share the same method. COMP1 rules all mean "field must be present".
VALD regex rules all mean "value must match pattern". CONS rules all mean
"two fields must match". Writing one function per index would duplicate the same
logic hundreds of times.
```

Better explanation:

```text
Each rule has its own config entry, but rules with the same logic share one
scorer function. This keeps the code shorter, easier to test, and less likely
to drift.
```

## Files To Point To

For a rule:

```text
config.py -> RULE_CATALOG
```

For how it is checked:

```text
scorer.py
```

For how everything runs:

```text
main.py
```

For a readable one-rule-per-row catalog:

```text
rule_catalog.md
```

For generated reporting outputs:

```text
report.py
```
