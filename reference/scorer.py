"""
JNE DQ Rules
============

This file contains the executable rule functions for the structured governance
system.  It is organized around explicit rule families:

    COMP -> Completeness
    CONS -> Consistency
    VALD -> Validity
    TIME -> Timeliness
    UNIQ -> Uniqueness
    ACCU -> Accuracy

The exact table, field, and index-code mapping lives in config.py.
This file explains where each rule family is evaluated and how pass/fail values
are produced.
"""

from typing import Dict, List, Optional, Tuple

import hashlib
import pandas as pd


# =============================================================================
# Shared Helpers
# =============================================================================

ScoreRows = List[Optional[dict]]
MaskMap = Dict[str, pd.Series]
DQ_ELEMENTS = [
    "Accuracy", "Completeness", "Consistency",
    "Timeliness", "Validity", "Uniqueness",
]


def _clean(series: pd.Series) -> pd.Series:
    """Normalize values before comparisons."""
    return (
        series.astype("string")
        .fillna("")
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.upper()
    )


def _safe_float(series: pd.Series) -> pd.Series:
    """Convert values to numeric, returning NaN for invalid values."""
    return pd.to_numeric(series, errors="coerce")


def _safe_dt(series: pd.Series) -> pd.Series:
    """Convert values to datetimes, returning NaT for invalid values."""
    return pd.to_datetime(series, errors="coerce")


def _eligible_mask(df: pd.DataFrame, pk_cols: List[str]) -> pd.Series:
    """
    A row is eligible for a table's checks when all configured primary-key
    columns are present and non-empty.
    """
    present_pk = [c for c in pk_cols if c in df.columns]
    if not present_pk:
        return pd.Series(False, index=df.index)

    parts = [_clean(df[c]).ne("") for c in present_pk]
    return pd.concat(parts, axis=1).all(axis=1)


def _empty_result(n: int) -> ScoreRows:
    return [None] * n


def _mask_rows(n: int, masks: MaskMap, eligible: pd.Series) -> ScoreRows:
    """Convert {label: boolean-mask} into per-row {label: 1/0} dictionaries."""
    out = _empty_result(n)
    for idx in range(n):
        if not bool(eligible.iloc[idx]):
            continue
        row_result = {label: int(bool(mask.iloc[idx])) for label, mask in masks.items()}
        out[idx] = row_result or None
    return out


def _applicable_mask_rows(
    n: int,
    masks: MaskMap,
    applicable: MaskMap,
    eligible: pd.Series,
) -> ScoreRows:
    """
    Convert masks into row dictionaries, but only include a rule when its
    applicability mask is true for that row.
    """
    out = _empty_result(n)
    for idx in range(n):
        if not bool(eligible.iloc[idx]):
            continue
        row_result = {
            label: int(bool(masks[label].iloc[idx]))
            for label in masks
            if bool(applicable[label].iloc[idx])
        }
        out[idx] = row_result or None
    return out


def _has_column(df: pd.DataFrame, column: str) -> bool:
    return column in df.columns


def _has_columns(df: pd.DataFrame, columns: List[str]) -> bool:
    return all(c in df.columns for c in columns)


# =============================================================================
# COMP - Completeness
# =============================================================================

# Rule families implemented here:
#   COMP1: mandatory field must be present and non-empty.
#          Rules are listed in config.COMPLETENESS_FIELDS.
#   COMP2: field is required only when a gate condition fires.
#          Rules are listed in config.CONDITIONAL_COMPLETENESS and
#          config.VALUE_CONDITIONAL_COMPLETENESS.
#
# Example place to point:
#   config.COMPLETENESS_FIELDS["CMS_CNOTE"]["cnote_no"] -> COMP1B1


def _comp1_masks(df: pd.DataFrame, mandatory: List[str]) -> MaskMap:
    """Build COMP1 mandatory-field masks."""
    masks: MaskMap = {} #dictionary of {field: boolean mask} for mandatory fields
    for col in mandatory:
        if not _has_column(df, col):
            continue
        try:
            masks[col] = _clean(df[col]).ne("")
        except Exception:
            pass
    return masks


def _comp2_gate_masks(
    df: pd.DataFrame,
    conditional_rules: List[Tuple[str, str, str]],
) -> Dict[str, Tuple[pd.Series, pd.Series]]:
    """Build COMP2 masks for non-empty gate columns."""
    masks: Dict[str, Tuple[pd.Series, pd.Series]] = {}
    for gate_col, req_col, _label in conditional_rules:
        if not _has_columns(df, [gate_col, req_col]):
            continue
        try:
            masks[req_col] = (_clean(df[gate_col]).ne(""), _clean(df[req_col]).ne(""))
        except Exception:
            pass
    return masks


def _comp2_value_masks(
    df: pd.DataFrame,
    value_conditional_rules: List[Tuple],
) -> Dict[str, Tuple[pd.Series, pd.Series]]:
    """Build COMP2 masks for value-triggered conditions."""
    masks: Dict[str, Tuple[pd.Series, pd.Series]] = {}
    for gate_col, gate_value, req_col, req_value, label in value_conditional_rules:
        if not _has_columns(df, [gate_col, req_col]):
            continue
        try:
            gate_clean = _clean(df[gate_col])
            req_clean = _clean(df[req_col])
            gate_fired = (
                gate_clean.ne("") if gate_value is None
                else gate_clean.eq(gate_value.upper())
            )
            req_ok = (
                req_clean.ne("") if req_value is None
                else req_clean.eq(req_value.upper())
            )
            masks[label] = (gate_fired, req_ok)
        except Exception:
            pass
    return masks


def check_completeness(
    df: pd.DataFrame,
    mandatory: List[str],
    pk_cols: List[str],
    conditional_rules: List[Tuple[str, str, str]],
    value_conditional_rules: List[Tuple],
) -> ScoreRows:
    """
    Return per-row completeness results.

    Output keys are field names for COMP1 and COMP2 gate-triggered checks, or
    configured labels for value-triggered COMP2 checks.
    """
    n = len(df)
    eligible = _eligible_mask(df, pk_cols)

    comp1 = _comp1_masks(df, mandatory)
    comp2_gate = _comp2_gate_masks(df, conditional_rules)
    comp2_value = _comp2_value_masks(df, value_conditional_rules)

    out = _empty_result(n)
    for idx in range(n):
        if not bool(eligible.iloc[idx]):
            continue

        row_result: Dict[str, int] = {}

        # COMP1: always evaluated for eligible rows.
        for field, mask in comp1.items():
            row_result[field] = int(bool(mask.iloc[idx]))

        # COMP2: evaluated only when the configured gate fires.
        for field, (gate_fired, req_ok) in comp2_gate.items():
            if bool(gate_fired.iloc[idx]):
                row_result[field] = int(bool(req_ok.iloc[idx]))

        for label, (gate_fired, req_ok) in comp2_value.items():
            if bool(gate_fired.iloc[idx]):
                row_result[label] = int(bool(req_ok.iloc[idx]))

        out[idx] = row_result or None

    return out


# =============================================================================
# CONS - Consistency
# =============================================================================

# Rule families implemented here:
#   CONS1: equivalent fields across tables must agree.
#   CONS2: operational cross-table values must agree.
#   CONS3: aggregate value checks, with expected values calculated below.
#   CONS4: aggregate count checks, with expected values calculated below.
#
# Field pairs and exact index codes live in config.CONSISTENCY_PAIRS.


def _prepare_consistency_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add calculated columns needed by CONS3 and CONS4 rules.

    Keeping these calculations beside check_consistency() keeps the rule family,
    expected-value calculation, and pass/fail comparison in one rules module.
    """
    work = df

    # CONS3J3 / CONS3J4:
    # Expected MFBAG weight = sum(mfcnote_weight) for the same mfbag_no.
    if (
        "mfbag_calculated_weight" not in work.columns
        and _has_columns(work, ["mfbag_no", "mfcnote_weight"])
    ):
        work = work.copy()
        tmp = work[["mfbag_no"]].copy()
        tmp["mfcnote_weight_num"] = _safe_float(work["mfcnote_weight"])
        agg = tmp.groupby("mfbag_no", dropna=False)["mfcnote_weight_num"].sum().reset_index()
        agg.columns = ["mfbag_no", "mfbag_calculated_weight"]
        work = work.merge(agg, on="mfbag_no", how="left")

    # CONS4L6:
    # Expected MMBAG quantity = count(cnote_no) for the same mmbag_no.
    if (
        "mmbag_calculated_qty" not in work.columns
        and _has_columns(work, ["mmbag_no", "cnote_no"])
    ):
        work = work.copy()
        agg = work.groupby("mmbag_no", dropna=False)["cnote_no"].count().reset_index()
        agg.columns = ["mmbag_no", "mmbag_calculated_qty"]
        work = work.merge(agg, on="mmbag_no", how="left")

    # CONS3N10:
    # Expected MSMU weight = sum(dsmu_weight) for the same msmu_no.
    if (
        "msmu_calculated_weight" not in work.columns
        and _has_columns(work, ["msmu_no", "dsmu_weight"])
    ):
        work = work.copy()
        tmp = work[["msmu_no"]].copy()
        tmp["dsmu_weight_num"] = _safe_float(work["dsmu_weight"])
        agg = tmp.groupby("msmu_no", dropna=False)["dsmu_weight_num"].sum().reset_index()
        agg.columns = ["msmu_no", "msmu_calculated_weight"]
        work = work.merge(agg, on="msmu_no", how="left")

    # CONS4N9:
    # Expected MSMU quantity = count of distinct dsmu_bag_no for the same msmu_no.
    if (
        "msmu_calculated_qty" not in work.columns
        and _has_columns(work, ["msmu_no", "dsmu_bag_no"])
    ):
        work = work.copy()
        agg = work.groupby("msmu_no", dropna=False)["dsmu_bag_no"].nunique().reset_index()
        agg.columns = ["msmu_no", "msmu_calculated_qty"]
        work = work.merge(agg, on="msmu_no", how="left")

    return work


def check_consistency(
    df: pd.DataFrame,
    pairs: List[Tuple[str, str]],
    pk_cols: List[str],
) -> ScoreRows:
    """
    Return per-row consistency results.

    A pair is applicable only when both sides are non-empty.  The output key is
    the left-hand field name from the configured pair.
    """
    df = _prepare_consistency_aggregates(df)
    n = len(df)
    eligible = _eligible_mask(df, pk_cols)
    masks: MaskMap = {}
    applicable: MaskMap = {}

    for left, right in pairs:
        if not _has_columns(df, [left, right]):
            continue
        try:
            left_clean = _clean(df[left])
            right_clean = _clean(df[right])
            both_present = left_clean.ne("") & right_clean.ne("")
            applicable[left] = both_present
            masks[left] = left_clean.eq(right_clean) & both_present
        except Exception:
            pass

    return _applicable_mask_rows(n, masks, applicable, eligible)


# =============================================================================
# VALD - Validity
# =============================================================================

# Rule families implemented here:
#   VALD1  -> generic alphanumeric/code format checks
#   VALD2  -> numeric value checks
#   VALD3  -> numeric/range-style numeric checks
#   VALD4  -> datetime/date parseability checks
#   VALD5  -> enum/value-set checks
#   VALD6  -> currency-code checks
#   VALD7  -> payment-code checks
#   VALD8  -> Y/N flag checks
#   VALD9  -> ZIP/postcode checks
#   VALD10 -> binary 1/0 flag checks
#   VALD11 -> status-code checks
#   VALD12 -> branch-ID checks
#   VALD13 -> zone/user/location-code checks
#
# VALD1/2/3/5/6/7/8/9/10/11/12/13 use regex rules from
# config.VALIDITY_REGEX.  VALD4 uses datetime parsing from
# config.VALIDITY_DATETIMES.


def _validity_regex_masks(df: pd.DataFrame, regex_rules: Dict[str, str]) -> MaskMap:
    masks: MaskMap = {}
    for col, pattern in regex_rules.items():
        if not _has_column(df, col):
            continue
        try:
            masks[col] = _clean(df[col]).str.fullmatch(pattern, na=False)
        except Exception:
            pass
    return masks


def _validity_datetime_masks(df: pd.DataFrame, datetime_fields: List[str]) -> MaskMap:
    masks: MaskMap = {}
    for col in datetime_fields:
        if not _has_column(df, col):
            continue
        try:
            masks[col] = _safe_dt(df[col]).notna()
        except Exception:
            pass
    return masks


def check_validity(
    df: pd.DataFrame,
    regex_rules: Dict[str, str],
    datetime_fields: List[str],
    pk_cols: List[str],
) -> ScoreRows:
    """Return per-row validity results."""
    n = len(df)
    eligible = _eligible_mask(df, pk_cols)

    masks = {}
    masks.update(_validity_regex_masks(df, regex_rules))
    masks.update(_validity_datetime_masks(df, datetime_fields))

    return _mask_rows(n, masks, eligible)


# =============================================================================
# TIME - Timeliness
# =============================================================================

# Rule family implemented here:
#   TIME1: indexed ordering rules from config.TIMELINESS_RULES.
#
# A rule is applicable only when both date columns are present and parseable.
# Applicable rows pass when start <= end.


def _add_time_rule(
    df: pd.DataFrame,
    masks: MaskMap,
    applicable: MaskMap,
    start_col: str,
    end_col: str,
    output_label: str,
) -> None:
    if not _has_columns(df, [start_col, end_col]):
        return

    try:
        start = _safe_dt(df[start_col])
        end = _safe_dt(df[end_col])
        app = start.notna() & end.notna()
        ok = app & (end >= start)

        if output_label not in masks:
            masks[output_label] = ok | ~app
            applicable[output_label] = app
        else:
            masks[output_label] = masks[output_label] & (ok | ~app)
            applicable[output_label] = applicable[output_label] | app
    except Exception:
        pass


def check_timeliness(
    df: pd.DataFrame,
    timeliness_rules: List[Dict[str, str]],
    pk_cols: List[str],
) -> ScoreRows:
    """Return per-row timeliness results."""
    n = len(df)
    eligible = _eligible_mask(df, pk_cols)
    masks: MaskMap = {}
    applicable: MaskMap = {}

    # TIME1: indexed configured checks.
    for rule in timeliness_rules:
        _add_time_rule(
            df,
            masks,
            applicable,
            rule["start"],
            rule["end"],
            rule.get("assign_to", rule["end"]),
        )

    return _applicable_mask_rows(n, masks, applicable, eligible)


# =============================================================================
# UNIQ - Uniqueness
# =============================================================================

# Rule families implemented here:
#   UNIQ1: single-column key uniqueness.
#   UNIQ2: composite-key uniqueness.
#
# Exact keys and index comments live in config.UNIQUENESS_KEYS.


def _uniqueness_count_column(key_cols: List[str], pk_cols: List[str]) -> str:
    """Return the helper column used for a database-wide uniqueness count."""
    signature = "|".join([*pk_cols, "--", *key_cols]).encode("utf-8")
    return f"__dq_uniq_count_{hashlib.sha1(signature).hexdigest()[:12]}"


def _duplicate_mask(
    df: pd.DataFrame,
    key_cols: List[str],
    eligible: pd.Series,
) -> pd.Series:
    present = [c for c in key_cols if c in df.columns]
    if not present:
        return pd.Series(False, index=df.index)

    cleaned = df[present].apply(_clean)
    composite = cleaned.agg("|".join, axis=1).where(eligible, "")
    return composite.duplicated(keep=False) & eligible


def check_uniqueness(
    df: pd.DataFrame,
    uniqueness_keys: List[List[str]],
    pk_cols: List[str],
) -> ScoreRows:
    """Return per-row uniqueness results."""
    n = len(df)
    eligible = _eligible_mask(df, pk_cols)
    masks: MaskMap = {}

    for key_group in uniqueness_keys:
        present = [c for c in key_group if c in df.columns]
        if not present:
            continue
        try:
            output_label = present[0]
            count_col = _uniqueness_count_column(present, pk_cols)
            if count_col in df.columns:
                masks[output_label] = _safe_float(df[count_col]).le(1)
            else:
                masks[output_label] = ~_duplicate_mask(df, present, eligible)
        except Exception:
            pass

    return _mask_rows(n, masks, eligible)


# =============================================================================
# ACCU - Accuracy
# =============================================================================

# Accuracy rules are hardcoded because they use custom cross-field/reference
# logic.  Unlike the generic rule families above, individual ACCU index codes
# are labeled directly beside their implementation.


def _accuracy_masks(df: pd.DataFrame) -> Dict[str, Optional[pd.Series]]:
    masks: Dict[str, Optional[pd.Series]] = {
        "cnote_weight": None,
        "apicust_weight": None,
        "cnote_services_code": None,
        "apicust_services_code": None,
    }

    # ACCU4B15: CNOTE weight must be greater than or equal to zero.
    # The official description also excludes rows recorded in T_CORRECT_AWB;
    # that reference table is not yet present in the unified input.
    if _has_column(df, "cnote_weight"):
        try:
            masks["cnote_weight"] = _safe_float(df["cnote_weight"]).ge(0)
        except Exception:
            pass

    # ACCU1A29: APICUST weight must be greater than or equal to zero.
    if _has_column(df, "apicust_weight"):
        try:
            masks["apicust_weight"] = _safe_float(df["apicust_weight"]).ge(0)
        except Exception:
            pass

    # ACCU6B6: CNOTE service code must match pre-joined DROURATE service.
    if _has_columns(df, ["cnote_services_code", "drourate_service"]):
        try:
            service = _clean(df["cnote_services_code"])
            reference = _clean(df["drourate_service"])
            masks["cnote_services_code"] = service.ne("") & service.eq(reference)
        except Exception:
            pass

    # ACCU5A6: APICUST service code must match pre-joined DROURATE service.
    if _has_columns(df, ["apicust_services_code", "drourate_service"]):
        try:
            service = _clean(df["apicust_services_code"])
            reference = _clean(df["drourate_service"])
            masks["apicust_services_code"] = service.ne("") & service.eq(reference)
        except Exception:
            pass

    return masks


def check_accuracy(df: pd.DataFrame) -> ScoreRows:
    """Return per-row accuracy results."""
    n = len(df)
    masks = _accuracy_masks(df)
    out = _empty_result(n)

    for idx in range(n):
        row_result: Dict[str, int] = {}

        try:
            if masks["cnote_weight"] is not None and pd.notna(df["cnote_weight"].iloc[idx]):
                row_result["cnote_weight"] = int(bool(masks["cnote_weight"].iloc[idx]))
        except Exception:
            pass

        try:
            if masks["apicust_weight"] is not None and pd.notna(df["apicust_weight"].iloc[idx]):
                row_result["apicust_weight"] = int(bool(masks["apicust_weight"].iloc[idx]))
        except Exception:
            pass

        try:
            service_present = _clean(df["cnote_services_code"].iloc[idx:idx + 1]).iloc[0] != ""
            if masks["cnote_services_code"] is not None and service_present:
                row_result["cnote_services_code"] = int(
                    bool(masks["cnote_services_code"].iloc[idx])
                )
        except Exception:
            pass

        try:
            service_present = _clean(df["apicust_services_code"].iloc[idx:idx + 1]).iloc[0] != ""
            if masks["apicust_services_code"] is not None and service_present:
                row_result["apicust_services_code"] = int(
                    bool(masks["apicust_services_code"].iloc[idx])
                )
        except Exception:
            pass

        out[idx] = row_result or None

    return out


# =============================================================================
# Score Aggregation
# =============================================================================


def _safe_pct(numerator: float, denominator: float) -> Optional[float]:
    if denominator <= 0:
        return None
    return round(100.0 * numerator / denominator, 2)


def _score_dict(obj: Optional[dict]) -> Optional[float]:
    if not obj:
        return None
    return _safe_pct(sum(obj.values()), len(obj))


def _merge(base: Optional[dict], addition: Optional[dict]) -> Optional[dict]:
    if not base and not addition:
        return None

    merged = {}
    if base:
        merged.update(base)
    if addition:
        merged.update(addition)
    return merged or None


def compute_scores(
    n: int,
    per_element: Dict[str, ScoreRows],
) -> pd.DataFrame:
    """
    Combine element-level pass/fail dictionaries into element scores and an
    overall row score.
    """
    out: Dict[str, list] = {}

    for element in DQ_ELEMENTS:
        rows = per_element.get(element, _empty_result(n))
        out[f"{element.lower()}_score"] = [_score_dict(rows[i]) for i in range(n)]

    overall_scores = []
    for idx in range(n):
        element_scores = [
            _score_dict(per_element.get(element, _empty_result(n))[idx])
            for element in DQ_ELEMENTS
        ]
        applicable_scores = [s for s in element_scores if s is not None]
        overall_scores.append(
            round(sum(applicable_scores) / len(applicable_scores), 2)
            if applicable_scores else None
        )

    out["overall_score"] = overall_scores
    return pd.DataFrame(out)
