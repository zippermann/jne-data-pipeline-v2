"""Literal governance rule registry.

Index codes are materialized as source-code keys on purpose. Do not load rule
codes from the spreadsheet at runtime; the spreadsheet is source material, and
this registry is the executable contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RuleSpec:
    code: str
    element: str
    rule_family: str
    child_table: str
    child_fk: str
    parent_table: str
    parent_pk: str
    description: str
    active: bool = True
    needs_confirmation: bool = False
    child_date_column: Optional[str] = None


RULES: dict[str, RuleSpec] = {
    # TODO: Confirm detail link column with IT.
    "INTG1D": RuleSpec(
        code="INTG1D",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DRCNOTE",
        child_fk="DRCNOTE_NO",
        parent_table="CMS_MRCNOTE",
        parent_pk="MRCNOTE_NO",
        description="Referential: every CMS_DRCNOTE row must link to an existing CMS_MRCNOTE master (MRCNOTE_NO). Flag rows whose master key has no match.",
        needs_confirmation=True,
    ),
    # TODO: Confirm detail link column with IT.
    "INTG1F": RuleSpec(
        code="INTG1F",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DHI_HOC",
        child_fk="DHI_NO",
        parent_table="CMS_MHI_HOC",
        parent_pk="MHI_NO",
        description="Referential: every CMS_DHI_HOC row must link to an existing CMS_MHI_HOC master (MHI_NO). Flag rows whose master key has no match.",
        needs_confirmation=True,
    ),
    "INTG1I": RuleSpec(
        code="INTG1I",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_MFCNOTE",
        child_fk="MFCNOTE_MAN_NO",
        parent_table="CMS_MANIFEST",
        parent_pk="MANIFEST_NO",
        description="Referential: MFCNOTE_MAN_NO must exist in CMS_MANIFEST (MANIFEST_NO). Flag MFCNOTE rows whose manifest has no match.",
        child_date_column="MFCNOTE_CRDATE",
    ),
    "INTG1J": RuleSpec(
        code="INTG1J",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_MFBAG",
        child_fk="MFBAG_MAN_NO",
        parent_table="CMS_MANIFEST",
        parent_pk="MANIFEST_NO",
        description="Referential: MFBAG_MAN_NO must exist in CMS_MANIFEST (MANIFEST_NO). Flag orphan manifest bag rows.",
        child_date_column="MFBAG_CRDATE",
    ),
    "INTG1K": RuleSpec(
        code="INTG1K",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DMBAG",
        child_fk="DMBAG_NO",
        parent_table="CMS_MMBAG",
        parent_pk="MMBAG_NO",
        description="Referential: DMBAG_NO must exist in CMS_MMBAG (MMBAG_NO) master bag. Flag orphan detail bag rows.",
        child_date_column="ESB_TIME",
    ),
    "INTG1M": RuleSpec(
        code="INTG1M",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DSMU",
        child_fk="DSMU_NO",
        parent_table="CMS_MSMU",
        parent_pk="MSMU_NO",
        description="Referential: DSMU_NO must exist in CMS_MSMU (MSMU_NO) master. Flag orphan SMU detail rows.",
        child_date_column="ESB_TIME",
    ),
    # TODO: Confirm detail link column with IT.
    "INTG1W": RuleSpec(
        code="INTG1W",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DHOCNOTE",
        child_fk="DHOCNOTE_NO",
        parent_table="CMS_MHOCNOTE",
        parent_pk="MHOCNOTE_NO",
        description="Referential: every CMS_DHOCNOTE row must link to an existing CMS_MHOCNOTE master (MHOCNOTE_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    # TODO: Confirm detail link column with IT.
    "INTG1AC": RuleSpec(
        code="INTG1AC",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DHICNOTE",
        child_fk="DHICNOTE_NO",
        parent_table="CMS_MHICNOTE",
        parent_pk="MHICNOTE_NO",
        description="Referential: every CMS_DHICNOTE row must link to an existing CMS_MHICNOTE master (MHICNOTE_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    # TODO: Confirm detail link column with IT.
    "INTG1AA": RuleSpec(
        code="INTG1AA",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DSJ",
        child_fk="DSJ_NO",
        parent_table="CMS_MSJ",
        parent_pk="MSJ_NO",
        description="Referential: every CMS_DSJ row must link to an existing CMS_MSJ master (MSJ_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    # TODO: Confirm detail link column with IT.
    "INTG1P": RuleSpec(
        code="INTG1P",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DRSHEET",
        child_fk="DRSHEET_NO",
        parent_table="CMS_MRSHEET",
        parent_pk="MRSHEET_NO",
        description="Referential: every CMS_DRSHEET row must link to an existing CMS_MRSHEET master (MRSHEET_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    # TODO: Confirm detail link column with IT.
    "INTG1U": RuleSpec(
        code="INTG1U",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_DHOUNDEL_POD",
        child_fk="DHOUNDEL_NO",
        parent_table="CMS_MHOUNDEL_POD",
        parent_pk="MHOUNDEL_NO",
        description="Referential: every CMS_DHOUNDEL_POD row must link to an existing CMS_MHOUNDEL_POD master (MHOUNDEL_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    "INTG1Y": RuleSpec(
        code="INTG1Y",
        element="INTG",
        rule_family="INTG1",
        child_table="CMS_COST_DTRANSIT_AGEN",
        child_fk="DMANIFEST_NO",
        parent_table="CMS_COST_MTRANSIT_AGEN",
        parent_pk="MANIFEST_NO",
        description="Referential: COST_D_MANIFEST_NO/DMANIFEST_NO must exist in CMS_COST_MTRANSIT_AGEN (COST_M_MANIFEST_NO/MANIFEST_NO). Flag orphan cost-manifest rows.",
        child_date_column="ESB_TIME",
    ),
}


def active_rules() -> list[RuleSpec]:
    return [rule for rule in RULES.values() if rule.active]


def get_rule(code: str) -> RuleSpec:
    return RULES[code.upper()]
