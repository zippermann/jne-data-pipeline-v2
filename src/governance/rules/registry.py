"""Governance rule registry.

The Excel workbook is reference material only. Runtime governance uses the
checked-in Python catalog so Airflow and container deployments are deterministic.
Integrity rules still carry explicit parent mappings because the workbook
describes the intent but not enough executable join detail.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from src.governance.rules.catalog import CATALOG


@dataclass(frozen=True)
class RuleSpec:
    code: str
    element: str
    rule_family: str
    table: str
    columns: tuple[str, ...]
    description: str
    active: bool = True
    needs_confirmation: bool = False
    child_table: str = ""
    child_fk: str = ""
    parent_table: str = ""
    parent_pk: str = ""
    child_date_column: Optional[str] = None


INTEGRITY_RULES: dict[str, RuleSpec] = {
    "INTG1D": RuleSpec(
        code="INTG1D",
        element="INTG",
        rule_family="INTG1",
        table="CMS_DRCNOTE",
        columns=("DRCNOTE_NO",),
        child_table="CMS_DRCNOTE",
        child_fk="DRCNOTE_NO",
        parent_table="CMS_MRCNOTE",
        parent_pk="MRCNOTE_NO",
        description="Referential: every CMS_DRCNOTE row must link to an existing CMS_MRCNOTE master (MRCNOTE_NO). Flag rows whose master key has no match.",
        needs_confirmation=True,
    ),
    "INTG1F": RuleSpec(
        code="INTG1F",
        element="INTG",
        rule_family="INTG1",
        table="CMS_DHI_HOC",
        columns=("DHI_NO",),
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
        table="CMS_MFCNOTE",
        columns=("MFCNOTE_MAN_NO",),
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
        table="CMS_MFBAG",
        columns=("MFBAG_MAN_NO",),
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
        table="CMS_DMBAG",
        columns=("DMBAG_NO",),
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
        table="CMS_DSMU",
        columns=("DSMU_NO",),
        child_table="CMS_DSMU",
        child_fk="DSMU_NO",
        parent_table="CMS_MSMU",
        parent_pk="MSMU_NO",
        description="Referential: DSMU_NO must exist in CMS_MSMU (MSMU_NO) master. Flag orphan SMU detail rows.",
        child_date_column="ESB_TIME",
    ),
    "INTG1W": RuleSpec(
        code="INTG1W",
        element="INTG",
        rule_family="INTG1",
        table="CMS_DHOCNOTE",
        columns=("DHOCNOTE_NO",),
        child_table="CMS_DHOCNOTE",
        child_fk="DHOCNOTE_NO",
        parent_table="CMS_MHOCNOTE",
        parent_pk="MHOCNOTE_NO",
        description="Referential: every CMS_DHOCNOTE row must link to an existing CMS_MHOCNOTE master (MHOCNOTE_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    "INTG1AC": RuleSpec(
        code="INTG1AC",
        element="INTG",
        rule_family="INTG1",
        table="CMS_DHICNOTE",
        columns=("DHICNOTE_NO",),
        child_table="CMS_DHICNOTE",
        child_fk="DHICNOTE_NO",
        parent_table="CMS_MHICNOTE",
        parent_pk="MHICNOTE_NO",
        description="Referential: every CMS_DHICNOTE row must link to an existing CMS_MHICNOTE master (MHICNOTE_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    "INTG1AA": RuleSpec(
        code="INTG1AA",
        element="INTG",
        rule_family="INTG1",
        table="CMS_DSJ",
        columns=("DSJ_NO",),
        child_table="CMS_DSJ",
        child_fk="DSJ_NO",
        parent_table="CMS_MSJ",
        parent_pk="MSJ_NO",
        description="Referential: every CMS_DSJ row must link to an existing CMS_MSJ master (MSJ_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    "INTG1P": RuleSpec(
        code="INTG1P",
        element="INTG",
        rule_family="INTG1",
        table="CMS_DRSHEET",
        columns=("DRSHEET_NO",),
        child_table="CMS_DRSHEET",
        child_fk="DRSHEET_NO",
        parent_table="CMS_MRSHEET",
        parent_pk="MRSHEET_NO",
        description="Referential: every CMS_DRSHEET row must link to an existing CMS_MRSHEET master (MRSHEET_NO). Flag orphan rows.",
        needs_confirmation=True,
    ),
    "INTG1U": RuleSpec(
        code="INTG1U",
        element="INTG",
        rule_family="INTG1",
        table="CMS_DHOUNDEL_POD",
        columns=("DHOUNDEL_NO",),
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
        table="CMS_COST_DTRANSIT_AGEN",
        columns=("DMANIFEST_NO",),
        child_table="CMS_COST_DTRANSIT_AGEN",
        child_fk="DMANIFEST_NO",
        parent_table="CMS_COST_MTRANSIT_AGEN",
        parent_pk="MANIFEST_NO",
        description="Referential: COST_D_MANIFEST_NO/DMANIFEST_NO must exist in CMS_COST_MTRANSIT_AGEN (COST_M_MANIFEST_NO/MANIFEST_NO). Flag orphan cost-manifest rows.",
        child_date_column="ESB_TIME",
    ),
}


@lru_cache(maxsize=1)
def rules() -> dict[str, RuleSpec]:
    catalog_rules = {
        row["code"]: RuleSpec(
            code=row["code"],
            element=row["element"],
            rule_family=row["rule_family"],
            table=row["table"],
            columns=tuple(row["columns"]),
            description=row["description"],
            active=row.get("active", True),
            needs_confirmation=row.get("needs_confirmation", False),
            child_table=row.get("child_table", ""),
            child_fk=row.get("child_fk", ""),
            parent_table=row.get("parent_table", ""),
            parent_pk=row.get("parent_pk", ""),
            child_date_column=row.get("child_date_column"),
        )
        for row in CATALOG
    }
    catalog_rules.update(INTEGRITY_RULES)
    return dict(sorted(catalog_rules.items()))


def active_rules() -> list[RuleSpec]:
    return [rule for rule in rules().values() if rule.active]


def get_rule(code: str) -> RuleSpec:
    return rules()[code.upper()]
