"""Governance rule registry.

The relational index workbook is the checked-in governance contract for the
non-integrity rule catalog. Integrity rules still carry explicit parent mappings
because the workbook describes the intent but not enough executable join detail.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET
from zipfile import ZipFile


WORKBOOK_PATH = Path(__file__).resolve().parents[2] / "governance/JNE Index List Relational.xlsx"
SHEET_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
INDEX_FAMILY = re.compile(r"^([A-Z]+[0-9]+)")


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


def _cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    value = cell.find("a:v", SHEET_NS)
    if cell_type == "s" and value is not None:
        return shared_strings[int(value.text or "0")]
    if cell_type == "inlineStr":
        return "".join(text.text or "" for text in cell.findall(".//a:t", SHEET_NS))
    if value is not None:
        return value.text or ""
    return ""


def _read_workbook(path: Path) -> dict[str, list[dict[str, str]]]:
    if not path.exists():
        raise FileNotFoundError(f"Governance index workbook not found: {path}")

    with ZipFile(path) as workbook:
        shared_root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
        shared_strings = [
            "".join(text.text or "" for text in item.findall(".//a:t", SHEET_NS))
            for item in shared_root.findall("a:si", SHEET_NS)
        ]

        workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
        rel_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        rel_targets = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rel_root}

        sheets: dict[str, list[dict[str, str]]] = {}
        for sheet in workbook_root.findall(".//a:sheet", SHEET_NS):
            name = sheet.attrib["name"]
            rel_id = sheet.attrib[f"{{{REL_NS}}}id"]
            target = rel_targets[rel_id]
            xml_path = target[1:] if target.startswith("/") else f"xl/{target}"
            root = ET.fromstring(workbook.read(xml_path))
            rows: list[dict[str, str]] = []
            for row in root.findall(".//a:sheetData/a:row", SHEET_NS):
                values: dict[str, str] = {}
                for cell in row.findall("a:c", SHEET_NS):
                    ref = cell.attrib.get("r", "")
                    column = "".join(char for char in ref if char.isalpha())
                    if not column:
                        continue
                    text = _cell_text(cell, shared_strings).strip()
                    values[column] = text
                if any(values.values()):
                    rows.append(values)
            sheets[name] = rows
    return sheets


def _table_map(rows: list[dict[str, str]]) -> dict[str, str]:
    element_codes = {"ACCU", "COMP", "CONS", "TIME", "VALD", "VALI", "UNIQ", "INTG"}
    mapping = {}
    for row in rows:
        code = row.get("A", "").strip()
        table = row.get("B", "").strip()
        if (
            code
            and table
            and code not in {"Element Code", "Table Code", *element_codes}
            and table not in {"Element", "Table"}
        ):
            mapping[code] = table
    return mapping


def _rule_family(code: str, fallback: str) -> str:
    match = INDEX_FAMILY.match(code)
    return match.group(1) if match else fallback


def _split_columns(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _generic_rules(sheets: dict[str, list[dict[str, str]]], tables: dict[str, str]) -> dict[str, RuleSpec]:
    rules: dict[str, RuleSpec] = {}

    for row in sheets.get("COMPLETENESS", [])[1:]:
        code = row.get("N", "").strip()
        table = tables.get(row.get("J", "").strip(), "")
        column = row.get("L", "").strip()
        if code and table and column:
            rules[code] = RuleSpec(
                code=code,
                element="COMP",
                rule_family="COMP",
                table=table,
                columns=(column,),
                description=row.get("M", "If data is null then Not Complete").strip(),
            )

    for row in sheets.get("ACCURACY", [])[1:]:
        code = row.get("N", "").strip()
        table = tables.get(row.get("J", "").strip(), "")
        column = row.get("L", "").strip()
        if code and table and column:
            rules[code] = RuleSpec(
                code=code,
                element="ACCU",
                rule_family=_rule_family(code, "ACCU"),
                table=table,
                columns=(column,),
                description=row.get("M", "").strip(),
                needs_confirmation=True,
            )

    for sheet_name, element in (
        ("CONSISTENCY", "CONS"),
        ("VALIDITY", "VALD"),
        ("UNIQUENESS", "UNIQ"),
        ("TIMELINESS", "TIME"),
    ):
        for row in sheets.get(sheet_name, [])[1:]:
            code = row.get("K", "").strip()
            table = tables.get(row.get("G", "").strip(), "")
            columns = _split_columns(row.get("I", "").strip())
            if not code or not table or not columns or code == "Index Number":
                continue
            rules[code] = RuleSpec(
                code=code,
                element=element,
                rule_family=_rule_family(code, element),
                table=table,
                columns=columns,
                description=row.get("J", "").strip(),
                needs_confirmation=element in {"ACCU", "CONS", "TIME"},
            )

    return rules


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
    sheets = _read_workbook(WORKBOOK_PATH)
    all_rules = _generic_rules(sheets, _table_map(sheets.get("INDEX GUIDE", [])))
    all_rules.update(INTEGRITY_RULES)
    return dict(sorted(all_rules.items()))


def active_rules() -> list[RuleSpec]:
    return [rule for rule in rules().values() if rule.active]


def get_rule(code: str) -> RuleSpec:
    return rules()[code.upper()]
