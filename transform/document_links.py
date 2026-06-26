"""Build reusable document-to-CNOTE bridge links from relational bronze tables."""

from __future__ import annotations

from typing import Iterable

import pandas as pd


DOCUMENT_LINKS_TABLE = "document_cnote_links"
DOCUMENT_LINKS_SOURCE_TABLE = "DOCUMENT_CNOTE_LINKS"
DOCUMENT_LINK_COLUMNS = [
    "source_table",
    "document_type",
    "document_id",
    "cnote_no",
    "link_method",
    "link_confidence",
]

BRIDGE_COLUMNS: dict[str, set[str]] = {
    "CMS_CNOTE": {"CNOTE_NO"},
    "CMS_MFCNOTE": {"MFCNOTE_NO", "MFCNOTE_BAG_NO", "MFCNOTE_MAN_NO"},
    "CMS_MANIFEST": {"MANIFEST_NO"},
    "CMS_MFBAG": {"MFBAG_NO"},
    "CMS_DMBAG": {"DMBAG_NO", "DMBAG_BAG_NO"},
    "CMS_MMBAG": {"MMBAG_NO"},
    "CMS_DRSHEET": {"DRSHEET_NO", "DRSHEET_CNOTE_NO"},
    "CMS_MRSHEET": {"MRSHEET_NO"},
    "CMS_DHICNOTE": {"DHICNOTE_NO", "DHICNOTE_CNOTE_NO"},
    "CMS_MHICNOTE": {"MHICNOTE_NO"},
    "CMS_DHI_HOC": {"DHI_NO", "DHI_CNOTE_NO"},
    "CMS_MHI_HOC": {"MHI_NO"},
    "CMS_DHOCNOTE": {"DHOCNOTE_NO", "DHOCNOTE_CNOTE_NO"},
    "CMS_MHOCNOTE": {"MHOCNOTE_NO"},
    "CMS_DHOUNDEL_POD": {"DHOUNDEL_NO", "DHOUNDEL_CNOTE_NO"},
    "CMS_MHOUNDEL_POD": {"MHOUNDEL_NO"},
    "CMS_DSMU": {"DSMU_NO", "DSMU_BAG_NO"},
    "CMS_MSMU": {"MSMU_NO"},
    "CMS_DSJ": {"DSJ_NO", "DSJ_HVO_NO"},
    "CMS_MSJ": {"MSJ_NO"},
    "CMS_RDSJ": {"RDSJ_NO", "RDSJ_HVO_NO", "RDSJ_HVI_NO"},
}


def required_link_columns() -> dict[str, set[str]]:
    return {table: set(columns) for table, columns in BRIDGE_COLUMNS.items()}


def _string_key_values(values: pd.Series) -> pd.Series:
    return values.fillna("").astype("string").str.strip()


def _group_unique_strings(frame: pd.DataFrame | None, key_column: str, value_column: str) -> dict[str, list[str]]:
    if frame is None or frame.empty or key_column not in frame.columns or value_column not in frame.columns:
        return {}
    work = pd.DataFrame({
        "key": _string_key_values(frame[key_column]),
        "value": _string_key_values(frame[value_column]),
    })
    work = work.loc[work["key"].ne("") & work["value"].ne("")].drop_duplicates()
    if work.empty:
        return {}
    grouped = work.groupby("key", sort=False)["value"].agg(list)
    return {str(key): [str(value) for value in values] for key, values in grouped.items()}


def _map_through_bridge(
    frame: pd.DataFrame | None,
    key_column: str,
    link_column: str,
    link_to_cnotes: dict[str, list[str]],
) -> dict[str, list[str]]:
    if frame is None or not link_to_cnotes or key_column not in frame.columns or link_column not in frame.columns:
        return {}

    rows = pd.DataFrame({
        "key": _string_key_values(frame[key_column]),
        "link": _string_key_values(frame[link_column]),
    })
    rows = rows.loc[rows["key"].ne("") & rows["link"].ne("")].drop_duplicates()
    bridge: dict[str, list[str]] = {}
    for key, group in rows.groupby("key", sort=False):
        cnotes: list[str] = []
        seen: set[str] = set()
        for link in group["link"]:
            for cnote_no in link_to_cnotes.get(str(link), []):
                if cnote_no not in seen:
                    seen.add(cnote_no)
                    cnotes.append(cnote_no)
        if cnotes:
            bridge[str(key)] = cnotes
    return bridge


def _bag_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    return _group_unique_strings(data.get("CMS_MFCNOTE"), "MFCNOTE_BAG_NO", "MFCNOTE_NO")


def _dmbag_to_cnotes(data: dict[str, pd.DataFrame], bag_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    dmbag = data.get("CMS_DMBAG")
    if dmbag is None or not bag_to_cnotes:
        return {}

    bridge = dict(bag_to_cnotes)
    if "DMBAG_NO" not in dmbag.columns or "DMBAG_BAG_NO" not in dmbag.columns:
        return bridge

    bag_rows = pd.DataFrame({
        "dmbag_no": _string_key_values(dmbag["DMBAG_NO"]),
        "bag_no": _string_key_values(dmbag["DMBAG_BAG_NO"]),
    })
    bag_rows = bag_rows.loc[bag_rows["dmbag_no"].ne("") & bag_rows["bag_no"].ne("")].drop_duplicates()
    for dmbag_no, group in bag_rows.groupby("dmbag_no", sort=False):
        cnotes: list[str] = []
        seen: set[str] = set()
        for bag_no in group["bag_no"]:
            for cnote_no in bag_to_cnotes.get(str(bag_no), []):
                if cnote_no not in seen:
                    seen.add(cnote_no)
                    cnotes.append(cnote_no)
        if cnotes:
            bridge[str(dmbag_no)] = cnotes
    return bridge


def _mmbag_to_cnotes(data: dict[str, pd.DataFrame], dmbag_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    mmbag = data.get("CMS_MMBAG")
    if mmbag is None or "MMBAG_NO" not in mmbag.columns:
        return {}

    bridge: dict[str, list[str]] = {}
    for mmbag_no in _string_key_values(mmbag["MMBAG_NO"])[lambda values: values.ne("")].drop_duplicates():
        cnotes = dmbag_to_cnotes.get(str(mmbag_no), [])
        if cnotes:
            bridge[str(mmbag_no)] = cnotes
    return bridge


def _manifest_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    return _group_unique_strings(data.get("CMS_MFCNOTE"), "MFCNOTE_MAN_NO", "MFCNOTE_NO")


def _mrsheet_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    return _group_unique_strings(data.get("CMS_DRSHEET"), "DRSHEET_NO", "DRSHEET_CNOTE_NO")


def _mhicnote_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    return _group_unique_strings(data.get("CMS_DHICNOTE"), "DHICNOTE_NO", "DHICNOTE_CNOTE_NO")


def _mhi_hoc_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    return _group_unique_strings(data.get("CMS_DHI_HOC"), "DHI_NO", "DHI_CNOTE_NO")


def _mhocnote_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    return _group_unique_strings(data.get("CMS_DHOCNOTE"), "DHOCNOTE_NO", "DHOCNOTE_CNOTE_NO")


def _mhoundel_pod_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    return _group_unique_strings(data.get("CMS_DHOUNDEL_POD"), "DHOUNDEL_NO", "DHOUNDEL_CNOTE_NO")


def _dsmu_to_cnotes(data: dict[str, pd.DataFrame], dmbag_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    return _map_through_bridge(data.get("CMS_DSMU"), "DSMU_NO", "DSMU_BAG_NO", dmbag_to_cnotes)


def _msmu_to_cnotes(data: dict[str, pd.DataFrame], dsmu_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    msmu = data.get("CMS_MSMU")
    if msmu is None or "MSMU_NO" not in msmu.columns:
        return {}
    bridge: dict[str, list[str]] = {}
    for msmu_no in _string_key_values(msmu["MSMU_NO"])[lambda values: values.ne("")].drop_duplicates():
        cnotes = dsmu_to_cnotes.get(str(msmu_no), [])
        if cnotes:
            bridge[str(msmu_no)] = cnotes
    return bridge


def _dsj_to_cnotes(data: dict[str, pd.DataFrame], mhicnote_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    rdsj_to_mhicnote = _group_unique_strings(data.get("CMS_RDSJ"), "RDSJ_HVO_NO", "RDSJ_HVI_NO")
    hvo_to_cnotes: dict[str, list[str]] = {}
    for hvo_no, mhicnote_numbers in rdsj_to_mhicnote.items():
        cnotes: list[str] = []
        seen: set[str] = set()
        for mhicnote_no in mhicnote_numbers:
            for cnote_no in mhicnote_to_cnotes.get(str(mhicnote_no), []):
                if cnote_no not in seen:
                    seen.add(cnote_no)
                    cnotes.append(cnote_no)
        if cnotes:
            hvo_to_cnotes[str(hvo_no)] = cnotes
    return _map_through_bridge(data.get("CMS_DSJ"), "DSJ_NO", "DSJ_HVO_NO", hvo_to_cnotes)


def _rdsj_to_cnotes(data: dict[str, pd.DataFrame], mhocnote_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    # RDSJ is keyed by RDSJ_NO, but the validated path to CNOTE is through the
    # HVO number into MHOCNOTE/DHOCNOTE, not through DSJ_NO equality.
    return _map_through_bridge(data.get("CMS_RDSJ"), "RDSJ_NO", "RDSJ_HVO_NO", mhocnote_to_cnotes)


def _msj_to_cnotes(data: dict[str, pd.DataFrame], dsj_to_cnotes: dict[str, list[str]]) -> dict[str, list[str]]:
    msj = data.get("CMS_MSJ")
    if msj is None or "MSJ_NO" not in msj.columns:
        return {}
    bridge: dict[str, list[str]] = {}
    for msj_no in _string_key_values(msj["MSJ_NO"])[lambda values: values.ne("")].drop_duplicates():
        cnotes = dsj_to_cnotes.get(str(msj_no), [])
        if cnotes:
            bridge[str(msj_no)] = cnotes
    return bridge


def _mfcnote_to_cnotes(data: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    mfcnote = data.get("CMS_MFCNOTE")
    if mfcnote is None or "MFCNOTE_NO" not in mfcnote.columns:
        return {}
    values = _string_key_values(mfcnote["MFCNOTE_NO"])
    return {str(value): [str(value)] for value in values[values.ne("")].drop_duplicates()}


def build_document_bridges(data: dict[str, pd.DataFrame]) -> dict[str, dict[str, list[str]]]:
    bag_to_cnotes = _bag_to_cnotes(data)
    dmbag_to_cnotes = _dmbag_to_cnotes(data, bag_to_cnotes)
    mmbag_to_cnotes = _mmbag_to_cnotes(data, dmbag_to_cnotes)
    manifest_to_cnotes = _manifest_to_cnotes(data)
    mrsheet_to_cnotes = _mrsheet_to_cnotes(data)
    mhicnote_to_cnotes = _mhicnote_to_cnotes(data)
    mhi_hoc_to_cnotes = _mhi_hoc_to_cnotes(data)
    mhocnote_to_cnotes = _mhocnote_to_cnotes(data)
    mhoundel_pod_to_cnotes = _mhoundel_pod_to_cnotes(data)
    dsmu_to_cnotes = _dsmu_to_cnotes(data, dmbag_to_cnotes)
    msmu_to_cnotes = _msmu_to_cnotes(data, dsmu_to_cnotes)
    dsj_to_cnotes = _dsj_to_cnotes(data, mhicnote_to_cnotes)
    rdsj_to_cnotes = _rdsj_to_cnotes(data, mhocnote_to_cnotes)
    msj_to_cnotes = _msj_to_cnotes(data, dsj_to_cnotes)
    return {
        "CMS_MFCNOTE": _mfcnote_to_cnotes(data),
        "CMS_MFBAG": bag_to_cnotes,
        "CMS_DMBAG": dmbag_to_cnotes,
        "CMS_MMBAG": mmbag_to_cnotes,
        "CMS_MANIFEST": manifest_to_cnotes,
        "CMS_MRSHEET": mrsheet_to_cnotes,
        "CMS_MHICNOTE": mhicnote_to_cnotes,
        "CMS_MHI_HOC": mhi_hoc_to_cnotes,
        "CMS_MHOCNOTE": mhocnote_to_cnotes,
        "CMS_MHOUNDEL_POD": mhoundel_pod_to_cnotes,
        "CMS_DSMU": dsmu_to_cnotes,
        "CMS_MSMU": msmu_to_cnotes,
        "CMS_DSJ": dsj_to_cnotes,
        "CMS_RDSJ": rdsj_to_cnotes,
        "CMS_MSJ": msj_to_cnotes,
    }


def _cnote_universe(data: dict[str, pd.DataFrame]) -> set[str]:
    cnote = data.get("CMS_CNOTE")
    if cnote is None or "CNOTE_NO" not in cnote.columns:
        return set()
    values = _string_key_values(cnote["CNOTE_NO"])
    return set(values[values.ne("")].drop_duplicates())


def _safe_cnotes(values: Iterable[str], cnote_universe: set[str]) -> list[str]:
    linked: list[str] = []
    seen: set[str] = set()
    for value in values:
        cnote_no = str(value)
        if not cnote_no or cnote_no in seen:
            continue
        if cnote_universe and cnote_no not in cnote_universe:
            continue
        seen.add(cnote_no)
        linked.append(cnote_no)
    return linked


def _document_type(source_table: str) -> str:
    return source_table.removeprefix("CMS_") if source_table.startswith("CMS_") else source_table


def _method_for_table(source_table: str) -> str:
    return {
        "CMS_MFCNOTE": "direct_mfcnote",
        "CMS_MFBAG": "mfbag_to_mfcnote",
        "CMS_DMBAG": "dmbag_to_mfbag_mfcnote",
        "CMS_MMBAG": "mmbag_to_dmbag_mfbag_mfcnote",
        "CMS_MANIFEST": "manifest_to_mfcnote",
        "CMS_MRSHEET": "mrsheet_to_drsheet",
        "CMS_MHICNOTE": "mhicnote_to_dhicnote",
        "CMS_MHI_HOC": "mhi_hoc_to_dhi_hoc",
        "CMS_MHOCNOTE": "mhocnote_to_dhocnote",
        "CMS_MHOUNDEL_POD": "mhoundel_to_dhoundel",
        "CMS_DSMU": "dsmu_to_dmbag_mfcnote",
        "CMS_MSMU": "msmu_to_dsmu_dmbag_mfcnote",
        "CMS_DSJ": "dsj_to_rdsj_mhicnote",
        "CMS_RDSJ": "rdsj_hvo_to_mhocnote",
        "CMS_MSJ": "msj_to_dsj_rdsj_mhicnote",
    }.get(source_table, f"{source_table.lower()}_bridge")


def build_document_cnote_links(data: dict[str, pd.DataFrame], cnote_universe: set[str] | None = None) -> pd.DataFrame:
    cnotes = cnote_universe if cnote_universe is not None else _cnote_universe(data)
    rows: list[dict[str, str]] = []
    for source_table, bridge in build_document_bridges(data).items():
        method = _method_for_table(source_table)
        for document_id, linked_cnotes in bridge.items():
            for cnote_no in _safe_cnotes(linked_cnotes, cnotes):
                rows.append({
                    "source_table": source_table,
                    "document_type": _document_type(source_table),
                    "document_id": str(document_id),
                    "cnote_no": str(cnote_no),
                    "link_method": method,
                    "link_confidence": "safe",
                })
    if not rows:
        return pd.DataFrame(columns=DOCUMENT_LINK_COLUMNS)
    return pd.DataFrame(rows, columns=DOCUMENT_LINK_COLUMNS).drop_duplicates(ignore_index=True)
