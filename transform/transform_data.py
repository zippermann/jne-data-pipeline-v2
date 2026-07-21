"""Transform bronze CNOTE data into analysis-ready derived tables.

This module is the seed of the future cnote_spine. Add future CNOTE-level
derived columns here instead of creating parallel one-off transforms.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from extractor.bronze import MinioSettings, load_config
from transform.document_links import (
    DOCUMENT_LINKS_SOURCE_TABLE,
    DOCUMENT_LINKS_TABLE,
    build_document_cnote_links,
    required_link_columns,
)


DERIVED_TABLE = "cms_cnote_transformed"
DERIVED_SOURCE_TABLE = "CMS_CNOTE_TRANSFORMED"
DERIVED_PARENT = "derived"
DERIVED_PREFIX = f"{DERIVED_PARENT}/{DERIVED_TABLE}/"


@dataclass(frozen=True)
class DerivedSource:
    manifest: dict[str, Any]
    run_prefix: str | None = None
    run_path: Path | None = None
    client: Any | None = None
    bucket: str | None = None
    settings: MinioSettings | None = None


def _log(message: str) -> None:
    print(message, flush=True)


def _quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _document_links_mode(config: dict[str, Any]) -> str:
    transform_config = config.get("transform", {}) or {}
    mode = str(transform_config.get("document_links_mode", "python")).strip().lower()
    if mode not in {"python", "clickhouse", "skip"}:
        raise ValueError(
            "transform.document_links_mode must be one of: python, clickhouse, skip"
        )
    return mode


def shipment_scope(origin: Any, destination: Any) -> str:
    """Classify a CNOTE origin/destination pair without touching bronze."""
    origin_text = str(origin or "").strip().upper()
    destination_text = str(destination or "").strip().upper()
    if len(origin_text) < 4 or len(destination_text) < 4:
        return "Unknown"
    o_code, d_code = origin_text[:3], destination_text[:3]
    o_digit, d_digit = origin_text[3], destination_text[3]
    if not (o_code.isalpha() and d_code.isalpha() and o_digit.isdigit() and d_digit.isdigit()):
        return "Unknown"
    if o_code == d_code and o_digit == d_digit:
        return "Intracity"
    if o_code == d_code:
        return "Intercity"
    return "Domestic"


def delivery_category(delivery_type: Any, scope: Any) -> str:
    """Combine delivery type and shipment scope for dashboard grouping."""
    delivery_text = str(delivery_type or "").strip()
    scope_text = str(scope or "").strip()
    if scope_text in {"Intracity", "Intercity"}:
        return scope_text
    if scope_text == "Domestic" and delivery_text == "Transit":
        return "Transit Domestic"
    if scope_text == "Domestic" and delivery_text == "Direct":
        return "Direct Domestic"
    return scope_text or "Unknown"


def _duckdb_connection(config: dict):
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required to build derived tables. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the Airflow image."
        ) from exc

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("INSTALL json")
    con.execute("LOAD json")
    return con


def _configure_s3(con: Any, settings: MinioSettings) -> None:
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_endpoint={_quote_sql(settings.endpoint)}")
    con.execute(f"SET s3_access_key_id={_quote_sql(settings.access_key)}")
    con.execute(f"SET s3_secret_access_key={_quote_sql(settings.secret_key)}")
    con.execute(f"SET s3_use_ssl={'true' if _as_bool(settings.secure) else 'false'}")


def _minio_client(settings: MinioSettings):
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "minio is required for transform --source minio. Install dependencies "
            "with `pip install -r requirements.txt` or rebuild the Airflow image."
        ) from exc

    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )


def _read_json_response(response: Any) -> dict[str, Any]:
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def _source_from_minio(config_path: str, run_prefix: str | None) -> DerivedSource:
    config = load_config(config_path)
    settings = MinioSettings.from_config(config)
    prefix = (run_prefix or os.getenv("BRONZE_RUN_PREFIX") or "").strip("/")
    if not prefix:
        raise ValueError("BRONZE_RUN_PREFIX or --bronze-run-prefix is required for transform --source minio")
    client = _minio_client(settings)
    response = client.get_object(settings.bucket, f"{prefix}/run_manifest.json")
    manifest = _read_json_response(response)
    return DerivedSource(
        manifest=manifest,
        run_prefix=prefix,
        client=client,
        bucket=settings.bucket,
        settings=settings,
    )


def _source_from_local(run_path: str | Path) -> DerivedSource:
    path = Path(run_path)
    manifest = json.loads((path / "run_manifest.json").read_text(encoding="utf-8"))
    return DerivedSource(manifest=manifest, run_path=path)


def _manifest_table(manifest: dict[str, Any], table: str) -> dict[str, Any]:
    table_upper = table.upper()
    for item in manifest.get("tables", []):
        if str(item.get("table", "")).upper() == table_upper:
            return item
    raise KeyError(f"run manifest does not include required table {table_upper}")


def _source_prefix(source: DerivedSource, table_info: dict[str, Any]) -> str:
    output_name = str(table_info["output_name"])
    if table_info.get("source_prefix"):
        return str(table_info["source_prefix"]).rstrip("/") + "/"
    assert source.run_prefix is not None
    return f"{source.run_prefix}/{output_name}/"


def _parquet_glob(source: DerivedSource, table: str) -> str:
    table_info = _manifest_table(source.manifest, table)
    output_name = str(table_info["output_name"])
    if source.run_path is not None:
        return (source.run_path / output_name / "*.parquet").as_posix()
    assert source.bucket is not None
    return f"s3://{source.bucket}/{_source_prefix(source, table_info)}*.parquet"


def _derived_prefix(source: DerivedSource, output_name: str = DERIVED_TABLE) -> str:
    relative = f"{DERIVED_PARENT}/{output_name}/"
    if source.run_prefix is None:
        return relative
    return f"{source.run_prefix}/{relative}"


def _build_cnote_transform_query(
    cnote_path: str,
    mfcnote_path: str,
    mfbag_path: str,
    manifest_path: str,
    drcnote_path: str,
    mrcnote_path: str,
    dhicnote_path: str,
    dhocnote_path: str,
    mhocnote_path: str,
    drsheet_path: str,
    cnote_pod_path: str,
) -> str:
    return f"""
        WITH transit_cnotes AS (
            SELECT mf.MFCNOTE_NO AS cnote_no,
                COUNT(*) AS transit_leg_count,
                MIN(TRY_CAST(mf.MFCNOTE_CRDATE AS TIMESTAMP)) AS first_manifest_ts
            FROM read_parquet({_quote_sql(mfcnote_path)}) mf
            JOIN read_parquet({_quote_sql(manifest_path)}) m
              ON mf.MFCNOTE_MAN_NO = m.MANIFEST_NO
            WHERE TRY_CAST(m.MANIFEST_CODE AS INTEGER) = 3
            GROUP BY mf.MFCNOTE_NO
        ),
        -- Pickup timestamp: CMS_MRCNOTE has no direct CNOTE column, so join via
        -- CMS_DRCNOTE.DRCNOTE_NO = CMS_MRCNOTE.MRCNOTE_NO to reach DRCNOTE_CNOTE_NO.
        pickup_events AS (
            SELECT d.DRCNOTE_CNOTE_NO AS cnote_no,
                MIN(TRY_CAST(r.MRCNOTE_DATE AS TIMESTAMP)) AS pickup_ts
            FROM read_parquet({_quote_sql(drcnote_path)}) d
            JOIN read_parquet({_quote_sql(mrcnote_path)}) r ON d.DRCNOTE_NO = r.MRCNOTE_NO
            WHERE d.DRCNOTE_CNOTE_NO IS NOT NULL
            GROUP BY d.DRCNOTE_CNOTE_NO
        ),
        manifest_bag_events AS (
            SELECT mf.MFCNOTE_NO AS cnote_no,
                MIN(TRY_CAST(b.MFBAG_CRDATE AS TIMESTAMP)) AS mfbag_create_ts
            FROM read_parquet({_quote_sql(mfcnote_path)}) mf
            JOIN read_parquet({_quote_sql(mfbag_path)}) b ON mf.MFCNOTE_BAG_NO = b.MFBAG_NO
            WHERE mf.MFCNOTE_NO IS NOT NULL
            GROUP BY mf.MFCNOTE_NO
        ),
        handover_in_events AS (
            SELECT DHICNOTE_CNOTE_NO AS cnote_no,
                MIN(TRY_CAST(DHICNOTE_TDATE AS TIMESTAMP)) AS handover_in_ts
            FROM read_parquet({_quote_sql(dhicnote_path)})
            WHERE DHICNOTE_CNOTE_NO IS NOT NULL
            GROUP BY DHICNOTE_CNOTE_NO
        ),
        handover_out_events AS (
            SELECT DHOCNOTE_CNOTE_NO AS cnote_no,
                MIN(TRY_CAST(DHOCNOTE_TDATE AS TIMESTAMP)) AS handover_out_ts
            FROM read_parquet({_quote_sql(dhocnote_path)})
            WHERE DHOCNOTE_CNOTE_NO IS NOT NULL
            GROUP BY DHOCNOTE_CNOTE_NO
        ),
        mhocnote_events AS (
            SELECT d.DHOCNOTE_CNOTE_NO AS cnote_no,
                MIN(TRY_CAST(m.MHOCNOTE_DATE AS TIMESTAMP)) AS mhocnote_create_ts
            FROM read_parquet({_quote_sql(dhocnote_path)}) d
            JOIN read_parquet({_quote_sql(mhocnote_path)}) m ON d.DHOCNOTE_NO = m.MHOCNOTE_NO
            WHERE d.DHOCNOTE_CNOTE_NO IS NOT NULL
            GROUP BY d.DHOCNOTE_CNOTE_NO
        ),
        runsheet_events AS (
            SELECT DRSHEET_CNOTE_NO AS cnote_no,
                MIN(TRY_CAST(DRSHEET_DATE AS TIMESTAMP)) AS runsheet_ts
            FROM read_parquet({_quote_sql(drsheet_path)})
            WHERE DRSHEET_CNOTE_NO IS NOT NULL
            GROUP BY DRSHEET_CNOTE_NO
        ),
        -- CMS_CNOTE_POD.CNOTE_POD_NO is joined directly to CMS_DRSHEET.DRSHEET_CNOTE_NO
        -- elsewhere in the governance catalog (rule TIME1C9), confirming it holds
        -- cnote_no values rather than a separate POD record id.
        pod_events AS (
            SELECT CNOTE_POD_NO AS cnote_no,
                MIN(TRY_CAST(CNOTE_POD_DATE AS TIMESTAMP)) AS pod_ts,
                MIN(TRY_CAST(CNOTE_POD_CREATION_DATE AS TIMESTAMP)) AS cnote_pod_create_ts
            FROM read_parquet({_quote_sql(cnote_pod_path)})
            WHERE CNOTE_POD_NO IS NOT NULL
            GROUP BY CNOTE_POD_NO
        ),
        parts AS (
            SELECT c.*,
                regexp_extract(upper(trim(c.CNOTE_ORIGIN)), '^([A-Z]{{3}})', 1) AS o_code,
                regexp_extract(upper(trim(c.CNOTE_DESTINATION)), '^([A-Z]{{3}})', 1) AS d_code,
                regexp_extract(upper(trim(c.CNOTE_ORIGIN)), '^[A-Z]{{3}}([0-9])', 1) AS o_digit,
                regexp_extract(upper(trim(c.CNOTE_DESTINATION)), '^[A-Z]{{3}}([0-9])', 1) AS d_digit,
                (t.cnote_no IS NOT NULL) AS has_transit,
                COALESCE(t.transit_leg_count, 0) AS transit_leg_count,
                t.first_manifest_ts,
                p.pickup_ts,
                mb.mfbag_create_ts,
                hi.handover_in_ts,
                ho.handover_out_ts,
                mh.mhocnote_create_ts,
                rs.runsheet_ts,
                pod.pod_ts,
                pod.cnote_pod_create_ts
            FROM read_parquet({_quote_sql(cnote_path)}) c
            LEFT JOIN transit_cnotes t ON c.CNOTE_NO = t.cnote_no
            LEFT JOIN pickup_events p ON c.CNOTE_NO = p.cnote_no
            LEFT JOIN manifest_bag_events mb ON c.CNOTE_NO = mb.cnote_no
            LEFT JOIN handover_in_events hi ON c.CNOTE_NO = hi.cnote_no
            LEFT JOIN handover_out_events ho ON c.CNOTE_NO = ho.cnote_no
            LEFT JOIN mhocnote_events mh ON c.CNOTE_NO = mh.cnote_no
            LEFT JOIN runsheet_events rs ON c.CNOTE_NO = rs.cnote_no
            LEFT JOIN pod_events pod ON c.CNOTE_NO = pod.cnote_no
        ),
        classified AS (
            SELECT * EXCLUDE (
                    o_code, d_code, o_digit, d_digit, has_transit, transit_leg_count,
                    first_manifest_ts, pickup_ts, mfbag_create_ts, handover_in_ts, handover_out_ts,
                    mhocnote_create_ts, runsheet_ts, pod_ts, cnote_pod_create_ts
                ),
                TRY_CAST(CNOTE_CRDATE AS TIMESTAMP) AS cms_cnote_create_date,
                pickup_ts AS cms_mrcnote_create_date,
                mfbag_create_ts AS cms_mfbag_create_date,
                cnote_pod_create_ts AS cms_cnote_pod_create_date,
                CASE WHEN has_transit THEN 'Transit' ELSE 'Direct' END AS delivery_type,
                CASE
                    WHEN o_code = '' OR d_code = '' OR o_digit = '' OR d_digit = '' THEN 'Unknown'
                    WHEN o_code = d_code AND o_digit = d_digit THEN 'Intracity'
                    WHEN o_code = d_code THEN 'Intercity'
                    ELSE 'Domestic'
                END AS shipment_scope,
                transit_leg_count AS transit_manifest_count,
                transit_leg_count
                    + CASE WHEN pickup_ts IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN handover_in_ts IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN handover_out_ts IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN runsheet_ts IS NOT NULL THEN 1 ELSE 0 END AS handover_count,
                CASE WHEN pickup_ts IS NOT NULL AND pod_ts IS NOT NULL
                     THEN (epoch(pod_ts) - epoch(pickup_ts)) / 3600.0
                END AS sla_total_hours,
                CASE WHEN pickup_ts IS NOT NULL AND first_manifest_ts IS NOT NULL
                     THEN (epoch(first_manifest_ts) - epoch(pickup_ts)) / 3600.0
                END AS sla_pickup_to_firstmanifest_hours,
                CASE WHEN TRY_CAST(CNOTE_CRDATE AS TIMESTAMP) IS NOT NULL AND pickup_ts IS NOT NULL
                     THEN (epoch(pickup_ts) - epoch(TRY_CAST(CNOTE_CRDATE AS TIMESTAMP))) / 3600.0
                END AS total_duration_hour_to_receival,
                CASE WHEN pickup_ts IS NOT NULL AND mfbag_create_ts IS NOT NULL
                     THEN (epoch(mfbag_create_ts) - epoch(pickup_ts)) / 3600.0
                END AS total_duration_hour_to_manifest,
                CASE WHEN mfbag_create_ts IS NOT NULL AND mhocnote_create_ts IS NOT NULL
                     THEN (epoch(mhocnote_create_ts) - epoch(mfbag_create_ts)) / 3600.0
                END AS total_duration_hour_to_handover,
                CASE WHEN mhocnote_create_ts IS NOT NULL AND cnote_pod_create_ts IS NOT NULL
                     THEN (epoch(cnote_pod_create_ts) - epoch(mhocnote_create_ts)) / 3600.0
                END AS total_duration_hour_to_runsheet,
                to_json(struct_pack(
                    pickup := pickup_ts,
                    first_manifest := first_manifest_ts,
                    handover_in := handover_in_ts,
                    handover_out := handover_out_ts,
                    delivery := runsheet_ts,
                    pod := pod_ts
                )) AS sla_per_step
            FROM parts
        )
        SELECT *,
            CASE
                WHEN shipment_scope IN ('Intracity', 'Intercity') THEN shipment_scope
                WHEN shipment_scope = 'Domestic' AND delivery_type = 'Transit' THEN 'Transit Domestic'
                WHEN shipment_scope = 'Domestic' AND delivery_type = 'Direct' THEN 'Direct Domestic'
                ELSE shipment_scope
            END AS delivery_category
        FROM classified
    """


def _copy_to_parquet(con: Any, query: str, output_dir: Path) -> int:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    part_path = output_dir / "part-00001.parquet"
    con.execute(f"COPY ({query}) TO {_quote_sql(part_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)")
    row_count = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_quote_sql(part_path.as_posix())})").fetchone()[0])
    (output_dir / "_SUCCESS").write_text(f"{row_count}\n", encoding="ascii")
    return row_count


def _classification_matrix(con: Any) -> list[tuple[str, str, str, int]]:
    return [
        (str(row[0]), str(row[1]), str(row[2]), int(row[3]))
        for row in con.execute(
            """
            SELECT delivery_category, delivery_type, shipment_scope, count(*) AS cnotes
            FROM cms_cnote_transformed
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """
        ).fetchall()
    ]


def _log_quality_summary(con: Any, input_rows: int, output_rows: int) -> None:
    _log(f"{DERIVED_TABLE} row count: input={input_rows:,}, output={output_rows:,}")
    if input_rows != output_rows:
        raise RuntimeError(f"{DERIVED_TABLE} row count mismatch: input={input_rows:,}, output={output_rows:,}")

    _log(f"{DERIVED_TABLE} classification matrix:")
    matrix = _classification_matrix(con)
    counts = {(delivery, scope): count for _category, delivery, scope, count in matrix}
    for category, delivery, scope, count in matrix:
        _log(f"  {category} ({delivery} / {scope}): {count:,}")

    transit_intracity = counts.get(("Transit", "Intracity"), 0)
    transit_intercity = counts.get(("Transit", "Intercity"), 0)
    unknown = sum(count for _category, _delivery, scope, count in matrix if scope == "Unknown")
    _log(f"Transit Intracity anomaly count: {transit_intracity:,}")
    _log(f"Transit Intercity anomaly count: {transit_intercity:,}")
    _log(f"Unknown shipment_scope count: {unknown:,}")
    if input_rows and unknown / input_rows > 0.01:
        _log(f"WARNING: shipment_scope Unknown is above 1 percent ({unknown / input_rows:.2%})")


def _derived_manifest_entry(
    row_count: int,
    output_dir: Path | None,
    source: DerivedSource,
    *,
    output_name: str = DERIVED_TABLE,
    source_table: str = DERIVED_SOURCE_TABLE,
) -> dict[str, Any]:
    size_bytes = 0
    file_count = 0
    if output_dir is not None:
        parts = list(output_dir.glob("part-*.parquet"))
        file_count = len(parts)
        size_bytes = sum(path.stat().st_size for path in parts)
    return {
        "table": source_table,
        "output_name": output_name,
        "stage": "derived",
        "row_count": row_count,
        "file_count": file_count,
        "size_bytes": size_bytes,
        "source_prefix": _derived_prefix(source, output_name),
    }


def _update_manifest(manifest: dict[str, Any], entry: dict[str, Any]) -> None:
    derived = [
        item
        for item in manifest.get("derived", [])
        if item.get("output_name") != entry["output_name"]
    ]
    derived.append(entry)
    manifest["derived"] = derived


def _write_local_manifest(source: DerivedSource) -> None:
    assert source.run_path is not None
    (source.run_path / "run_manifest.json").write_text(
        json.dumps(source.manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _upload_derived_to_minio(source: DerivedSource, output_dir: Path, output_name: str = DERIVED_TABLE) -> None:
    assert source.client is not None and source.bucket is not None
    prefix = _derived_prefix(source, output_name)
    for path in sorted(output_dir.iterdir()):
        if path.is_file():
            source.client.fput_object(source.bucket, f"{prefix}{path.name}", str(path))


def _read_distinct_columns(con: Any, parquet_path: str, columns: set[str]) -> Any:
    available_frame = con.execute(f"SELECT * FROM read_parquet({_quote_sql(parquet_path)}) LIMIT 0").fetchdf()
    available = set(available_frame.columns)
    selected = [column for column in sorted(columns) if column in available]
    if not selected:
        return available_frame.iloc[0:0]
    select_list = ", ".join(_quote_ident(column) for column in selected)
    return con.execute(
        f"SELECT DISTINCT {select_list} FROM read_parquet({_quote_sql(parquet_path)})"
    ).fetchdf()


def _load_document_link_inputs(con: Any, source: DerivedSource) -> dict[str, Any]:
    data = {}
    for table, columns in sorted(required_link_columns().items()):
        try:
            parquet_path = _parquet_glob(source, table)
        except KeyError:
            continue
        data[table] = _read_distinct_columns(con, parquet_path, columns)
    return data


def _write_document_links(con: Any, source: DerivedSource, tmpdir: Path | None = None) -> dict[str, Any]:
    started = time.monotonic()
    link_inputs = _load_document_link_inputs(con, source)
    links = build_document_cnote_links(link_inputs)
    con.register(DOCUMENT_LINKS_TABLE, links)

    if source.run_path is not None:
        output_dir = source.run_path / DERIVED_PARENT / DOCUMENT_LINKS_TABLE
        row_count = _copy_to_parquet(con, f"SELECT * FROM {DOCUMENT_LINKS_TABLE}", output_dir)
        entry = _derived_manifest_entry(
            row_count,
            output_dir,
            source,
            output_name=DOCUMENT_LINKS_TABLE,
            source_table=DOCUMENT_LINKS_SOURCE_TABLE,
        )
        _update_manifest(source.manifest, entry)
        _write_local_manifest(source)
    else:
        if tmpdir is None:
            raise ValueError("tmpdir is required for minio derived output")
        output_dir = tmpdir / DOCUMENT_LINKS_TABLE
        row_count = _copy_to_parquet(con, f"SELECT * FROM {DOCUMENT_LINKS_TABLE}", output_dir)
        entry = _derived_manifest_entry(
            row_count,
            output_dir,
            source,
            output_name=DOCUMENT_LINKS_TABLE,
            source_table=DOCUMENT_LINKS_SOURCE_TABLE,
        )
        _update_manifest(source.manifest, entry)
        _upload_derived_to_minio(source, output_dir, DOCUMENT_LINKS_TABLE)
        _upload_manifest_to_minio(source, tmpdir)

    _log(f"Transformed derived.{DOCUMENT_LINKS_TABLE}: {row_count:,} rows in {time.monotonic() - started:.1f}s")
    return entry


def _upload_manifest_to_minio(source: DerivedSource, tmpdir: Path) -> None:
    assert source.client is not None and source.bucket is not None and source.run_prefix is not None
    manifest_path = tmpdir / "run_manifest.json"
    manifest_path.write_text(json.dumps(source.manifest, indent=2, sort_keys=True), encoding="utf-8")
    source.client.fput_object(source.bucket, f"{source.run_prefix}/run_manifest.json", str(manifest_path))


def transform_data(source: DerivedSource, config: dict, tmpdir: Path | None = None) -> dict[str, Any]:
    started = time.monotonic()
    con = _duckdb_connection(config)
    if source.settings is not None:
        _configure_s3(con, source.settings)

    cnote_path = _parquet_glob(source, "CMS_CNOTE")
    mfcnote_path = _parquet_glob(source, "CMS_MFCNOTE")
    mfbag_path = _parquet_glob(source, "CMS_MFBAG")
    manifest_path = _parquet_glob(source, "CMS_MANIFEST")
    drcnote_path = _parquet_glob(source, "CMS_DRCNOTE")
    mrcnote_path = _parquet_glob(source, "CMS_MRCNOTE")
    dhicnote_path = _parquet_glob(source, "CMS_DHICNOTE")
    dhocnote_path = _parquet_glob(source, "CMS_DHOCNOTE")
    mhocnote_path = _parquet_glob(source, "CMS_MHOCNOTE")
    drsheet_path = _parquet_glob(source, "CMS_DRSHEET")
    cnote_pod_path = _parquet_glob(source, "CMS_CNOTE_POD")
    query = _build_cnote_transform_query(
        cnote_path,
        mfcnote_path,
        mfbag_path,
        manifest_path,
        drcnote_path,
        mrcnote_path,
        dhicnote_path,
        dhocnote_path,
        mhocnote_path,
        drsheet_path,
        cnote_pod_path,
    )

    con.execute(f"CREATE TEMP VIEW {DERIVED_TABLE} AS {query}")
    input_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({_quote_sql(cnote_path)})").fetchone()[0])
    output_rows = int(con.execute(f"SELECT COUNT(*) FROM {DERIVED_TABLE}").fetchone()[0])
    _log_quality_summary(con, input_rows, output_rows)

    if source.run_path is not None:
        output_dir = source.run_path / DERIVED_PREFIX
        row_count = _copy_to_parquet(con, f"SELECT * FROM {DERIVED_TABLE}", output_dir)
        entry = _derived_manifest_entry(row_count, output_dir, source)
        _update_manifest(source.manifest, entry)
        _write_local_manifest(source)
    else:
        if tmpdir is None:
            raise ValueError("tmpdir is required for minio derived output")
        output_dir = tmpdir / DERIVED_TABLE
        row_count = _copy_to_parquet(con, f"SELECT * FROM {DERIVED_TABLE}", output_dir)
        entry = _derived_manifest_entry(row_count, output_dir, source)
        _update_manifest(source.manifest, entry)
        _upload_derived_to_minio(source, output_dir)
        _upload_manifest_to_minio(source, tmpdir)

    _log(f"Transformed derived.{DERIVED_TABLE}: {row_count:,} rows in {time.monotonic() - started:.1f}s")
    document_links_mode = _document_links_mode(config)
    if document_links_mode == "python":
        _write_document_links(con, source, tmpdir)
    elif document_links_mode == "clickhouse":
        _log("Skipping derived.document_cnote_links in transform; ClickHouse mart load will build it")
    else:
        _log("Skipping derived.document_cnote_links in transform because transform.document_links_mode=skip")
    return entry


def run(
    config_path: str = "config/config.yaml",
    source_name: str = "minio",
    bronze_run_prefix: str | None = None,
    bronze_run_path: str | Path | None = None,
) -> None:
    config = load_config(config_path)
    if source_name == "local":
        if bronze_run_path is None:
            raise ValueError("--bronze-run-path is required for transform --source local")
        source = _source_from_local(bronze_run_path)
        transform_data(source, config)
        return
    if source_name == "minio":
        source = _source_from_minio(config_path, bronze_run_prefix)
        with tempfile.TemporaryDirectory() as tmp:
            transform_data(source, config, Path(tmp))
        return
    raise ValueError(f"Unsupported transform source: {source_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform JNE bronze tables into derived outputs.")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--source", choices=["minio", "local"], default="minio")
    parser.add_argument("--bronze-run-prefix")
    parser.add_argument("--bronze-run-path")
    args = parser.parse_args()
    run(
        config_path=args.config,
        source_name=args.source,
        bronze_run_prefix=args.bronze_run_prefix,
        bronze_run_path=args.bronze_run_path,
    )


if __name__ == "__main__":
    main()
