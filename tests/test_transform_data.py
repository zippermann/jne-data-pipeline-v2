import json

import pyarrow as pa

from transform.transform_data import (
    DERIVED_TABLE,
    _copy_to_parquet,
    _derived_manifest_entry,
    _update_manifest,
    delivery_category,
    shipment_scope,
)


def test_shipment_scope_classification_cases():
    assert shipment_scope("CGK10000", "CGK10209") == "Intracity"
    assert shipment_scope("CGK10000", "CGK20000") == "Intercity"
    assert shipment_scope("CGK10000", "BKI10044") == "Domestic"
    assert shipment_scope("CGK", "BKI10044") == "Unknown"
    assert shipment_scope("CGKA0000", "BKI10044") == "Unknown"


def test_delivery_category_combines_domestic_only():
    assert delivery_category("Direct", "Domestic") == "Direct Domestic"
    assert delivery_category("Transit", "Domestic") == "Transit Domestic"
    assert delivery_category("Direct", "Intracity") == "Intracity"
    assert delivery_category("Transit", "Intercity") == "Intercity"
    assert delivery_category("Direct", "Unknown") == "Unknown"


def test_update_manifest_replaces_existing_derived_entry(tmp_path):
    output_dir = tmp_path / "derived" / DERIVED_TABLE
    output_dir.mkdir(parents=True)
    (output_dir / "part-00001.parquet").write_bytes(b"placeholder")
    manifest = {
        "tables": [],
        "derived": [{"output_name": DERIVED_TABLE, "row_count": 1, "source_prefix": "old/"}],
    }

    class Source:
        run_prefix = "bronze/jne/run_id=R_TEST"

    entry = _derived_manifest_entry(10, output_dir, Source())
    _update_manifest(manifest, entry)

    assert json.loads(json.dumps(manifest))["derived"] == [
        {
            "table": "CMS_CNOTE_TRANSFORMED",
            "output_name": DERIVED_TABLE,
            "stage": "derived",
            "row_count": 10,
            "file_count": 1,
            "size_bytes": len(b"placeholder"),
            "source_prefix": "bronze/jne/run_id=R_TEST/derived/cms_cnote_transformed/",
        }
    ]


def test_copy_to_parquet_chunks_output(tmp_path):
    batches = [
        pa.record_batch([pa.array([1, 2])], names=["id"]),
        pa.record_batch([pa.array([3, 4])], names=["id"]),
        pa.record_batch([pa.array([5])], names=["id"]),
    ]

    class Result:
        def __init__(self, value):
            self.value = value

        def fetchone(self):
            return (self.value,)

        def fetch_record_batch(self, _rows_per_file):
            return iter(batches)

    class Connection:
        def __init__(self):
            self.commands = []

        def execute(self, sql):
            self.commands.append(sql)
            if sql.startswith("SELECT COUNT(*) FROM"):
                return Result(5)
            return Result(None)

    con = Connection()
    row_count = _copy_to_parquet(con, "SELECT * FROM source_table", tmp_path / "derived", rows_per_file=2)

    assert row_count == 5
    assert con.commands == [
        "SELECT COUNT(*) FROM (SELECT * FROM source_table)",
        "SELECT * FROM source_table",
    ]
    assert sorted(path.name for path in (tmp_path / "derived").glob("part-*.parquet")) == [
        "part-00001.parquet",
        "part-00002.parquet",
        "part-00003.parquet",
    ]
    assert (tmp_path / "derived" / "_SUCCESS").read_text(encoding="ascii") == "5\n"
