import json

from transform.transform_data import (
    DERIVED_TABLE,
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
