from pathlib import Path

from loader.mart_load_clickhouse import (
    MartClickHouseConfig,
    MinioConfig,
    BronzeConfig,
    ClickHouseConfig,
    GovernanceConfig,
    SchemaConfig,
    _load_table_entries,
    _s3_url,
    _table_object_prefix,
    load_config,
)


def _config() -> MartClickHouseConfig:
    return MartClickHouseConfig(
        minio=MinioConfig("minio:9000", "minioadmin", "minioadmin123", False),
        bronze=BronzeConfig("jne-bronze", "bronze/jne/run_id=R_TEST"),
        clickhouse=ClickHouseConfig("mart-clickhouse", 8123, "jne_mart", "default", "jne_mart", False),
        schemas=SchemaConfig(
            bronze="bronze",
            bronze_staging="bronze_staging",
            derived="derived",
            derived_staging="derived_staging",
            governance="governance",
        ),
        governance=GovernanceConfig(
            True,
            Path("governance/outputs/R_TEST/governance_results.csv"),
            Path("governance/outputs/R_TEST/governance_rule_summary.csv"),
        ),
        reuse_existing_stages=("reference",),
    )


def test_clickhouse_config_expands_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("BRONZE_RUN_PREFIX", "bronze/jne/run_id=R_TEST")
    monkeypatch.setenv("RUN_ID", "R_TEST")
    monkeypatch.setenv("MART_CLICKHOUSE_PASSWORD", "secret")
    config_path = tmp_path / "mart_clickhouse.yaml"
    config_path.write_text(
        """
minio:
  endpoint: "${MINIO_ENDPOINT:-minio:9000}"
  access_key: "${MINIO_ACCESS_KEY:-minioadmin}"
  secret_key: "${MINIO_SECRET_KEY:-minioadmin123}"
  secure: "${MINIO_SECURE:-false}"
bronze:
  bucket: "${MINIO_BUCKET:-jne-bronze}"
  run_prefix: "${BRONZE_RUN_PREFIX}"
clickhouse:
  host: "${MART_CLICKHOUSE_HOST:-mart-clickhouse}"
  port: "${MART_CLICKHOUSE_PORT:-8123}"
  database: "${MART_CLICKHOUSE_DB:-jne_mart}"
  user: "${MART_CLICKHOUSE_USER:-default}"
  password: "${MART_CLICKHOUSE_PASSWORD:-jne_mart}"
schemas:
  bronze: "bronze"
  bronze_staging: "bronze_staging"
  derived: "derived"
  derived_staging: "derived_staging"
  governance: "governance"
governance:
  enabled: true
  results_path: "governance/outputs/${RUN_ID}/governance_results.csv"
  summary_path: "governance/outputs/${RUN_ID}/governance_rule_summary.csv"
mart:
  load_mode: "latest_snapshot"
  skip_stages: []
  reuse_existing_stages: ["reference"]
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.bronze.run_prefix == "bronze/jne/run_id=R_TEST"
    assert config.clickhouse.password == "secret"
    assert config.skip_stages == ()
    assert config.reuse_existing_stages == ("reference",)
    assert config.governance.results_path.as_posix() == "governance/outputs/R_TEST/governance_results.csv"
    assert config.governance.summary_path.as_posix() == "governance/outputs/R_TEST/governance_rule_summary.csv"


def test_clickhouse_s3_url_uses_source_prefix_for_reused_tables():
    config = _config()
    table_info = {
        "output_name": "cms_drourate",
        "source_prefix": "bronze/jne/old_run/cms_drourate/",
    }

    prefix = _table_object_prefix(config, table_info)

    assert prefix == "bronze/jne/old_run/cms_drourate/"
    assert _s3_url(config, prefix) == "http://minio:9000/jne-bronze/bronze/jne/old_run/cms_drourate/part-*.parquet"


def test_clickhouse_derived_prefix_defaults_under_derived_folder():
    config = _config()
    table_info = {"output_name": "cms_cnote_transformed"}

    prefix = _table_object_prefix(config, table_info, default_parent="derived")

    assert prefix == "bronze/jne/run_id=R_TEST/derived/cms_cnote_transformed/"


def test_clickhouse_loader_replaces_raw_cnote_and_loads_missing_reference(monkeypatch):
    loaded = []

    def fake_load(client, config, schema, table_info, label, default_parent=None):
        loaded.append((table_info["output_name"], default_parent))
        return 12

    monkeypatch.setattr("loader.mart_load_clickhouse._load_s3_table", fake_load)
    monkeypatch.setattr("loader.mart_load_clickhouse._table_exists", lambda client, schema, table: False)
    entries = [
        {"output_name": "cms_cnote", "stage": "anchor", "row_count": 12},
        {"output_name": "cms_cnote_transformed", "stage": "derived", "row_count": 12},
        {"output_name": "cms_drourate", "stage": "reference", "row_count": 78_000_000},
    ]

    result = _load_table_entries(
        object(),
        _config(),
        entries,
        "bronze_staging",
        "bronze",
        default_parent="derived",
        target_schema="bronze",
    )

    assert result == {"cms_cnote": 12, "cms_drourate": 12}
    assert loaded == [("cms_cnote_transformed", "derived"), ("cms_drourate", "derived")]


def test_clickhouse_loader_reuses_existing_reference_tables(monkeypatch):
    loaded = []

    def fake_load(client, config, schema, table_info, label, default_parent=None):
        loaded.append(table_info["output_name"])
        return 12

    def fake_exists(client, schema, table):
        return table == "cms_drourate"

    monkeypatch.setattr("loader.mart_load_clickhouse._load_s3_table", fake_load)
    monkeypatch.setattr("loader.mart_load_clickhouse._table_exists", fake_exists)
    entries = [
        {"output_name": "cms_drourate", "stage": "reference", "row_count": 78_000_000},
        {"output_name": "cms_manifest", "stage": "bag_manifest", "row_count": 12},
    ]

    result = _load_table_entries(object(), _config(), entries, "bronze_staging", "bronze", target_schema="bronze")

    assert result == {"cms_manifest": 12}
    assert loaded == ["cms_manifest"]
