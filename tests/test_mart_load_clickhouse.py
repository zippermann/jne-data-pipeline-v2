from pathlib import Path

from loader.mart_load_clickhouse import (
    MartClickHouseConfig,
    MinioConfig,
    BronzeConfig,
    ClickHouseConfig,
    GovernanceConfig,
    SchemaConfig,
    UnifiedMartConfig,
    UNIFIED_REQUIRED_TABLES,
    _load_unified_mart,
    _load_table_entries,
    _load_governance_csv,
    _render_unified_sql,
    _s3_url,
    _table_object_prefix,
    load_config,
)


def _config() -> MartClickHouseConfig:
    return MartClickHouseConfig(
        minio=MinioConfig("minio:9000", "minioadmin", "minioadmin123", False),
        bronze=BronzeConfig("jne-bronze", "bronze/jne/run_id=R_TEST"),
        clickhouse=ClickHouseConfig("mart-clickhouse", 8123, "mart", "default", "jne_mart", False),
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
            Path("governance/outputs/R_TEST/governance_result_cnotes.csv"),
            Path("governance/outputs/R_TEST/governance_rule_summary.csv"),
        ),
        unified_mart=UnifiedMartConfig(
            True,
            "mart",
            "unified_shipments",
            Path("loader/sql/unified_shipments.sql"),
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
  database: "${MART_CLICKHOUSE_DB:-mart}"
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
  result_cnotes_path: "governance/outputs/${RUN_ID}/governance_result_cnotes.csv"
  summary_path: "governance/outputs/${RUN_ID}/governance_rule_summary.csv"
unified_mart:
  enabled: true
  schema: "mart"
  table: "unified_shipments"
  sql_path: "loader/sql/unified_shipments.sql"
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
    assert config.governance.result_cnotes_path.as_posix() == "governance/outputs/R_TEST/governance_result_cnotes.csv"
    assert config.governance.summary_path.as_posix() == "governance/outputs/R_TEST/governance_rule_summary.csv"
    assert config.unified_mart.enabled is True
    assert config.unified_mart.schema == "mart"
    assert config.unified_mart.table == "unified_shipments"


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


def test_clickhouse_governance_csv_rejects_mixed_column_counts(tmp_path):
    class Client:
        def __init__(self):
            self.inserts = []

        def command(self, sql):
            return None

        def insert(self, table, batch, column_names, database):
            self.inserts.append((table, batch, column_names, database))

    csv_path = tmp_path / "governance_results.csv"
    csv_path.write_text("cnote_no,index_code\nC1,COMP1\nC2,COMP2,extra\n", encoding="utf-8")
    client = Client()

    try:
        _load_governance_csv(client, "governance", "governance_results", csv_path, batch_size=100)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected ValueError for mixed governance CSV row width")

    assert "header has 2" in message
    assert f"{csv_path}:3" in message
    assert client.inserts == []


def test_unified_mart_sql_template_renders_qualified_names(tmp_path):
    sql_path = tmp_path / "unified.sql"
    sql_path.write_text("CREATE TABLE {target_table} AS SELECT * FROM {bronze_schema}.`cms_cnote`", encoding="utf-8")
    config = _config()
    config = MartClickHouseConfig(
        config.minio,
        config.bronze,
        config.clickhouse,
        config.schemas,
        config.governance,
        UnifiedMartConfig(True, "mart", "unified_shipments", sql_path),
        config.skip_stages,
        config.reuse_existing_stages,
        config.load_mode,
    )

    sql = _render_unified_sql(config)

    assert "CREATE TABLE `mart`.`unified_shipments`" in sql
    assert "FROM `bronze`.`cms_cnote`" in sql


def test_unified_mart_validates_required_sources_before_rebuild(monkeypatch, tmp_path):
    sql_path = tmp_path / "unified.sql"
    sql_path.write_text("CREATE TABLE {target_table} AS SELECT 1", encoding="utf-8")
    config = _config()
    config = MartClickHouseConfig(
        config.minio,
        config.bronze,
        config.clickhouse,
        config.schemas,
        config.governance,
        UnifiedMartConfig(True, "mart", "unified_shipments", sql_path),
        config.skip_stages,
        config.reuse_existing_stages,
        config.load_mode,
    )

    monkeypatch.setattr(
        "loader.mart_load_clickhouse._table_exists",
        lambda client, schema, table: table != "cms_mstatus",
    )

    try:
        _load_unified_mart(object(), config)
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected unified mart source validation to fail")

    assert "bronze.cms_mstatus" in message


def test_unified_mart_rebuilds_target_after_source_validation(monkeypatch, tmp_path):
    sql_path = tmp_path / "unified.sql"
    sql_path.write_text("CREATE TABLE {target_table} AS SELECT * FROM {bronze_schema}.`cms_cnote`", encoding="utf-8")
    config = _config()
    config = MartClickHouseConfig(
        config.minio,
        config.bronze,
        config.clickhouse,
        config.schemas,
        config.governance,
        UnifiedMartConfig(True, "mart", "unified_shipments", sql_path),
        config.skip_stages,
        config.reuse_existing_stages,
        config.load_mode,
    )
    commands = []

    monkeypatch.setattr("loader.mart_load_clickhouse._table_exists", lambda client, schema, table: True)
    monkeypatch.setattr("loader.mart_load_clickhouse._command", lambda client, sql: commands.append(sql))
    monkeypatch.setattr("loader.mart_load_clickhouse._query_scalar", lambda client, sql: 42)

    rows = _load_unified_mart(object(), config)

    assert rows == 42
    assert any("CREATE DATABASE IF NOT EXISTS `mart`" in sql for sql in commands)
    assert any("DROP TABLE IF EXISTS `mart`.`unified_shipments`" in sql for sql in commands)
    assert any("CREATE TABLE `mart`.`unified_shipments`" in sql for sql in commands)


def test_unified_mart_sql_references_declared_required_sources():
    sql = Path("loader/sql/unified_shipments.sql").read_text(encoding="utf-8")

    for table in UNIFIED_REQUIRED_TABLES:
        assert f"`{table}`" in sql


def test_unified_mart_manifest_joins_match_pipeline_mapping_workbook():
    sql = Path("loader/sql/unified_shipments.sql").read_text(encoding="utf-8")

    assert "f.`MFCNOTE_MAN_NO` AS `MF_NO`" in sql
    assert "f.`MFCNOTE_NO` AS `MF_CNOTE_NO`" in sql
    assert "f.`MFCNOTE_BAG_NO` AS `MF_BAG_NO`" in sql
    assert "ON f.`MFCNOTE_MAN_NO` = m.`MANIFEST_NO`" in sql
    assert "ON c.`CNOTE_NO` = m.`MF_CNOTE_NO`" in sql
    assert "ON m.`OM_BAG_NO` = sm.`SM_BAG`" in sql
