from src.extractor.bronze import (
    TABLE_SPECS,
    ScopeSettings,
    _build_sql,
    _cnote_limit,
    _expand_required_scopes,
    _scope_index_name,
    scope_predicate,
    sanitize_run_id,
)


def test_sanitize_run_id_for_oracle_identifier_suffix():
    assert sanitize_run_id("2026-05-29T10:20:30+07:00") == "R_2026_05_29T10_20_30_07_00"


def test_sanitize_run_id_limits_length():
    assert len(sanitize_run_id("x" * 100)) == 40


def test_scope_index_name_uses_full_table_hash_to_avoid_prefix_collisions():
    first = _scope_index_name("HOA.BRONZE_SCOPE_CNOTE_MAY_100K_CNOTES_20260610T084415")
    second = _scope_index_name("HOA.BRONZE_SCOPE_CNOTE_MAY_100K_CNOTES_20260610T084516")

    assert first != second
    assert len(first) <= 30
    assert len(second) <= 30


def _spec(table: str):
    return next(spec for spec in TABLE_SPECS if spec.table == table)


def test_master_tables_scope_through_detail_doc_numbers():
    expected = {
        "CMS_MRCNOTE": ("MRCNOTE_NO", "DRCNOTE"),
        "CMS_MHI_HOC": ("MHI_NO", "DHI_HOC"),
        "CMS_MHOUNDEL_POD": ("MHOUNDEL_NO", "DHOUNDEL"),
        "CMS_MRSHEET": ("MRSHEET_NO", "DRSHEET"),
        "CMS_MHICNOTE": ("MHICNOTE_NO", "HVI"),
        "CMS_MHOCNOTE": ("MHOCNOTE_NO", "HVO"),
        "CMS_MSMU": ("MSMU_NO", "SMU"),
        "CMS_COST_MTRANSIT_AGEN": ("MANIFEST_NO", "COST_MANIFEST"),
    }

    for table, (scope_column, scope_name) in expected.items():
        spec = _spec(table)
        assert spec.scope_column == scope_column
        assert spec.scope_name == scope_name


def test_bag_chain_uses_manifest_bags_not_handover_bags():
    assert _spec("CMS_MFBAG").scope_name == "MANIFEST"
    assert _spec("CMS_DMBAG").scope_column == "DMBAG_BAG_NO"
    assert _spec("CMS_DMBAG").scope_name == "MFBAG"
    assert _spec("CMS_MMBAG").scope_name == "DMBAG"
    assert _spec("CMS_DSMU").scope_name == "DMBAG"


def test_scope_dependencies_cover_parent_chains():
    scopes = _expand_required_scopes({"MMBAG", "SMU", "MSJ", "COST_MANIFEST"})

    assert {"CNOTE", "MANIFEST", "MFBAG", "DMBAG", "MMBAG", "SMU"} <= scopes
    assert {"HVI", "RDSJ_HVO", "MSJ"} <= scopes
    assert "COST_MANIFEST" in scopes
    assert "BAG" not in scopes
    assert "RUNSHEET" not in scopes


def test_scope_predicate_uses_correct_parent_key_columns():
    scope = ScopeSettings("JNE", "HOA", "BRONZE_SCOPE_", "R_TEST")

    assert scope_predicate(scope, "src", "MFBAG", "DMBAG_BAG_NO") == (
        "src.DMBAG_BAG_NO IN (SELECT MFBAG_NO FROM HOA.BRONZE_SCOPE_MFBAG_R_TEST)"
    )
    assert scope_predicate(scope, "src", "DMBAG", "MMBAG_NO") == (
        "src.MMBAG_NO IN (SELECT DMBAG_NO FROM HOA.BRONZE_SCOPE_DMBAG_R_TEST)"
    )
    assert scope_predicate(scope, "src", "RDSJ_HVO", "DSJ_HVO_NO") == (
        "src.DSJ_HVO_NO IN (SELECT HVO_NO FROM HOA.BRONZE_SCOPE_RDSJ_HVO_R_TEST)"
    )


def test_date_guardrail_adds_window_filter_to_scoped_operational_tables():
    scope = ScopeSettings("JNE", "HOA", "BRONZE_SCOPE_", "R_TEST")
    config = {
        "oracle": {"source_schema": "JNE"},
        "extraction": {"anchor_date_column": "CNOTE_DATE"},
        "scoping": {
            "date_guardrails_enabled": True,
            "date_guardrail_lookback_days": 0,
            "date_guardrail_lookahead_days": 14,
        },
    }

    sql, _binds = _build_sql(
        config,
        _spec("CMS_CNOTE_POD"),
        ["CNOTE_POD_NO", "CNOTE_POD_DATE"],
        scope,
        ["CNOTE_POD_NO", "CNOTE_POD_DATE"],
    )

    assert "src.CNOTE_POD_NO IN (SELECT CNOTE_NO FROM HOA.BRONZE_SCOPE_CNOTE_R_TEST)" in sql
    assert "src.CNOTE_POD_DATE >= :start_date - 0" in sql
    assert "src.CNOTE_POD_DATE < :end_date + 14" in sql


def test_date_guardrail_can_be_disabled():
    scope = ScopeSettings("JNE", "HOA", "BRONZE_SCOPE_", "R_TEST")
    config = {
        "oracle": {"source_schema": "JNE"},
        "extraction": {"anchor_date_column": "CNOTE_DATE"},
        "scoping": {"date_guardrails_enabled": False},
    }

    sql, _binds = _build_sql(
        config,
        _spec("CMS_CNOTE_POD"),
        ["CNOTE_POD_NO", "CNOTE_POD_DATE"],
        scope,
        ["CNOTE_POD_NO", "CNOTE_POD_DATE"],
    )

    assert "src.CNOTE_POD_NO IN (SELECT CNOTE_NO FROM HOA.BRONZE_SCOPE_CNOTE_R_TEST)" in sql
    assert "CNOTE_POD_DATE >= :start_date" not in sql


def test_anchor_table_uses_cnote_scope_when_limit_is_configured():
    scope = ScopeSettings("JNE", "HOA", "BRONZE_SCOPE_", "R_TEST")
    config = {
        "oracle": {"source_schema": "JNE"},
        "extraction": {"anchor_date_column": "CNOTE_DATE", "cnote_limit": 100000},
    }

    sql, _binds = _build_sql(
        config,
        _spec("CMS_CNOTE"),
        ["CNOTE_NO", "CNOTE_DATE"],
        scope,
        ["CNOTE_NO", "CNOTE_DATE"],
    )

    assert "src.CNOTE_DATE >= :start_date AND src.CNOTE_DATE < :end_date" in sql
    assert "src.CNOTE_NO IN (SELECT CNOTE_NO FROM HOA.BRONZE_SCOPE_CNOTE_R_TEST)" in sql


def test_cnote_limit_must_be_positive_when_set():
    assert _cnote_limit({"extraction": {"cnote_limit": "100000"}}) == 100000
    assert _cnote_limit({"extraction": {}}) is None

    try:
        _cnote_limit({"extraction": {"cnote_limit": 0}})
    except ValueError as exc:
        assert "extraction.cnote_limit" in str(exc)
    else:
        raise AssertionError("Expected ValueError for zero cnote_limit")
