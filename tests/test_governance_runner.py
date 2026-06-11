from governance.runner import _entry_tables, _required_tables


def test_required_tables_include_reference_and_master_tables():
    required = _required_tables()

    assert "CMS_DROURATE" in required
    assert "T_CORRECT_AWB" in required
    assert "CMS_MSMU" in required
    assert "CMS_DSMU" in required


def test_entry_tables_collect_all_param_table_roles():
    entry = {
        "table": "BASE_TABLE",
        "params": {
            "left_table": "LEFT_TABLE",
            "right_table": "RIGHT_TABLE",
            "start_table": "START_TABLE",
            "end_table": "END_TABLE",
            "child_table": "CHILD_TABLE",
            "parent_table": "PARENT_TABLE",
            "reference_table": "REFERENCE_TABLE",
            "master_table": "MASTER_TABLE",
        },
    }

    assert _entry_tables(entry) == {
        "BASE_TABLE",
        "LEFT_TABLE",
        "RIGHT_TABLE",
        "START_TABLE",
        "END_TABLE",
        "CHILD_TABLE",
        "PARENT_TABLE",
        "REFERENCE_TABLE",
        "MASTER_TABLE",
    }
