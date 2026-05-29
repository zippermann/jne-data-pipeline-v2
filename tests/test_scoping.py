from src.bronze import sanitize_run_id


def test_sanitize_run_id_for_oracle_identifier_suffix():
    assert sanitize_run_id("2026-05-29T10:20:30+07:00") == "R_2026_05_29T10_20_30_07_00"


def test_sanitize_run_id_limits_length():
    assert len(sanitize_run_id("x" * 100)) == 40
