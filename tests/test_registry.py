from src.rules.registry import active_rules


def test_removed_governance_checks_are_inactive():
    removed_codes = {"COMP2H17", "COMP2S3", "COMP2S5"}
    active_codes = {rule.code for rule in active_rules()}

    assert removed_codes.isdisjoint(active_codes)
