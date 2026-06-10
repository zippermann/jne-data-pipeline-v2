from datetime import date

from src.extractor.bronze import resolve_window


def test_relative_window_adds_months():
    config = {"extraction": {"window": {"mode": "relative", "anchor_month": "2026-03", "num_months": 2}}}

    window = resolve_window(config)

    assert window.start == date(2026, 3, 1)
    assert window.end == date(2026, 5, 1)


def test_explicit_window():
    config = {
        "extraction": {
            "window": {
                "mode": "explicit",
                "start_date": "2026-03-15",
                "end_date": "2026-04-01",
            }
        }
    }

    window = resolve_window(config)

    assert window.start == date(2026, 3, 15)
    assert window.end == date(2026, 4, 1)
