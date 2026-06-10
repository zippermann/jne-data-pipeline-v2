"""Compatibility wrapper for the governance runner.

Prefer `python -m src.governance.runner` and imports from `src.governance.runner`.
"""

from src.governance import runner as _impl

globals().update({
    name: getattr(_impl, name)
    for name in dir(_impl)
    if not (name.startswith("__") and name.endswith("__"))
})


if __name__ == "__main__":
    _impl.main()

