"""Compatibility wrapper for the bronze extractor.

Prefer `python -m src.extractor.bronze` and imports from `src.extractor.bronze`.
"""

from src.extractor import bronze as _impl

globals().update({
    name: getattr(_impl, name)
    for name in dir(_impl)
    if not (name.startswith("__") and name.endswith("__"))
})


if __name__ == "__main__":
    _impl.main()

