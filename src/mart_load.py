"""Compatibility wrapper for the mart loader.

Prefer `python -m src.loader.mart_load` and imports from `src.loader.mart_load`.
"""

from src.loader import mart_load as _impl

globals().update({
    name: getattr(_impl, name)
    for name in dir(_impl)
    if not (name.startswith("__") and name.endswith("__"))
})


if __name__ == "__main__":
    _impl.main()

