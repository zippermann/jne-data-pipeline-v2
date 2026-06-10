"""Compatibility wrapper for governance DuckDB helpers."""

from src.governance import duck as _impl

globals().update({
    name: getattr(_impl, name)
    for name in dir(_impl)
    if not (name.startswith("__") and name.endswith("__"))
})

