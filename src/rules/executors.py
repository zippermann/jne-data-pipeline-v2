"""Compatibility wrapper for governance rule executors."""

from src.governance.rules import executors as _impl

globals().update({
    name: getattr(_impl, name)
    for name in dir(_impl)
    if not (name.startswith("__") and name.endswith("__"))
})

