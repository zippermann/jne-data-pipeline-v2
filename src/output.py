"""Compatibility wrapper for governance output writers."""

from src.governance import output as _impl

globals().update({
    name: getattr(_impl, name)
    for name in dir(_impl)
    if not (name.startswith("__") and name.endswith("__"))
})

