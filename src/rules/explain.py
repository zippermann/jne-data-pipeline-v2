"""Explain registered governance rules."""

from __future__ import annotations

import inspect

from src.rules.executors import EXECUTORS
from src.rules.registry import get_rule


def explain(code: str) -> str:
    spec = get_rule(code)
    executor = EXECUTORS.get(spec.rule_family)
    mechanism = inspect.getdoc(executor) if executor else "No executor registered."
    lines = [
        f"Code: {spec.code}",
        f"Element: {spec.element}",
        f"Rule family: {spec.rule_family}",
        f"Description: {spec.description}",
        f"Child: {spec.child_table}.{spec.child_fk}",
        f"Parent: {spec.parent_table}.{spec.parent_pk}",
        f"Active: {spec.active}",
        f"Needs confirmation: {spec.needs_confirmation}",
        "",
        f"Executor: {executor.__name__ if executor else 'NONE'}",
        f"Mechanism: {mechanism}",
    ]
    return "\n".join(lines)


def print_explanation(code: str) -> None:
    print(explain(code))
