"""Tests for the Activity-row 'object' label rules.

Pins behavior from the 2026-05-15 false-positive incident:
  - PREEMPTIVE jobs (anomaly-detector responses) are labelled with the
    resource name + "Preemptive" prefix, NOT a generic "N resources".
  - Plural agreement: "1 resource" (singular), "5 resources" (plural).
  - Empty groups fall back to "Bulk Operation".

The pure helper `_render_object_label` is loaded via importlib so we
don't pull in the rest of audit-service/main.py (FastAPI, DB session,
RabbitMQ, etc.) for what is fundamentally a string-formatting rule.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest


_AUDIT_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "audit-service" / "main.py"
)


def _load_render_helper():
    """Extract just `_render_object_label` from main.py.

    Walks the AST, picks the FunctionDef for _render_object_label, and
    exec()'s the resulting module. Avoids triggering main.py's
    top-level imports (FastAPI, SQLAlchemy, RabbitMQ, etc.) that fail
    without a full service env.
    """
    import ast, textwrap
    source = _AUDIT_MAIN.read_text()
    tree = ast.parse(source)
    helper = next(
        (n for n in tree.body
         if isinstance(n, ast.FunctionDef) and n.name == "_render_object_label"),
        None,
    )
    if helper is None:
        pytest.skip("_render_object_label not found in audit-service/main.py",
                    allow_module_level=True)
    module = type(sys)("audit_label_helper_under_test")
    module.__dict__["Optional"] = __import__("typing").Optional
    code = compile(
        ast.Module(body=[helper], type_ignores=[]),
        str(_AUDIT_MAIN),
        "exec",
    )
    exec(code, module.__dict__)
    return module._render_object_label


render = _load_render_helper()


def test_preemptive_with_name_uses_preemptive_dash_name():
    """PREEMPTIVE + caller pre-fetched display_name → operator sees
    exactly which resource the anomaly detector responded to."""
    out = render(
        is_preemptive=True,
        preemptive_name="Mail — Hemant Singh",
        total_resources=1,
    )
    assert out == "Preemptive — Mail — Hemant Singh"


def test_preemptive_without_name_still_signals_preemptive():
    """Caller couldn't resolve a name (resource vanished, race).
    Label still must NOT revert to '1 resource' — operator needs to
    know this row is the anomaly response, not a normal click."""
    out = render(
        is_preemptive=True,
        preemptive_name=None,
        total_resources=1,
    )
    assert out == "Preemptive backup"


def test_one_resource_is_singular():
    """Plural-agreement fix. Pre-fix code printed '1 resources'."""
    assert render(
        is_preemptive=False, preemptive_name=None, total_resources=1,
    ) == "1 resource"


def test_two_resources_is_plural():
    assert render(
        is_preemptive=False, preemptive_name=None, total_resources=2,
    ) == "2 resources"


def test_five_resources_is_plural():
    assert render(
        is_preemptive=False, preemptive_name=None, total_resources=5,
    ) == "5 resources"


def test_zero_resources_falls_back_to_bulk_operation():
    """Legacy non-batch_resource_ids paths still surface as
    'Bulk Operation' so dashboards don't show a numeric '0 resources'
    that misleads operators about what ran."""
    assert render(
        is_preemptive=False, preemptive_name=None, total_resources=0,
    ) == "Bulk Operation"


def test_preemptive_overrides_resource_count():
    """Even with non-1 total_resources, PREEMPTIVE label wins —
    anomaly jobs are always single-resource by construction; if a
    multi-resource preemptive ever lands, prefer the operator-clear
    label over the resource count."""
    out = render(
        is_preemptive=True,
        preemptive_name="Some Mailbox",
        total_resources=3,
    )
    assert out == "Preemptive — Some Mailbox"
