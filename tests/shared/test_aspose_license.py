"""Tests for the post-removal ``shared.aspose_license`` shim.

The real Aspose.Email license loader was removed when the PST pipeline
switched to the bundled ``pst_convert`` CLI. ``apply_license`` is now a
no-op that emits a DeprecationWarning so out-of-tree callers (legacy
imports we don't control) keep loading without crashing.
"""
from __future__ import annotations

import warnings

from shared.aspose_license import apply_license, reset_for_testing


def test_apply_license_is_a_noop_and_warns():
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        apply_license()
    assert any(
        issubclass(w.category, DeprecationWarning)
        and "no-op" in str(w.message)
        for w in captured
    )


def test_reset_for_testing_returns_none_and_does_not_raise():
    assert reset_for_testing() is None
