"""Deprecated — Aspose.Email license loader.

The PST export pipeline no longer uses Aspose.Email. PST files are now
produced by the bundled ``pst_convert`` CLI built from
``vendor/pstwriter`` (see :mod:`shared.pstwriter_cli`).

This module remains as a no-op shim so any out-of-tree caller (downstream
service, ad-hoc script) that still imports :func:`apply_license` keeps
loading without error. New code should not use it.
"""
from __future__ import annotations

import logging
import warnings

logger = logging.getLogger(__name__)


def apply_license() -> None:
    """No-op. Retained for back-compat; emits a DeprecationWarning."""
    warnings.warn(
        "shared.aspose_license.apply_license is a no-op; the PST pipeline "
        "uses the bundled pst_convert CLI now. Remove this call.",
        DeprecationWarning,
        stacklevel=2,
    )


def reset_for_testing() -> None:
    """No-op. Retained for back-compat with the old test surface."""
    return None
