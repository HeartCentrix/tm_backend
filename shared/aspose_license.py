"""Aspose.Email license loader.

Applies the Aspose.Email license exactly once per process using a
threading.Lock + _applied flag.  The aspose.email package is imported
lazily (inside apply_license) so this module can be imported without
aspose-email being installed — useful for unit tests.

Priority chain for locating the .lic file
------------------------------------------
1. Azure Key Vault  — ASPOSE_LICENSE_KV_SECRET_NAME + ASPOSE_LICENSE_KV_URL
2. Local file       — ASPOSE_LICENSE_PATH (must exist on disk)
3. Base64 env var   — ASPOSE_EMAIL_LIC_B64 (base64-encoded .lic bytes)
4. Trial mode       — no env vars set; caller receives a warning
"""
from __future__ import annotations

import base64
import logging
import os
import tempfile
import threading

logger = logging.getLogger(__name__)

_lock: threading.Lock = threading.Lock()
_applied: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_temp(data: bytes) -> str:
    """Write *data* to a NamedTemporaryFile with suffix `.lic` and return the path."""
    # delete=False: tempfile persists until OS cleanup — intentional, .lic files are tiny
    with tempfile.NamedTemporaryFile(suffix=".lic", delete=False) as fh:
        fh.write(data)
        return fh.name


def _resolve_license_path() -> str | None:
    """Return a filesystem path to the .lic file, or *None* for trial mode."""

    # --- Priority 1: Azure Key Vault ----------------------------------------
    kv_secret_name = os.environ.get("ASPOSE_LICENSE_KV_SECRET_NAME", "")
    kv_url = os.environ.get("ASPOSE_LICENSE_KV_URL", "")
    if kv_secret_name and kv_url:
        try:
            from azure.identity import DefaultAzureCredential  # type: ignore
            from azure.keyvault.secrets import SecretClient  # type: ignore

            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=kv_url, credential=credential)
            secret = client.get_secret(kv_secret_name)
            lic_bytes = base64.b64decode(secret.value)
            path = _write_temp(lic_bytes)
            logger.info("[aspose_license] License loaded from Azure Key Vault secret '%s'", kv_secret_name)
            return path
        except Exception:
            logger.exception("[aspose_license] Failed to fetch license from Key Vault — falling through to next priority")

    # --- Priority 2: Local file ---------------------------------------------
    local_path = os.environ.get("ASPOSE_LICENSE_PATH", "")
    if local_path and os.path.exists(local_path):
        logger.info("[aspose_license] License loaded from local file: %s", local_path)
        return local_path

    # --- Priority 3: Base64 env var -----------------------------------------
    b64_value = os.environ.get("ASPOSE_EMAIL_LIC_B64", "")
    if b64_value:
        try:
            lic_bytes = base64.b64decode(b64_value)
            path = _write_temp(lic_bytes)
            logger.info("[aspose_license] License loaded from ASPOSE_EMAIL_LIC_B64 env var")
            return path
        except Exception:
            logger.exception("[aspose_license] Failed to decode ASPOSE_EMAIL_LIC_B64")

    # --- Priority 4: Trial mode ---------------------------------------------
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_license() -> None:
    """Apply the Aspose.Email license exactly once per process (thread-safe)."""
    global _applied

    with _lock:
        if _applied:
            return

        path = _resolve_license_path()

        if path:
            # Lazy import via importlib — ensures sys.modules lookup is used so that
            # tests can stub aspose.email without installing the real package.
            import importlib  # noqa: PLC0415
            _aspose_email = importlib.import_module("aspose.email")  # type: ignore
            license_obj = _aspose_email.License()
            license_obj.set_license(path)
            logger.info("[aspose_license] Aspose.Email license applied from: %s", path)
        else:
            logger.warning(
                "[aspose_license] No license found — running in TRIAL mode "
                "(50 msg/folder cap, watermarked output)"
            )

        _applied = True


def reset_for_testing() -> None:
    """Reset the applied flag.  **For unit tests only** — do not call in production."""
    global _applied
    _applied = False
