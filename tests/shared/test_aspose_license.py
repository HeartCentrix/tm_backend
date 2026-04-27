"""Unit tests for shared.aspose_license — all priority paths, thread safety, idempotency.

aspose-email is NOT installed in the test environment.  We mock the entire
aspose module tree at sys.modules level before importing aspose_license so
that the lazy `import aspose.email` inside apply_license() resolves to our
MagicMock without requiring the real package.
"""
from __future__ import annotations

import base64
import os
import sys
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Stub the aspose package tree BEFORE importing our module under test.
# ---------------------------------------------------------------------------
_aspose_mock = MagicMock()
_aspose_email_mock = MagicMock()
sys.modules.setdefault("aspose", _aspose_mock)
sys.modules.setdefault("aspose.email", _aspose_email_mock)

# Now safe to import — lazy import inside apply_license will hit our stubs.
import shared.aspose_license as aspose_license_mod
from shared.aspose_license import apply_license, reset_for_testing  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_state():
    """Reset module-level _applied flag before every test."""
    reset_for_testing()
    yield
    reset_for_testing()


@pytest.fixture()
def mock_license_cls(monkeypatch):
    """Return a fresh MagicMock that stands in for aspose.email.License."""
    license_cls = MagicMock()
    license_instance = MagicMock()
    license_cls.return_value = license_instance
    monkeypatch.setattr(_aspose_email_mock, "License", license_cls)
    return license_cls, license_instance


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestKeyVaultPriority:
    """Priority 1 — Azure Key Vault."""

    def test_kv_used_when_both_vars_set(self, tmp_path, mock_license_cls, monkeypatch):
        """When KV env vars are set, secret is fetched and license applied via tempfile."""
        license_cls, license_instance = mock_license_cls
        lic_bytes = b"<License>kv-data</License>"
        b64_secret = _b64(lic_bytes)

        fake_secret = MagicMock()
        fake_secret.value = b64_secret

        fake_client = MagicMock()
        fake_client.get_secret.return_value = fake_secret

        mock_secret_client = MagicMock(return_value=fake_client)
        mock_credential = MagicMock()

        monkeypatch.setenv("ASPOSE_LICENSE_KV_SECRET_NAME", "my-lic-secret")
        monkeypatch.setenv("ASPOSE_LICENSE_KV_URL", "https://myvault.vault.azure.net")

        with patch.dict(sys.modules, {
            "azure": MagicMock(),
            "azure.identity": MagicMock(DefaultAzureCredential=mock_credential),
            "azure.keyvault": MagicMock(),
            "azure.keyvault.secrets": MagicMock(SecretClient=mock_secret_client),
        }):
            apply_license()

        fake_client.get_secret.assert_called_once_with("my-lic-secret")
        license_instance.set_license.assert_called_once()
        # The path passed should be a string (tempfile path)
        path_arg = license_instance.set_license.call_args[0][0]
        assert isinstance(path_arg, str)
        assert path_arg.endswith(".lic")


class TestFilePriority:
    """Priority 2 — local file path."""

    def test_file_path_used_when_set_and_exists(self, tmp_path, mock_license_cls, monkeypatch):
        """ASPOSE_LICENSE_PATH is passed directly to set_license when it exists."""
        license_cls, license_instance = mock_license_cls
        lic_file = tmp_path / "aspose.lic"
        lic_file.write_bytes(b"<License>file</License>")

        monkeypatch.setenv("ASPOSE_LICENSE_PATH", str(lic_file))

        apply_license()

        license_instance.set_license.assert_called_once_with(str(lic_file))

    def test_file_path_ignored_when_not_exists(self, mock_license_cls, monkeypatch):
        """A non-existent ASPOSE_LICENSE_PATH is skipped (falls through to trial)."""
        license_cls, license_instance = mock_license_cls
        monkeypatch.setenv("ASPOSE_LICENSE_PATH", "/nonexistent/path/aspose.lic")

        apply_license()

        license_instance.set_license.assert_not_called()


class TestBase64Fallback:
    """Priority 3 — base64 env var."""

    def test_b64_env_var_decoded_and_applied(self, mock_license_cls, monkeypatch):
        """ASPOSE_EMAIL_LIC_B64 bytes are decoded, written to tempfile, and applied."""
        license_cls, license_instance = mock_license_cls
        lic_bytes = b"<License>b64-content</License>"
        monkeypatch.setenv("ASPOSE_EMAIL_LIC_B64", _b64(lic_bytes))

        apply_license()

        license_instance.set_license.assert_called_once()
        path_arg = license_instance.set_license.call_args[0][0]
        assert path_arg.endswith(".lic")
        # Verify the tempfile contains the decoded bytes
        with open(path_arg, "rb") as fh:
            assert fh.read() == lic_bytes


class TestPriorityOrdering:
    """Verify that higher-priority sources beat lower-priority ones."""

    def test_kv_beats_file(self, tmp_path, mock_license_cls, monkeypatch):
        """KV env vars take precedence over ASPOSE_LICENSE_PATH."""
        license_cls, license_instance = mock_license_cls
        lic_bytes = b"<License>kv</License>"
        b64_secret = _b64(lic_bytes)

        lic_file = tmp_path / "file.lic"
        lic_file.write_bytes(b"<License>file</License>")

        fake_secret = MagicMock()
        fake_secret.value = b64_secret
        fake_client = MagicMock()
        fake_client.get_secret.return_value = fake_secret
        mock_secret_client = MagicMock(return_value=fake_client)
        mock_credential = MagicMock()

        monkeypatch.setenv("ASPOSE_LICENSE_KV_SECRET_NAME", "my-secret")
        monkeypatch.setenv("ASPOSE_LICENSE_KV_URL", "https://vault.azure.net")
        monkeypatch.setenv("ASPOSE_LICENSE_PATH", str(lic_file))

        with patch.dict(sys.modules, {
            "azure": MagicMock(),
            "azure.identity": MagicMock(DefaultAzureCredential=mock_credential),
            "azure.keyvault": MagicMock(),
            "azure.keyvault.secrets": MagicMock(SecretClient=mock_secret_client),
        }):
            apply_license()

        # KV was consulted
        fake_client.get_secret.assert_called_once_with("my-secret")
        # License was applied once (from KV tempfile, not the local file path)
        license_instance.set_license.assert_called_once()
        path_arg = license_instance.set_license.call_args[0][0]
        assert path_arg != str(lic_file), "Expected KV tempfile path, not the local file path"

    def test_file_beats_b64(self, tmp_path, mock_license_cls, monkeypatch):
        """ASPOSE_LICENSE_PATH beats ASPOSE_EMAIL_LIC_B64 when file exists."""
        license_cls, license_instance = mock_license_cls
        lic_file = tmp_path / "aspose.lic"
        lic_file.write_bytes(b"<License>file</License>")

        monkeypatch.setenv("ASPOSE_LICENSE_PATH", str(lic_file))
        monkeypatch.setenv("ASPOSE_EMAIL_LIC_B64", _b64(b"<License>b64</License>"))

        apply_license()

        license_instance.set_license.assert_called_once_with(str(lic_file))


class TestTrialMode:
    """Priority 4 — no env vars set."""

    def test_no_license_set_when_no_env_vars(self, mock_license_cls, monkeypatch):
        """When no env vars are set, set_license is NOT called and warning is logged."""
        license_cls, license_instance = mock_license_cls

        # Ensure none of the license env vars are present
        for var in ("ASPOSE_LICENSE_KV_SECRET_NAME", "ASPOSE_LICENSE_KV_URL",
                    "ASPOSE_LICENSE_PATH", "ASPOSE_EMAIL_LIC_B64"):
            monkeypatch.delenv(var, raising=False)

        import logging
        with patch.object(logging.getLogger("shared.aspose_license"), "warning") as mock_warn:
            apply_license()

        license_instance.set_license.assert_not_called()
        mock_warn.assert_called_once()
        warning_msg = mock_warn.call_args[0][0]
        assert "TRIAL" in warning_msg


class TestIdempotency:
    """License is applied at most once per process."""

    def test_apply_twice_calls_set_license_once(self, tmp_path, mock_license_cls, monkeypatch):
        """Calling apply_license() twice only applies the license once.

        _resolve_license_path must also be called exactly once — confirming the
        early-return guard fires before any path resolution on the second call.
        """
        license_cls, license_instance = mock_license_cls
        lic_file = tmp_path / "aspose.lic"
        lic_file.write_bytes(b"<License>x</License>")
        monkeypatch.setenv("ASPOSE_LICENSE_PATH", str(lic_file))

        with patch.object(aspose_license_mod, "_resolve_license_path",
                          wraps=aspose_license_mod._resolve_license_path) as spy:
            apply_license()
            apply_license()

            assert spy.call_count == 1, (
                "_resolve_license_path should only be called once; "
                f"was called {spy.call_count} times"
            )

        assert license_instance.set_license.call_count == 1

    def test_thread_safety_single_application(self, tmp_path, mock_license_cls, monkeypatch):
        """Concurrent calls from multiple threads still apply the license exactly once."""
        import threading

        license_cls, license_instance = mock_license_cls
        lic_file = tmp_path / "aspose.lic"
        lic_file.write_bytes(b"<License>thread</License>")
        monkeypatch.setenv("ASPOSE_LICENSE_PATH", str(lic_file))

        errors: list[Exception] = []

        def worker():
            try:
                apply_license()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert license_instance.set_license.call_count == 1


class TestResetForTesting:
    """reset_for_testing() allows re-application in subsequent calls."""

    def test_reset_allows_reapplication(self, tmp_path, mock_license_cls, monkeypatch):
        """After reset_for_testing(), the next apply_license() re-applies the license."""
        license_cls, license_instance = mock_license_cls
        lic_file = tmp_path / "aspose.lic"
        lic_file.write_bytes(b"<License>reset</License>")
        monkeypatch.setenv("ASPOSE_LICENSE_PATH", str(lic_file))

        apply_license()
        assert license_instance.set_license.call_count == 1

        reset_for_testing()
        apply_license()
        assert license_instance.set_license.call_count == 2
