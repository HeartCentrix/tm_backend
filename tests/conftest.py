"""Shared pytest fixtures for tm_backend tests."""
import asyncio
import importlib.util
import os
import pathlib
import sys
import types

import pytest


def _register_hyphenated_worker_aliases() -> None:
    """Expose hyphenated worker dirs (e.g. ``workers/chat-export-worker``)
    under underscore-based Python import paths so tests can write
    ``from workers.chat_export_worker.render.normalizer import ...``
    without renaming the on-disk directory (which is tied to the
    Dockerfile/docker-compose WORKDIR).
    """
    here = pathlib.Path(__file__).resolve().parent
    root = here.parent

    if "workers" not in sys.modules:
        workers_pkg = types.ModuleType("workers")
        workers_pkg.__path__ = [str(root / "workers")]
        sys.modules["workers"] = workers_pkg

    aliases = {
        "workers.chat_export_worker": root / "workers" / "chat-export-worker",
    }
    for mod_name, disk_path in aliases.items():
        if not disk_path.is_dir():
            continue
        if mod_name in sys.modules:
            continue
        pkg = types.ModuleType(mod_name)
        pkg.__path__ = [str(disk_path)]
        sys.modules[mod_name] = pkg


_register_hyphenated_worker_aliases()


@pytest.fixture(scope="session")
def event_loop():
    """Create a single event loop for the session (avoids pytest-asyncio re-creating)."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def tmpdir_path(tmp_path):
    return str(tmp_path)


_AZURITE_DEFAULT_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


@pytest.fixture
def azure_test_connection_string():
    """Azure Storage connection string for integration tests.

    Resolution order:
      1. AZURE_TEST_CONNECTION_STRING env var (use real Azure or custom endpoint)
      2. Azurite running at localhost:10000 (start with `docker-compose --profile test up -d azurite`)
      3. Skip the test

    Tests that need the (Azurite-unsupported) put_block_from_url op skip
    themselves when the connection string points at Azurite."""
    conn = os.getenv("AZURE_TEST_CONNECTION_STRING")
    if conn:
        return conn

    # Fall back to local Azurite if reachable.
    import socket
    try:
        with socket.create_connection(("localhost", 10000), timeout=0.25):
            return _AZURITE_DEFAULT_CONN
    except OSError:
        pytest.skip(
            "No Azure target available. Either start Azurite "
            "(`docker-compose --profile test up -d azurite`) or export "
            "AZURE_TEST_CONNECTION_STRING against a real Azure account."
        )
