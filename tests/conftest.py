"""Shared pytest fixtures for tm_backend tests."""
import asyncio
import os
import pytest


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
