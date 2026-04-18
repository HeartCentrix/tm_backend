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


@pytest.fixture
def azurite_connection_string():
    """Azurite default connection string — used by integration tests.
    The docker-compose `azurite` service must be running before integration tests."""
    return os.getenv(
        "AZURITE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
    )
