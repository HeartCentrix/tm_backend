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
def azure_test_connection_string():
    """Azure Storage connection string for integration tests — real Azure, no emulator.

    Set `AZURE_TEST_CONNECTION_STRING` in the shell or CI secret before running
    `pytest -m integration`. Tests depending on this fixture skip gracefully
    when the env var is absent so unit-test runs stay green without credentials."""
    conn = os.getenv("AZURE_TEST_CONNECTION_STRING")
    if not conn:
        pytest.skip(
            "AZURE_TEST_CONNECTION_STRING not set — skipping Azure integration test. "
            "Export a real connection string to run these tests against Azure."
        )
    return conn
