"""Shared fixtures for storage-layer tests.

Spins up Azurite + SeaweedFS containers via docker-compose profile
`test` before the test session and exposes connected backend
instances as pytest fixtures.

Skips the startup if TMVAULT_SKIP_STORAGE_SERVICES=1 so CI can manage
containers externally.
"""
from __future__ import annotations

import os
import subprocess
import time

import pytest


COMPOSE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "docker-compose.yml",
)
COMPOSE_FILE = os.path.abspath(COMPOSE_FILE)
PROFILE = "test"


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "--profile", PROFILE, *args],
        check=True,
    )


def _health(name: str) -> str:
    try:
        out = subprocess.check_output(
            ["docker", "inspect", "-f", "{{.State.Status}}", name],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        return out
    except subprocess.CalledProcessError:
        return "absent"


@pytest.fixture(scope="session", autouse=True)
def storage_test_services():
    """Start Azurite + SeaweedFS for the whole session."""
    if os.getenv("TMVAULT_SKIP_STORAGE_SERVICES") == "1":
        yield
        return

    _compose("up", "-d", "azurite", "seaweedfs")

    deadline = time.time() + 90
    while time.time() < deadline:
        az = _health("tm_vault_azurite")
        sw = _health("tm_vault_seaweedfs")
        if az == "running" and sw == "running":
            break
        time.sleep(2)

    # Ensure the contract-test bucket exists (idempotent). Must be created
    # with ObjectLockEnabledForBucket=True — retrofit is not supported.
    try:
        import boto3
        from botocore.exceptions import ClientError

        s3 = boto3.client(
            "s3", endpoint_url="http://localhost:8333",
            aws_access_key_id="testaccess",
            aws_secret_access_key="testsecret",
            region_name="us-east-1",
        )
        try:
            s3.create_bucket(
                Bucket="tmvault-test-0",
                ObjectLockEnabledForBucket=True,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] not in (
                "BucketAlreadyOwnedByYou", "BucketAlreadyExists",
            ):
                raise
        try:
            s3.put_bucket_versioning(
                Bucket="tmvault-test-0",
                VersioningConfiguration={"Status": "Enabled"},
            )
        except ClientError:
            # SeaweedFS may auto-enable when bucket has Object Lock on.
            pass
    except Exception as e:
        # Don't block the session — individual tests will surface the issue.
        print(f"[storage-conftest] warning: bucket prep failed: {e}")

    yield

    if os.getenv("TMVAULT_KEEP_STORAGE_SERVICES") != "1":
        _compose("stop", "azurite", "seaweedfs")
