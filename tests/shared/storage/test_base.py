"""Protocol + DTO sanity tests."""
from datetime import datetime, timezone

from shared.storage.base import BackendStore, BlobInfo, BlobProps


def test_blob_info_fields_present():
    info = BlobInfo(
        backend_id="00000000-0000-0000-0000-000000000001",
        container="c",
        path="p",
        size=10,
        etag="e",
        url="u",
        content_md5=None,
        last_modified=datetime.now(timezone.utc),
    )
    assert info.backend_id
    assert info.size == 10


def test_blob_props_defaults():
    props = BlobProps(
        size=0,
        content_type=None,
        last_modified=datetime.now(timezone.utc),
        metadata={},
        copy_status=None,
        copy_progress=None,
        retention_until=None,
        legal_hold=False,
    )
    assert props.legal_hold is False


def test_backend_store_is_protocol():
    # runtime_checkable protocols expose _is_runtime_protocol
    assert getattr(BackendStore, "_is_runtime_protocol", False) is True
