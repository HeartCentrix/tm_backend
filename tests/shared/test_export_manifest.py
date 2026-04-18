import json

from shared.export_manifest import ExportManifestBuilder


def test_manifest_accumulates_successes_and_failures():
    m = ExportManifestBuilder(job_id="job-1", snapshot_ids=["snap-1"])
    m.record_success(item_id="a", name="msg-a", folder="Inbox", size_bytes=1024)
    m.record_failure(item_id="b", name="msg-b", folder="Inbox", error="BlobNotFound", error_class="ResourceNotFoundError")

    payload = m.to_json()
    data = json.loads(payload)
    assert data["job_id"] == "job-1"
    assert data["snapshot_ids"] == ["snap-1"]
    assert data["exported_count"] == 1
    assert data["failed_count"] == 1
    assert len(data["items"]) == 2

    success = next(i for i in data["items"] if i["id"] == "a")
    assert success["status"] == "success"
    assert success["size_bytes"] == 1024

    fail = next(i for i in data["items"] if i["id"] == "b")
    assert fail["status"] == "failed"
    assert fail["error"] == "BlobNotFound"
