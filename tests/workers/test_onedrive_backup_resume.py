"""Resume scenario: an existing backup_checkpoint on the Job row filters out
already-processed file IDs from a second run so the worker only downloads the
pending tail."""
from shared.backup_checkpoint import BackupCheckpoint


def test_resume_skips_processed_files():
    cp_payload = {
        "resource_id": "r", "drive_id": "d",
        "delta_token_at_resume": None,
        "files_done_ext_ids": ["a", "b", "c"],
        "files_done_count": 3,
        "bytes_done": 300,
        "last_commit_at": "2026-04-19T00:00:00Z",
    }
    cp = BackupCheckpoint.from_dict(cp_payload)
    incoming = [{"id": x} for x in ("a", "b", "c", "d", "e")]
    pending = [f for f in incoming if not cp.is_done(f["id"])]
    assert [f["id"] for f in pending] == ["d", "e"]


def test_resume_empty_checkpoint_processes_all():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    incoming = [{"id": x} for x in ("a", "b", "c")]
    pending = [f for f in incoming if not cp.is_done(f["id"])]
    assert len(pending) == 3
