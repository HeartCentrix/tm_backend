import sys
import os
from types import SimpleNamespace

# Make the restore-worker package importable as a flat module namespace
# (it isn't a proper python package on disk; the worker bootstraps via
# sys.path at container start).
_WORKER_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "workers", "restore-worker")
)
if _WORKER_DIR not in sys.path:
    sys.path.insert(0, _WORKER_DIR)

from onedrive_restore import OneDriveRestoreEngine, Mode  # noqa: E402


def _mk_item(name, folder_path=None, ext_id="e1"):
    return SimpleNamespace(
        id="i1", external_id=ext_id, name=name,
        folder_path=folder_path, content_size=100, blob_path="t/b", extra_data={},
    )


def test_original_overwrite_preserves_folder_path():
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("report.docx", "/drive/root:/Projects/Q1"),
        mode=Mode.OVERWRITE,
        separate_folder_root=None,
    )
    assert path == "Projects/Q1/report.docx"
    assert conflict == "replace"


def test_original_overwrite_flat_file_at_root():
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("a.txt", None),
        mode=Mode.OVERWRITE,
        separate_folder_root=None,
    )
    assert path == "a.txt"
    assert conflict == "replace"


def test_separate_folder_preserves_tree_under_root():
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("report.docx", "/drive/root:/Projects/Q1"),
        mode=Mode.SEPARATE_FOLDER,
        separate_folder_root="Restored by TM 2026-04-20",
    )
    assert path == "Restored by TM 2026-04-20/Projects/Q1/report.docx"
    assert conflict == "rename"


def test_separate_folder_strips_parent_reference_anchor():
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("x.pdf", "/drives/abc/root:/folder"),
        mode=Mode.SEPARATE_FOLDER,
        separate_folder_root="MyRestored",
    )
    assert path == "MyRestored/folder/x.pdf"
    assert conflict == "rename"
