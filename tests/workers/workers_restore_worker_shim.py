"""Shim so tests can import FolderExportTask + MailExportOrchestrator without
triggering RabbitMQ setup in the main restore-worker module."""
import importlib.util
import os
import sys

_here = os.path.dirname(__file__)
# Make the restore-worker dir importable so sibling modules like `mail_fetch`
# resolve via lazy `from mail_fetch import ...` calls inside the methods.
_worker_dir = os.path.abspath(
    os.path.join(_here, "..", "..", "workers", "restore-worker")
)
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)
_target = os.path.abspath(
    os.path.join(_here, "..", "..", "workers", "restore-worker", "mail_export.py")
)
spec = importlib.util.spec_from_file_location("mail_export", _target)
mod = importlib.util.module_from_spec(spec)
sys.modules["mail_export"] = mod
spec.loader.exec_module(mod)

FolderExportTask = mod.FolderExportTask
FolderExportResult = mod.FolderExportResult
MailExportOrchestrator = mod.MailExportOrchestrator

# Load file_export — same pattern to bridge the hyphenated `restore-worker` dir.
_file_target = os.path.abspath(
    os.path.join(_here, "..", "..", "workers", "restore-worker", "file_export.py")
)
_file_spec = importlib.util.spec_from_file_location("file_export", _file_target)
_file_mod = importlib.util.module_from_spec(_file_spec)
sys.modules["file_export"] = _file_mod
_file_spec.loader.exec_module(_file_mod)

FileExportOrchestrator = _file_mod.FileExportOrchestrator
FileFolderExportTask = _file_mod.FileFolderExportTask
FileFolderExportResult = _file_mod.FileFolderExportResult
