"""Shim so tests can import FolderExportTask + MailExportOrchestrator without
triggering RabbitMQ setup in the main restore-worker module."""
import importlib.util
import os
import sys

_here = os.path.dirname(__file__)
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
