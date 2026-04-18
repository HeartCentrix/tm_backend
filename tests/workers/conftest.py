"""Preload mail_export module so sibling tests can `from mail_export import ...`."""
import tests.workers.workers_restore_worker_shim  # noqa: F401 — side effect registers mail_export
