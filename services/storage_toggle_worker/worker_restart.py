"""Worker pool restart — pluggable per deployment.

Strategies (WORKER_RESTART_STRATEGY env):
  noop     — do nothing (pilot; both sides are already running)
  kubectl  — scale old-zone deployments to 0, new-zone to configured replicas
  railway  — no-op (services pick up state via router notify)
"""
import logging
import os
import subprocess

log = logging.getLogger("toggle.workers")


async def restart_workers(from_zone: str, to_zone: str) -> None:
    strategy = os.getenv("WORKER_RESTART_STRATEGY", "noop")
    if strategy in ("noop", "railway"):
        log.info("WORKER_RESTART_STRATEGY=%s — skipping", strategy)
        return
    if strategy == "kubectl":
        from_ctx = os.getenv(f"KUBE_CONTEXT_{from_zone.upper()}")
        to_ctx = os.getenv(f"KUBE_CONTEXT_{to_zone.upper()}")
        if not (from_ctx and to_ctx):
            raise RuntimeError(
                f"KUBE_CONTEXT_{from_zone.upper()} + KUBE_CONTEXT_{to_zone.upper()} required",
            )
        deployments = ["backup-worker", "restore-worker", "chat-export-worker",
                       "azure-workload-worker", "dr-replication-worker"]
        for d in deployments:
            subprocess.run(["kubectl", "--context", from_ctx, "-n", "tmvault",
                            "scale", "deploy", d, "--replicas=0"], check=True)
        for d in deployments:
            subprocess.run(["kubectl", "--context", to_ctx, "-n", "tmvault",
                            "scale", "deploy", d, "--replicas=4"], check=True)
        log.info("kubectl rescale done: %s -> %s", from_zone, to_zone)
        return
    raise ValueError(f"unknown WORKER_RESTART_STRATEGY: {strategy}")
