"""DNS flip — pluggable per deployment."""
import logging
import os
import subprocess

log = logging.getLogger("toggle.dns")


async def flip_dns_to(target_zone: str) -> None:
    strategy = os.getenv("DNS_FLIP_STRATEGY", "noop")
    if strategy == "noop":
        log.info("DNS_FLIP_STRATEGY=noop — skipping")
        return
    if strategy == "script":
        script = os.getenv("DNS_FLIP_SCRIPT", "/opt/tmvault/ops/dns/flip.sh")
        subprocess.run([script, target_zone], check=True)
        log.info("DNS flipped via %s %s", script, target_zone)
        return
    raise ValueError(f"unknown DNS_FLIP_STRATEGY: {strategy}")
