"""Graph API priority scheduling — priority-aware token bucket dispatch.

Priorities align with the existing RabbitMQ queue priority model
(backup.urgent, restore.urgent, etc.). Higher numeric value = higher
priority = jumps the queue when the per-app token bucket is contended.

Ranges kept small (0/1/2) to keep the wait-time math in
`AsyncTokenBucket.acquire` well-behaved and predictable. The multiplier
is `1 + 3*priority`, so:

    priority=0  -> wait = deficit / rate                 (current behaviour)
    priority=1  -> wait = deficit / rate / 4             (~4x faster check)
    priority=2  -> wait = deficit / rate / 7             (~7x faster check)

When NORMAL and URGENT are both waiting on the same bucket, URGENT wakes
up first and grabs the next refilled token.

Mapping from workload -> priority (worker derives at job-intake time from
the consuming queue, so a single worker handles multiple priorities):

    restore.urgent                -> PRIORITY_URGENT
    admin UI ops / previews       -> PRIORITY_URGENT
    restore.normal                -> PRIORITY_HIGH
    export.normal (user download) -> PRIORITY_HIGH
    backup.urgent (user-click)    -> PRIORITY_HIGH
    report.normal                 -> PRIORITY_HIGH
    backup.high (SLA escalation)  -> PRIORITY_HIGH
    discovery.*                   -> PRIORITY_HIGH
    sla.monitor / audit.events    -> PRIORITY_HIGH
    azure.restore.*               -> PRIORITY_HIGH
    backup.normal (scheduled)     -> PRIORITY_NORMAL
    backup.low / restore.low      -> PRIORITY_NORMAL
    dr-replication                -> PRIORITY_NORMAL
    azure.vm / azure.sql / azure.postgres -> PRIORITY_NORMAL
    delete.low / retention        -> PRIORITY_NORMAL

Feature-flagged via `settings.GRAPH_PRIORITY_SCHEDULING_ENABLED`. When
false, every caller is treated as PRIORITY_NORMAL (identical to the
pre-priority behaviour) without code changes.
"""
from __future__ import annotations

PRIORITY_NORMAL: int = 0
PRIORITY_HIGH: int = 1
PRIORITY_URGENT: int = 2


# Queue -> priority mapping. Workers look this up when a message arrives
# from a given queue and pass the result into every Graph call made for
# that job. Unknown queues default to NORMAL (safe).
QUEUE_PRIORITY: dict[str, int] = {
    # Restore
    "restore.urgent": PRIORITY_URGENT,
    "restore.normal": PRIORITY_HIGH,
    "restore.low": PRIORITY_NORMAL,
    # Backup
    "backup.urgent": PRIORITY_HIGH,
    "backup.high": PRIORITY_HIGH,
    "backup.normal": PRIORITY_NORMAL,
    "backup.low": PRIORITY_NORMAL,
    # Export (user-triggered downloads)
    "export.normal": PRIORITY_HIGH,
    # Discovery
    "discovery.m365": PRIORITY_HIGH,
    "discovery.azure": PRIORITY_HIGH,
    # Azure workloads
    "azure.vm": PRIORITY_NORMAL,
    "azure.sql": PRIORITY_NORMAL,
    "azure.postgres": PRIORITY_NORMAL,
    "azure.restore.vm": PRIORITY_HIGH,
    "azure.restore.sql": PRIORITY_HIGH,
    "azure.restore.postgres": PRIORITY_HIGH,
    # Control plane
    "sla.monitor": PRIORITY_HIGH,
    "audit.events": PRIORITY_HIGH,
    "report.normal": PRIORITY_HIGH,
    "notification": PRIORITY_NORMAL,
    "delete.low": PRIORITY_NORMAL,
}


def priority_for_queue(queue_name: str | None) -> int:
    """Return the Graph-priority level for a given RabbitMQ queue name.

    Unknown / None queue defaults to NORMAL. Safe fallback: no caller
    accidentally gets an elevated priority from a typo.
    """
    if not queue_name:
        return PRIORITY_NORMAL
    return QUEUE_PRIORITY.get(queue_name, PRIORITY_NORMAL)
