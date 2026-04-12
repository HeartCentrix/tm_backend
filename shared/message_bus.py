"""Shared message bus for inter-service communication via RabbitMQ"""
import asyncio
import json
from typing import Optional, Callable, Dict, Any
from datetime import datetime
import aio_pika
from aio_pika import Message, DeliveryMode

from shared.config import settings


class MessageBus:
    """RabbitMQ message bus for async communication between microservices"""
    
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
    
    async def connect(self, max_retries: int = 10, retry_delay: int = 5):
        if not settings.RABBITMQ_ENABLED:
            print("[MESSAGE_BUS] RabbitMQ disabled by config")
            return

        # Retry with exponential backoff
        for attempt in range(1, max_retries + 1):
            try:
                self.connection = await aio_pika.connect_robust(
                    settings.RABBITMQ_URL,
                )
                self.channel = await self.connection.channel()
                self.exchange = await self.channel.declare_exchange(
                    "tm.exchange", aio_pika.ExchangeType.DIRECT, durable=True
                )

                # Declare queues with QoS settings for mass backup
                await self._declare_queue("backup.urgent", routing_key="backup.urgent")
                await self._declare_queue("backup.high", routing_key="backup.high")
                await self._declare_queue("backup.normal", routing_key="backup.normal")
                await self._declare_queue("backup.low", routing_key="backup.low")
                await self._declare_queue("restore.urgent", routing_key="restore.urgent")
                await self._declare_queue("restore.normal", routing_key="restore.normal")
                await self._declare_queue("restore.low", routing_key="restore.low")
                await self._declare_queue("discovery.m365", routing_key="discovery.m365")
                await self._declare_queue("discovery.azure", routing_key="discovery.azure")
                await self._declare_queue("notification", routing_key="notification")
                await self._declare_queue("export.normal", routing_key="export.normal")
                await self._declare_queue("delete.low", routing_key="delete.low")
                await self._declare_queue("sla.monitor", routing_key="sla.monitor")
                await self._declare_queue("report.normal", routing_key="report.normal")
                await self._declare_queue("audit.events", routing_key="audit.events")

                # Azure workload queues (separate from M365 backup for LRO isolation)
                await self._declare_queue("azure.vm", routing_key="azure.vm")
                await self._declare_queue("azure.sql", routing_key="azure.sql")
                await self._declare_queue("azure.postgres", routing_key="azure.postgres")

                # Set channel QoS (per-consumer prefetch)
                await self.channel.set_qos(prefetch_count=50)

                print("[MESSAGE_BUS] Connected to RabbitMQ successfully")
                return  # Success!

            except Exception as e:
                if attempt < max_retries:
                    print(f"[MESSAGE_BUS] Connection attempt {attempt}/{max_retries} failed: {e}")
                    print(f"[MESSAGE_BUS] Retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                else:
                    print(f"[MESSAGE_BUS] Failed to connect after {max_retries} attempts: {e}")
                    print(f"[MESSAGE_BUS] Services requiring RabbitMQ will not function properly")
                    raise
    
    async def disconnect(self):
        if self.connection:
            await self.connection.close()
    
    async def _declare_queue(self, queue_name: str, routing_key: str):
        # Declare a Dead Letter Queue for failed messages
        dlq_name = f"{queue_name}.dlq"
        dlq = await self.channel.declare_queue(dlq_name, durable=True)
        dlq_routing_key = f"{routing_key}.dlq"
        await dlq.bind(self.exchange, dlq_routing_key)

        # Main queue routes rejected messages to the DLQ
        queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "tm.exchange",
                "x-dead-letter-routing-key": dlq_routing_key,
            },
        )
        await queue.bind(self.exchange, routing_key)
    
    async def publish(self, routing_key: str, message: Dict[str, Any], priority: int = 5):
        """Publish message to queue"""
        if not settings.RABBITMQ_ENABLED or not self.exchange:
            return
        
        message_body = json.dumps(message).encode()
        await self.exchange.publish(
            Message(
                message_body,
                delivery_mode=DeliveryMode.PERSISTENT,
                priority=priority,
                headers={"published_at": datetime.utcnow().isoformat()},
            ),
            routing_key=routing_key,
        )
    
    async def consume(self, queue_name: str, callback: Callable):
        """Consume messages from queue"""
        if not settings.RABBITMQ_ENABLED:
            return
        
        queue = await self.channel.get_queue(queue_name)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        body = json.loads(message.body.decode())
                        await callback(body)
                    except Exception as e:
                        # In production: send to DLQ
                        print(f"Error processing message: {e}")


# Global message bus instance
message_bus = MessageBus()


def create_backup_message(job_id: str, resource_id: str, tenant_id: str, full_backup: bool = False) -> dict:
    return {
        "jobId": job_id,
        "resourceId": resource_id,
        "tenantId": tenant_id,
        "type": "FULL" if full_backup else "INCREMENTAL",
        "priority": 1 if full_backup else 5,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_mass_backup_message(
    job_id: str,
    tenant_id: str,
    resource_type: str,
    resource_ids: list[str],
    sla_policy_id: str = None,
    full_backup: bool = False
) -> dict:
    """Create a mass backup message for batch processing"""
    return {
        "jobId": job_id,
        "tenantId": tenant_id,
        "resourceType": resource_type,
        "resourceIds": resource_ids,  # Batch of resource IDs
        "type": "FULL" if full_backup else "INCREMENTAL",
        "priority": 1 if sla_policy_id is None else 5,
        "slaPolicyId": sla_policy_id,
        "triggeredBy": "SCHEDULED",
        "snapshotLabel": "scheduled",
        "forceFullBackup": full_backup,
        "createdAt": datetime.utcnow().isoformat(),
        "batchSize": len(resource_ids),
    }


def create_restore_message(
    job_id: str,
    restore_type: str = "IN_PLACE",
    snapshot_ids: list = None,
    item_ids: list = None,
    resource_id: str = None,
    tenant_id: str = None,
    spec: dict = None
) -> dict:
    """Create a restore message for the restore worker"""
    priority_map = {
        "IN_PLACE": 5,
        "CROSS_USER": 3,
        "CROSS_RESOURCE": 4,
        "EXPORT_PST": 6,
        "EXPORT_ZIP": 7,
        "DOWNLOAD": 8,
    }

    queue_map = {
        "IN_PLACE": "restore.normal",
        "CROSS_USER": "restore.urgent",
        "CROSS_RESOURCE": "restore.normal",
        "EXPORT_PST": "restore.normal",
        "EXPORT_ZIP": "restore.low",
        "DOWNLOAD": "restore.low",
    }

    return {
        "jobId": job_id,
        "restoreType": restore_type,
        "snapshotIds": snapshot_ids or [],
        "itemIds": item_ids or [],
        "resourceId": resource_id,
        "tenantId": tenant_id,
        "spec": spec or {},
        "type": "RESTORE",
        "priority": priority_map.get(restore_type, 5),
        "queue": queue_map.get(restore_type, "restore.normal"),
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_discovery_message(tenant_id: str, tenant_type: str) -> dict:
    return {
        "tenantId": tenant_id,
        "type": tenant_type,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_notification_message(alert_id: str, severity: str, message: str) -> dict:
    return {
        "alertId": alert_id,
        "severity": severity,
        "message": message,
        "createdAt": datetime.utcnow().isoformat(),
    }


def create_audit_event_message(
    action: str,
    tenant_id: str,
    org_id: str = None,
    actor_type: str = "SYSTEM",
    actor_id: str = None,
    actor_email: str = None,
    resource_id: str = None,
    resource_type: str = None,
    resource_name: str = None,
    outcome: str = "SUCCESS",
    job_id: str = None,
    snapshot_id: str = None,
    details: dict = None,
) -> dict:
    """Create an audit event message for the audit.events queue"""
    return {
        "action": action,
        "tenantId": tenant_id,
        "orgId": org_id,
        "actorType": actor_type,
        "actorId": actor_id,
        "actorEmail": actor_email,
        "resourceId": resource_id,
        "resourceType": resource_type,
        "resourceName": resource_name,
        "outcome": outcome,
        "jobId": job_id,
        "snapshotId": snapshot_id,
        "details": details or {},
        "createdAt": datetime.utcnow().isoformat(),
    }
