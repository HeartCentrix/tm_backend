"""Shared message bus for inter-service communication via RabbitMQ"""
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
    
    async def connect(self):
        if not settings.RABBITMQ_ENABLED:
            return
        
        self.connection = await aio_pika.connect_robust(
            settings.RABBITMQ_URL,
        )
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            "tm.exchange", aio_pika.ExchangeType.DIRECT, durable=True
        )
        
        # Declare queues
        await self._declare_queue("backup.urgent", routing_key="backup.urgent")
        await self._declare_queue("backup.normal", routing_key="backup.normal")
        await self._declare_queue("restore.urgent", routing_key="restore.urgent")
        await self._declare_queue("discovery.m365", routing_key="discovery.m365")
        await self._declare_queue("discovery.azure", routing_key="discovery.azure")
        await self._declare_queue("notification", routing_key="notification")
        await self._declare_queue("export.normal", routing_key="export.normal")
        await self._declare_queue("delete.low", routing_key="delete.low")
    
    async def disconnect(self):
        if self.connection:
            await self.connection.close()
    
    async def _declare_queue(self, queue_name: str, routing_key: str):
        queue = await self.channel.declare_queue(queue_name, durable=True)
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


def create_restore_message(job_id: str, snapshot_id: str, item_ids: list) -> dict:
    return {
        "jobId": job_id,
        "snapshotId": snapshot_id,
        "itemIds": item_ids,
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
