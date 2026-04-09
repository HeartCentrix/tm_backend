"""Backup Worker - Processes backup jobs from RabbitMQ queue"""
import asyncio
from datetime import datetime
from shared.message_bus import message_bus
from shared.config import settings


async def process_backup_message(message: dict):
    """Process a single backup job message"""
    job_id = message.get("jobId")
    resource_id = message.get("resourceId")
    print(f"Processing backup job {job_id} for resource {resource_id}")
    
    # TODO: Implement actual backup logic
    # 1. Connect to Microsoft Graph API
    # 2. Fetch data via delta sync
    # 3. Encrypt and upload to Azure Blob Storage
    # 4. Create snapshot records
    # 5. Update job status
    
    print(f"Backup job {job_id} completed (stub)")


async def main():
    await message_bus.connect()
    print("Backup worker started, waiting for messages...")
    
    await message_bus.consume("backup.urgent", process_backup_message)
    await message_bus.consume("backup.normal", process_backup_message)
    
    # Keep running
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
