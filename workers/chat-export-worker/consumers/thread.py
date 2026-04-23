"""v1 full flow: resolve -> normalize -> package -> upload -> sign -> complete.

Uses Job.type (not job_type), Job.spec (not input_params),
Job.result (not output_params). JobType.EXPORT + spec["kind"]="chat_export_thread"
is the discriminator.
"""
import json
import logging
import os
import tempfile
import aio_pika
from sqlalchemy import select, update

from shared.database import async_session_factory
from shared.models import Job, JobStatus, Resource
from shared.azure_storage import (
    azure_storage_manager,
    upload_blob_with_retry_from_file,
)
from shared.config import settings

from workers.chat_export_worker.blob_shard import sign_download_url
from workers.chat_export_worker.progress import publish
from workers.chat_export_worker.scope import resolve
from workers.chat_export_worker.render.normalizer import normalize_messages
from workers.chat_export_worker.packager.thread_packager import (
    ThreadPackager, AttachmentSource,
)

log = logging.getLogger("chat-export.thread")


class BlobAttachmentSource(AttachmentSource):
    """Stream bytes from the shared tenant blob store.

    Adapts to whichever helper the existing codebase exposes for blob
    reads. Pattern returned: (async_iter, content_type, content_length).
    """

    async def open(self, blob_path: str):
        from shared.azure_storage import azure_storage_manager

        # Prefer a streaming API if present.
        if hasattr(azure_storage_manager, "open_stream"):
            return await azure_storage_manager.open_stream(blob_path)

        # Fallback: use the default shard to download bytes.
        shard = azure_storage_manager.get_default_shard()
        # blob_path in DB is of the form "<container>/<path>" or just "<path>".
        # Our chat blobs are persisted under the tenant's default container;
        # backup-worker stores full paths like "users/.../messages/<id>/hosted/<hc>".
        # The shard uses a single container — download_blob takes (container, path).
        container = "tenant-data"
        path = blob_path
        if "/" in blob_path:
            head, tail = blob_path.split("/", 1)
            # Heuristic: if the first segment looks like a known container prefix,
            # treat it as the container name. Otherwise keep the default.
            if head in {"tenant-data", "exports", "chat-blobs"}:
                container, path = head, tail
        data = await shard.download_blob(container, path)
        if data is None:
            data = b""

        async def _gen():
            yield data

        return _gen(), "application/octet-stream", len(data)


async def _update_job(sess, job_id, **values):
    await sess.execute(update(Job).where(Job.id == job_id).values(**values))
    await sess.commit()


async def _check_cancelled(sess, job_id) -> bool:
    from sqlalchemy import select
    from shared.models import Job, JobStatus
    st = (await sess.execute(select(Job.status).where(Job.id == job_id))).scalar_one()
    return st == JobStatus.CANCELLING


async def consume_thread(message: aio_pika.IncomingMessage) -> None:
    async with message.process(requeue=False):
        body = json.loads(message.body)
        job_id = body["jobId"]
        log.info("job_started id=%s", job_id)
        await publish(job_id, "progress", {"stage": "resolving", "percent": 5})

        async with async_session_factory() as sess:
            job = (await sess.execute(select(Job).where(Job.id == job_id))).scalar_one()
            if job.status == JobStatus.CANCELLING:
                await _update_job(sess, job_id, status=JobStatus.CANCELLED)
                await publish(job_id, "cancelled", {})
                return

            spec = job.spec or {}
            try:
                scope = await resolve(
                    sess,
                    resource_id=spec["resourceId"],
                    snapshot_ids=spec["snapshotIds"],
                    thread_path=spec.get("threadPath"),
                    item_ids=spec.get("itemIds") or [],
                )
            except ValueError as e:
                await _update_job(
                    sess, job_id, status=JobStatus.FAILED,
                    result={"error": {"code": str(e)}},
                )
                await publish(job_id, "error", {"code": str(e)})
                return

            # The rest of the pipeline (normalize → render → package → upload
            # → sign) is wrapped in a try/except so an unexpected exception
            # transitions the Job row to FAILED instead of leaving it stuck
            # in RUNNING. RabbitMQ acks the message on exit (requeue=False
            # on message.process) but does not touch the DB, so without
            # this block the UI would poll forever after any failure.
            try:
                await _run_pipeline(sess, job_id, spec, scope, job)
            except Exception as exc:
                log.exception("job_failed id=%s", job_id)
                await _update_job(
                    sess, job_id, status=JobStatus.FAILED,
                    result={"error": {"code": "pipeline_error", "message": str(exc)}},
                )
                await publish(job_id, "error", {"code": "pipeline_error", "message": str(exc)})


async def _run_pipeline(sess, job_id, spec, scope, job) -> None:
    await publish(
        job_id, "progress",
        {"stage": "rendering", "percent": 20,
         "messagesTotal": len(scope.messages)},
    )

    hosted_by_msg = {
        mid: [
            {"hc_id": h["hc_id"],
             "local_path": f"./{scope.thread_path.split('/')[-1]}-attachments/inline/{h['hc_id']}{h.get('ext', '.bin')}"}
            for h in hs
        ]
        for mid, hs in scope.hosted_map.items()
    }

    render_messages = normalize_messages(
        scope.messages,
        attachments_by_msg={},
        hosted_by_msg=hosted_by_msg,
        layout=scope.layout,
    )

    resource = (await sess.execute(
        select(Resource).where(Resource.id == spec["resourceId"])
    )).scalar_one()
    pkg_ctx = {
        "job_id": str(job_id),
        "user_email": spec.get("userEmail", ""),
        "tenant_name": str(resource.tenant_id),
        "resource_name": resource.display_name,
        "scope": f"{scope.layout} - {scope.thread_path}",
        "snapshot_at": job.created_at,
    }

    if await _check_cancelled(sess, job_id):
        await _update_job(sess, job_id, status=JobStatus.CANCELLED)
        await publish(job_id, "cancelled", {})
        return

    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f"chat-export-{job_id}-", suffix=".zip")
    os.close(tmp_fd)
    try:
        with open(tmp_path, "wb") as buf:
            pkg = ThreadPackager(
                layout=scope.layout,
                thread_name=scope.thread_path.rsplit("/", 1)[-1],
                thread_path=scope.thread_path,
                format=spec["exportFormat"],
                attachment_source=BlobAttachmentSource(),
            )
            summary = await pkg.write(
                render_messages,
                attachment_map=scope.attachment_map,
                hosted_map=scope.hosted_map,
                out=buf,
                context=pkg_ctx,
            )

        await publish(job_id, "progress", {"stage": "uploading", "percent": 85})
        if await _check_cancelled(sess, job_id):
            await _update_job(sess, job_id, status=JobStatus.CANCELLED)
            await publish(job_id, "cancelled", {})
            return

        account = settings.AZURE_STORAGE_ACCOUNT_NAME
        container = "exports"
        blob_path = f"{job_id}/export.zip"
        shard = azure_storage_manager.get_default_shard()
        up_result = await upload_blob_with_retry_from_file(
            container, blob_path, tmp_path,
            shard=shard, max_retries=3,
        )
        if not up_result.get("success"):
            raise RuntimeError(f"blob upload failed: {up_result.get('error')}")
    finally:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    ttl = getattr(settings, "chat_export_sas_ttl_hours", 168)
    url = await shard.get_blob_sas_url(container, blob_path, valid_for_hours=ttl)

    await _update_job(
        sess, job_id, status=JobStatus.COMPLETED,
        result={
            "export_zip_blob_path": f"{account}/{container}/{blob_path}",
            "signed_url": url,
            "total_msgs": len(render_messages),
            "total_bytes": summary["total_bytes"],
            "sha256": summary["sha256"],
        },
    )
    await publish(
        job_id, "complete",
        {"url": url,
         "sizeBytes": summary["total_bytes"],
         "sha256": summary["sha256"]},
    )
    log.info("job_completed id=%s bytes=%d", job_id, summary["total_bytes"])
