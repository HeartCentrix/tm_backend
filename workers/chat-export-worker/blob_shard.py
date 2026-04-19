"""Blob shard selection + download SAS signing."""
from datetime import datetime, timedelta, timezone
import os, zlib
from typing import Sequence

from azure.storage.blob import BlobSasPermissions, generate_blob_sas

from shared.config import settings


def pick_shard(job_id: str, n: int | None = None) -> int:
    n = n if n is not None else settings.chat_export_blob_account_shards
    return zlib.crc32(job_id.encode("utf-8")) % n


def shard_account(idx: int, accounts: Sequence[str] | None = None) -> str:
    accs = list(accounts or settings.chat_export_blob_accounts)
    return accs[idx]


def _sas_sig(**kw) -> str:
    # Azure SDK returns the full SAS query string (se=…&sp=…&sv=…&sr=b&sig=…),
    # not just the signature value. Don't prepend "sig=".
    return generate_blob_sas(**kw)


def sign_download_url(*, account: str, container: str, blob_path: str,
                      expires_in_hours: int | None = None) -> str:
    ttl = expires_in_hours or settings.chat_export_sas_ttl_hours
    key = (getattr(settings, "AZURE_STORAGE_ACCOUNT_KEY", None)
           or os.environ.get(f"CHAT_EXPORT_KEY_{account.upper()}", ""))
    expiry = datetime.now(timezone.utc) + timedelta(hours=ttl)
    sas_qs = _sas_sig(account_name=account, container_name=container, blob_name=blob_path,
                      account_key=key, permission=BlobSasPermissions(read=True),
                      expiry=expiry, protocol="https")
    return f"https://{account}.blob.core.windows.net/{container}/{blob_path}?{sas_qs}"
