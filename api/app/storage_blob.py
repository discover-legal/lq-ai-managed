"""Native Azure Blob Storage backend for :mod:`app.storage` (managed fork).

Selected with ``STORAGE_BACKEND=azureblob``. :mod:`app.storage` keeps its
public surface (``stream_upload``, ``stream_download``, ``delete_object``,
``upload_bytes``, ``presigned_get_url``, ``ensure_bucket``,
``check_storage``) and dispatches each call here, so no caller changes.
Design rationale: ``docs/managed/adr/M-0001-azure-blob-storage-backend.md``.

Mapping from the S3 path
------------------------

* Bucket → container (``AZURE_STORAGE_CONTAINER``).
* Multipart upload → block blob: ``stage_block`` per
  ``MULTIPART_PART_SIZE`` part, then ``commit_block_list``. There is no
  abort call — uncommitted blocks are garbage-collected by the service
  (after 7 days), and nothing is visible under the key until commit.
* Presigned GET → read-only SAS. With a credential (managed identity) we
  sign with a *user delegation key*; with a connection string we sign with
  the account key.
* 404 on delete → success (idempotent), as on the S3 path.

Auth
----

``AZURE_STORAGE_CONNECTION_STRING`` wins when set (Azurite in dev/CI,
account-key deployments). Otherwise ``DefaultAzureCredential`` is used
against ``AZURE_STORAGE_ACCOUNT_URL`` — managed identity in Azure, so no
storage keys exist in the deployment. The identity needs the "Storage
Blob Data Contributor" role on the container (create, read, write,
delete, user delegation key).
"""

from __future__ import annotations

import base64
import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    BlobBlock,
    BlobSasPermissions,
    ContentSettings,
    generate_blob_sas,
)
from azure.storage.blob.aio import BlobServiceClient

from app.config import get_settings
from app.errors import InternalError, PayloadTooLarge
from app.storage import MULTIPART_PART_SIZE, StreamUploadResult

log = logging.getLogger(__name__)

# User delegation keys (and therefore SAS signed with them) are capped at
# 7 days by the service — the same ceiling as S3 presigned URLs.
MAX_SAS_SECONDS = 7 * 24 * 60 * 60

# Clock-skew allowance on SAS start times.
SAS_CLOCK_SKEW = timedelta(minutes=5)


def _block_id(n: int) -> str:
    """Return the base64 block ID for part ``n``.

    All block IDs within a blob must have the same encoded length, so the
    index is zero-padded before encoding.
    """

    return base64.b64encode(f"{n:08d}".encode()).decode()


@asynccontextmanager
async def blob_service_client() -> AsyncIterator[BlobServiceClient]:
    """Yield a configured async ``BlobServiceClient``; close it (and any
    credential) on exit."""

    settings = get_settings()
    if settings.azure_storage_connection_string:
        async with BlobServiceClient.from_connection_string(
            settings.azure_storage_connection_string
        ) as service:
            yield service
        return

    if not settings.azure_storage_account_url:
        raise InternalError(
            "Azure Blob storage is selected but not configured",
            details={
                "required": "AZURE_STORAGE_ACCOUNT_URL or AZURE_STORAGE_CONNECTION_STRING",
            },
        )

    # Imported lazily: azure-identity is only needed for credential auth.
    from azure.identity.aio import DefaultAzureCredential

    async with (
        DefaultAzureCredential() as credential,
        BlobServiceClient(settings.azure_storage_account_url, credential=credential) as service,
    ):
        yield service


async def ensure_container() -> None:
    """Create the configured container if it does not exist."""

    container_name = get_settings().azure_storage_container
    async with blob_service_client() as service:
        try:
            await service.get_container_client(container_name).create_container()
            log.info("Created Azure Blob container: %s", container_name)
        except ResourceExistsError:
            return


async def check_storage() -> bool:
    """Readiness check: True if the configured container is reachable."""

    container_name = get_settings().azure_storage_container
    try:
        async with blob_service_client() as service:
            await service.get_container_client(container_name).get_container_properties()
        return True
    except Exception as exc:
        log.warning("Storage readiness check failed: %s", exc)
        return False


async def stream_upload(
    *,
    storage_path: str,
    chunks: AsyncIterator[bytes],
    content_type: str,
    max_size_bytes: int,
) -> StreamUploadResult:
    """Stream ``chunks`` into a block blob at ``storage_path``.

    Same contract as :func:`app.storage.stream_upload`: SHA-256 over the
    stream, :class:`PayloadTooLarge` the moment ``max_size_bytes`` is
    exceeded, :class:`InternalError` on backend failure. Nothing becomes
    visible at ``storage_path`` unless the whole body is committed.
    """

    if max_size_bytes <= 0:
        raise InternalError(
            "stream_upload called with non-positive max_size_bytes",
            details={"max_size_bytes": max_size_bytes},
        )

    container_name = get_settings().azure_storage_container
    sha = hashlib.sha256()
    total_bytes = 0
    buffer = bytearray()
    block_ids: list[str] = []

    async with blob_service_client() as service:
        blob = service.get_blob_client(container=container_name, blob=storage_path)
        try:

            async def _stage(data: bytes) -> None:
                block_id = _block_id(len(block_ids))
                await blob.stage_block(block_id=block_id, data=data, length=len(data))
                block_ids.append(block_id)

            async for chunk in chunks:
                if not chunk:
                    continue
                total_bytes += len(chunk)
                if total_bytes > max_size_bytes:
                    # No abort call exists: staged blocks are never
                    # committed and the service garbage-collects them.
                    raise PayloadTooLarge(
                        message=(
                            f"Uploaded file exceeds the {max_size_bytes // (1024 * 1024)} MB "
                            "per-request limit."
                        ),
                        details={
                            "limit_bytes": max_size_bytes,
                            "received_bytes": total_bytes,
                        },
                    )
                sha.update(chunk)
                buffer.extend(chunk)
                while len(buffer) >= MULTIPART_PART_SIZE:
                    part = bytes(buffer[:MULTIPART_PART_SIZE])
                    del buffer[:MULTIPART_PART_SIZE]
                    await _stage(part)

            if buffer:
                await _stage(bytes(buffer))
                buffer.clear()

            # An empty block list commits a valid zero-byte blob.
            await blob.commit_block_list(
                [BlobBlock(block_id=block_id) for block_id in block_ids],
                content_settings=ContentSettings(content_type=content_type),
            )
        except PayloadTooLarge:
            raise
        except Exception as exc:
            log.exception(
                "stream_upload failed",
                extra={
                    "event": "storage_stream_upload_failed",
                    "container": container_name,
                    "storage_path": storage_path,
                },
            )
            raise InternalError(
                "Failed to write uploaded file to object storage",
                details={"storage_path": storage_path},
            ) from exc

    return StreamUploadResult(
        size_bytes=total_bytes,
        sha256_hex=sha.hexdigest(),
        storage_path=storage_path,
    )


@asynccontextmanager
async def stream_download(*, storage_path: str) -> AsyncIterator[AsyncIterator[bytes]]:
    """Open a streaming read of ``storage_path``; yield an async byte-iterator.

    The iterator must be consumed inside the ``async with`` block.
    """

    container_name = get_settings().azure_storage_container
    async with blob_service_client() as service:
        blob = service.get_blob_client(container=container_name, blob=storage_path)
        try:
            downloader = await blob.download_blob()
        except Exception as exc:
            log.warning(
                "stream_download download_blob failed",
                extra={
                    "event": "storage_get_object_failed",
                    "container": container_name,
                    "storage_path": storage_path,
                    "error": str(exc),
                },
            )
            raise InternalError(
                "Failed to read uploaded file from object storage",
                details={"storage_path": storage_path},
            ) from exc

        async def _iterate() -> AsyncIterator[bytes]:
            async for chunk in downloader.chunks():
                if chunk:
                    yield chunk

        yield _iterate()


async def delete_object(*, storage_path: str) -> None:
    """Delete the blob at ``storage_path`` (and its snapshots); 404 is success.

    With account-level soft delete enabled the blob stays recoverable for
    the retention window; crypto-shred on offboarding covers that copy.
    """

    container_name = get_settings().azure_storage_container
    async with blob_service_client() as service:
        blob = service.get_blob_client(container=container_name, blob=storage_path)
        try:
            await blob.delete_blob(delete_snapshots="include")
        except ResourceNotFoundError:
            return
        except Exception as exc:
            log.warning(
                "delete_object failed",
                extra={
                    "event": "storage_delete_failed",
                    "container": container_name,
                    "storage_path": storage_path,
                    "error": str(exc),
                },
            )
            raise


async def upload_bytes(*, storage_path: str, body: bytes, content_type: str) -> None:
    """Single-shot upload of in-memory ``body`` to ``storage_path``."""

    container_name = get_settings().azure_storage_container
    async with blob_service_client() as service:
        blob = service.get_blob_client(container=container_name, blob=storage_path)
        await blob.upload_blob(
            body,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )


async def presigned_get_url(*, storage_path: str, expires_in_seconds: int) -> str:
    """Return a read-only SAS URL for ``storage_path``.

    Signed with a user delegation key under credential auth, or with the
    account key under a connection string. ``expires_in_seconds`` is
    clamped to the 7-day service ceiling.
    """

    settings = get_settings()
    container_name = settings.azure_storage_container
    now = datetime.now(UTC)
    start = now - SAS_CLOCK_SKEW
    expiry = now + timedelta(seconds=min(expires_in_seconds, MAX_SAS_SECONDS))

    async with blob_service_client() as service:
        blob = service.get_blob_client(container=container_name, blob=storage_path)
        account_name = service.account_name
        if not account_name:
            # The SDK derives this from the endpoint; a custom domain can hide it.
            raise InternalError(
                "Cannot sign a download link: storage account name is unknown",
                details={"storage_path": storage_path},
            )
        sign_kwargs: dict[str, Any]
        account_key = getattr(service.credential, "account_key", None)
        if account_key:
            sign_kwargs = {"account_key": account_key}
        else:
            delegation_key = await service.get_user_delegation_key(
                key_start_time=start, key_expiry_time=expiry
            )
            sign_kwargs = {"user_delegation_key": delegation_key}

        sas = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=storage_path,
            permission=BlobSasPermissions(read=True),
            start=start,
            expiry=expiry,
            **sign_kwargs,
        )
        return f"{blob.url}?{sas}"
