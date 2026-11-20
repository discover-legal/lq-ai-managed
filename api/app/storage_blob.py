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

Exactly one of two modes; ``app.config`` refuses to load settings that
name neither or both:

* ``AZURE_STORAGE_CONNECTION_STRING`` — account key (Azurite in dev/CI,
  account-key deployments). No Azure credential object is ever built.
* ``AZURE_STORAGE_ACCOUNT_URL`` — ``DefaultAzureCredential``: managed
  identity in Azure, so no storage keys exist in the deployment. The
  identity needs the "Storage Blob Data Contributor" role assigned at
  **storage-account scope** (or above): Get User Delegation Key, which
  signs download links, is an account-level operation and is refused
  (403) under a container-scoped assignment, as is creating the container
  at startup. This mode has never been run against a real Azure account;
  it is exercised only by unit tests with a fake credential and transport.

Client lifetime
---------------

One ``BlobServiceClient`` (and, under managed identity, its credential)
serves every call in the process, so connections are pooled and tokens
are cached and refreshed by the SDK. ``_SharedClient`` holds it together
with the guards that make sharing safe: it is never used from another
event loop or a forked process, it is discarded when the SDK reports an
authentication failure (the next call builds a fresh client and
credential), and a discarded client is closed only after its last
in-flight call has finished. ``close_client`` closes it at shutdown.

Errors
------

Backend failures surface as :class:`app.errors.InternalError` chained
from the SDK error, after one structured log line (``_backend_errors``).
:class:`PayloadTooLarge` passes through; a 404 on delete is success.
Errors raised while a caller iterates a download stream are not
translated — the same as on the S3 path.

Retries
-------

``_CLIENT_OPTIONS`` documents the retry schedule and what it costs when
the account is unreachable. The readiness check makes a single attempt
(``retry_total=0`` on the call) under ``READINESS_TIMEOUT_SECONDS``.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from azure.core.exceptions import (
    AzureError,
    ClientAuthenticationError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.storage.blob import (
    BlobBlock,
    BlobSasPermissions,
    ContentSettings,
    generate_blob_sas,
)
from azure.storage.blob.aio import BlobServiceClient

from app.config import get_settings
from app.errors import InternalError, LQAIError, PayloadTooLarge
from app.storage import MULTIPART_PART_SIZE, StreamUploadResult

log = logging.getLogger(__name__)

# User delegation keys (and therefore SAS signed with them) are capped at
# 7 days by the service — the same ceiling as S3 presigned URLs.
MAX_SAS_SECONDS = 7 * 24 * 60 * 60

# Clock-skew allowance on SAS start times.
SAS_CLOCK_SKEW = timedelta(minutes=5)

# Client-wide transport/retry tuning.
#
# The storage SDK's ExponentialRetry sleeps
# ``initial_backoff + increment_base ** n`` seconds (± ``random_jitter_range``)
# before retry n = 1, 2, 3. The exponent starts at 1, not 0: the retry
# counter is incremented before the sleep is computed (``increment`` in
# azure/storage/blob/_shared/policies.py, ``get_backoff_time`` in
# policies_async.py). So:
#
# * SDK defaults (15, 3, jitter 3) sleep 18 + 24 + 42 = 84 s. Measured
#   against a closed loopback port: 85 s and 90 s before the error surfaced.
# * The earlier tuning here (1, 2, jitter 1) slept 3 + 5 + 9 = 17 s, not the
#   1 + 2 + 4 it was written for. Measured: 16.5 s.
# * These values (0, 2, jitter 1) sleep 2 + 4 + 8 = 14 s, 11-17 s with
#   jitter — the shortest doubling schedule the policy can express.
#   Measured: 12.9-14.5 s over five runs.
#
# An authentication error raised by the credential is retried on the same
# schedule (the retry policy sits outside the credential policy): measured
# 14.4 s for one call with no identity available.
#
# ``connection_timeout`` bounds each connection attempt, so a host that
# drops packets instead of refusing can add up to 4 x 10 s to the sleeps.
# That case has not been measured.
_CLIENT_OPTIONS: dict[str, Any] = {
    "retry_total": 3,
    "initial_backoff": 0,
    "increment_base": 2,
    "random_jitter_range": 1,
    "connection_timeout": 10,
}

# Deadline for the readiness check, covering credential acquisition as
# well as the storage call. Derived from what else bounds GET /ready:
#
# * /ready gathers its four checks and answers when the slowest returns.
#   The only sibling with an explicit bound is the gateway check, 5.0 s
#   (``health_check`` in app/clients/gateway.py); ``check_db`` and
#   ``check_redis`` set none. Matching it means storage never holds /ready
#   open longer than the gateway check already can.
# * The Helm readinessProbe for the api polls /ready every 10 s
#   (``periodSeconds``) and sets no ``timeoutSeconds``, so the kubelet
#   gives up after the Kubernetes default of 1 s. 5 s ends well inside one
#   period. It is deliberately not lowered to 1 s: a check cancelled there
#   abandons a slow first token request and starts it again on the next
#   poll, where 5 s lets it finish and be cached.
# * docker-compose's api healthcheck calls /health, not /ready.
READINESS_TIMEOUT_SECONDS = 5.0


def _block_id(n: int) -> str:
    """Return the base64 block ID for part ``n``.

    All block IDs within a blob must have the same encoded length, so the
    index is zero-padded before encoding.
    """

    return base64.b64encode(f"{n:08d}".encode()).decode()


class _SharedClient:
    """The process-wide ``BlobServiceClient``, the credential it owns (managed
    identity only), and the state that makes sharing them safe."""

    def __init__(self, service: BlobServiceClient, credential: Any = None) -> None:
        self.service = service
        self.credential = credential
        self.loop = asyncio.get_running_loop()
        self.pid = os.getpid()
        self.users = 0
        self.discarded = False
        self.closed = False

    def usable(self) -> bool:
        """True while this client may be handed to a new call."""

        return (
            not self.discarded
            and self.pid == os.getpid()
            and self.loop is asyncio.get_running_loop()
        )

    async def close_if_idle(self) -> None:
        """Close a discarded client once no call is using it. Never raises."""

        if not self.discarded or self.users > 0 or self.closed:
            return
        self.closed = True
        if self.pid != os.getpid() or self.loop is not asyncio.get_running_loop():
            return  # its connections belong to another process/loop: drop, don't await
        for resource in (self.service, self.credential):
            if resource is None:
                continue
            try:
                await resource.close()
            except Exception as exc:
                log.warning("Closing Azure Blob client failed: %s", _summary(exc))


_client: _SharedClient | None = None


def _build_client() -> _SharedClient:
    settings = get_settings()
    if not log.isEnabledFor(logging.DEBUG):
        # At INFO the SDK logs every request's URL and headers, and
        # azure-identity logs the whole credential chain on each failed
        # token request. This module logs one line per failure instead.
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
            logging.WARNING
        )
        logging.getLogger("azure.identity").setLevel(logging.ERROR)
    try:
        if settings.azure_storage_connection_string:
            return _SharedClient(
                BlobServiceClient.from_connection_string(
                    settings.azure_storage_connection_string, **_CLIENT_OPTIONS
                )
            )
        # Imported lazily: azure-identity is only needed for credential auth.
        from azure.identity.aio import DefaultAzureCredential

        credential = DefaultAzureCredential()
        return _SharedClient(
            BlobServiceClient(
                settings.azure_storage_account_url, credential=credential, **_CLIENT_OPTIONS
            ),
            credential,
        )
    except Exception as exc:
        # e.g. a malformed connection string; the SDK's message names no secrets.
        log.warning("Azure Blob client could not be built: %s", _summary(exc))
        raise InternalError("Azure Blob storage is misconfigured") from exc


@asynccontextmanager
async def blob_service_client() -> AsyncIterator[BlobServiceClient]:
    """Lend out the shared ``BlobServiceClient`` for one storage call.

    Tests substitute a fake backend by patching this name with a
    zero-argument async context manager that yields the fake service.
    """

    global _client

    holder = _client
    if holder is None or not holder.usable():
        # The replaced client is either discarded (its last user closes it)
        # or belongs to another loop/process (dropped without awaiting).
        holder = _client = _build_client()
    holder.users += 1
    try:
        yield holder.service
    finally:
        holder.users -= 1
        await holder.close_if_idle()


async def close_client() -> None:
    """Close the shared client and its credential (process shutdown). Idempotent."""

    global _client

    holder, _client = _client, None
    if holder is not None:
        holder.discarded = True
        await holder.close_if_idle()


def _summary(exc: BaseException) -> str:
    """One line for the log: exception type and the first line of its message."""

    lines = str(exc).strip().splitlines()
    return f"{type(exc).__name__}: {lines[0]}" if lines else type(exc).__name__


def _discard_on_auth_failure(service: object, exc: BaseException) -> bool:
    """Discard the shared client if ``exc`` is an authentication failure on it.

    The next call then builds a fresh client and credential instead of
    reusing one the service (or the identity endpoint) has refused; the
    discarded one is closed when the last call using it returns.
    Authorization failures (403, missing role) are ``HttpResponseError`` and
    do not match.
    """

    holder = _client
    if (
        isinstance(exc, ClientAuthenticationError)
        and holder is not None
        and holder.service is service
        and not holder.discarded
    ):
        holder.discarded = True
        return True
    return False


@contextmanager
def _backend_errors(
    service: object, message: str, *, event: str, storage_path: str | None = None
) -> Iterator[None]:
    """Turn any backend failure in the block into a logged ``InternalError``.

    The single place SDK errors are caught, so it is also where an
    authentication failure discards the shared client. Typed application
    errors (``PayloadTooLarge``, ``InternalError``) pass through.
    """

    try:
        yield
    except LQAIError:
        raise
    except Exception as exc:
        discarded = _discard_on_auth_failure(service, exc)
        # SDK errors describe themselves in one line (their full text can run
        # to a screenful); anything else is unexpected and keeps its traceback.
        expected = isinstance(exc, AzureError)
        log.warning(
            "%s: %s%s",
            message,
            _summary(exc),
            " (shared client discarded)" if discarded else "",
            extra={
                "event": event,
                "container": get_settings().azure_storage_container,
                "storage_path": storage_path,
            },
            exc_info=not expected,
        )
        if expected:
            log.debug("%s: full error", message, exc_info=True)
        details = {"storage_path": storage_path} if storage_path is not None else None
        raise InternalError(message, details=details) from exc


def _account_key(connection_string: str) -> str:
    """Return the ``AccountKey`` of a storage connection string."""

    for part in connection_string.split(";"):
        name, _, value = part.partition("=")
        if name.strip().lower() == "accountkey" and value.strip():
            return value.strip()
    raise InternalError(
        "Cannot sign a download link: the storage connection string has no AccountKey"
    )


async def ensure_container() -> None:
    """Create the configured container if it does not exist."""

    container_name = get_settings().azure_storage_container
    async with blob_service_client() as service:
        with _backend_errors(
            service, "Failed to create the storage container", event="storage_ensure_failed"
        ):
            try:
                await service.get_container_client(container_name).create_container()
            except ResourceExistsError:
                return
        log.info("Created Azure Blob container: %s", container_name)


async def check_storage() -> bool:
    """Readiness check: True if the configured container is reachable.

    One attempt, bounded by ``READINESS_TIMEOUT_SECONDS``; never raises.
    Polled, so a failure logs one WARNING line (full detail at DEBUG).
    """

    container_name = get_settings().azure_storage_container
    try:
        async with asyncio.timeout(READINESS_TIMEOUT_SECONDS), blob_service_client() as service:
            with _backend_errors(
                service, "Storage readiness check failed", event="storage_readiness_failed"
            ):
                # retry_total=0 on the call overrides the client-wide retry count.
                await service.get_container_client(container_name).get_container_properties(
                    retry_total=0
                )
        return True
    except InternalError:
        return False  # already logged, by _backend_errors or _build_client
    except TimeoutError:
        log.warning(
            "Storage readiness check failed: no answer within %g s", READINESS_TIMEOUT_SECONDS
        )
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

        async def _stage(data: bytes) -> None:
            block_id = _block_id(len(block_ids))
            await blob.stage_block(block_id=block_id, data=data, length=len(data))
            block_ids.append(block_id)

        with _backend_errors(
            service,
            "Failed to write uploaded file to object storage",
            event="storage_stream_upload_failed",
            storage_path=storage_path,
        ):
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

    return StreamUploadResult(
        size_bytes=total_bytes,
        sha256_hex=sha.hexdigest(),
        storage_path=storage_path,
    )


@asynccontextmanager
async def stream_download(*, storage_path: str) -> AsyncIterator[AsyncIterator[bytes]]:
    """Open a streaming read of ``storage_path``; yield an async byte-iterator.

    The iterator must be consumed inside the ``async with`` block, which
    also keeps the shared client open for the length of the stream.
    """

    container_name = get_settings().azure_storage_container
    async with blob_service_client() as service:
        blob = service.get_blob_client(container=container_name, blob=storage_path)
        with _backend_errors(
            service,
            "Failed to read uploaded file from object storage",
            event="storage_get_object_failed",
            storage_path=storage_path,
        ):
            downloader = await blob.download_blob()

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
        with _backend_errors(
            service,
            "Failed to delete file from object storage",
            event="storage_delete_failed",
            storage_path=storage_path,
        ):
            try:
                await blob.delete_blob(delete_snapshots="include")
            except ResourceNotFoundError:
                return  # Already gone; idempotent success.


async def upload_bytes(*, storage_path: str, body: bytes, content_type: str) -> None:
    """Single-shot upload of in-memory ``body`` to ``storage_path``."""

    container_name = get_settings().azure_storage_container
    async with blob_service_client() as service:
        blob = service.get_blob_client(container=container_name, blob=storage_path)
        with _backend_errors(
            service,
            "Failed to write file to object storage",
            event="storage_upload_bytes_failed",
            storage_path=storage_path,
        ):
            await blob.upload_blob(
                body,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
            )


async def presigned_get_url(*, storage_path: str, expires_in_seconds: int) -> str:
    """Return a read-only SAS URL for ``storage_path``.

    The signing mode follows the configured auth mode: the account key
    under a connection string, a user delegation key under managed
    identity. ``expires_in_seconds`` is clamped to the 7-day service
    ceiling.
    """

    settings = get_settings()
    container_name = settings.azure_storage_container
    now = datetime.now(UTC)
    start = now - SAS_CLOCK_SKEW
    expiry = now + timedelta(seconds=min(expires_in_seconds, MAX_SAS_SECONDS))

    async with blob_service_client() as service:
        with _backend_errors(
            service,
            "Failed to sign a download link",
            event="storage_presign_failed",
            storage_path=storage_path,
        ):
            blob = service.get_blob_client(container=container_name, blob=storage_path)
            account_name = service.account_name
            if not account_name:
                # The SDK derives this from the endpoint; a custom domain can hide it.
                raise InternalError(
                    "Cannot sign a download link: storage account name is unknown",
                    details={"storage_path": storage_path},
                )
            sign_kwargs: dict[str, Any]
            if settings.azure_storage_connection_string:
                sign_kwargs = {
                    "account_key": _account_key(settings.azure_storage_connection_string)
                }
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
