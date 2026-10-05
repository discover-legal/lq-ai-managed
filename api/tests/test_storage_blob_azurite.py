"""Round-trip tests for the Azure Blob backend against a real endpoint.

Runs only when ``AZURITE_CONNECTION_STRING`` is set — Azurite, Microsoft's
local Blob emulator, in dev/CI::

    docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \\
        azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck --loose
    export AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=http;\\
    AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;\\
    BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

Drives the public :mod:`app.storage` API with ``STORAGE_BACKEND=azureblob``.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
from azure.storage.blob.aio import BlobServiceClient

from app.config import get_settings
from app.errors import InternalError, PayloadTooLarge
from app.storage import (
    MULTIPART_PART_SIZE,
    check_storage,
    delete_object,
    ensure_bucket,
    presigned_get_url,
    stream_download,
    stream_upload,
    upload_bytes,
)

CONNECTION_STRING = os.environ.get("AZURITE_CONNECTION_STRING", "")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not CONNECTION_STRING, reason="AZURITE_CONNECTION_STRING not set"),
]


@pytest.fixture(autouse=True)
def _azurite_backend(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    container = f"lqtest-{secrets.token_hex(4)}"
    monkeypatch.setenv("STORAGE_BACKEND", "azureblob")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", CONNECTION_STRING)
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", container)
    get_settings.cache_clear()
    yield container
    get_settings.cache_clear()


async def _aiter(*chunks: bytes) -> AsyncIterator[bytes]:
    for c in chunks:
        yield c


async def _blob_names(container: str) -> list[str]:
    async with BlobServiceClient.from_connection_string(CONNECTION_STRING) as service:
        client = service.get_container_client(container)
        return [b.name async for b in client.list_blobs()]


async def test_round_trip_through_public_api(_azurite_backend: str) -> None:
    await ensure_bucket()
    await ensure_bucket()  # idempotent
    assert await check_storage() is True

    # Two full blocks plus a tail exercises stage/commit across boundaries.
    payload = secrets.token_bytes(2 * MULTIPART_PART_SIZE + 4321)
    result = await stream_upload(
        storage_path="doc-1",
        chunks=_aiter(*(payload[i : i + 1_000_000] for i in range(0, len(payload), 1_000_000))),
        content_type="application/pdf",
        max_size_bytes=len(payload),
    )
    assert result.size_bytes == len(payload)
    assert result.sha256_hex == hashlib.sha256(payload).hexdigest()

    async with stream_download(storage_path="doc-1") as chunks:
        downloaded = b"".join([c async for c in chunks])
    assert downloaded == payload

    await upload_bytes(storage_path="export.zip", body=b"zip-bytes", content_type="application/zip")
    url = await presigned_get_url(storage_path="export.zip", expires_in_seconds=600)
    async with httpx.AsyncClient() as http:
        response = await http.get(url)
    assert response.status_code == 200
    assert response.content == b"zip-bytes"
    assert response.headers["content-type"] == "application/zip"

    await delete_object(storage_path="doc-1")
    await delete_object(storage_path="doc-1")  # idempotent
    assert await _blob_names(_azurite_backend) == ["export.zip"]


async def test_oversized_upload_commits_nothing(_azurite_backend: str) -> None:
    await ensure_bucket()
    with pytest.raises(PayloadTooLarge):
        await stream_upload(
            storage_path="too-big",
            chunks=_aiter(b"x" * 700, b"x" * 700),
            content_type="text/plain",
            max_size_bytes=1_000,
        )
    assert await _blob_names(_azurite_backend) == []


async def test_empty_upload_and_missing_download(_azurite_backend: str) -> None:
    await ensure_bucket()
    result = await stream_upload(
        storage_path="empty",
        chunks=_aiter(),
        content_type="text/plain",
        max_size_bytes=10,
    )
    assert result.size_bytes == 0
    async with stream_download(storage_path="empty") as chunks:
        assert b"".join([c async for c in chunks]) == b""

    with pytest.raises(InternalError):
        async with stream_download(storage_path="missing"):
            pass
