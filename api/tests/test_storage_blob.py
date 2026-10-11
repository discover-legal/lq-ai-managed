"""Unit tests for the Azure Blob storage backend (managed fork).

``app.storage_blob.blob_service_client`` is patched to yield an in-memory
fake, and ``STORAGE_BACKEND=azureblob`` is set, so every test drives the
public :mod:`app.storage` API through the dispatch hook — the same entry
points the handlers and workers use.

What's covered:

* Block-blob streaming upload: SHA-256/size, part rollover, equal-length
  block IDs, empty body, content type.
* The 413 branch and mid-upload failures: nothing is committed.
* Streaming download, missing-blob error, idempotent delete, single-shot
  upload.
* SAS generation with an account key and with a user delegation key,
  including the 7-day clamp.
* Container bootstrap, readiness, and the missing-configuration error.

Round-trip against a real Blob endpoint (Azurite) is in
``test_storage_blob_azurite.py``.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import UserDelegationKey

from app import storage_blob
from app.config import get_settings
from app.errors import InternalError, PayloadTooLarge
from app.storage import (
    MULTIPART_PART_SIZE,
    StreamUploadResult,
    check_storage,
    delete_object,
    ensure_bucket,
    presigned_get_url,
    stream_download,
    stream_upload,
    upload_bytes,
)

CONTAINER = "test-files"
ACCOUNT = "fakeaccount"
ACCOUNT_KEY = base64.b64encode(b"k" * 64).decode()

# The real client builder, captured before the autouse fixture patches it.
_REAL_BLOB_SERVICE_CLIENT = storage_blob.blob_service_client

# ---------------------------------------------------------------------------
# In-memory fake of the async Blob SDK surface we use
# ---------------------------------------------------------------------------


class _FakeDownloader:
    def __init__(self, payload: bytes, chunk_size: int = 4) -> None:
        self._payload = payload
        self._chunk_size = chunk_size

    async def chunks(self) -> AsyncIterator[bytes]:
        for offset in range(0, len(self._payload), self._chunk_size):
            yield self._payload[offset : offset + self._chunk_size]


class _FakeBlobClient:
    def __init__(self, store: FakeBlobService, container: str, name: str) -> None:
        self._store = store
        self._container = container
        self._name = name
        self.url = f"https://{ACCOUNT}.blob.core.windows.net/{container}/{name}"

    async def stage_block(self, *, block_id: str, data: bytes, length: int) -> None:
        self._store.events.append(("stage_block", block_id))
        if self._store.fail_stage_at == len(
            [e for e in self._store.events if e[0] == "stage_block"]
        ):
            raise RuntimeError("simulated stage_block failure")
        assert length == len(data)
        self._store.staged.setdefault(self._name, {})[block_id] = data

    async def commit_block_list(self, blocks: list[Any], *, content_settings: Any) -> None:
        self._store.events.append(("commit_block_list", len(blocks)))
        staged = self._store.staged.pop(self._name, {})
        self._store.blobs[self._name] = b"".join(staged[b.id] for b in blocks)
        self._store.content_types[self._name] = content_settings.content_type

    async def download_blob(self) -> _FakeDownloader:
        if self._name not in self._store.blobs:
            raise ResourceNotFoundError("BlobNotFound")
        return _FakeDownloader(self._store.blobs[self._name])

    async def delete_blob(self, *, delete_snapshots: str) -> None:
        self._store.events.append(("delete_blob", delete_snapshots))
        if self._name not in self._store.blobs:
            raise ResourceNotFoundError("BlobNotFound")
        del self._store.blobs[self._name]

    async def upload_blob(self, data: bytes, *, overwrite: bool, content_settings: Any) -> None:
        assert overwrite is True
        self._store.blobs[self._name] = data
        self._store.content_types[self._name] = content_settings.content_type


class _FakeContainerClient:
    def __init__(self, store: FakeBlobService, name: str) -> None:
        self._store = store
        self._name = name

    async def create_container(self) -> None:
        if self._name in self._store.containers:
            raise ResourceExistsError("ContainerAlreadyExists")
        self._store.containers.add(self._name)

    async def get_container_properties(self) -> dict[str, Any]:
        if self._name not in self._store.containers:
            raise ResourceNotFoundError("ContainerNotFound")
        return {"name": self._name}


class _AccountKeyCredential:
    account_key = ACCOUNT_KEY


class FakeBlobService:
    account_name = ACCOUNT

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.staged: dict[str, dict[str, bytes]] = {}
        self.content_types: dict[str, str] = {}
        self.containers: set[str] = set()
        self.events: list[tuple[str, Any]] = []
        self.fail_stage_at: int | None = None
        self.credential: Any = _AccountKeyCredential()
        self.delegation_requests: list[tuple[datetime, datetime]] = []

    def get_blob_client(self, *, container: str, blob: str) -> _FakeBlobClient:
        assert container == CONTAINER
        return _FakeBlobClient(self, container, blob)

    def get_container_client(self, name: str) -> _FakeContainerClient:
        return _FakeContainerClient(self, name)

    async def get_user_delegation_key(
        self, *, key_start_time: datetime, key_expiry_time: datetime
    ) -> UserDelegationKey:
        self.delegation_requests.append((key_start_time, key_expiry_time))
        key = UserDelegationKey()
        key.signed_oid = "00000000-0000-0000-0000-000000000001"
        key.signed_tid = "00000000-0000-0000-0000-000000000002"
        key.signed_start = key_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        key.signed_expiry = key_expiry_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        key.signed_service = "b"
        key.signed_version = "2021-08-06"
        key.value = base64.b64encode(b"d" * 32).decode()
        return key


@pytest.fixture
def fake_blob() -> FakeBlobService:
    return FakeBlobService()


@pytest.fixture(autouse=True)
def _azureblob_backend(
    monkeypatch: pytest.MonkeyPatch, fake_blob: FakeBlobService
) -> Iterator[None]:
    monkeypatch.setenv("STORAGE_BACKEND", "azureblob")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", CONTAINER)
    get_settings.cache_clear()

    @asynccontextmanager
    async def _ctx() -> AsyncIterator[FakeBlobService]:
        yield fake_blob

    with patch("app.storage_blob.blob_service_client", _ctx):
        yield
    get_settings.cache_clear()


async def _aiter(*chunks: bytes) -> AsyncIterator[bytes]:
    for c in chunks:
        yield c


# ---------------------------------------------------------------------------
# stream_upload
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_stream_upload_records_size_sha256_and_content_type(
    fake_blob: FakeBlobService,
) -> None:
    payload = b"hello world"
    result = await stream_upload(
        storage_path="my-key",
        chunks=_aiter(b"hello ", b"", b"world"),
        content_type="text/plain",
        max_size_bytes=1_000,
    )

    assert isinstance(result, StreamUploadResult)
    assert result.size_bytes == len(payload)
    assert result.sha256_hex == hashlib.sha256(payload).hexdigest()
    assert result.storage_path == "my-key"
    assert fake_blob.blobs["my-key"] == payload
    assert fake_blob.content_types["my-key"] == "text/plain"


@pytest.mark.unit
async def test_stream_upload_rolls_over_part_boundary(fake_blob: FakeBlobService) -> None:
    payload = b"A" * (2 * MULTIPART_PART_SIZE + 1234)
    result = await stream_upload(
        storage_path="big",
        chunks=_aiter(payload),
        content_type="application/octet-stream",
        max_size_bytes=10 * MULTIPART_PART_SIZE,
    )

    assert result.size_bytes == len(payload)
    assert fake_blob.blobs["big"] == payload
    staged_ids = [e[1] for e in fake_blob.events if e[0] == "stage_block"]
    assert len(staged_ids) == 3
    # Block IDs within one blob must share an encoded length and be unique.
    assert len({len(i) for i in staged_ids}) == 1
    assert len(set(staged_ids)) == 3
    assert ("commit_block_list", 3) in fake_blob.events


@pytest.mark.unit
async def test_stream_upload_empty_body_commits_zero_byte_blob(
    fake_blob: FakeBlobService,
) -> None:
    result = await stream_upload(
        storage_path="empty",
        chunks=_aiter(),
        content_type="application/octet-stream",
        max_size_bytes=1_000,
    )

    assert result.size_bytes == 0
    assert result.sha256_hex == hashlib.sha256(b"").hexdigest()
    assert fake_blob.blobs["empty"] == b""
    assert ("commit_block_list", 0) in fake_blob.events


@pytest.mark.unit
async def test_stream_upload_rejects_oversized_body_without_commit(
    fake_blob: FakeBlobService,
) -> None:
    with pytest.raises(PayloadTooLarge) as exc_info:
        await stream_upload(
            storage_path="too-big",
            chunks=_aiter(b"X" * 600, b"X" * 600),
            content_type="application/octet-stream",
            max_size_bytes=1_000,
        )

    assert exc_info.value.details["limit_bytes"] == 1_000
    assert exc_info.value.details["received_bytes"] == 1_200
    assert "too-big" not in fake_blob.blobs
    assert not [e for e in fake_blob.events if e[0] == "commit_block_list"]


@pytest.mark.unit
async def test_stream_upload_backend_failure_is_internal_error(
    fake_blob: FakeBlobService,
) -> None:
    fake_blob.fail_stage_at = 1
    with pytest.raises(InternalError):
        await stream_upload(
            storage_path="will-fail",
            chunks=_aiter(b"some bytes"),
            content_type="text/plain",
            max_size_bytes=1_000_000,
        )
    assert "will-fail" not in fake_blob.blobs


@pytest.mark.unit
async def test_stream_upload_rejects_non_positive_limit() -> None:
    with pytest.raises(InternalError):
        await stream_upload(
            storage_path="x",
            chunks=_aiter(b"a"),
            content_type="text/plain",
            max_size_bytes=0,
        )


# ---------------------------------------------------------------------------
# download / delete / upload_bytes
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_stream_download_yields_blob_bytes(fake_blob: FakeBlobService) -> None:
    fake_blob.blobs["doc"] = b"0123456789"
    async with stream_download(storage_path="doc") as chunks:
        received = b"".join([c async for c in chunks])
    assert received == b"0123456789"


@pytest.mark.unit
async def test_stream_download_missing_blob_is_internal_error() -> None:
    with pytest.raises(InternalError):
        async with stream_download(storage_path="nope"):
            pass


@pytest.mark.unit
async def test_delete_object_removes_blob_and_snapshots(fake_blob: FakeBlobService) -> None:
    fake_blob.blobs["gone"] = b"x"
    await delete_object(storage_path="gone")
    assert "gone" not in fake_blob.blobs
    assert ("delete_blob", "include") in fake_blob.events


@pytest.mark.unit
async def test_delete_object_is_idempotent_on_missing() -> None:
    await delete_object(storage_path="never-existed")


@pytest.mark.unit
async def test_upload_bytes_overwrites(fake_blob: FakeBlobService) -> None:
    fake_blob.blobs["export.zip"] = b"old"
    await upload_bytes(storage_path="export.zip", body=b"new", content_type="application/zip")
    assert fake_blob.blobs["export.zip"] == b"new"
    assert fake_blob.content_types["export.zip"] == "application/zip"


# ---------------------------------------------------------------------------
# presigned_get_url (SAS)
# ---------------------------------------------------------------------------


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(url).query)


@pytest.mark.unit
async def test_presigned_url_with_account_key_is_read_only() -> None:
    url = await presigned_get_url(storage_path="export.zip", expires_in_seconds=3600)

    assert url.startswith(f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/export.zip?")
    q = _query(url)
    assert q["sp"] == ["r"]
    assert "sig" in q
    assert "skoid" not in q  # account-key SAS, not user-delegation


@pytest.mark.unit
async def test_presigned_url_with_credential_uses_user_delegation_key(
    fake_blob: FakeBlobService,
) -> None:
    fake_blob.credential = object()  # token credential: no account_key
    url = await presigned_get_url(storage_path="export.zip", expires_in_seconds=24 * 3600)

    q = _query(url)
    assert q["sp"] == ["r"]
    assert q["skoid"] == ["00000000-0000-0000-0000-000000000001"]
    assert len(fake_blob.delegation_requests) == 1


@pytest.mark.unit
async def test_presigned_url_expiry_is_clamped_to_seven_days(
    fake_blob: FakeBlobService,
) -> None:
    fake_blob.credential = object()
    await presigned_get_url(storage_path="x", expires_in_seconds=30 * 24 * 3600)

    start, expiry = fake_blob.delegation_requests[0]
    window = (expiry - start).total_seconds()
    assert window <= storage_blob.MAX_SAS_SECONDS + storage_blob.SAS_CLOCK_SKEW.total_seconds()


# ---------------------------------------------------------------------------
# container bootstrap / readiness / configuration
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_ensure_bucket_creates_container_once(fake_blob: FakeBlobService) -> None:
    await ensure_bucket()
    await ensure_bucket()  # ResourceExistsError swallowed
    assert fake_blob.containers == {CONTAINER}


@pytest.mark.unit
async def test_check_storage_reports_reachability(fake_blob: FakeBlobService) -> None:
    assert await check_storage() is False
    fake_blob.containers.add(CONTAINER)
    assert await check_storage() is True


@pytest.mark.unit
async def test_missing_configuration_raises_internal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    get_settings.cache_clear()

    with pytest.raises(InternalError) as exc_info:
        async with _REAL_BLOB_SERVICE_CLIENT():
            pass
    assert "AZURE_STORAGE_ACCOUNT_URL" in exc_info.value.details["required"]
