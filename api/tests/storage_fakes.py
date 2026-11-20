"""Shared in-memory storage backends for the cross-backend storage tests.

Used by ``test_storage_dispatch.py`` and ``test_storage_parity.py`` (managed
fork, docs/managed/adr/M-0001). Not collected by pytest (no ``test_`` prefix).

The fakes themselves are the existing ones — ``FakeS3Client`` from
``test_storage_streaming.py`` and ``FakeBlobService`` from
``test_storage_blob.py`` — subclassed here only to add what the contract
tests need and those modules do not have:

* S3: ``put_object``, ``generate_presigned_url``, ``create_bucket``, a
  bucket that can be missing, and failure injection per client method.
* Blob: the size of each staged block, and failure injection on commit and
  on download.
* Both: a download body that fails part-way through.

``select_s3`` / ``select_blob`` point :mod:`app.storage` at one fake and
make the *other* backend raise if it is touched, so a test can never pass
by exercising the wrong code path.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, NoReturn

import pytest

from app import storage, storage_blob
from app.config import get_settings
from tests.test_storage_blob import (
    CONNECTION_STRING,
    CONTAINER,
    FakeBlobService,
    _FakeBlobClient,
    _FakeDownloader,
)
from tests.test_storage_streaming import FakeS3Client

S3_BUCKET = "contract-bucket"

#: Bytes a failing download hands over before it raises.
DOWNLOAD_PREFIX = b"partial-"


class BackendTouched(Exception):
    """Raised by a tripwire: code reached a backend that is not selected."""


class FakeClientError(Exception):
    """Shaped like botocore's ``ClientError``: carries ``response['Error']['Code']``."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _FailingS3Body:
    """A ``StreamingBody`` that yields one chunk and then fails."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def iter_chunks(self, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
        yield DOWNLOAD_PREFIX
        raise self._exc

    async def close(self) -> None:
        return None


class ContractS3Client(FakeS3Client):
    """``FakeS3Client`` plus the calls and failure hooks the contract tests need.

    Set ``fail[<client method>]`` to make that call raise. ``download_error``
    makes ``get_object`` succeed and its body fail mid-stream.
    """

    def __init__(self) -> None:
        super().__init__()
        self.buckets: set[str] = {S3_BUCKET}
        self.fail: dict[str, Exception] = {}
        self.download_error: Exception | None = None

    def _maybe_fail(self, method: str) -> None:
        if method in self.fail:
            raise self.fail[method]

    def part_sizes(self) -> list[int]:
        return [e[1]["Body_len"] for e in self.events if e[0] == "upload_part"]

    def call_names(self) -> list[str]:
        return [e[0] for e in self.events]

    async def head_bucket(self, *, Bucket: str) -> dict[str, Any]:
        self.events.append(("head_bucket", {"Bucket": Bucket}))
        self._maybe_fail("head_bucket")
        if Bucket not in self.buckets:
            raise FakeClientError("404")
        return {}

    async def create_bucket(self, **kwargs: Any) -> dict[str, Any]:
        self.events.append(("create_bucket", kwargs))
        self._maybe_fail("create_bucket")
        self.buckets.add(kwargs["Bucket"])
        return {}

    async def complete_multipart_upload(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict[str, Any],
    ) -> dict[str, Any]:
        self._maybe_fail("complete_multipart_upload")
        return await super().complete_multipart_upload(
            Bucket=Bucket, Key=Key, UploadId=UploadId, MultipartUpload=MultipartUpload
        )

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        response = await super().get_object(Bucket=Bucket, Key=Key)
        if self.download_error is not None:
            response["Body"] = _FailingS3Body(self.download_error)
        return response

    async def delete_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self._maybe_fail("delete_object")
        return await super().delete_object(Bucket=Bucket, Key=Key)

    async def put_object(
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str
    ) -> dict[str, Any]:
        self.events.append(("put_object", {"Bucket": Bucket, "Key": Key}))
        self._maybe_fail("put_object")
        self.objects[Key] = Body
        self.content_types[Key] = ContentType
        return {}

    async def generate_presigned_url(
        self, operation: str, *, Params: dict[str, Any], ExpiresIn: int
    ) -> str:
        self.events.append(
            ("generate_presigned_url", {"operation": operation, **Params, "ExpiresIn": ExpiresIn})
        )
        self._maybe_fail("generate_presigned_url")
        return f"https://s3.invalid/{Params['Bucket']}/{Params['Key']}?X-Amz-Expires={ExpiresIn}"


class _FailingBlobDownloader(_FakeDownloader):
    """A blob downloader that yields one chunk and then fails."""

    def __init__(self, exc: Exception) -> None:
        super().__init__(b"")
        self._exc = exc

    async def chunks(self) -> AsyncIterator[bytes]:
        yield DOWNLOAD_PREFIX
        raise self._exc


class _ContractBlobClient(_FakeBlobClient):
    def __init__(self, store: ContractBlobService, container: str, name: str) -> None:
        super().__init__(store, container, name)
        self._contract = store

    async def stage_block(self, *, block_id: str, data: bytes, length: int) -> None:
        await super().stage_block(block_id=block_id, data=data, length=length)
        self._contract.block_sizes.append(len(data))

    async def commit_block_list(self, blocks: list[Any], *, content_settings: Any) -> None:
        self._contract.maybe_fail("commit_block_list")
        await super().commit_block_list(blocks, content_settings=content_settings)

    async def download_blob(self) -> _FakeDownloader:
        downloader = await super().download_blob()
        if self._contract.download_error is not None:
            return _FailingBlobDownloader(self._contract.download_error)
        return downloader


class ContractBlobService(FakeBlobService):
    """``FakeBlobService`` that records block sizes and can fail commit/download.

    The container exists from the start (the base fake starts without one).
    """

    def __init__(self) -> None:
        super().__init__()
        self.containers.add(CONTAINER)
        self.block_sizes: list[int] = []
        self.download_error: Exception | None = None

    def call_names(self) -> list[str]:
        return [e[0] for e in self.events]

    def get_blob_client(self, *, container: str, blob: str) -> _FakeBlobClient:
        assert container == CONTAINER
        return _ContractBlobClient(self, container, blob)


def _tripwire(what: str) -> Any:
    def _raise(*args: Any, **kwargs: Any) -> NoReturn:
        raise BackendTouched(f"{what} was used, but that backend is not selected")

    return _raise


def reset_storage_globals(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop the process-wide state both backends keep (undone at teardown)."""

    monkeypatch.setattr(storage_blob, "_client", None)
    get_settings.cache_clear()


def select_s3(
    monkeypatch: pytest.MonkeyPatch, fake: FakeS3Client, *, explicit: bool = True
) -> None:
    """Serve :mod:`app.storage` from ``fake`` over the S3 path; trip on any Blob use.

    ``explicit=False`` leaves ``STORAGE_BACKEND`` unset, to exercise the default.
    """

    if explicit:
        monkeypatch.setenv("STORAGE_BACKEND", "s3")
    else:
        monkeypatch.delenv("STORAGE_BACKEND", raising=False)
    monkeypatch.setenv("S3_BUCKET", S3_BUCKET)
    monkeypatch.setenv("S3_REGION", "us-east-1")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    reset_storage_globals(monkeypatch)

    @asynccontextmanager
    async def _ctx() -> AsyncIterator[FakeS3Client]:
        yield fake

    monkeypatch.setattr(storage, "s3_client", _ctx)
    monkeypatch.setattr(storage_blob, "blob_service_client", _tripwire("the Azure Blob client"))
    monkeypatch.setattr(storage_blob, "_build_client", _tripwire("the Azure Blob client"))


def select_blob(monkeypatch: pytest.MonkeyPatch, fake: FakeBlobService) -> None:
    """Serve :mod:`app.storage` from ``fake`` over the Blob path; trip on any S3 use.

    Connection-string auth, which also selects account-key SAS signing.
    """

    monkeypatch.setenv("STORAGE_BACKEND", "azureblob")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", CONTAINER)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", CONNECTION_STRING)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    reset_storage_globals(monkeypatch)

    @asynccontextmanager
    async def _ctx() -> AsyncIterator[FakeBlobService]:
        yield fake

    monkeypatch.setattr(storage_blob, "blob_service_client", _ctx)
    monkeypatch.setattr(storage, "s3_client", _tripwire("the S3 client"))
    monkeypatch.setattr(storage, "_build_session", _tripwire("the S3 session"))
