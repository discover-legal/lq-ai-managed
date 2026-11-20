"""Unit tests for the Azure Blob storage backend (managed fork).

Every test sets ``STORAGE_BACKEND=azureblob`` and drives the public
:mod:`app.storage` API through the dispatch hook — the same entry points
the handlers and workers use. Two levels of fake:

* **Data-plane tests** patch ``app.storage_blob.blob_service_client`` (a
  zero-argument async context manager) to yield ``FakeBlobService``, an
  in-memory stand-in for the SDK surface the backend calls.
* **Client-lifetime tests** (``real_client`` fixture) keep the real
  ``BlobServiceClient`` and its real pipeline — retry policy, bearer-token
  policy, error mapping — and fake only the network (``_FakeTransport``,
  injected through ``storage_blob._CLIENT_OPTIONS["transport"]``) and the
  credential (``azure.identity.aio.DefaultAzureCredential`` patched with
  ``_FakeCredential``).

What's covered:

* Block-blob streaming upload: SHA-256/size, part rollover, equal-length
  block IDs, empty body, content type.
* The 413 branch and mid-upload failures: nothing is committed.
* Streaming download, missing-blob error, idempotent delete, single-shot
  upload; every backend failure surfaces as ``InternalError``.
* SAS generation with an account key and with a user delegation key,
  chosen from settings, including the 7-day clamp.
* Container bootstrap and readiness: single attempt, deadline, one log line.
* The shared client: built once, never shared across loops or a fork,
  discarded on authentication failure only, closed after its last user and
  at shutdown.

The auth-mode validation is tested in ``test_config.py``. Round-trip against
a real Blob endpoint (Azurite) is in ``test_storage_blob_azurite.py``.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import os
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, ClassVar
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pytest
from azure.core.credentials import AccessToken
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
    ServiceRequestError,
)
from azure.core.pipeline.transport import AsyncHttpResponse, AsyncHttpTransport, HttpRequest
from azure.storage.blob import BlobSasPermissions, UserDelegationKey, generate_blob_sas

from app import storage_blob
from app.config import get_settings
from app.errors import InternalError, PayloadTooLarge
from app.storage import (
    MULTIPART_PART_SIZE,
    StreamUploadResult,
    check_storage,
    close_storage,
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
ACCOUNT_URL = f"https://{ACCOUNT}.blob.core.windows.net"
CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT};AccountKey={ACCOUNT_KEY};"
    "EndpointSuffix=core.windows.net"
)

# The real client lender, captured before the autouse fixture patches it.
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
        self._store.maybe_fail("delete_blob")
        self._store.events.append(("delete_blob", delete_snapshots))
        if self._name not in self._store.blobs:
            raise ResourceNotFoundError("BlobNotFound")
        del self._store.blobs[self._name]

    async def upload_blob(self, data: bytes, *, overwrite: bool, content_settings: Any) -> None:
        assert overwrite is True
        self._store.maybe_fail("upload_blob")
        self._store.blobs[self._name] = data
        self._store.content_types[self._name] = content_settings.content_type


class _FakeContainerClient:
    def __init__(self, store: FakeBlobService, name: str) -> None:
        self._store = store
        self._name = name

    async def create_container(self) -> None:
        self._store.maybe_fail("create_container")
        if self._name in self._store.containers:
            raise ResourceExistsError("ContainerAlreadyExists")
        self._store.containers.add(self._name)

    async def get_container_properties(self, **kwargs: Any) -> dict[str, Any]:
        self._store.events.append(("get_container_properties", kwargs))
        if self._name not in self._store.containers:
            raise ResourceNotFoundError("ContainerNotFound")
        return {"name": self._name}


class FakeBlobService:
    """In-memory stand-in for ``BlobServiceClient``.

    Deliberately has no ``credential`` attribute: the backend must not read
    SDK internals to decide how to sign. Set ``fail[<sdk method>]`` to make
    that call raise.
    """

    account_name = ACCOUNT

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.staged: dict[str, dict[str, bytes]] = {}
        self.content_types: dict[str, str] = {}
        self.containers: set[str] = set()
        self.events: list[tuple[str, Any]] = []
        self.fail_stage_at: int | None = None
        self.fail: dict[str, Exception] = {}
        self.delegation_requests: list[tuple[datetime, datetime]] = []

    def maybe_fail(self, method: str) -> None:
        if method in self.fail:
            raise self.fail[method]

    def get_blob_client(self, *, container: str, blob: str) -> _FakeBlobClient:
        assert container == CONTAINER
        return _FakeBlobClient(self, container, blob)

    def get_container_client(self, name: str) -> _FakeContainerClient:
        return _FakeContainerClient(self, name)

    async def get_user_delegation_key(
        self, *, key_start_time: datetime, key_expiry_time: datetime
    ) -> UserDelegationKey:
        self.maybe_fail("get_user_delegation_key")
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
    """Blob backend with connection-string auth, served by ``fake_blob``."""

    monkeypatch.setenv("STORAGE_BACKEND", "azureblob")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", CONTAINER)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", CONNECTION_STRING)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    monkeypatch.setattr(storage_blob, "_client", None)
    get_settings.cache_clear()

    @asynccontextmanager
    async def _ctx() -> AsyncIterator[FakeBlobService]:
        yield fake_blob

    with patch("app.storage_blob.blob_service_client", _ctx):
        yield
    get_settings.cache_clear()


@pytest.fixture
def managed_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Switch the settings to managed-identity auth (account URL, no key)."""

    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_URL", ACCOUNT_URL)
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
async def test_presigned_url_with_connection_string_signs_with_account_key(
    fake_blob: FakeBlobService,
) -> None:
    """Regression: the mode used to be read off ``service.credential.account_key``
    (an SDK internal — the fake has no ``credential`` at all)."""

    url = await presigned_get_url(storage_path="export.zip", expires_in_seconds=3600)

    assert url.startswith(f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/export.zip?")
    q = _query(url)
    assert q["sp"] == ["r"]
    assert "skoid" not in q  # account-key SAS, not user-delegation
    assert fake_blob.delegation_requests == []
    # The signature is the one the connection string's AccountKey produces.
    expected = generate_blob_sas(
        account_name=ACCOUNT,
        container_name=CONTAINER,
        blob_name="export.zip",
        account_key=ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        start=q["st"][0],
        expiry=q["se"][0],
    )
    assert q["sig"] == parse_qs(expected)["sig"]


@pytest.mark.unit
async def test_presigned_url_with_managed_identity_uses_user_delegation_key(
    managed_identity: None, fake_blob: FakeBlobService
) -> None:
    url = await presigned_get_url(storage_path="export.zip", expires_in_seconds=24 * 3600)

    q = _query(url)
    assert q["sp"] == ["r"]
    assert q["skoid"] == ["00000000-0000-0000-0000-000000000001"]
    assert len(fake_blob.delegation_requests) == 1


@pytest.mark.unit
async def test_presigned_url_expiry_is_clamped_to_seven_days(
    managed_identity: None, fake_blob: FakeBlobService
) -> None:
    await presigned_get_url(storage_path="x", expires_in_seconds=30 * 24 * 3600)

    start, expiry = fake_blob.delegation_requests[0]
    window = (expiry - start).total_seconds()
    assert window <= storage_blob.MAX_SAS_SECONDS + storage_blob.SAS_CLOCK_SKEW.total_seconds()


@pytest.mark.unit
def test_account_key_is_parsed_from_the_connection_string() -> None:
    # Keys are case-insensitive; the base64 value keeps its own "=" padding.
    assert storage_blob._account_key("AccountName=a;accountkey=abc==;") == "abc=="
    with pytest.raises(InternalError, match="no AccountKey"):
        storage_blob._account_key("BlobEndpoint=https://a.example;SharedAccessSignature=sig=x")


# ---------------------------------------------------------------------------
# container bootstrap / readiness
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
    # The no-retry override travels on the call, not on a separate client.
    assert ("get_container_properties", {"retry_total": 0}) in fake_blob.events


@pytest.mark.unit
async def test_check_storage_gives_up_at_the_deadline(
    monkeypatch: pytest.MonkeyPatch, fake_blob: FakeBlobService
) -> None:
    async def _hang(self: _FakeContainerClient, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(60)
        return {}

    monkeypatch.setattr(_FakeContainerClient, "get_container_properties", _hang)
    monkeypatch.setattr(storage_blob, "READINESS_TIMEOUT_SECONDS", 0.05)
    fake_blob.containers.add(CONTAINER)

    assert await check_storage() is False


# ---------------------------------------------------------------------------
# error translation
# ---------------------------------------------------------------------------


def _storage_log(caplog: pytest.LogCaptureFixture, level: int) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == "app.storage_blob" and r.levelno == level]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("sdk_method", "call", "event"),
    [
        (
            "delete_blob",
            lambda: delete_object(storage_path="k"),
            "storage_delete_failed",
        ),
        (
            "upload_blob",
            lambda: upload_bytes(storage_path="k", body=b"x", content_type="text/plain"),
            "storage_upload_bytes_failed",
        ),
        (
            "get_user_delegation_key",
            lambda: presigned_get_url(storage_path="k", expires_in_seconds=60),
            "storage_presign_failed",
        ),
        ("create_container", lambda: ensure_bucket(), "storage_ensure_failed"),
    ],
)
async def test_backend_failures_surface_as_internal_error(
    managed_identity: None,
    fake_blob: FakeBlobService,
    caplog: pytest.LogCaptureFixture,
    sdk_method: str,
    call: Callable[[], Awaitable[object]],
    event: str,
) -> None:
    """Regression: these three used to leak raw Azure SDK exceptions."""

    raw = HttpResponseError("service said no\nwith a second line of detail")
    fake_blob.fail[sdk_method] = raw

    with (
        caplog.at_level(logging.WARNING, logger="app.storage_blob"),
        pytest.raises(InternalError) as exc_info,
    ):
        await call()

    assert exc_info.value.__cause__ is raw
    [record] = _storage_log(caplog, logging.WARNING)
    assert record.__dict__["event"] == event
    assert record.getMessage().endswith("HttpResponseError: service said no")
    assert not record.exc_info


@pytest.mark.unit
async def test_unexpected_errors_keep_their_traceback(
    fake_blob: FakeBlobService, caplog: pytest.LogCaptureFixture
) -> None:
    fake_blob.fail["upload_blob"] = RuntimeError("a bug, not a backend answer")

    with caplog.at_level(logging.WARNING, logger="app.storage_blob"), pytest.raises(InternalError):
        await upload_bytes(storage_path="k", body=b"x", content_type="text/plain")

    [record] = _storage_log(caplog, logging.WARNING)
    assert record.exc_info


# ---------------------------------------------------------------------------
# the shared client — real BlobServiceClient, fake network and credential
# ---------------------------------------------------------------------------

# What DefaultAzureCredential raises with no identity available (abridged).
_CHAIN_DUMP = (
    "DefaultAzureCredential failed to retrieve a token from the included credentials.\n"
    "Attempted credentials:\n"
    "\tEnvironmentCredential: EnvironmentCredential authentication unavailable.\n"
    "\tManagedIdentityCredential: ManagedIdentityCredential authentication unavailable."
)

_SUCCESS_STATUS = {"PUT": 201, "DELETE": 202}


class _FakeResponse(AsyncHttpResponse):
    """A bodiless success, or a storage XML error when ``error_code`` is given."""

    def __init__(self, request: HttpRequest, status: int, error_code: str | None = None) -> None:
        super().__init__(request, None)
        self.status_code = status
        self.reason = error_code or "OK"
        self.headers = {}
        self._body = b""
        if error_code is not None:
            self.headers = {"x-ms-error-code": error_code, "Content-Type": "application/xml"}
            self._body = (
                '<?xml version="1.0" encoding="utf-8"?>'
                f"<Error><Code>{error_code}</Code><Message>fake</Message></Error>"
            ).encode()
        self.content_type = self.headers.get("Content-Type")

    def body(self) -> bytes:
        return self._body

    async def load_body(self) -> None:
        return None


def _succeed(request: HttpRequest) -> _FakeResponse:
    return _FakeResponse(request, _SUCCESS_STATUS.get(request.method, 200))


def _answer(status: int, error_code: str) -> Callable[[HttpRequest], _FakeResponse]:
    return lambda request: _FakeResponse(request, status, error_code)


def _refuse_connection(request: HttpRequest) -> _FakeResponse:
    raise ServiceRequestError("connection refused")


class _FakeTransport(AsyncHttpTransport):
    """The network: records each request and answers with ``respond``."""

    def __init__(self) -> None:
        self.requests: list[HttpRequest] = []
        self.close_calls = 0
        self.respond: Callable[[HttpRequest], _FakeResponse] = _succeed

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        self.close_calls += 1

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def send(self, request: HttpRequest, **kwargs: Any) -> _FakeResponse:
        self.requests.append(request)
        return self.respond(request)


class _FakeCredential:
    """Stands in for ``DefaultAzureCredential``; set ``error`` to refuse tokens."""

    instances: ClassVar[list[_FakeCredential]] = []
    error: ClassVar[Exception | None] = None

    def __init__(self) -> None:
        self.token_requests = 0
        self.close_calls = 0
        _FakeCredential.instances.append(self)

    async def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        self.token_requests += 1
        if _FakeCredential.error is not None:
            raise _FakeCredential.error
        return AccessToken("fake-token", int(time.time()) + 3600)

    async def close(self) -> None:
        self.close_calls += 1


@pytest.fixture
async def real_client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_FakeTransport]:
    """Un-fake the client: the real ``BlobServiceClient`` over ``_FakeTransport``.

    Connection-string auth by default; request ``managed_identity`` as well
    for credential auth (``DefaultAzureCredential`` is ``_FakeCredential``).
    """

    transport = _FakeTransport()
    monkeypatch.setitem(storage_blob._CLIENT_OPTIONS, "transport", transport)
    # Backoff 0 + 0**n: the retry policy makes its attempts without sleeping.
    monkeypatch.setitem(storage_blob._CLIENT_OPTIONS, "increment_base", 0)
    monkeypatch.setitem(storage_blob._CLIENT_OPTIONS, "random_jitter_range", 0)
    _FakeCredential.instances = []
    _FakeCredential.error = None
    with (
        patch("azure.identity.aio.DefaultAzureCredential", _FakeCredential),
        patch("app.storage_blob.blob_service_client", _REAL_BLOB_SERVICE_CLIENT),
    ):
        yield transport
        await close_storage()


@pytest.fixture
def builds(monkeypatch: pytest.MonkeyPatch) -> list[storage_blob._SharedClient]:
    """Every shared client built during the test, in order."""

    built: list[storage_blob._SharedClient] = []
    build = storage_blob._build_client

    def _recording() -> storage_blob._SharedClient:
        built.append(build())
        return built[-1]

    monkeypatch.setattr(storage_blob, "_build_client", _recording)
    return built


async def _upload() -> None:
    await upload_bytes(storage_path="k", body=b"x", content_type="text/plain")


@pytest.mark.unit
async def test_one_client_serves_every_call(
    real_client: _FakeTransport, builds: list[storage_blob._SharedClient]
) -> None:
    """Regression: a new client (and TCP/TLS handshake) was built per call."""

    await ensure_bucket()
    assert await check_storage() is True
    await _upload()
    await delete_object(storage_path="k")

    assert len(builds) == 1
    assert [r.method for r in real_client.requests] == ["PUT", "GET", "PUT", "DELETE"]
    assert real_client.close_calls == 0  # left open for the next call


@pytest.mark.unit
async def test_connection_string_auth_never_builds_a_credential(
    real_client: _FakeTransport, builds: list[storage_blob._SharedClient]
) -> None:
    await _upload()

    assert _FakeCredential.instances == []
    assert builds[0].credential is None
    assert real_client.requests[0].headers["Authorization"].startswith("SharedKey ")


@pytest.mark.unit
async def test_managed_identity_shares_one_credential_and_one_token(
    real_client: _FakeTransport,
    builds: list[storage_blob._SharedClient],
    managed_identity: None,
) -> None:
    await ensure_bucket()
    assert await check_storage() is True
    await _upload()

    assert len(builds) == 1
    [credential] = _FakeCredential.instances
    assert builds[0].credential is credential
    assert credential.token_requests == 1  # cached by the client's bearer-token policy
    assert {r.headers["Authorization"] for r in real_client.requests} == {"Bearer fake-token"}
    assert credential.close_calls == 0


@pytest.mark.unit
@pytest.mark.parametrize("refused_by", ["credential", "service"])
async def test_authentication_failure_discards_client_and_credential(
    real_client: _FakeTransport,
    builds: list[storage_blob._SharedClient],
    managed_identity: None,
    refused_by: str,
) -> None:
    if refused_by == "credential":
        _FakeCredential.error = ClientAuthenticationError(_CHAIN_DUMP)
    else:
        real_client.respond = _answer(401, "InvalidAuthenticationInfo")

    with pytest.raises(InternalError) as exc_info:
        await _upload()

    # Detected on the SDK error itself, where it is first caught.
    assert isinstance(exc_info.value.__cause__, ClientAuthenticationError)
    assert builds[0].discarded and builds[0].closed
    assert _FakeCredential.instances[0].close_calls == 1
    assert real_client.close_calls == 1

    _FakeCredential.error = None
    real_client.respond = _succeed
    await _upload()  # rebuilt from scratch
    assert len(builds) == 2
    assert len(_FakeCredential.instances) == 2
    assert _FakeCredential.instances[1].close_calls == 0


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["authorization", "network"])
async def test_other_failures_keep_the_client(
    real_client: _FakeTransport,
    builds: list[storage_blob._SharedClient],
    managed_identity: None,
    failure: str,
) -> None:
    """A missing role (403) is not a reason to throw the credential away."""

    if failure == "authorization":
        real_client.respond = _answer(403, "AuthorizationPermissionMismatch")
    else:
        real_client.respond = _refuse_connection

    with pytest.raises(InternalError) as exc_info:
        await _upload()
    assert not isinstance(exc_info.value.__cause__, ClientAuthenticationError)

    real_client.respond = _succeed
    await _upload()
    assert len(builds) == 1
    assert _FakeCredential.instances[0].close_calls == 0
    assert real_client.close_calls == 0


@pytest.mark.unit
async def test_discarded_client_is_closed_only_after_its_last_user(
    real_client: _FakeTransport,
    builds: list[storage_blob._SharedClient],
    managed_identity: None,
) -> None:
    async with storage_blob.blob_service_client():  # an in-flight call, e.g. a long download
        _FakeCredential.error = ClientAuthenticationError("token rejected")
        with pytest.raises(InternalError):
            await _upload()
        first = builds[0]
        assert first.discarded and not first.closed  # still in use

        _FakeCredential.error = None
        await _upload()  # new calls get a fresh client meanwhile
        assert len(builds) == 2
        assert _FakeCredential.instances[0].close_calls == 0
        assert real_client.close_calls == 0

    assert first.closed
    assert _FakeCredential.instances[0].close_calls == 1
    assert real_client.close_calls == 1
    assert not builds[1].closed


@pytest.mark.unit
async def test_client_is_not_reused_across_event_loops_or_after_a_fork(
    real_client: _FakeTransport, builds: list[storage_blob._SharedClient]
) -> None:
    await _upload()

    other_loop = asyncio.new_event_loop()
    other_loop.close()
    builds[0].loop = other_loop  # as if built on a loop that has since gone
    await _upload()
    assert len(builds) == 2

    builds[1].pid = os.getpid() + 1  # as seen from a forked child
    await _upload()
    assert len(builds) == 3

    # A foreign client is dropped, never awaited — even when asked to close.
    builds[0].discarded = True
    await builds[0].close_if_idle()
    assert real_client.close_calls == 0

    await close_storage()
    assert real_client.close_calls == 1  # only the client that belongs here


@pytest.mark.unit
async def test_close_storage_closes_client_and_credential_once(
    real_client: _FakeTransport, managed_identity: None
) -> None:
    await _upload()

    await close_storage()
    await close_storage()  # idempotent

    assert storage_blob._client is None
    assert _FakeCredential.instances[0].close_calls == 1
    assert real_client.close_calls == 1


@pytest.mark.unit
async def test_readiness_makes_one_attempt_while_other_calls_retry(
    real_client: _FakeTransport,
) -> None:
    """``retry_total=0`` on the call overrides the client-wide retry count."""

    real_client.respond = _refuse_connection

    assert await check_storage() is False
    assert len(real_client.requests) == 1

    with pytest.raises(InternalError):
        await ensure_bucket()
    assert len(real_client.requests) == 1 + 4  # one try and retry_total=3 retries


@pytest.mark.unit
async def test_readiness_authentication_failure_logs_one_line_and_discards(
    real_client: _FakeTransport,
    builds: list[storage_blob._SharedClient],
    managed_identity: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Regression: every failed poll logged the multi-line credential chain."""

    _FakeCredential.error = ClientAuthenticationError(_CHAIN_DUMP)

    with caplog.at_level(logging.DEBUG, logger="app.storage_blob"):
        assert await check_storage() is False

    [warning] = _storage_log(caplog, logging.WARNING)
    assert warning.getMessage() == (
        "Storage readiness check failed: ClientAuthenticationError: DefaultAzureCredential "
        "failed to retrieve a token from the included credentials. (shared client discarded)"
    )
    assert not warning.exc_info
    [detail] = _storage_log(caplog, logging.DEBUG)
    assert detail.exc_info  # the full chain, at DEBUG only

    assert _FakeCredential.instances[0].token_requests == 1  # no retries
    assert builds[0].discarded and builds[0].closed
    assert _FakeCredential.instances[0].close_calls == 1


@pytest.mark.unit
async def test_sdk_loggers_are_quiet_unless_debugging(real_client: _FakeTransport) -> None:
    ours = logging.getLogger("app.storage_blob")
    http = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    identity = logging.getLogger("azure.identity")
    try:
        ours.setLevel(logging.INFO)
        await _upload()
        assert not http.isEnabledFor(logging.INFO)  # per-request URL and header dumps
        assert not identity.isEnabledFor(logging.WARNING)  # per-failure credential chain
    finally:
        for logger in (ours, http, identity):
            logger.setLevel(logging.NOTSET)


@pytest.mark.unit
async def test_malformed_connection_string_is_internal_error(
    real_client: _FakeTransport, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "not-a-connection-string")
    get_settings.cache_clear()

    with pytest.raises(InternalError, match="misconfigured"):
        await _upload()
    assert await check_storage() is False


@pytest.mark.unit
async def test_check_storage_fails_fast_when_endpoint_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: the SDK's default retry backoff held /ready for ~85 s.

    Real transport against a closed loopback port.
    """

    with socket.socket() as sock:  # reserve a port, then free it: nothing listens
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setenv(
        "AZURE_STORAGE_CONNECTION_STRING",
        f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={ACCOUNT_KEY};"
        f"BlobEndpoint=http://127.0.0.1:{port}/{ACCOUNT};",
    )
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    get_settings.cache_clear()

    started = time.monotonic()
    with patch("app.storage_blob.blob_service_client", _REAL_BLOB_SERVICE_CLIENT):
        assert await check_storage() is False
        await close_storage()
    assert time.monotonic() - started < 2
