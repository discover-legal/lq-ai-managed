"""Contract tests: the S3 and Azure Blob storage backends behave the same.

``stream_upload``'s buffering / SHA-256 / size-cap loop exists twice — in
``app/storage.py`` (upstream's S3 code, deliberately left untouched) and in
``app/storage_blob.py`` (managed fork, docs/managed/adr/M-0001). A fix made
to one copy does not reach the other. Every test in the first part of this
file runs once per backend (the ``backend`` fixture), through the public
``app.storage`` API, with identical assertions; if the two copies drift, one
of the two runs fails.

The second part, "Where the backends differ today", lists the behaviours
that are *not* the same and pins each side, so that changing either one is
noticed. Nothing there is asserted to be equivalent.

Both backends are in-memory fakes (``tests/storage_fakes.py``, built on the
fakes in ``test_storage_streaming.py`` and ``test_storage_blob.py``). These
tests therefore compare the two code paths in ``app``, not the two services:
what real S3 and real Blob Storage do with the calls is outside their reach.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pytest

from app.config import get_settings
from app.errors import InternalError, NotFound, PayloadTooLarge
from app.storage import (
    DOWNLOAD_CHUNK_SIZE,
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
from tests.storage_fakes import (
    DOWNLOAD_PREFIX,
    S3_BUCKET,
    ContractBlobService,
    ContractS3Client,
    FakeClientError,
    reset_storage_globals,
    select_blob,
    select_s3,
)
from tests.test_storage_blob import ACCOUNT_URL

PART = MULTIPART_PART_SIZE
MIB = 1024 * 1024
KEY = "contract-key"
WRITE_FAILED = "Failed to write uploaded file to object storage"
READ_FAILED = "Failed to read uploaded file from object storage"


# ---------------------------------------------------------------------------
# One harness per backend: the same questions, answered from each fake
# ---------------------------------------------------------------------------


class Backend(ABC):
    """What the contract tests need to set up and observe, per backend."""

    name: str

    @abstractmethod
    def stored(self, key: str) -> bytes | None:
        """Committed bytes at ``key`` (``None`` if no object is visible)."""

    @abstractmethod
    def seed(self, key: str, data: bytes) -> None:
        """Put an object at ``key`` without going through ``app.storage``."""

    @abstractmethod
    def content_type(self, key: str) -> str | None: ...

    @abstractmethod
    def part_sizes(self) -> list[int]:
        """Sizes of the parts (S3) / blocks (Blob) sent, in order."""

    @abstractmethod
    def calls(self) -> list[str]:
        """Names of the backend calls made so far."""

    @abstractmethod
    def fail_part(self, number: int, /) -> None:
        """Make the ``number``-th part/block upload (1-based) raise ``RuntimeError``."""

    @abstractmethod
    def fail_commit(self, exc: Exception) -> None:
        """Make the final complete/commit call raise ``exc``."""

    @abstractmethod
    def fail_download_midstream(self, exc: Exception) -> None:
        """Make a download raise ``exc`` after its first chunk."""

    @abstractmethod
    def remove_bucket(self) -> None:
        """Make the configured bucket/container not exist."""


class S3Backend(Backend):
    name = "s3"

    def __init__(self, fake: ContractS3Client) -> None:
        self.fake = fake

    def stored(self, key: str) -> bytes | None:
        return self.fake.objects.get(key)

    def seed(self, key: str, data: bytes) -> None:
        self.fake.objects[key] = data

    def content_type(self, key: str) -> str | None:
        return self.fake.content_types.get(key)

    def part_sizes(self) -> list[int]:
        return self.fake.part_sizes()

    def calls(self) -> list[str]:
        return self.fake.call_names()

    def fail_part(self, number: int, /) -> None:
        self.fake.fail_upload_part_at(number)

    def fail_commit(self, exc: Exception) -> None:
        self.fake.fail["complete_multipart_upload"] = exc

    def fail_download_midstream(self, exc: Exception) -> None:
        self.fake.download_error = exc

    def remove_bucket(self) -> None:
        self.fake.buckets.clear()


class BlobBackend(Backend):
    name = "azureblob"

    def __init__(self, fake: ContractBlobService) -> None:
        self.fake = fake

    def stored(self, key: str) -> bytes | None:
        return self.fake.blobs.get(key)

    def seed(self, key: str, data: bytes) -> None:
        self.fake.blobs[key] = data

    def content_type(self, key: str) -> str | None:
        return self.fake.content_types.get(key)

    def part_sizes(self) -> list[int]:
        return list(self.fake.block_sizes)

    def calls(self) -> list[str]:
        return self.fake.call_names()

    def fail_part(self, number: int, /) -> None:
        self.fake.fail_stage_at = number

    def fail_commit(self, exc: Exception) -> None:
        self.fake.fail["commit_block_list"] = exc

    def fail_download_midstream(self, exc: Exception) -> None:
        self.fake.download_error = exc

    def remove_bucket(self) -> None:
        self.fake.containers.clear()


@pytest.fixture(autouse=True)
def _isolated_storage_globals(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Each test starts and ends with no cached settings and no shared client."""

    reset_storage_globals(monkeypatch)
    yield
    get_settings.cache_clear()


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> ContractS3Client:
    """The S3 backend selected; any use of the Blob client raises."""

    fake = ContractS3Client()
    select_s3(monkeypatch, fake)
    return fake


@pytest.fixture
def blob(monkeypatch: pytest.MonkeyPatch) -> ContractBlobService:
    """The Blob backend selected; any use of the S3 client raises."""

    fake = ContractBlobService()
    select_blob(monkeypatch, fake)
    return fake


@pytest.fixture(params=["s3", "azureblob"])
def backend(request: pytest.FixtureRequest) -> Backend:
    """Runs the requesting test once per backend."""

    if request.param == "s3":
        return S3Backend(request.getfixturevalue("s3"))
    return BlobBackend(request.getfixturevalue("blob"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Period 251 (prime), so a part written at the wrong offset, twice, or not at
# all changes the bytes: no part size here is a multiple of it.
_PATTERN = bytes(range(251))


def _payload(size: int) -> bytes:
    return (_PATTERN * (size // len(_PATTERN) + 1))[:size]


def _split(payload: bytes, how: str) -> list[bytes]:
    """Cut ``payload`` into the chunks one upload request delivers."""

    if how == "whole":
        return [payload]
    if how == "part-sized":  # what the upload handler does: read(MULTIPART_PART_SIZE)
        return [payload[i : i + PART] for i in range(0, len(payload), PART)]
    assert how == "ragged"  # odd sizes straddling part boundaries, with empty chunks
    sizes = [1, 65_536, 0, 1_000_003, 7, 0, 0, 3 * MIB + 5]
    chunks: list[bytes] = [b""]
    offset = 0
    turn = 0
    while offset < len(payload):
        size = sizes[turn % len(sizes)]
        chunks.append(payload[offset : offset + size])
        offset += size
        turn += 1
    chunks.append(b"")
    return chunks


class Source:
    """An upload body that records how much of it was consumed."""

    def __init__(self, *chunks: bytes, then_raise: Exception | None = None) -> None:
        self._chunks = chunks
        self._then_raise = then_raise
        self.pulled = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            self.pulled += 1
            yield chunk
        if self._then_raise is not None:
            raise self._then_raise


async def _upload(
    *chunks: bytes, key: str = KEY, max_size_bytes: int = 64 * MIB, content_type: str = "text/plain"
) -> StreamUploadResult:
    return await stream_upload(
        storage_path=key,
        chunks=aiter(Source(*chunks)),
        content_type=content_type,
        max_size_bytes=max_size_bytes,
    )


async def _download(key: str = KEY) -> list[bytes]:
    async with stream_download(storage_path=key) as chunks:
        return [chunk async for chunk in chunks]


async def _assert_nothing_readable(backend: Backend, key: str = KEY) -> None:
    """No object at ``key``: not in the store, and not through the public API."""

    assert backend.stored(key) is None
    with pytest.raises(InternalError):
        await _download(key)


def _expected_parts(size: int) -> list[int]:
    """Part sizes for a non-empty body: full parts, then the remainder."""

    return [PART] * (size // PART) + ([size % PART] if size % PART else [])


# ===========================================================================
# PART 1 — THE CONTRACT: identical assertions against both backends
# ===========================================================================

# --- stream_upload: size, SHA-256, stored bytes, part boundaries -----------


@pytest.mark.unit
async def test_upload_reports_size_and_sha256_over_multiple_chunks(backend: Backend) -> None:
    chunks = [b"", b"hello ", b"", b"", b"wor", b"ld", b""]
    payload = b"hello world"

    result = await _upload(*chunks, content_type="text/markdown")

    assert result == StreamUploadResult(
        size_bytes=len(payload),
        sha256_hex=hashlib.sha256(payload).hexdigest(),
        storage_path=KEY,
    )
    assert backend.stored(KEY) == payload
    assert backend.content_type(KEY) == "text/markdown"
    assert backend.part_sizes() == [len(payload)]


@pytest.mark.unit
@pytest.mark.parametrize("how", ["whole", "part-sized", "ragged"])
@pytest.mark.parametrize(
    "size",
    [PART - 1, PART, PART + 1, 2 * PART, 2 * PART + 1234],
    ids=["part-1", "part", "part+1", "2-parts", "2-parts+1234"],
)
async def test_upload_around_part_boundaries(backend: Backend, size: int, how: str) -> None:
    payload = _payload(size)

    result = await _upload(*_split(payload, how))

    assert result.size_bytes == size
    assert result.sha256_hex == hashlib.sha256(payload).hexdigest()
    stored = backend.stored(KEY)
    assert stored is not None
    assert len(stored) == size
    assert stored == payload
    # Same boundaries on both backends whatever the inbound chunking: full
    # parts, then one remainder, and no trailing empty part.
    assert backend.part_sizes() == _expected_parts(size)


@pytest.mark.unit
@pytest.mark.parametrize("chunks", [(), (b"",), (b"", b"", b"")], ids=["none", "one", "three"])
async def test_upload_of_an_empty_body(backend: Backend, chunks: tuple[bytes, ...]) -> None:
    result = await _upload(*chunks)

    assert result.size_bytes == 0
    assert result.sha256_hex == hashlib.sha256(b"").hexdigest()
    assert backend.stored(KEY) == b""
    assert await _download() == []


@pytest.mark.unit
async def test_upload_replaces_an_existing_object(backend: Backend) -> None:
    backend.seed(KEY, b"old and longer")

    await _upload(b"new")

    assert backend.stored(KEY) == b"new"
    assert b"".join(await _download()) == b"new"


# --- stream_upload: the size cap ------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("limit", "chunk_sizes"),
    [
        (1_000, [999]),
        (1_000, [1_000]),
        (1_000, [400, 0, 600]),
        (PART, [PART]),
        (PART + 1, [PART, 1]),
    ],
    ids=["one-under", "exactly", "exactly-in-chunks", "exactly-one-part", "exactly-past-a-part"],
)
async def test_upload_up_to_the_cap_is_accepted(
    backend: Backend, limit: int, chunk_sizes: list[int]
) -> None:
    payload = _payload(sum(chunk_sizes))
    chunks = []
    offset = 0
    for size in chunk_sizes:
        chunks.append(payload[offset : offset + size])
        offset += size

    result = await _upload(*chunks, max_size_bytes=limit)

    assert result.size_bytes == len(payload)
    assert result.sha256_hex == hashlib.sha256(payload).hexdigest()
    assert backend.stored(KEY) == payload


@pytest.mark.unit
@pytest.mark.parametrize(
    ("limit", "chunk_sizes", "received"),
    [
        (1_000, [1_001], 1_001),
        (1_000, [600, 600], 1_200),
        (1_000, [1_000, 0, 1], 1_001),
        (PART, [PART + 1], PART + 1),
        (PART, [PART, 1], PART + 1),  # a full part is already uploaded
        (2 * PART + 10, [PART, PART, 5, 6], 2 * PART + 11),
    ],
    ids=["one-over", "second-chunk", "last-byte", "one-part+1", "after-a-part", "after-two-parts"],
)
async def test_upload_over_the_cap_is_rejected_and_leaves_nothing(
    backend: Backend, limit: int, chunk_sizes: list[int], received: int
) -> None:
    source = Source(*(b"X" * size for size in chunk_sizes), b"never read")

    with pytest.raises(PayloadTooLarge) as exc_info:
        await stream_upload(
            storage_path=KEY, chunks=aiter(source), content_type="text/plain", max_size_bytes=limit
        )

    error = exc_info.value
    assert error.details == {"limit_bytes": limit, "received_bytes": received}
    assert error.effective_http_status == 413
    assert error.message == f"Uploaded file exceeds the {limit // MIB} MB per-request limit."
    # The body is not read past the chunk that crossed the cap.
    assert source.pulled == len(chunk_sizes)
    await _assert_nothing_readable(backend)


@pytest.mark.unit
async def test_upload_over_the_cap_keeps_the_previous_object(backend: Backend) -> None:
    backend.seed(KEY, b"previous")

    with pytest.raises(PayloadTooLarge):
        await _upload(b"X" * PART, b"X", max_size_bytes=PART)

    assert backend.stored(KEY) == b"previous"
    assert b"".join(await _download()) == b"previous"


@pytest.mark.unit
@pytest.mark.parametrize("limit", [0, -1])
async def test_upload_with_a_non_positive_cap_is_refused_before_any_io(
    backend: Backend, limit: int
) -> None:
    source = Source(b"a")

    with pytest.raises(InternalError) as exc_info:
        await stream_upload(
            storage_path=KEY, chunks=aiter(source), content_type="text/plain", max_size_bytes=limit
        )

    assert exc_info.value.details == {"max_size_bytes": limit}
    assert exc_info.value.message == "stream_upload called with non-positive max_size_bytes"
    assert source.pulled == 0
    assert backend.calls() == []


# --- stream_upload: backend and source failures ----------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("failing_part", "size"),
    [(1, 10), (1, PART + 10), (2, PART + 10), (3, 2 * PART + 10)],
    ids=["only-part", "first-of-two", "second-of-two", "third-of-three"],
)
async def test_upload_part_failure_is_internal_error_and_leaves_nothing(
    backend: Backend, failing_part: int, size: int
) -> None:
    backend.fail_part(failing_part)

    with pytest.raises(InternalError) as exc_info:
        await _upload(_payload(size))

    assert exc_info.value.message == WRITE_FAILED
    assert exc_info.value.details == {"storage_path": KEY}
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    await _assert_nothing_readable(backend)


@pytest.mark.unit
async def test_upload_commit_failure_is_internal_error_and_leaves_nothing(
    backend: Backend,
) -> None:
    boom = RuntimeError("commit refused")
    backend.fail_commit(boom)

    with pytest.raises(InternalError) as exc_info:
        await _upload(_payload(PART + 10))

    assert exc_info.value.message == WRITE_FAILED
    assert exc_info.value.details == {"storage_path": KEY}
    assert exc_info.value.__cause__ is boom
    await _assert_nothing_readable(backend)


@pytest.mark.unit
async def test_upload_failure_keeps_the_previous_object(backend: Backend) -> None:
    backend.seed(KEY, b"previous")
    backend.fail_part(2)

    with pytest.raises(InternalError):
        await _upload(_payload(PART + 10))

    assert backend.stored(KEY) == b"previous"


@pytest.mark.unit
async def test_upload_whose_body_fails_is_internal_error_and_leaves_nothing(
    backend: Backend,
) -> None:
    """The request body breaks off (client disconnect) after a full part."""

    boom = ConnectionResetError("client went away")
    source = Source(_payload(PART + 10), then_raise=boom)

    with pytest.raises(InternalError) as exc_info:
        await stream_upload(
            storage_path=KEY,
            chunks=aiter(source),
            content_type="text/plain",
            max_size_bytes=64 * MIB,
        )

    assert exc_info.value.message == WRITE_FAILED
    assert exc_info.value.details == {"storage_path": KEY}
    assert exc_info.value.__cause__ is boom
    await _assert_nothing_readable(backend)


# --- stream_download -------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "size",
    [1, DOWNLOAD_CHUNK_SIZE, DOWNLOAD_CHUNK_SIZE + 1, PART + 1],
    ids=["1-byte", "one-download-chunk", "download-chunk+1", "part+1"],
)
async def test_upload_then_download_round_trips(backend: Backend, size: int) -> None:
    payload = _payload(size)
    result = await _upload(*_split(payload, "ragged"))

    chunks = await _download()

    assert all(chunks), "an empty chunk was yielded"
    received = b"".join(chunks)
    assert received == payload
    assert hashlib.sha256(received).hexdigest() == result.sha256_hex
    assert len(received) == result.size_bytes


@pytest.mark.unit
async def test_download_of_a_missing_key_is_internal_error(backend: Backend) -> None:
    with pytest.raises(InternalError) as exc_info:
        async with stream_download(storage_path="never-uploaded"):
            pytest.fail("the context must not be entered")

    assert exc_info.value.message == READ_FAILED
    assert exc_info.value.details == {"storage_path": "never-uploaded"}
    assert exc_info.value.__cause__ is not None


@pytest.mark.unit
async def test_download_error_while_iterating_is_not_translated(backend: Backend) -> None:
    """Both paths translate errors opening the stream, not errors reading it."""

    backend.seed(KEY, b"whatever")
    boom = ConnectionResetError("storage went away")
    backend.fail_download_midstream(boom)
    received: list[bytes] = []

    with pytest.raises(ConnectionResetError) as exc_info:
        async with stream_download(storage_path=KEY) as chunks:
            async for chunk in chunks:
                received.append(chunk)

    assert exc_info.value is boom
    assert received == [DOWNLOAD_PREFIX]


@pytest.mark.unit
async def test_error_raised_by_the_caller_inside_download_propagates(backend: Backend) -> None:
    backend.seed(KEY, b"whatever")
    boom = ValueError("caller's own error")

    with pytest.raises(ValueError) as exc_info:
        async with stream_download(storage_path=KEY):
            raise boom

    assert exc_info.value is boom
    # The backend is still usable afterwards.
    assert b"".join(await _download()) == b"whatever"


# --- delete_object ---------------------------------------------------------


@pytest.mark.unit
async def test_delete_removes_the_object(backend: Backend) -> None:
    await _upload(b"to be deleted")

    await delete_object(storage_path=KEY)

    await _assert_nothing_readable(backend)


@pytest.mark.unit
async def test_delete_is_idempotent(backend: Backend) -> None:
    backend.seed(KEY, b"x")
    backend.seed("bystander", b"untouched")

    await delete_object(storage_path=KEY)
    await delete_object(storage_path=KEY)
    await delete_object(storage_path="never-existed")

    assert backend.stored(KEY) is None
    assert backend.stored("bystander") == b"untouched"


# --- upload_bytes ----------------------------------------------------------


@pytest.mark.unit
async def test_upload_bytes_stores_and_overwrites(backend: Backend) -> None:
    await upload_bytes(storage_path=KEY, body=b"first version", content_type="text/plain")
    assert backend.stored(KEY) == b"first version"
    assert backend.content_type(KEY) == "text/plain"

    await upload_bytes(storage_path=KEY, body=b"v2", content_type="application/zip")

    assert backend.stored(KEY) == b"v2"
    assert backend.content_type(KEY) == "application/zip"
    assert b"".join(await _download()) == b"v2"


@pytest.mark.unit
async def test_upload_bytes_overwrites_a_streamed_upload_and_accepts_empty(
    backend: Backend,
) -> None:
    await _upload(b"streamed")

    await upload_bytes(storage_path=KEY, body=b"", content_type="application/octet-stream")

    assert backend.stored(KEY) == b""
    assert await _download() == []


# --- bucket/container bootstrap, readiness, download links -----------------


@pytest.mark.unit
async def test_readiness_follows_the_bucket_and_never_raises(backend: Backend) -> None:
    assert await check_storage() is True

    backend.remove_bucket()

    assert await check_storage() is False


@pytest.mark.unit
async def test_ensure_bucket_creates_once_and_is_idempotent(backend: Backend) -> None:
    backend.remove_bucket()

    await ensure_bucket()
    assert await check_storage() is True
    await ensure_bucket()

    # S3 looks before it creates; Blob creates and tolerates "already exists".
    # Either way the second call is harmless (see D5 for failures).
    assert await check_storage() is True


@pytest.mark.unit
async def test_download_link_is_an_https_url_for_the_key(backend: Backend) -> None:
    url = await presigned_get_url(storage_path="export.zip", expires_in_seconds=3600)

    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.path.endswith("/export.zip")
    assert parsed.query


# ===========================================================================
# PART 2 — WHERE THE BACKENDS DIFFER TODAY
#
# Real differences between the two code paths, found by reading both and
# confirmed by the tests below. Each is pinned on BOTH sides so a change to
# either is noticed; none is asserted to be equivalent. If you change one of
# these on purpose, update the test and this list.
#
#   D1  delete_object, backend failure:    S3 re-raises the client's own
#       error; Blob raises InternalError chained from it.
#   D2  upload_bytes, backend failure:     as D1.
#   D3  presigned_get_url, signing failure: as D1.
#   D4  presigned_get_url, expiry > 7 days: S3 passes ExpiresIn through
#       unchanged (a real S3 endpoint decides); Blob clamps to 7 days.
#   D5  ensure_bucket, backend failure:    S3 re-raises the client's own
#       error; Blob raises InternalError. (They also probe differently: S3
#       HeadBucket then CreateBucket; Blob create, tolerating "exists".)
#   D6  stream_upload rejected after parts were sent: S3 aborts the multipart
#       upload (no parts remain); Blob has no abort — staged blocks stay
#       uncommitted until the service garbage-collects them.
#   D7  stream_upload rejected on the first chunk: S3 has already opened a
#       multipart upload (create + abort = two calls); Blob has made none.
#   D8  stream_upload of an empty body:    S3 sends one zero-length part;
#       Blob commits an empty block list (no blocks).
#   D9  stream_upload whose body raises a typed app error (LQAIError other
#       than PayloadTooLarge): S3 wraps it in InternalError; Blob lets it
#       through unchanged.
#
# Documented in M-0001: D1-D3 ("One error type"), D6, D8 ("Upload mapping").
# Not documented there: D4 (S3 side), D5, D7, D9.
#
# Not compared at all (one backend only): the shared Blob client and its
# discard-on-auth-failure, the 5 s readiness deadline, SAS signing modes
# (test_storage_blob.py); S3's swallowed abort failure
# (test_storage_streaming.py).
# ===========================================================================


# --- D1: delete_object failure ---------------------------------------------


@pytest.mark.unit
async def test_d1_s3_delete_failure_raises_the_client_error(s3: ContractS3Client) -> None:
    boom = FakeClientError("AccessDenied")
    s3.objects[KEY] = b"x"
    s3.fail["delete_object"] = boom

    with pytest.raises(FakeClientError) as exc_info:
        await delete_object(storage_path=KEY)

    assert exc_info.value is boom
    assert s3.objects[KEY] == b"x"


@pytest.mark.unit
async def test_d1_blob_delete_failure_raises_internal_error(blob: ContractBlobService) -> None:
    boom = RuntimeError("AuthorizationFailure")
    blob.blobs[KEY] = b"x"
    blob.fail["delete_blob"] = boom

    with pytest.raises(InternalError) as exc_info:
        await delete_object(storage_path=KEY)

    assert exc_info.value.message == "Failed to delete file from object storage"
    assert exc_info.value.details == {"storage_path": KEY}
    assert exc_info.value.__cause__ is boom
    assert blob.blobs[KEY] == b"x"


# --- D2: upload_bytes failure ----------------------------------------------


@pytest.mark.unit
async def test_d2_s3_upload_bytes_failure_raises_the_client_error(s3: ContractS3Client) -> None:
    boom = FakeClientError("AccessDenied")
    s3.fail["put_object"] = boom

    with pytest.raises(FakeClientError) as exc_info:
        await upload_bytes(storage_path=KEY, body=b"x", content_type="text/plain")

    assert exc_info.value is boom
    assert KEY not in s3.objects


@pytest.mark.unit
async def test_d2_blob_upload_bytes_failure_raises_internal_error(
    blob: ContractBlobService,
) -> None:
    boom = RuntimeError("AuthorizationFailure")
    blob.fail["upload_blob"] = boom

    with pytest.raises(InternalError) as exc_info:
        await upload_bytes(storage_path=KEY, body=b"x", content_type="text/plain")

    assert exc_info.value.message == "Failed to write file to object storage"
    assert exc_info.value.details == {"storage_path": KEY}
    assert exc_info.value.__cause__ is boom
    assert KEY not in blob.blobs


# --- D3: presigned_get_url failure -----------------------------------------


@pytest.mark.unit
async def test_d3_s3_presign_failure_raises_the_client_error(s3: ContractS3Client) -> None:
    boom = FakeClientError("InvalidAccessKeyId")
    s3.fail["generate_presigned_url"] = boom

    with pytest.raises(FakeClientError) as exc_info:
        await presigned_get_url(storage_path=KEY, expires_in_seconds=60)

    assert exc_info.value is boom


@pytest.mark.unit
async def test_d3_blob_presign_failure_raises_internal_error(
    blob: ContractBlobService, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Managed-identity auth: signing needs a user delegation key from the service.
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_URL", ACCOUNT_URL)
    get_settings.cache_clear()
    boom = RuntimeError("AuthorizationPermissionMismatch")
    blob.fail["get_user_delegation_key"] = boom

    with pytest.raises(InternalError) as exc_info:
        await presigned_get_url(storage_path=KEY, expires_in_seconds=60)

    assert exc_info.value.message == "Failed to sign a download link"
    assert exc_info.value.details == {"storage_path": KEY}
    assert exc_info.value.__cause__ is boom


# --- D4: expiry beyond 7 days ----------------------------------------------

EIGHT_DAYS = 8 * 24 * 60 * 60


@pytest.mark.unit
async def test_d4_s3_passes_an_expiry_over_seven_days_through(s3: ContractS3Client) -> None:
    await presigned_get_url(storage_path=KEY, expires_in_seconds=EIGHT_DAYS)

    assert s3.events == [
        (
            "generate_presigned_url",
            {"operation": "get_object", "Bucket": S3_BUCKET, "Key": KEY, "ExpiresIn": EIGHT_DAYS},
        )
    ]


@pytest.mark.unit
@pytest.mark.usefixtures("blob")
async def test_d4_blob_clamps_an_expiry_over_seven_days() -> None:
    before = datetime.now(UTC).replace(microsecond=0)

    url = await presigned_get_url(storage_path=KEY, expires_in_seconds=EIGHT_DAYS)

    expiry = datetime.strptime(parse_qs(urlparse(url).query)["se"][0], "%Y-%m-%dT%H:%M:%SZ")
    expiry = expiry.replace(tzinfo=UTC)
    seven_days = timedelta(days=7)
    assert before + seven_days <= expiry <= datetime.now(UTC) + seven_days


# --- D5: ensure_bucket failure ---------------------------------------------


@pytest.mark.unit
async def test_d5_s3_ensure_bucket_failure_raises_the_client_error(s3: ContractS3Client) -> None:
    boom = FakeClientError("403")
    s3.fail["head_bucket"] = boom

    with pytest.raises(FakeClientError) as exc_info:
        await ensure_bucket()

    assert exc_info.value is boom
    assert s3.call_names() == ["head_bucket"]  # anything but "not found" is not "create it"


@pytest.mark.unit
async def test_d5_blob_ensure_bucket_failure_raises_internal_error(
    blob: ContractBlobService,
) -> None:
    boom = RuntimeError("AuthorizationFailure")
    blob.fail["create_container"] = boom

    with pytest.raises(InternalError) as exc_info:
        await ensure_bucket()

    assert exc_info.value.message == "Failed to create the storage container"
    assert exc_info.value.details == {}
    assert exc_info.value.__cause__ is boom


# --- D6: rejected after parts were sent ------------------------------------


@pytest.mark.unit
async def test_d6_s3_aborts_the_multipart_upload_when_the_cap_is_crossed(
    s3: ContractS3Client,
) -> None:
    with pytest.raises(PayloadTooLarge):
        await _upload(b"X" * PART, b"X", max_size_bytes=PART)

    assert s3.call_names() == ["create_multipart_upload", "upload_part", "abort_multipart_upload"]
    assert s3.parts_in_progress == {}


@pytest.mark.unit
async def test_d6_blob_leaves_staged_blocks_uncommitted_when_the_cap_is_crossed(
    blob: ContractBlobService,
) -> None:
    with pytest.raises(PayloadTooLarge):
        await _upload(b"X" * PART, b"X", max_size_bytes=PART)

    assert blob.call_names() == ["stage_block"]  # no abort call exists, and no commit
    assert [len(block) for block in blob.staged[KEY].values()] == [PART]
    assert KEY not in blob.blobs


@pytest.mark.unit
async def test_d6_s3_aborts_the_multipart_upload_on_a_backend_failure(
    s3: ContractS3Client,
) -> None:
    s3.fail_upload_part_at(2)

    with pytest.raises(InternalError):
        await _upload(_payload(PART + 10))

    assert s3.call_names()[-1] == "abort_multipart_upload"
    assert s3.parts_in_progress == {}


@pytest.mark.unit
async def test_d6_blob_leaves_staged_blocks_uncommitted_on_a_backend_failure(
    blob: ContractBlobService,
) -> None:
    blob.fail_stage_at = 2

    with pytest.raises(InternalError):
        await _upload(_payload(PART + 10))

    assert blob.call_names() == ["stage_block", "stage_block"]
    assert [len(block) for block in blob.staged[KEY].values()] == [PART]


# --- D7: rejected on the first chunk ---------------------------------------


@pytest.mark.unit
async def test_d7_s3_has_opened_a_multipart_upload_before_the_first_chunk(
    s3: ContractS3Client,
) -> None:
    with pytest.raises(PayloadTooLarge):
        await _upload(b"X" * 1_001, max_size_bytes=1_000)

    assert s3.call_names() == ["create_multipart_upload", "abort_multipart_upload"]


@pytest.mark.unit
async def test_d7_blob_makes_no_call_when_the_first_chunk_is_over_the_cap(
    blob: ContractBlobService,
) -> None:
    with pytest.raises(PayloadTooLarge):
        await _upload(b"X" * 1_001, max_size_bytes=1_000)

    assert blob.call_names() == []
    assert blob.staged == {}


# --- D8: empty body ---------------------------------------------------------


@pytest.mark.unit
async def test_d8_s3_sends_one_zero_length_part_for_an_empty_body(s3: ContractS3Client) -> None:
    await _upload()

    assert s3.part_sizes() == [0]
    assert s3.call_names() == [
        "create_multipart_upload",
        "upload_part",
        "complete_multipart_upload",
    ]


@pytest.mark.unit
async def test_d8_blob_commits_an_empty_block_list_for_an_empty_body(
    blob: ContractBlobService,
) -> None:
    await _upload()

    assert blob.block_sizes == []
    assert blob.events == [("commit_block_list", 0)]


# --- D9: the body raises a typed application error --------------------------


@pytest.mark.unit
async def test_d9_s3_wraps_a_typed_error_from_the_body_in_internal_error(
    s3: ContractS3Client,
) -> None:
    typed = NotFound("raised by the code producing the body")

    with pytest.raises(InternalError) as exc_info:
        await stream_upload(
            storage_path=KEY,
            chunks=aiter(Source(b"abc", then_raise=typed)),
            content_type="text/plain",
            max_size_bytes=1_000,
        )

    assert exc_info.value.message == WRITE_FAILED
    assert exc_info.value.__cause__ is typed
    assert KEY not in s3.objects


@pytest.mark.unit
async def test_d9_blob_lets_a_typed_error_from_the_body_through(
    blob: ContractBlobService,
) -> None:
    typed = NotFound("raised by the code producing the body")

    with pytest.raises(NotFound) as exc_info:
        await stream_upload(
            storage_path=KEY,
            chunks=aiter(Source(b"abc", then_raise=typed)),
            content_type="text/plain",
            max_size_bytes=1_000,
        )

    assert exc_info.value is typed
    assert KEY not in blob.blobs


# ===========================================================================
# PART 3 — SUSPECTED DEFECT IN PRODUCTION CODE (reported, not fixed)
#
# app/storage.py, stream_upload:
#
#     except PayloadTooLarge:
#         # Already aborted above; re-raise unchanged.
#         raise
#
# "Already aborted" holds only for the PayloadTooLarge that stream_upload
# raises itself. One raised by the *body iterator* reaches the same handler
# with the multipart upload still open, and it is never aborted: the parts
# already uploaded stay in the bucket (and are billed) until a lifecycle rule
# removes them. No caller's iterator raises PayloadTooLarge today
# (app/api/files.py::_upload_file_stream yields raw reads), so this is latent.
# It is upstream's S3 code; the Blob path has no abort and is unaffected.
#
# The test below REPRODUCES the defect: it passes while the defect exists and
# will fail when it is fixed — at which point delete it (or invert it).
# ===========================================================================


@pytest.mark.unit
async def test_defect_s3_does_not_abort_when_the_body_raises_payload_too_large(
    s3: ContractS3Client,
) -> None:
    too_large = PayloadTooLarge(message="raised by the code producing the body")

    with pytest.raises(PayloadTooLarge) as exc_info:
        await stream_upload(
            storage_path=KEY,
            chunks=aiter(Source(_payload(PART + 10), then_raise=too_large)),
            content_type="text/plain",
            max_size_bytes=64 * MIB,
        )

    assert exc_info.value is too_large
    assert KEY not in s3.objects
    # The defect: no abort, and the uploaded part is still held.
    assert "abort_multipart_upload" not in s3.call_names()
    assert [[len(body) for _, body in parts] for parts in s3.parts_in_progress.values()] == [[PART]]


@pytest.mark.unit
def test_backends_share_one_part_size() -> None:
    """The Blob path imports the part size from app.storage rather than
    defining its own, so the boundary tests in part 1 compare like with like."""

    from app import storage, storage_blob

    assert storage_blob.MULTIPART_PART_SIZE is storage.MULTIPART_PART_SIZE
