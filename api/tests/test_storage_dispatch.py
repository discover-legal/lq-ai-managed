"""Every public function of ``app.storage`` must dispatch to Azure Blob (managed fork).

``app/storage.py`` is upstream's S3 module. The fork adds a guard at the top
of each public function::

    if _use_azure_blob():
        from app import storage_blob
        return await storage_blob.<fn>(...)

The guards are written out per function (docs/managed/adr/M-0001), so a
function upstream adds later has none until somebody writes it — and until
then an Azure deployment would send that call to S3 without any error. These
tests close that gap:

* The public API of ``app.storage`` is found by introspection, not listed by
  hand. Each name must be in ``DISPATCH`` (it has a Blob counterpart and a
  guard) or in ``EXEMPT`` (with a reason). A name in neither fails
  ``test_every_public_storage_name_is_classified`` with instructions.
* For each ``DISPATCH`` entry, under ``STORAGE_BACKEND=azureblob`` the call
  must reach the Blob counterpart exactly once with the same arguments and
  must not touch S3; under the default backend it must not touch
  ``app.storage_blob`` at all.
* The last section proves the checks fail when they should, against stubs
  patched onto ``app.storage``.

What this cannot see: a guard that is present but passes *different*
arguments is caught (arguments are compared by identity); a Blob counterpart
that behaves differently from the S3 one is the job of
``test_storage_parity.py``.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import ModuleType
from typing import Any, NoReturn

import pytest

from app import storage, storage_blob
from app.config import get_settings
from tests.storage_fakes import (
    S3_BUCKET,
    BackendTouched,
    ContractS3Client,
    reset_storage_globals,
    select_s3,
)
from tests.test_storage_blob import CONNECTION_STRING, CONTAINER

KEY = "dispatch-key"


async def _chunks() -> AsyncIterator[bytes]:
    yield b"dispatch"


@dataclass(frozen=True)
class Route:
    """One guarded function: where it must go on Azure, and a valid call."""

    #: Name of the counterpart in ``app.storage_blob``.
    blob_name: str
    #: Builds the keyword arguments for one call (fresh each time: the upload
    #: body is a single-use iterator).
    kwargs: Callable[[], dict[str, Any]]
    #: False when the guard does not return the counterpart's result.
    returns_blob_result: bool = True
    #: False when the S3 path makes no client call at all.
    s3_path_does_io: bool = True


# ---------------------------------------------------------------------------
# The classification. Every public name in app.storage is in exactly one.
# ---------------------------------------------------------------------------

#: Functions that do storage I/O: each needs a Blob counterpart and a guard.
DISPATCH: dict[str, Route] = {
    "ensure_bucket": Route("ensure_container", dict),
    # The S3 path holds no process-wide resources, so it does nothing; the
    # guard awaits close_client() and returns None.
    "close_storage": Route("close_client", dict, returns_blob_result=False, s3_path_does_io=False),
    "check_storage": Route("check_storage", dict),
    "stream_upload": Route(
        "stream_upload",
        lambda: {
            "storage_path": KEY,
            "chunks": _chunks(),
            "content_type": "text/plain",
            "max_size_bytes": 1024,
        },
    ),
    "stream_download": Route("stream_download", lambda: {"storage_path": KEY}),
    "delete_object": Route("delete_object", lambda: {"storage_path": KEY}),
    "upload_bytes": Route(
        "upload_bytes",
        lambda: {"storage_path": KEY, "body": b"dispatch", "content_type": "text/plain"},
    ),
    "presigned_get_url": Route(
        "presigned_get_url", lambda: {"storage_path": KEY, "expires_in_seconds": 60}
    ),
}

#: Public names that must NOT be dispatched, each with the reason.
EXEMPT: dict[str, str] = {
    # S3-internal: the aioboto3 client factory. Nothing outside storage.py may
    # rely on it under Azure (it would talk to S3); the Blob equivalent is
    # storage_blob.blob_service_client.
    "s3_client": "S3 client factory; only the S3 code paths use it",
    # Not I/O: the result dataclass, shared by both backends.
    "StreamUploadResult": "dataclass returned by stream_upload on both backends",
    # First-party names storage.py imports. Listed (rather than skipped as
    # "imported") so that a storage function moved into another app module
    # and re-exported from app.storage is still seen by the check.
    "get_settings": "imported from app.config",
    "InternalError": "imported from app.errors",
    "PayloadTooLarge": "imported from app.errors",
}


# ---------------------------------------------------------------------------
# Discovery and the checks themselves
# ---------------------------------------------------------------------------


def _public_api(module: ModuleType) -> dict[str, Any]:
    """Public callables ``module`` defines, or re-exports from first-party code.

    Skipped: underscore-prefixed names, non-callables (constants, the
    logger) and anything imported from outside ``app`` (stdlib, aioboto3,
    botocore).
    """

    found: dict[str, Any] = {}
    for name, obj in vars(module).items():
        if name.startswith("_") or not callable(obj):
            continue
        origin = getattr(obj, "__module__", None) or ""
        if origin == module.__name__ or origin == "app" or origin.startswith("app."):
            found[name] = obj
    return found


def _unclassified(module: ModuleType) -> list[str]:
    return sorted(set(_public_api(module)) - set(DISPATCH) - set(EXEMPT))


def _assert_all_classified(module: ModuleType) -> None:
    missing = _unclassified(module)
    if missing:
        raise AssertionError(
            f"app.storage has public name(s) the Azure Blob dispatch does not cover: {missing}.\n"
            "With STORAGE_BACKEND=azureblob a call to an uncovered function goes to S3.\n"
            "For each one, either:\n"
            "  1. add a counterpart to api/app/storage_blob.py, start the function in\n"
            "     api/app/storage.py with the `if _use_azure_blob():` guard that delegates\n"
            "     to it, and add a Route to DISPATCH in api/tests/test_storage_dispatch.py\n"
            "     (and contract tests to api/tests/test_storage_parity.py); or\n"
            "  2. if it does no storage I/O or is S3-internal, add it to EXEMPT in\n"
            "     api/tests/test_storage_dispatch.py with the reason.\n"
            "See docs/managed/adr/M-0001-azure-blob-storage-backend.md."
        )


def _is_context_manager_factory(fn: Any) -> bool:
    """True for a function decorated with ``@asynccontextmanager``."""

    return inspect.isasyncgenfunction(inspect.unwrap(fn))


async def _invoke(fn: Any, kwargs: dict[str, Any]) -> Any:
    """Call ``fn`` whatever its kind; return what a caller would receive.

    Coroutine functions are awaited; ``@asynccontextmanager`` functions
    (``stream_download``) are entered and exited, returning the value bound
    by ``async with``; a plain function's result is returned as is.
    """

    result = fn(**kwargs)
    if hasattr(result, "__aenter__"):
        async with result as value:
            return value
    if inspect.isawaitable(result):
        return await result
    return result


class _Recorder:
    """Stands in for one ``app.storage_blob`` function and records its calls."""

    def __init__(self, real: Any) -> None:
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.result = object()
        self.exited = 0
        self.replacement: Any
        if _is_context_manager_factory(real):

            @asynccontextmanager
            async def _cm(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
                self.calls.append((args, kwargs))
                try:
                    yield self.result
                finally:
                    self.exited += 1

            self.replacement = _cm
        elif inspect.iscoroutinefunction(real):

            async def _coro(*args: Any, **kwargs: Any) -> Any:
                self.calls.append((args, kwargs))
                self.exited += 1
                return self.result

            self.replacement = _coro
        else:

            def _plain(*args: Any, **kwargs: Any) -> Any:
                self.calls.append((args, kwargs))
                self.exited += 1
                return self.result

            self.replacement = _plain


async def _assert_dispatches_to_blob(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, name: str, route: Route
) -> None:
    """Fail unless ``module.<name>`` hands the call to ``storage_blob.<route.blob_name>``.

    Requires ``STORAGE_BACKEND=azureblob`` to be in effect. Raises
    ``AssertionError``; S3 use is reported first because the S3 paths catch
    and re-wrap exceptions (``check_storage`` swallows them entirely), so the
    tripwire's own exception cannot be relied on to surface.
    """

    assert get_settings().storage_backend == "azureblob"
    s3_touches: list[str] = []

    def _s3_tripwire(what: str) -> Callable[..., NoReturn]:
        def _raise(*args: Any, **kwargs: Any) -> NoReturn:
            s3_touches.append(what)
            raise BackendTouched(f"app.storage.{what} used under STORAGE_BACKEND=azureblob")

        return _raise

    recorder = _Recorder(getattr(storage_blob, route.blob_name))
    kwargs = route.kwargs()
    result: Any = None
    error: Exception | None = None
    with monkeypatch.context() as patched:
        patched.setattr(module, "s3_client", _s3_tripwire("s3_client"))
        patched.setattr(module, "_build_session", _s3_tripwire("_build_session"))
        patched.setattr(storage_blob, route.blob_name, recorder.replacement)
        try:
            result = await _invoke(getattr(module, name), kwargs)
        except Exception as exc:
            error = exc

    hint = (
        f"Start app.storage.{name} with the `if _use_azure_blob():` guard delegating to "
        f"app.storage_blob.{route.blob_name}."
    )
    if s3_touches:
        raise AssertionError(
            f"app.storage.{name} touched S3 ({', '.join(s3_touches)}) under "
            f"STORAGE_BACKEND=azureblob. {hint}"
        )
    if error is not None:
        raise AssertionError(
            f"app.storage.{name} raised {error!r} under STORAGE_BACKEND=azureblob. {hint}"
        ) from error
    if len(recorder.calls) != 1:
        raise AssertionError(
            f"app.storage.{name} called app.storage_blob.{route.blob_name} "
            f"{len(recorder.calls)} times under STORAGE_BACKEND=azureblob (expected once). {hint}"
        )
    args, received = recorder.calls[0]
    same_arguments = (
        args == ()
        and received.keys() == kwargs.keys()
        and all(received[k] is kwargs[k] for k in kwargs)
    )
    if not same_arguments:
        raise AssertionError(
            f"app.storage.{name} passed different arguments to "
            f"app.storage_blob.{route.blob_name}: sent {kwargs!r}, received {args!r} {received!r}."
        )
    if recorder.exited != 1:
        raise AssertionError(
            f"app.storage.{name} left app.storage_blob.{route.blob_name} open "
            "(its context was not exited)."
        )
    if route.returns_blob_result and result is not recorder.result:
        raise AssertionError(
            f"app.storage.{name} did not return what app.storage_blob.{route.blob_name} "
            f"returned (got {result!r})."
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolated_storage_globals(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Each test starts and ends with no cached settings and no shared client."""

    reset_storage_globals(monkeypatch)
    yield
    get_settings.cache_clear()


@pytest.fixture
def azureblob(monkeypatch: pytest.MonkeyPatch) -> None:
    """Select the Blob backend. No Blob client may be built: every call under
    test is intercepted at the ``app.storage_blob`` function it should reach."""

    def _no_client(*args: Any, **kwargs: Any) -> NoReturn:
        raise BackendTouched("a real Azure Blob client was about to be built")

    monkeypatch.setenv("STORAGE_BACKEND", "azureblob")
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER", CONTAINER)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", CONNECTION_STRING)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    monkeypatch.setattr(storage_blob, "_build_client", _no_client)
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# The classification is complete and not stale
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_every_public_storage_name_is_classified() -> None:
    _assert_all_classified(storage)


@pytest.mark.unit
def test_discovery_sees_the_known_public_api() -> None:
    """Guards the introspection itself: if it found nothing, every other test
    here would pass vacuously."""

    assert set(_public_api(storage)) == set(DISPATCH) | set(EXEMPT)
    assert len(DISPATCH) >= 8
    assert not set(DISPATCH) & set(EXEMPT)


@pytest.mark.unit
@pytest.mark.parametrize("name", sorted(DISPATCH))
def test_route_matches_its_blob_counterpart(name: str) -> None:
    """Same kind of callable and same parameters on both sides of the guard."""

    route = DISPATCH[name]
    ours = getattr(storage, name)
    theirs = getattr(storage_blob, route.blob_name, None)
    assert theirs is not None, f"app.storage_blob.{route.blob_name} does not exist"
    assert getattr(theirs, "__module__", None) == storage_blob.__name__

    assert inspect.iscoroutinefunction(ours) == inspect.iscoroutinefunction(theirs)
    assert _is_context_manager_factory(ours) == _is_context_manager_factory(theirs)

    def _shape(fn: Any) -> list[tuple[str, Any, Any]]:
        return [(p.name, p.kind, p.default) for p in inspect.signature(fn).parameters.values()]

    assert _shape(ours) == _shape(theirs)
    # The sample call in DISPATCH is a complete, valid call.
    inspect.signature(ours).bind(**route.kwargs())


# ---------------------------------------------------------------------------
# STORAGE_BACKEND=azureblob: every call reaches Blob and never S3
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.usefixtures("azureblob")
@pytest.mark.parametrize("name", sorted(DISPATCH))
async def test_azureblob_backend_dispatches_to_blob(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _assert_dispatches_to_blob(monkeypatch, storage, name, DISPATCH[name])


# ---------------------------------------------------------------------------
# STORAGE_BACKEND=s3 (the default): nothing in storage_blob is called
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("explicit", [False, True], ids=["unset", "s3"])
@pytest.mark.parametrize("name", sorted(DISPATCH))
async def test_s3_backend_never_calls_storage_blob(
    name: str, explicit: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    route = DISPATCH[name]
    fake_s3 = ContractS3Client()
    fake_s3.objects[KEY] = b"already there"
    select_s3(monkeypatch, fake_s3, explicit=explicit)
    assert get_settings().storage_backend == "s3"

    blob_touches: list[str] = []

    def _blob_tripwire(what: str) -> Callable[..., NoReturn]:
        def _raise(*args: Any, **kwargs: Any) -> NoReturn:
            blob_touches.append(what)
            raise BackendTouched(f"app.storage_blob.{what} called under STORAGE_BACKEND=s3")

        return _raise

    blob_functions = [
        n
        for n, obj in vars(storage_blob).items()
        if inspect.isfunction(obj) and obj.__module__ == storage_blob.__name__
    ]
    assert route.blob_name in blob_functions
    for blob_function in blob_functions:
        monkeypatch.setattr(storage_blob, blob_function, _blob_tripwire(blob_function))

    await _invoke(getattr(storage, name), route.kwargs())

    assert blob_touches == []
    if route.s3_path_does_io:
        assert fake_s3.events, f"app.storage.{name} made no S3 call on the S3 backend"
        buckets = {e[1].get("Bucket") for e in fake_s3.events}
        assert buckets == {S3_BUCKET}
    else:
        assert fake_s3.events == []


# ---------------------------------------------------------------------------
# The checks have teeth: each failure mode is detected
# ---------------------------------------------------------------------------


def _defined_in(module_name: str, fn: Any) -> Any:
    """Make ``fn`` look as if it were written in ``module_name``."""

    fn.__module__ = module_name
    return fn


@pytest.mark.unit
def test_new_public_function_without_classification_is_flagged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upstream adds ``copy_object`` to storage.py; nobody ports it."""

    async def copy_object(*, source: str, destination: str) -> None:
        async with storage.s3_client() as s3:
            await s3.copy_object(source, destination)

    monkeypatch.setattr(
        storage, "copy_object", _defined_in(storage.__name__, copy_object), raising=False
    )

    assert _unclassified(storage) == ["copy_object"]
    with pytest.raises(AssertionError) as exc_info:
        _assert_all_classified(storage)
    message = str(exc_info.value)
    assert "copy_object" in message
    assert "api/app/storage_blob.py" in message
    assert "_use_azure_blob()" in message
    assert "EXEMPT" in message


@pytest.mark.unit
def test_new_public_context_manager_and_class_are_flagged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Not only coroutines: any public callable defined there must be classified."""

    @asynccontextmanager
    async def stream_range(*, storage_path: str) -> AsyncIterator[bytes]:
        yield b""

    class ObjectStat:
        pass

    def object_url(*, storage_path: str) -> str:
        return storage_path

    monkeypatch.setattr(
        storage, "stream_range", _defined_in(storage.__name__, stream_range), raising=False
    )
    monkeypatch.setattr(
        storage, "ObjectStat", _defined_in(storage.__name__, ObjectStat), raising=False
    )
    monkeypatch.setattr(
        storage, "object_url", _defined_in(storage.__name__, object_url), raising=False
    )

    assert _unclassified(storage) == ["ObjectStat", "object_url", "stream_range"]


@pytest.mark.unit
def test_function_reexported_from_another_app_module_is_flagged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upstream moves the S3 code to ``app/storage_s3.py`` and re-exports it."""

    async def list_objects(*, prefix: str) -> list[str]:
        return []

    monkeypatch.setattr(
        storage, "list_objects", _defined_in("app.storage_s3", list_objects), raising=False
    )

    assert _unclassified(storage) == ["list_objects"]


@pytest.mark.unit
def test_private_constant_and_third_party_names_are_not_flagged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _private_helper() -> None:
        return None

    async def third_party() -> None:
        return None

    monkeypatch.setattr(
        storage, "_private_helper", _defined_in(storage.__name__, _private_helper), raising=False
    )
    monkeypatch.setattr(
        storage, "third_party", _defined_in("botocore.client", third_party), raising=False
    )
    monkeypatch.setattr(storage, "NEW_CONSTANT", 5, raising=False)

    assert _unclassified(storage) == []


@pytest.mark.unit
@pytest.mark.usefixtures("azureblob")
async def test_missing_guard_that_goes_straight_to_s3_is_caught(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``delete_object`` as upstream wrote it: no guard, straight to S3."""

    async def delete_object(*, storage_path: str) -> None:
        async with storage.s3_client() as s3:
            await s3.delete_object(Bucket="b", Key=storage_path)

    monkeypatch.setattr(storage, "delete_object", delete_object)

    with pytest.raises(AssertionError, match=r"delete_object touched S3 \(s3_client\)"):
        await _assert_dispatches_to_blob(
            monkeypatch, storage, "delete_object", DISPATCH["delete_object"]
        )


@pytest.mark.unit
@pytest.mark.usefixtures("azureblob")
async def test_missing_guard_is_caught_even_when_the_s3_error_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``check_storage`` catches every exception, the tripwire's included."""

    async def check_storage() -> bool:
        try:
            async with storage.s3_client() as s3:
                await s3.head_bucket(Bucket="b")
            return True
        except Exception:
            return False

    monkeypatch.setattr(storage, "check_storage", check_storage)

    with pytest.raises(AssertionError, match=r"check_storage touched S3"):
        await _assert_dispatches_to_blob(
            monkeypatch, storage, "check_storage", DISPATCH["check_storage"]
        )


@pytest.mark.unit
@pytest.mark.usefixtures("azureblob")
async def test_missing_guard_on_the_context_manager_is_caught(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def stream_download(*, storage_path: str) -> AsyncIterator[AsyncIterator[bytes]]:
        async with storage.s3_client() as s3:
            yield s3

    monkeypatch.setattr(storage, "stream_download", stream_download)

    with pytest.raises(AssertionError, match=r"stream_download touched S3"):
        await _assert_dispatches_to_blob(
            monkeypatch, storage, "stream_download", DISPATCH["stream_download"]
        )


@pytest.mark.unit
@pytest.mark.usefixtures("azureblob")
async def test_function_that_reaches_neither_backend_is_caught(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Like ``close_storage`` would be without its guard: a silent no-op."""

    async def close_storage() -> None:
        return None

    monkeypatch.setattr(storage, "close_storage", close_storage)

    with pytest.raises(AssertionError, match=r"close_client 0 times"):
        await _assert_dispatches_to_blob(
            monkeypatch, storage, "close_storage", DISPATCH["close_storage"]
        )


@pytest.mark.unit
@pytest.mark.usefixtures("azureblob")
async def test_guard_that_alters_arguments_or_drops_the_result_is_caught(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def upload_bytes(*, storage_path: str, body: bytes, content_type: str) -> None:
        return await storage_blob.upload_bytes(
            storage_path=storage_path, body=body, content_type="application/octet-stream"
        )

    async def presigned_get_url(*, storage_path: str, expires_in_seconds: int) -> str:
        await storage_blob.presigned_get_url(
            storage_path=storage_path, expires_in_seconds=expires_in_seconds
        )
        return "https://elsewhere.invalid/"

    monkeypatch.setattr(storage, "upload_bytes", upload_bytes)
    monkeypatch.setattr(storage, "presigned_get_url", presigned_get_url)

    with pytest.raises(AssertionError, match=r"passed different arguments"):
        await _assert_dispatches_to_blob(
            monkeypatch, storage, "upload_bytes", DISPATCH["upload_bytes"]
        )
    with pytest.raises(AssertionError, match=r"did not return what"):
        await _assert_dispatches_to_blob(
            monkeypatch, storage, "presigned_get_url", DISPATCH["presigned_get_url"]
        )
