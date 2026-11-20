# M-0001 — Native Azure Blob Storage backend

**Status:** Accepted
**Scope:** managed fork only (`docs/managed/DECISIONS.md` §5, §15)
**Affected:** `api/app/storage.py`, `api/app/storage_blob.py`, `api/app/config.py`, `api/app/main.py`, `api/app/workers/arq_setup.py`, `api/app/workers/document_pipeline.py`, `api/pyproject.toml`, `.env.example`

Managed-fork records use the `M-` prefix under `docs/managed/adr/` so they
never collide with upstream's numbered ADRs on merge.

## Context

Upstream stores uploaded files through the S3 API (`aioboto3`) against
RustFS or any S3-compatible service (ADR 0005, ADR 0036). The managed
service runs on Azure, whose Blob Storage does not speak S3. Blob gives
per-account customer-managed keys (crypto-shred on offboarding),
immutability/legal hold, soft delete, versioning, lifecycle rules and
private networking — none of which a RustFS container provides.

## Decision

Add a second backend, selected by `STORAGE_BACKEND=azureblob`
(default `s3` — upstream behaviour unchanged).

* **Hook, not rewrite.** `app/storage.py` keeps its public functions and
  its S3 code. Each public function starts with a short guard that
  delegates to `app/storage_blob.py` when the Blob backend is selected
  (lazy import). Callers, and the many tests that patch
  `app.storage.s3_client` / `app.storage.upload_bytes`, are untouched.
  This keeps upstream merges into `storage.py` cheap (decision 15b).
* **Same contracts.** SHA-256 and byte count over the stream;
  `PayloadTooLarge` at the cap; 404-on-delete is success; temporary
  download links capped at 7 days.
* **One error type.** Every backend failure — upload, download, delete,
  single-shot upload, link signing, container bootstrap — surfaces as
  `InternalError` chained from the SDK error, after one structured log
  line with an `event` key (`_backend_errors` in `storage_blob.py`). This
  is stricter than the S3 path, where `delete_object`, `upload_bytes` and
  `presigned_get_url` raise the raw botocore error; no caller depends on
  the type (all catch `Exception` or let it propagate). Errors raised
  while a caller iterates a download stream are not translated, as on S3.
* **Upload mapping.** Multipart upload → block blob: `stage_block` per
  `MULTIPART_PART_SIZE` part, `commit_block_list` at the end (empty list =
  zero-byte blob). Nothing is visible under the key until commit. Blob has
  no abort call; uncommitted blocks are garbage-collected by the service.
* **Auth: exactly one mode, checked at startup.** Either
  `AZURE_STORAGE_CONNECTION_STRING` (Azurite, account key) or
  `AZURE_STORAGE_ACCOUNT_URL` (`DefaultAzureCredential` — managed
  identity, no storage keys). A validator on `Settings` refuses
  `STORAGE_BACKEND=azureblob` with neither, and with both: silently
  preferring one would leave the operator wrong about whether keys are in
  use. It raises `RuntimeError` rather than a pydantic `ValidationError`,
  which would print every setting, the account key included. Role for
  managed identity: Storage Blob Data Contributor assigned at
  **storage-account scope** (or above). A container-scoped assignment is
  not enough: Get User Delegation Key (which signs download links) is an
  account-level operation, and so is creating the container at startup.
* **Download links.** Read-only SAS. The signing mode follows the
  configured auth mode, not the SDK client's internals: account key
  (parsed from the connection string) under a connection string, user
  delegation key under managed identity.
* **One client for the life of the process, guarded.** A single
  `BlobServiceClient` — with its `DefaultAzureCredential` under managed
  identity — serves every call, so connections are pooled and the SDK
  caches and refreshes tokens. Measured against Azurite through a
  connection-counting proxy, 41 storage calls: 41 TCP connections with a
  client per call, 1 with the shared client. The holder (`_SharedClient`)
  keeps these guards: the client is never reused on another event loop or
  after a fork (a fresh one is built; the foreign one is dropped without
  awaiting its close); an authentication failure
  (`ClientAuthenticationError`, detected where the SDK error is first
  caught) discards client and credential so the next call rebuilds both;
  authorization failures (403, missing role) do not; a discarded client
  is closed only after its last in-flight call finishes; and it is closed
  at api and worker shutdown via `app.storage.close_storage()`.
  Connection-string auth never constructs a credential. There is no age
  cap: token refresh is the SDK's job, and a credential the service stops
  accepting is covered by discard-on-authentication-failure.
* **Retries.** The storage SDK sleeps
  `initial_backoff + increment_base ** n` seconds (± jitter) before retry
  n = 1, 2, 3 — the exponent starts at 1. Its defaults (15, 3) therefore
  sleep 18 + 24 + 42 = 84 s; with the account unreachable (connection
  refused) the error took 85–90 s to surface, against 3.5 s on the S3
  path. `_CLIENT_OPTIONS` keeps three retries with (0, 2): 2 + 4 + 8 =
  14 s nominal, measured 12.9–14.5 s over five runs. (An earlier tuning of
  (1, 2) was written as "1 + 2 + 4" but slept 3 + 5 + 9 = 17 s; measured
  16.5 s.) An authentication error from the credential is retried on the
  same schedule: 14.4 s measured with no identity available. A host that
  drops packets rather than refusing can add up to 4 × the 10 s connection
  timeout; that case has not been measured.
* **Readiness.** `check_storage` makes one attempt — `retry_total=0`
  passed on the call, which the SDK's retry policy honours per operation,
  so it uses the shared client — under a 5 s deadline
  (`READINESS_TIMEOUT_SECONDS`). The 5 s matches the gateway check, the
  only other `/ready` dependency with an explicit bound (`check_db` and
  `check_redis` have none), and is half the Helm probe period (10 s). The
  Helm probe sets no `timeoutSeconds`, so the kubelet's own limit is the
  Kubernetes default of 1 s; the deadline is not lowered to that because a
  check cancelled at 1 s would abandon a slow first token request on
  every poll. Measured, connection refused: 0.01 s, one request. A failed
  check logs one WARNING line (exception type and first line of the
  message); the full error is logged at DEBUG.
* **SDK logging.** Unless the backend's logger is at DEBUG, the SDK's
  per-request URL/header log (INFO) and `azure-identity`'s per-failure
  credential-chain dump (WARNING) are turned down when the client is
  built; the backend's own one-line records replace them.
* **Dependencies.** `azure-storage-blob` + `azure-identity` (+ `azure-core`,
  `msal`, `msal-extensions`, `isodate`). The async client uses `aiohttp`,
  already locked via `aiobotocore`. `azure-identity` is reused by the
  planned keyless Azure OpenAI auth in the gateway.

## Consequences

* Upstream changes to `storage.py` merge with at most a conflict in the
  guard lines; new upstream storage functions must get a Blob counterpart
  (tests in `api/tests/test_storage_blob.py` cover the full surface).
* Upstream object-store data migrations (ADR 0037, e.g. MinIO → RustFS)
  are S3-only and must be skipped or ported for Blob deployments.
* `delete_object` removes the blob and its snapshots; with soft delete
  enabled the copy stays recoverable for the retention window — covered by
  crypto-shred at offboarding.
* `docker-compose.yml` and the Helm chart do not pass the new settings
  yet; Azure deployments (Terraform) set them directly.

## Known differences from the S3 path

Pinned by paired tests in `api/tests/test_storage_parity.py`:

* Backend failures in `delete_object`, `upload_bytes`, `presigned_get_url`
  and `ensure_bucket`: S3 re-raises the raw client error; Blob raises
  `InternalError`.
* Link expiry over 7 days: S3 passes it through to the store; Blob clamps
  to 7 days.
* Upload rejected or failed after parts were sent: S3 aborts the
  multipart upload; Blob has no abort and leaves uncommitted blocks for
  the service to collect. Rejected on the first chunk, S3 has already
  opened (and aborts) a multipart upload; Blob has made no call.
* Empty body: S3 sends one zero-length part; Blob commits an empty block
  list.
* A typed application error other than `PayloadTooLarge` raised by the
  body iterator: S3 wraps it in `InternalError`; Blob lets it through.

## Verification

* `api/tests/test_storage_blob.py` — 38 unit test cases through the public
  `app.storage` API. Data-plane behaviour runs against an in-memory fake
  of the SDK surface. Client lifetime, retries, discard rules and logging
  run against the real `BlobServiceClient` and pipeline with only the
  network (a fake transport) and the credential class faked.
* `api/tests/test_config.py` — 5 cases for the auth-mode validator.
* `api/tests/test_storage_dispatch.py` — 43 tests. Every public name in
  `app.storage` must be either routed to a Blob counterpart (and proven
  never to touch S3 under `azureblob`) or exempted with a reason; a new
  upstream storage function with no guard fails this suite.
* `api/tests/test_storage_parity.py` — 132 tests running the same contract
  against both backends' in-memory fakes, because the upload loop is
  deliberately duplicated rather than shared (the S3 code stays
  upstream's). It also pins the known differences between the backends,
  listed below. It compares code paths against fakes, not the two
  services.
* `api/tests/test_storage_blob_azurite.py` — 4 tests against Azurite
  (multi-block upload, download, SAS fetched over HTTP, idempotent delete,
  oversize leaves nothing, calls share one client). Runs when
  `AZURITE_CONNECTION_STRING` is set; the api job in
  `.github/workflows/ci.yml` sets it against a pinned Azurite service
  (that job has not yet run on GitHub with this change).
* Run by hand with the real `DefaultAzureCredential` and no identity
  available: readiness answers False in under 0.3 s, logs one line,
  discards and closes the client, and leaves no unclosed sessions.
* **Not verified:** managed identity has never been run against a real
  Azure account. Token acquisition, user-delegation signing and the role
  requirements above rest on the SDK's documented behaviour and on tests
  with a fake credential.
