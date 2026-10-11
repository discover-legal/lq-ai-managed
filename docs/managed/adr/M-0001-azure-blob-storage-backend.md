# M-0001 — Native Azure Blob Storage backend

**Status:** Accepted
**Scope:** managed fork only (`docs/managed/DECISIONS.md` §5, §15)
**Affected:** `api/app/storage.py`, `api/app/storage_blob.py`, `api/app/config.py`, `api/pyproject.toml`

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
  `PayloadTooLarge` at the cap; `InternalError` on backend failure;
  404-on-delete is success; temporary download links capped at 7 days.
* **Upload mapping.** Multipart upload → block blob: `stage_block` per
  `MULTIPART_PART_SIZE` part, `commit_block_list` at the end (empty list =
  zero-byte blob). Nothing is visible under the key until commit. Blob has
  no abort call; uncommitted blocks are garbage-collected by the service.
* **Download links.** Read-only SAS — signed with a user delegation key
  under managed identity, or the account key under a connection string.
* **Auth.** `AZURE_STORAGE_CONNECTION_STRING` when set (Azurite, account
  key); otherwise `DefaultAzureCredential` on `AZURE_STORAGE_ACCOUNT_URL`
  (managed identity, no storage keys). Role: Storage Blob Data
  Contributor on the container.
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

## Verification

* `api/tests/test_storage_blob.py` — 17 unit tests through the public
  `app.storage` API with an in-memory fake.
* `api/tests/test_storage_blob_azurite.py` — round trip against Azurite
  (multi-block upload, download, SAS fetched over HTTP, idempotent delete,
  oversize leaves nothing). Runs when `AZURITE_CONNECTION_STRING` is set.
