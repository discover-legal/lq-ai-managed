# End-to-end check: Azure Blob storage backend

Runs the real stack with `STORAGE_BACKEND=azureblob` against Azurite and
drives it over HTTP: readiness, login, PDF upload, the bytes landing in
Blob (and not RustFS), download, worker ingest, GDPR export through a
read-only SAS link, and delete.

## Status: manual, not in CI

Nothing runs this harness automatically. It is run by hand when the Blob
backend or its wiring changes, and this file deliberately records no "last
result": a dated pass says nothing about the current code. Treat the
harness as unverified against your checkout until you have run it yourself;
the script prints one PASS/FAIL line per check and exits non-zero if any
check fails or if it has to abort early (failed login or upload).

What CI does run on every PR is the narrower storage-layer round trip,
`api/tests/test_storage_blob_azurite.py`, against the same pinned Azurite
image (`.github/workflows/ci.yml`, api job). That covers `app.storage`
only — not the HTTP API, the workers or the export flow checked here.

Azurite runs in strict mode (no `--loose`, no `--skipApiVersionCheck`) so
that requests real Azure would reject are not silently accepted.

## Run it

From the repo root, after the README quickstart's `.env` setup:

```bash
docker compose -f docker-compose.yml -f deploy/managed/e2e/compose.blob.yml up -d --build \
  api ingest-worker arq-worker

docker exec -w /app lq-ai-api-1 python -m app.cli reset-admin-password \
  --email admin@lq.ai --password 'LQ-AI-smoke-test-Pw1!' --no-force-change

set -a; . ./.env; set +a
docker cp deploy/managed/e2e/e2e_blob.py lq-ai-api-1:/tmp/e2e_blob.py
docker exec lq-ai-api-1 python /tmp/e2e_blob.py \
  "${S3_ACCESS_KEY:-${OBJECT_STORE_ACCESS_KEY:-lq_ai}}" "${OBJECT_STORE_SECRET_KEY}"
```

The script runs inside the api container because that image already has
every library it needs. To run it from the host instead, install `httpx`,
`boto3`, `azure-storage-blob` and `pymupdf`, and set:

```bash
export E2E_AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
export E2E_RUSTFS_URL=http://127.0.0.1:9000
export E2E_SAS_HOST_REWRITE=http://127.0.0.1:10000
```

## What it does not cover

- The web UI (add a browser login/upload once the `web` image is built).
- Chat and document search: they need a model provider. Embedding fails
  with "no provider" when none is configured; that is expected.
- A real Azure storage account with managed identity (no keys).

## Restricted networks

The images download from GitHub's container registry, GitHub releases,
PyPI, and (for `web`) Alpine, Debian, PyTorch and Hugging Face. On a
network that blocks some of these the build fails partway. Hosts we hit:
`pkg-containers.githubusercontent.com` (the `uv` image), `github.com`
releases (spaCy model), `pypi.org` / `files.pythonhosted.org`,
`dl-cdn.alpinelinux.org`, `deb.debian.org`, `download.pytorch.org`,
`huggingface.co`. A TLS-intercepting proxy also needs its CA trusted inside
the build.
