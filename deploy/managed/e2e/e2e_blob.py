"""End-to-end check of the running LQ.AI stack with STORAGE_BACKEND=azureblob.

Drives the real API over HTTP (as a client would) and inspects Azurite and
RustFS directly to confirm where bytes land. See README.md in this folder.

Defaults assume it runs inside the api container (which has every library
it needs); override the E2E_* environment variables to run it elsewhere.

Usage: python e2e_blob.py <rustfs-access-key> <rustfs-secret-key>
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import sys
import zipfile

import boto3
import httpx
import pymupdf
from azure.storage.blob import BlobServiceClient

BASE = os.environ.get("E2E_API_BASE", "http://127.0.0.1:8000")
API = f"{BASE}/api/v1"
EMAIL = os.environ.get("E2E_ADMIN_EMAIL", "admin@lq.ai")
PASSWORD = os.environ.get("E2E_ADMIN_PASSWORD", "LQ-AI-smoke-test-Pw1!")
AZURITE = os.environ.get("E2E_AZURITE_CONNECTION_STRING") or (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)
CONTAINER = "lq-ai-files"

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}", flush=True)


def make_pdf() -> bytes:
    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_text(
        (72, 72),
        "MUTUAL NON-DISCLOSURE AGREEMENT\n\n1. Confidential Information means all "
        "non-public information disclosed by either party.\n2. Term: three (3) years.",
        fontsize=11,
    )
    return doc.tobytes()


def blob_names() -> list[str]:
    svc = BlobServiceClient.from_connection_string(AZURITE)
    return [b.name for b in svc.get_container_client(CONTAINER).list_blobs()]


def rustfs_keys(access: str, secret: str) -> list[str] | str:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("E2E_RUSTFS_URL", "http://rustfs:9000"),
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    try:
        resp = s3.list_objects_v2(Bucket="lq-ai-files")
    except Exception as exc:  # bucket may not exist at all
        return f"unavailable: {type(exc).__name__}"
    return [o["Key"] for o in resp.get("Contents", [])]


async def main(rustfs_access: str, rustfs_secret: str) -> int:
    async with httpx.AsyncClient(timeout=120) as http:
        r = await http.get(f"{BASE}/ready")
        check("api readiness (incl. storage)", r.status_code == 200, r.text[:200])

        r = await http.post(f"{API}/auth/login", json={"email": EMAIL, "password": PASSWORD})
        check("login", r.status_code == 200, str(r.status_code))
        token = r.json().get("access_token")
        h = {"Authorization": f"Bearer {token}"}

        pdf = make_pdf()
        r = await http.post(
            f"{API}/files", headers=h, files={"file": ("nda.pdf", pdf, "application/pdf")}
        )
        check("upload PDF", r.status_code == 201, str(r.status_code))
        meta = r.json()
        file_id = meta["id"]
        check(
            "upload sha256 matches",
            meta.get("hash_sha256") == hashlib.sha256(pdf).hexdigest(),
            str(meta.get("hash_sha256"))[:16],
        )

        names = blob_names()
        check("file stored in Azure Blob (Azurite)", file_id in names, f"blobs={names}")
        keys = rustfs_keys(rustfs_access, rustfs_secret)
        check("nothing written to RustFS (S3)", keys == [] or isinstance(keys, str), f"{keys}")

        r = await http.get(f"{API}/files/{file_id}/content", headers=h)
        check(
            "download bytes identical",
            r.status_code == 200 and r.content == pdf,
            str(r.status_code),
        )

        status = None
        for _ in range(90):
            r = await http.get(f"{API}/files/{file_id}", headers=h)
            status = r.json().get("ingestion_status")
            if status in {"ready", "failed", "partial"}:
                break
            await asyncio.sleep(2)
        detail = r.json().get("ingestion_error") or r.json().get("error") or ""
        check(
            "ingest worker read file from Blob and parsed it",
            status in {"ready", "partial"},
            f"status={status} {str(detail)[:160]}",
        )

        r = await http.post(f"{API}/users/me/export", headers=h)
        check("GDPR export requested", r.status_code in {200, 201, 202}, str(r.status_code))
        job_id = r.json().get("job_id")
        url = None
        for _ in range(90):
            r = await http.get(f"{API}/users/me/export/{job_id}", headers=h)
            body = r.json()
            if body.get("status") in {"completed", "failed"}:
                url = body.get("download_url")
                break
            await asyncio.sleep(2)
        check("export completed with SAS download link", bool(url), f"status={body.get('status')}")
        if url:
            check(
                "download link is a read-only SAS",
                "sp=r" in url and "sig=" in url,
                url.split("?")[0],
            )
            # From the host, set E2E_SAS_HOST_REWRITE=http://127.0.0.1:10000 to reach
            # the in-network `azurite` host through its published port.
            rewrite = os.environ.get("E2E_SAS_HOST_REWRITE")
            r = await http.get(url.replace("http://azurite:10000", rewrite) if rewrite else url)
            ok = r.status_code == 200
            inner = []
            if ok:
                inner = zipfile.ZipFile(io.BytesIO(r.content)).namelist()
            check("export ZIP downloadable via SAS", ok, f"{r.status_code} entries={len(inner)}")
            has_pdf = False
            if ok:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                has_pdf = any(z.read(n) == pdf for n in z.namelist() if not n.endswith("/"))
            check("export ZIP contains the uploaded PDF bytes", has_pdf)

        r = await http.delete(f"{API}/files/{file_id}", headers=h)
        check("delete file (soft delete)", r.status_code == 204, str(r.status_code))
        r = await http.get(f"{API}/files/{file_id}", headers=h)
        check("deleted file no longer served", r.status_code == 404, str(r.status_code))

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1], sys.argv[2])))
