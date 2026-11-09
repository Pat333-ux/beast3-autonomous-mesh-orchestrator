import os
import json
import hmac
import hashlib
import base64
import tarfile
import io
from datetime import datetime, timezone
from typing import Dict, Any, List
import httpx
from fastapi import FastAPI, Request, HTTPException, Header
from google.cloud import firestore

app = FastAPI(title="BEAST 3.0 Autonomous Mesh Orchestrator", version="3.0.0")

# Environment Configuration
GITHUB_TOKEN = os.environ.get("BEAST_SYNC_PAT", "")
WEBHOOK_SECRET = os.environ.get("BEAST_WEBHOOK_SECRET", "")
TARGET_REPOS = json.loads(os.environ.get(
    "BEAST_TARGET_REPOS",
    '["Pat333-ux/beast3-lucricity-digital-constitution-engine"]'
))
GCP_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")

db = firestore.Client(project=GCP_PROJECT) if GCP_PROJECT else None


def verify_github_signature(payload: bytes, signature_header: str) -> bool:
    if not WEBHOOK_SECRET:
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


def build_artifact_bundle(source_repo: str, commit_sha: str, artifacts: Dict[str, Any]) -> str:
    """Packages clean ABIs, governance schemas, and manifest into a base64 tarball."""
    buf = io.BytesIO()
    manifest = {
        "source_repo": source_repo,
        "commit_sha": commit_sha,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "artifact_count": len(artifacts),
    }

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
        info = tarfile.TarInfo(name="manifest.json")
        info.size = len(manifest_bytes)
        tar.addfile(info, io.BytesIO(manifest_bytes))

        for filename, content in artifacts.items():
            raw = json.dumps(content, indent=2).encode("utf-8")
            f_info = tarfile.TarInfo(name=filename)
            f_info.size = len(raw)
            tar.addfile(f_info, io.BytesIO(raw))

    return base64.b64encode(buf.getvalue()).decode("utf-8")


async def dispatch_to_repos(source_repo: str, commit_sha: str, bundle_b64: str) -> List[Dict[str, Any]]:
    """Broadcasts repository_dispatch events to all configured downstream BEAST repos."""
    results = []
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        for repo in TARGET_REPOS:
            url = f"https://api.github.com/repos/{repo}/dispatches"
            body = {
                "event_type": "beast-artifact-sync",
                "client_payload": {
                    "source_repo": source_repo,
                    "commit_sha": commit_sha[:7],
                    "artifact_b64": bundle_b64,
                },
            }
            resp = await client.post(url, headers=headers, json=body)
            results.append({"repo": repo, "status_code": resp.status_code})

    return results


@app.get("/health")
async def health_check():
    return {
        "status": "operational",
        "system": "BEAST 3.0 Autonomous Mesh Orchestrator",
        "targets": TARGET_REPOS,
    }


@app.post("/webhook/github")
async def github_webhook(
    request: Request,
    x_hub_signature_256: str = Header(default=""),
    x_github_event: str = Header(default="push"),
):
    raw_body = await request.body()
    if not verify_github_signature(raw_body, x_hub_signature_256):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    payload = json.loads(raw_body)
    if x_github_event != "push" or payload.get("ref") != "refs/heads/main":
        return {"status": "ignored", "reason": "Non-main push or unsupported event"}

    source_repo = payload["repository"]["full_name"]
    commit_sha = payload["after"]

    # Default deterministic sync payload (extended via /sync/artifacts endpoint)
    bundle_b64 = build_artifact_bundle(
        source_repo,
        commit_sha,
        {"sync_trigger.json": {"head_commit": payload.get("head_commit", {})}},
    )
    dispatch_results = await dispatch_to_repos(source_repo, commit_sha, bundle_b64)

    if db:
        db.collection("beast_sync_ledger").document(commit_sha[:12]).set({
            "source_repo": source_repo,
            "commit_sha": commit_sha,
            "dispatches": dispatch_results,
            "timestamp": firestore.SERVER_TIMESTAMP,
        })

    return {"status": "dispatched", "commit": commit_sha[:7], "results": dispatch_results}


@app.post("/sync/artifacts")
async def sync_custom_artifacts(request: Request):
    """Direct API endpoint for CI pipelines to push compiled ABIs & schemas."""
    data = await request.json()
    source_repo = data.get("source_repo", "Pat333-ux/beast-system-saia-v3")
    commit_sha = data.get("commit_sha", "manual")
    artifacts = data.get("artifacts", {})

    bundle_b64 = build_artifact_bundle(source_repo, commit_sha, artifacts)
    dispatch_results = await dispatch_to_repos(source_repo, commit_sha, bundle_b64)

    return {"status": "synced", "targets": dispatch_results}
