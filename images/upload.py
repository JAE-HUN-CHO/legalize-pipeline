"""Upload cached images to Cloudflare R2 (S3-compatible API).

Reads credentials from environment variables:
  R2_ACCOUNT_ID        Cloudflare account ID
  R2_ACCESS_KEY_ID     R2 API token access key
  R2_SECRET_ACCESS_KEY R2 API token secret key
  R2_BUCKET            Bucket name (e.g. legalize-images)
"""

from __future__ import annotations

import logging
import mimetypes
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from . import config
from .config import LAW_IMAGE_PREFIX
from .manifest import load_manifest

logger = logging.getLogger(__name__)


def _get_r2_client():
    """Create boto3 S3 client configured for Cloudflare R2."""
    try:
        import boto3
    except ImportError:
        raise SystemExit("boto3 not installed. Run: pip install boto3")

    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not all([account_id, access_key, secret_key]):
        raise SystemExit(
            "Missing R2 credentials. Set environment variables:\n"
            "  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY"
        )

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )


def _get_remote_etags(client, bucket: str) -> dict[str, str]:
    """List all objects in bucket and return {key: etag} mapping."""
    etags: dict[str, str] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            etags[obj["Key"]] = obj["ETag"].strip('"')
    return etags


def _upload_file(
    client,
    bucket: str,
    local_path: Path,
    key: str,
    remote_etags: dict[str, str],
    dry_run: bool,
) -> str:
    """Upload a single file if not already present. Returns status string."""
    if key in remote_etags:
        return "skipped"

    if dry_run:
        return "would-upload"

    mime, _ = mimetypes.guess_type(str(local_path))
    extra = {"ContentType": mime or "image/gif"}
    client.upload_file(str(local_path), bucket, key, ExtraArgs=extra)
    return "uploaded"


def upload_images(
    workers: int = 5,
    limit: int | None = None,
    dry_run: bool = False,
    only_approved: bool = False,
) -> dict[str, int]:
    """Upload all cached images to R2.

    Args:
        workers: Number of concurrent upload threads.
        limit: Max number of files to upload (for testing).
        dry_run: Print actions without uploading.
        only_approved: Only upload images with status approved/replaced.

    Returns:
        Counts: {"uploaded": N, "skipped": N, "error": N}
    """
    bucket = os.environ.get("R2_BUCKET")
    if not bucket:
        raise SystemExit("R2_BUCKET environment variable not set")

    client = _get_r2_client()

    # Collect local image files
    image_files = sorted(config.IMAGE_CACHE_DIR.glob("*.*"))
    image_files = [f for f in image_files if f.is_file()]

    if only_approved:
        manifest = load_manifest()
        approved_ids = {
            e.image_id
            for e in manifest.entries
            if e.status in ("approved", "replaced")
        }
        image_files = [f for f in image_files if f.stem in approved_ids]

    if limit:
        image_files = image_files[:limit]

    logger.info(f"Fetching remote object list from R2 bucket '{bucket}'...")
    remote_etags = _get_remote_etags(client, bucket)
    logger.info(f"Remote: {len(remote_etags)} objects. Local: {len(image_files)} files.")

    counts: dict[str, int] = {"uploaded": 0, "skipped": 0, "would-upload": 0, "error": 0}

    def _task(path: Path) -> tuple[str, str]:
        try:
            status = _upload_file(client, bucket, path, f"{LAW_IMAGE_PREFIX}/{path.name}", remote_etags, dry_run)
            return path.name, status
        except Exception as e:
            logger.error(f"Upload failed for {path.name}: {e}")
            return path.name, "error"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_task, f): f for f in image_files}
        done = 0
        for future in as_completed(futures):
            name, status = future.result()
            counts[status] = counts.get(status, 0) + 1
            done += 1
            if done % 100 == 0 or done == len(image_files):
                logger.info(
                    f"Progress: {done}/{len(image_files)} — "
                    f"uploaded={counts.get('uploaded',0)} "
                    f"skipped={counts.get('skipped',0)} "
                    f"errors={counts.get('error',0)}"
                )

    return counts
