"""Download images from law.go.kr in parallel with SHA256 integrity verification.

Usage:
    python -m images.download
    python -m images.download --workers 3
"""

import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from core.atomic_io import atomic_write_bytes, atomic_write_text
from core.counter import Counter
from core.throttle import Throttle

from . import config
from .config import (
    BACKOFF_BASE_SECONDS,
    CONCURRENT_WORKERS,
    IMAGE_DOWNLOAD_URL,
    MAX_RETRIES,
    REQUEST_DELAY_SECONDS,
)
from .manifest import load_manifest

logger = logging.getLogger(__name__)

_CIRCUIT_BREAKER_THRESHOLD = 20
_PROGRESS_INTERVAL = 500
_HEADERS = {"User-Agent": "Mozilla/5.0 (legalize-kr-pipeline)"}

# Content-type → extension mapping
_CONTENT_TYPE_EXT: dict[str, str] = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/webp": ".webp",
    "image/tiff": ".tif",
}

# Magic bytes → extension
_MAGIC_EXT: list[tuple[bytes, str]] = [
    (b"\x89PNG", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"GIF8", ".gif"),
    (b"BM", ".bmp"),
    (b"RIFF", ".webp"),  # webp starts with RIFF
    (b"II\x2a\x00", ".tif"),
    (b"MM\x00\x2a", ".tif"),
]


def _detect_extension(content_type: str | None, data: bytes) -> str:
    """Detect image file extension from content-type header or magic bytes."""
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in _CONTENT_TYPE_EXT:
            return _CONTENT_TYPE_EXT[ct]

    for magic, ext in _MAGIC_EXT:
        if data.startswith(magic):
            return ext

    return ".png"


def _fetch_response(
    url: str,
    throttle: Throttle,
    max_retries: int,
    backoff_base: float,
) -> requests.Response | None:
    """Internal: throttled GET with retry. Returns Response, or None on 404."""
    for attempt in range(max_retries + 1):
        throttle.wait()
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=30)
            if resp.status_code == 404:
                logger.warning(f"404 not found: {url}")
                return None
            if resp.status_code == 429:
                wait = backoff_base * (2 ** attempt)
                logger.warning(f"Rate limited (429). Waiting {wait}s before retry.")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == max_retries:
                raise
            wait = backoff_base * (2 ** attempt)
            logger.warning(f"Request failed: {e}. Retry {attempt + 1}/{max_retries} in {wait}s")
            time.sleep(wait)

    raise RuntimeError(f"Exceeded {max_retries} retries for {url}")


def download_binary(
    url: str,
    throttle: Throttle,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = BACKOFF_BASE_SECONDS,
) -> bytes | None:
    """HTTP GET with retry + exponential backoff. Returns None on 404."""
    resp = _fetch_response(url, throttle, max_retries, backoff_base)
    return resp.content if resp is not None else None


def _load_checksums() -> dict[str, str]:
    if config.CHECKSUMS_PATH.exists():
        return json.loads(config.CHECKSUMS_PATH.read_text(encoding="utf-8"))
    return {}


def _save_checksums(checksums: dict[str, str]) -> None:
    config.CHECKSUMS_PATH.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(config.CHECKSUMS_PATH, json.dumps(checksums, ensure_ascii=False, indent=2))


def _cached_path_for(image_id: str) -> Path | None:
    """Return the cached file path if any extension variant exists."""
    for ext in _CONTENT_TYPE_EXT.values():
        p = config.IMAGE_CACHE_DIR / f"{image_id}{ext}"
        if p.exists():
            return p
    return None


def _download_one(
    image_id: str,
    throttle: Throttle,
    checksums: dict[str, str],
    checksums_lock: "threading.Lock",
    counter: Counter,
    consecutive_failures: list[int],
    failures_lock: "threading.Lock",
) -> tuple[str, str | None]:
    """Download a single image. Returns (image_id, sha256 | 'not_found' | 'error')."""
    url = f"{IMAGE_DOWNLOAD_URL}?flSeq={image_id}"
    try:
        resp = _fetch_response(url, throttle, MAX_RETRIES, BACKOFF_BASE_SECONDS)
    except Exception as e:
        logger.error(f"Failed to download image {image_id}: {e}")
        counter.inc("errors")
        with failures_lock:
            consecutive_failures[0] += 1
        return image_id, "error"

    if resp is None:
        counter.inc("errors")
        with failures_lock:
            consecutive_failures[0] += 1
        return image_id, "not_found"

    data = resp.content
    content_type = resp.headers.get("Content-Type")

    # Detect extension and save
    ext = _detect_extension(content_type, data)
    out_path = config.IMAGE_CACHE_DIR / f"{image_id}{ext}"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(out_path, data)

    sha256 = hashlib.sha256(data).hexdigest()
    with checksums_lock:
        checksums[image_id] = sha256

    counter.inc("fetched")
    with failures_lock:
        consecutive_failures[0] = 0
    return image_id, sha256


def download_images(workers: int = CONCURRENT_WORKERS) -> None:
    """Download all images with status 'extracted' from the manifest."""
    config.IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest()
    extracted = manifest.entries_by_status("extracted")

    # Deduplicate: collect unique image_ids (both src and id-only use flDownload.do)
    all_ids: set[str] = set()
    for entry in extracted:
        all_ids.add(entry.image_id)

    if not all_ids:
        logger.info("No images to download.")
        return

    # Skip already cached
    to_download = [iid for iid in all_ids if _cached_path_for(iid) is None]
    already_cached = len(all_ids) - len(to_download)
    logger.info(
        f"Images: {len(all_ids)} unique extracted, {already_cached} cached, "
        f"{len(to_download)} to download (workers={workers})"
    )

    checksums = _load_checksums()
    checksums_lock = threading.Lock()
    failures_lock = threading.Lock()
    throttle = Throttle(REQUEST_DELAY_SECONDS)
    counter = Counter()
    consecutive_failures = [0]  # mutable int via list

    results: dict[str, str | None] = {}  # image_id → sha256 | "not_found" | "error"

    done = 0
    total = len(to_download)
    circuit_open = False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _download_one,
                iid,
                throttle,
                checksums,
                checksums_lock,
                counter,
                consecutive_failures,
                failures_lock,
            ): iid
            for iid in to_download
        }

        for future in as_completed(futures):
            if circuit_open:
                future.cancel()
                continue

            iid, result = future.result()
            results[iid] = result
            done += 1

            if consecutive_failures[0] >= _CIRCUIT_BREAKER_THRESHOLD:
                logger.error(
                    f"Circuit breaker: {_CIRCUIT_BREAKER_THRESHOLD} consecutive failures. Stopping."
                )
                circuit_open = True

            if done % _PROGRESS_INTERVAL == 0:
                c, f, e = counter.snapshot()
                logger.info(f"Progress: {done}/{total} (fetched={f}, errors={e})")

    c, f, e = counter.snapshot()
    logger.info(f"Download done: fetched={f}, errors={e}, already_cached={already_cached}")

    # Persist updated checksums
    _save_checksums(checksums)

    # Update manifest entries
    for entry in manifest.entries:
        if entry.status != "extracted":
            continue

        iid = entry.image_id
        if iid in results:
            outcome = results[iid]
            if outcome == "not_found":
                entry.status = "not_found"
            elif outcome == "error":
                entry.status = "error"
            else:
                entry.status = "downloaded"
                entry.sha256 = outcome
        elif _cached_path_for(iid) is not None:
            # Was already cached before this run
            entry.status = "downloaded"
            entry.sha256 = checksums.get(iid, "")

    manifest.save()


def verify_checksums() -> list[str]:
    """Re-hash all cached image files and return list of mismatched image_ids."""
    if not config.CHECKSUMS_PATH.exists():
        logger.info("No checksums.json found.")
        return []

    checksums = _load_checksums()
    mismatches: list[str] = []

    for image_id, expected_sha256 in checksums.items():
        path = _cached_path_for(image_id)
        if path is None:
            logger.warning(f"Cached file missing for image_id {image_id}")
            mismatches.append(image_id)
            continue

        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected_sha256:
            logger.error(
                f"Checksum mismatch for {image_id}: expected {expected_sha256}, got {actual}"
            )
            mismatches.append(image_id)

    logger.info(f"Checksum verification: {len(checksums)} checked, {len(mismatches)} mismatched")
    return mismatches


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Download images from law.go.kr")
    parser.add_argument(
        "--workers",
        type=int,
        default=CONCURRENT_WORKERS,
        help=f"Concurrent workers (default: {CONCURRENT_WORKERS})",
    )
    parser.add_argument("--verify", action="store_true", help="Verify checksums only")
    args = parser.parse_args()

    if args.verify:
        mismatches = verify_checksums()
        if mismatches:
            print(f"Mismatches: {mismatches}")
    else:
        download_images(workers=args.workers)
