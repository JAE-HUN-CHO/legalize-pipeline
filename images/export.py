"""Export manifest data to web-viewer JSON format.

Groups entries by image_id and enriches with:
- ext: detected file extension from cache
- refs: list of law documents referencing this image

Supports two output modes:
- Legacy: single images.json file (default)
- Sharded: manifest.json + list-*.json + shard-*.json for scalability
"""

from __future__ import annotations

import json
import logging
import math
import re
from pathlib import Path

from . import config
from .manifest import load_manifest

logger = logging.getLogger(__name__)

GITHUB_LAW_REPO = "https://github.com/legalize-kr/legalize-kr"

_MST_RE = re.compile(r"^법령MST:\s*[\"']?(\d+)[\"']?", re.MULTILINE)
_DATE_RE = re.compile(r"^공포일자:\s*[\"']?(\d{4}-\d{2}-\d{2})[\"']?", re.MULTILINE)
_SOURCE_RE = re.compile(r"^출처:\s*(\S+)", re.MULTILINE)


def _detect_ext(image_id: str) -> str:
    """Detect file extension from cached image file."""
    for candidate in config.IMAGE_CACHE_DIR.glob(f"{image_id}.*"):
        if candidate.is_file():
            return candidate.suffix.lstrip(".")
    return "gif"  # fallback


def _normalize_path(doc_path: str) -> str:
    """Strip workspace-relative prefix (legalize-kr/) from doc_path."""
    return doc_path.removeprefix("legalize-kr/")


def _parse_doc_path(doc_path: str) -> tuple[str, str]:
    """Extract law_name and file_name from doc_path.

    doc_path format: [legalize-kr/]kr/{law_name}/{file_name}.md
    Returns: (law_name, file_name)
    """
    parts = _normalize_path(doc_path).split("/")
    # parts: ["kr", "{law_name}", "{file_name}.md"]
    if len(parts) >= 3 and parts[0] == "kr":
        law_name = parts[1]
        file_name = parts[2].removesuffix(".md")
    elif len(parts) >= 2:
        law_name = parts[-2]
        file_name = parts[-1].removesuffix(".md")
    else:
        law_name = doc_path
        file_name = doc_path
    return law_name, file_name


def _make_github_url(doc_path: str, line_number: int) -> str:
    """Build GitHub URL for a law document at a specific line."""
    path = _normalize_path(doc_path)
    return f"{GITHUB_LAW_REPO}/blob/main/{path}#L{line_number}"


def _read_law_meta(doc_path: str) -> tuple[str, str, str]:
    """Read 법령MST, 공포일자, 출처 from markdown frontmatter.

    Returns: (mst, date_compact, source_url) e.g. ("284415", "20260317", "https://www.law.go.kr/법령/민법")
    Returns ("", "", "") if file not found or fields missing.
    """
    norm = _normalize_path(doc_path)
    md_path = config.KR_DIR.parent / norm
    try:
        header = ""
        with open(md_path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= 20:
                    break
                header += line
    except (OSError, IOError):
        return ("", "", "")

    mst_m = _MST_RE.search(header)
    date_m = _DATE_RE.search(header)
    source_m = _SOURCE_RE.search(header)
    mst = mst_m.group(1) if mst_m else ""
    raw_date = date_m.group(1) if date_m else ""
    date_compact = raw_date.replace("-", "") if raw_date else ""
    source_url = source_m.group(1) if source_m else ""
    return (mst, date_compact, source_url)


_law_meta_cache: dict[str, tuple[str, str, str]] = {}


def _read_law_meta_cached(doc_path: str) -> tuple[str, str, str]:
    if doc_path not in _law_meta_cache:
        _law_meta_cache[doc_path] = _read_law_meta(doc_path)
    return _law_meta_cache[doc_path]


def _make_law_url(law_name: str, mst: str, date_compact: str, source_url: str) -> str:
    """Build versioned law.go.kr URL."""
    if mst and date_compact:
        return f"https://www.law.go.kr/LSW/lsInfoP.do?lsiSeq={mst}&ancYd={date_compact}"
    if source_url:
        return source_url
    return f"https://www.law.go.kr/법령/{law_name}"


def export_images(output: Path, include_statuses: set[str] | None = None) -> int:
    """Export manifest to web-viewer JSON.

    Args:
        output: Output file path for images.json
        include_statuses: Set of statuses to include. Defaults to all except error/not_found.

    Returns:
        Number of unique image_ids exported.
    """
    if include_statuses is None:
        include_statuses = {"extracted", "downloaded", "approved", "replaced", "skipped"}

    manifest = load_manifest()
    logger.info(f"Loaded manifest: {len(manifest.entries)} entries")

    # Group by image_id
    groups: dict[str, dict] = {}
    for entry in manifest.entries:
        if entry.status not in include_statuses:
            continue

        iid = entry.image_id
        if iid not in groups:
            law_name, file_name = _parse_doc_path(entry.doc_path)
            groups[iid] = {
                "image_id": iid,
                "ext": _detect_ext(iid),
                "sha256": entry.sha256,
                "converted_text": entry.converted_text,
                "status": entry.status,
                "refs": [],
            }

        law_name, file_name = _parse_doc_path(entry.doc_path)
        mst, date_compact, source_url = _read_law_meta_cached(entry.doc_path)
        groups[iid]["refs"].append({
            "doc_path": entry.doc_path,
            "line_number": entry.line_number,
            "law_name": law_name,
            "file_name": file_name,
            "github_url": _make_github_url(entry.doc_path, entry.line_number),
            "law_url": _make_law_url(law_name, mst, date_compact, source_url),
        })

    result = list(groups.values())
    # Sort by image_id for stable output
    result.sort(key=lambda x: x["image_id"])

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Exported {len(result)} images to {output}")
    return len(result)


# ---------------------------------------------------------------------------
# Sharded export for scalability (150K+ images)
# ---------------------------------------------------------------------------

NUM_SHARDS = 300
LIST_PAGE_SIZE = 1000


def _shard_number(image_id: str) -> int:
    """Deterministic shard assignment: int(image_id) % NUM_SHARDS."""
    return int(image_id) % NUM_SHARDS


def _write_json(path: Path, data: object) -> None:
    """Write JSON with consistent formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def export_sharded(output_dir: Path, include_statuses: set[str] | None = None) -> int:
    """Export manifest to sharded JSON files for web viewer.

    Produces:
        manifest.json   — { total, per_page, pages, shard_count }
        list-NNNN.json  — paginated lightweight entries (1000/page, id desc)
        shard-NNNN.json — full detail data grouped by int(image_id) % 300

    Args:
        output_dir: Directory to write sharded files into.
        include_statuses: Set of statuses to include.

    Returns:
        Number of unique image_ids exported.
    """
    if include_statuses is None:
        include_statuses = {"extracted", "downloaded", "approved", "replaced", "skipped"}

    manifest = load_manifest()
    logger.info(f"Loaded manifest: {len(manifest.entries)} entries")

    # Group by image_id (same logic as export_images)
    groups: dict[str, dict] = {}
    for entry in manifest.entries:
        if entry.status not in include_statuses:
            continue

        iid = entry.image_id
        if iid not in groups:
            law_name, file_name = _parse_doc_path(entry.doc_path)
            groups[iid] = {
                "image_id": iid,
                "ext": _detect_ext(iid),
                "sha256": entry.sha256,
                "converted_text": entry.converted_text,
                "status": entry.status,
                "refs": [],
            }

        law_name, file_name = _parse_doc_path(entry.doc_path)
        mst, date_compact, source_url = _read_law_meta_cached(entry.doc_path)
        groups[iid]["refs"].append({
            "doc_path": entry.doc_path,
            "line_number": entry.line_number,
            "law_name": law_name,
            "file_name": file_name,
            "github_url": _make_github_url(entry.doc_path, entry.line_number),
            "law_url": _make_law_url(law_name, mst, date_compact, source_url),
        })

    all_images = list(groups.values())
    total = len(all_images)
    logger.info(f"Grouped {total} unique images")

    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Detail shards: shard-NNNN.json ---
    detail_shards: dict[int, list[dict]] = {}
    for img in all_images:
        sn = _shard_number(img["image_id"])
        detail_shards.setdefault(sn, []).append(img)

    for sn, entries in detail_shards.items():
        entries.sort(key=lambda x: x["image_id"])
        _write_json(output_dir / f"shard-{sn:04d}.json", entries)

    populated_shards = len(detail_shards)
    logger.info(f"Wrote {populated_shards} detail shards (of {NUM_SHARDS} slots)")

    # --- List shards: list-NNNN.json (sorted by image_id desc, 1000/page) ---
    all_images.sort(key=lambda x: x["image_id"], reverse=True)
    num_pages = max(1, math.ceil(total / LIST_PAGE_SIZE))

    for page_num in range(num_pages):
        start = page_num * LIST_PAGE_SIZE
        end = start + LIST_PAGE_SIZE
        page_entries = [
            {
                "id": img["image_id"],
                "ext": img["ext"],
                "status": img["status"],
                "law": img["refs"][0]["law_name"] if img["refs"] else "",
            }
            for img in all_images[start:end]
        ]
        _write_json(output_dir / f"list-{page_num + 1:04d}.json", page_entries)

    logger.info(f"Wrote {num_pages} list shards ({LIST_PAGE_SIZE} entries/page)")

    # --- Manifest: manifest.json ---
    manifest_data = {
        "total": total,
        "per_page": LIST_PAGE_SIZE,
        "pages": num_pages,
        "shard_count": NUM_SHARDS,
    }
    _write_json(output_dir / "manifest.json", manifest_data)
    logger.info(f"Wrote manifest.json: {manifest_data}")

    # --- Law names index: law_names.json ---
    law_names = sorted({
        ref["law_name"]
        for img in all_images
        for ref in img["refs"]
    })
    _write_json(output_dir / "law_names.json", law_names)
    logger.info(f"Wrote law_names.json: {len(law_names)} unique law names")

    return total
