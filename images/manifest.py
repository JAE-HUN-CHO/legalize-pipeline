"""Manifest read/write/query for image tracking.

The manifest is a JSON file tracking all (document, image_id) pairs
through the pipeline stages: extracted → downloaded → approved → replaced.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path

from core.atomic_io import atomic_write_text

from . import config

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

VALID_STATUSES = frozenset({
    "extracted",
    "downloaded",
    "approved",
    "replaced",
    "skipped",
    "not_found",
    "error",
})


@dataclass
class ImageEntry:
    """A single image reference in a law document."""

    doc_path: str
    image_id: str
    image_url: str
    tag_format: str  # "src" | "id-only"
    original_tag: str
    line_number: int
    status: str = "extracted"
    sha256: str = ""
    image_size: list[int] | None = None  # [width, height]
    converted_text: str = ""
    priority: int = 9999

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> ImageEntry:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class Manifest:
    """Central tracking manifest for the images pipeline."""

    version: int = 1
    updated_at: str = ""
    entries: list[ImageEntry] = field(default_factory=list)

    def save(self, path: Path | None = None) -> None:
        path = path or config.MANIFEST_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        self.updated_at = datetime.now(KST).isoformat()
        data = {
            "version": self.version,
            "updated_at": self.updated_at,
            "stats": self._compute_stats(),
            "entries": [e.to_dict() for e in self.entries],
        }
        atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2))
        logger.info(f"Manifest saved: {len(self.entries)} entries → {path}")

    def _compute_stats(self) -> dict[str, int]:
        stats: dict[str, int] = {"total": len(self.entries)}
        for entry in self.entries:
            stats[entry.status] = stats.get(entry.status, 0) + 1
        return stats

    def unique_image_ids(self) -> set[str]:
        return {e.image_id for e in self.entries}

    def entries_by_status(self, status: str) -> list[ImageEntry]:
        return [e for e in self.entries if e.status == status]

    def entries_by_image_id(self, image_id: str) -> list[ImageEntry]:
        return [e for e in self.entries if e.image_id == image_id]

    def sorted_by_priority(self, status: str | None = None) -> list[ImageEntry]:
        entries = self.entries_by_status(status) if status else self.entries
        return sorted(entries, key=lambda e: e.priority)


def load_manifest(path: Path | None = None) -> Manifest:
    path = path or config.MANIFEST_PATH
    if not path.exists():
        logger.info("No manifest found, returning empty manifest")
        return Manifest()

    data = json.loads(path.read_text(encoding="utf-8"))
    entries = [ImageEntry.from_dict(e) for e in data.get("entries", [])]
    return Manifest(
        version=data.get("version", 1),
        updated_at=data.get("updated_at", ""),
        entries=entries,
    )
