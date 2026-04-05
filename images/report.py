"""Generate review reports from the image manifest."""

from __future__ import annotations

import fnmatch
import logging
import sys
from collections import Counter
from pathlib import Path

from .manifest import load_manifest

logger = logging.getLogger(__name__)


def generate_report(
    format: str = "tsv",
    status: str | None = None,
    doc_path: str | None = None,
    output: Path | None = None,
) -> None:
    """Generate a report from the manifest.

    Args:
        format: "tsv" for tab-separated values, "stats" for summary statistics.
        status: Filter entries by this status (exact match).
        doc_path: Filter entries by doc_path glob pattern (fnmatch).
        output: Write report to this file; defaults to stdout.
    """
    manifest = load_manifest()
    entries = manifest.entries

    if status is not None:
        entries = [e for e in entries if e.status == status]

    if doc_path is not None:
        entries = [e for e in entries if fnmatch.fnmatch(e.doc_path, doc_path)]

    if format == "tsv":
        lines = ["doc_path\timage_id\tstatus\tsha256\tconverted_text"]
        for e in entries:
            converted = e.converted_text.replace("\t", " ").replace("\n", " ")
            lines.append(f"{e.doc_path}\t{e.image_id}\t{e.status}\t{e.sha256}\t{converted}")
        text = "\n".join(lines) + "\n"
    elif format == "stats":
        status_counts = Counter(e.status for e in entries)
        tag_format_counts = Counter(e.tag_format for e in entries)

        lines = [
            f"Total entries: {len(entries)}",
            f"Unique image IDs: {len({e.image_id for e in entries})}",
            "",
            "By status:",
        ]
        for s, count in sorted(status_counts.items()):
            lines.append(f"  {s}: {count}")
        lines.append("")
        lines.append("By tag_format:")
        for tf, count in sorted(tag_format_counts.items()):
            lines.append(f"  {tf}: {count}")
        text = "\n".join(lines) + "\n"
    else:
        raise ValueError(f"Unknown format: {format!r}. Use 'tsv' or 'stats'.")

    if output is not None:
        output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)

    logger.info(f"Report generated: {len(entries)} entries (format={format})")


def print_stats() -> None:
    """Print a quick summary of the manifest to stdout."""
    manifest = load_manifest()
    entries = manifest.entries
    status_counts = Counter(e.status for e in entries)

    print(f"Total entries:    {len(entries)}")
    print(f"Unique image IDs: {len(manifest.unique_image_ids())}")
    print("By status:")
    for s, count in sorted(status_counts.items()):
        print(f"  {s}: {count}")
