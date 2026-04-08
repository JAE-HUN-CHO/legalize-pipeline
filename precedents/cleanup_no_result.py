"""One-time migration: move no-result precedent XMLs into the negative cache.

Scans ``.cache/precedent/*.xml`` and any file whose root tag is not ``PrecService``
is treated as a no-result response (upstream returned ``<Law>일치하는 판례가
없습니다...</Law>``). The ID is appended to the negative cache file and the
bogus XML is deleted.

Usage (from legalize-pipeline root):
    python -m precedents.cleanup_no_result           # apply
    python -m precedents.cleanup_no_result --dry-run # report only
"""

import argparse
import logging
from xml.etree import ElementTree

from . import cache
from .config import PREC_CACHE_DIR

logger = logging.getLogger(__name__)


def run(dry_run: bool = False) -> dict:
    xml_files = sorted(PREC_CACHE_DIR.glob("*.xml"))
    total = len(xml_files)
    logger.info(f"Scanning {total} cached XML files in {PREC_CACHE_DIR}")

    existing_no_result = cache.load_no_result_ids()
    migrated = 0
    kept = 0
    parse_errors = 0

    for i, path in enumerate(xml_files, 1):
        try:
            root = ElementTree.parse(path).getroot()
        except ElementTree.ParseError as e:
            logger.warning(f"Parse error on {path.name}: {e}")
            parse_errors += 1
            continue

        if root.tag == "PrecService":
            kept += 1
        else:
            prec_id = path.stem
            if dry_run:
                migrated += 1
            else:
                if prec_id not in existing_no_result:
                    cache.add_no_result_id(prec_id)
                    existing_no_result.add(prec_id)
                path.unlink()
                migrated += 1

        if i % 10000 == 0:
            logger.info(f"Progress: {i}/{total} (migrated={migrated}, kept={kept})")

    stats = {
        "total": total,
        "kept": kept,
        "migrated": migrated,
        "parse_errors": parse_errors,
    }
    logger.info(f"Cleanup done ({'DRY-RUN' if dry_run else 'APPLIED'}): {stats}")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate no-result precedent XMLs to negative cache")
    parser.add_argument("--dry-run", action="store_true", help="Report without modifying files")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
