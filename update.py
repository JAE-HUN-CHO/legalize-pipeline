"""Incremental updater for new/amended laws.

Uses search API to find recently changed laws, then fetches full history
for each to catch intermediate amendments.

Usage:
    python update.py                    # Update recent laws (default 7 days)
    python update.py --days 30          # Look back 30 days
    python update.py --law-type 법률    # Only 법률
    python update.py --dry-run          # Preview only
"""

import argparse
import logging
from datetime import datetime, timedelta

from api_client import search_laws
from checkpoint import get_last_update, set_last_update
from config import LAW_API_KEY
from converter import format_date, reset_path_registry
from import_laws import import_law_with_history

logger = logging.getLogger(__name__)


def update(days: int = 7, law_type_filter: str | None = None, dry_run: bool = False) -> int:
    """Query API for recently amended laws, then import full history for each."""
    if not LAW_API_KEY:
        logger.error("No API key (LAW_OC) configured. Cannot update.")
        return 0

    reset_path_registry()

    last = get_last_update()
    since = last.replace("-", "") if last else (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")

    logger.info(f"Searching amendments from {since} to {today}")

    # Collect unique law names from search results
    seen_names: set[str] = set()
    page = 1
    while True:
        result = search_laws(query="", page=page, display=100, date_from=since, date_to=today)
        for law in result["laws"]:
            name = law.get("법령명한글", "")
            if name:
                seen_names.add(name)
        if page * 100 >= result["totalCnt"]:
            break
        page += 1

    logger.info(f"Found {len(seen_names)} unique law names with amendments")

    committed = 0
    errors = 0

    for i, name in enumerate(sorted(seen_names), 1):
        try:
            count = import_law_with_history(name, law_type_filter, dry_run)
            committed += count
        except Exception as e:
            logger.error(f"Failed history import for {name}: {e}")
            errors += 1

        if i % 50 == 0:
            logger.info(f"Progress: {i}/{len(seen_names)} (committed={committed}, errors={errors})")

    if not dry_run and committed > 0:
        set_last_update(format_date(today))

    logger.info(f"Update done: committed={committed}, errors={errors}")
    return committed


def main():
    parser = argparse.ArgumentParser(description="Incremental law updater")
    parser.add_argument("--days", type=int, default=7, help="Look back N days (default: 7)")
    parser.add_argument("--law-type", help="Filter by 법령구분 (e.g., 법률)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    committed = update(days=args.days, law_type_filter=args.law_type, dry_run=args.dry_run)

    if not args.dry_run and committed > 0:
        from generate_metadata import save as save_metadata
        save_metadata()

    logger.info(f"Update complete: {committed} laws committed")


if __name__ == "__main__":
    main()
