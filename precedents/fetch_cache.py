"""Fetch and cache all raw precedent detail API responses.

Pages through the precedent search API to collect all 판례일련번호 values,
then fetches and caches the detail XML for each one concurrently.

Usage (from legalize-pipeline root):
    python -m precedents.fetch_cache                  # Fetch list + all details
    python -m precedents.fetch_cache --skip-list      # Load IDs from precedent_ids.json, skip pagination
    python -m precedents.fetch_cache --limit 10       # Limit for testing
    python -m precedents.fetch_cache --workers 3      # Override concurrent workers (default: 5)
"""

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from core.counter import Counter

from . import cache
from .api_client import NoResultError, get_precedent_detail, search_precedents
from .config import CONCURRENT_WORKERS, PREC_CACHE_DIR

logger = logging.getLogger(__name__)

_IDS_PATH = PREC_CACHE_DIR / "precedent_ids.json"
_KST = timezone(timedelta(hours=9))


def fetch_all_ids() -> list[str]:
    """Page through search API to collect all 판례일련번호 values."""
    all_ids: list[str] = []
    seen: set[str] = set()
    page = 1

    while True:
        result = search_precedents(query="", page=page, display=100, sort="dasc")
        total = result["totalCnt"]

        for prec in result["precedents"]:
            prec_id = prec.get("판례일련번호", "")
            if prec_id and prec_id not in seen:
                seen.add(prec_id)
                all_ids.append(prec_id)

        logger.info(f"Search page {page}: {len(all_ids)}/{total}")

        if page * 100 >= total or not result["precedents"]:
            break
        page += 1

    # Save for future --skip-list runs
    PREC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "collected_at": datetime.now(_KST).isoformat(),
        "total": len(all_ids),
        "ids": all_ids,
    }
    _IDS_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Saved {len(all_ids)} IDs to {_IDS_PATH}")

    return all_ids


def _fetch_detail_task(
    prec_id: str,
    counter: Counter,
    no_result_ids: set[str] | None = None,
) -> None:
    """Fetch a single precedent detail, skipping if already cached or known no-result."""
    if no_result_ids is not None and prec_id in no_result_ids:
        counter.inc("no_result")
        return
    if cache.get_detail(prec_id) is not None:
        counter.inc("cached")
        return
    try:
        get_precedent_detail(prec_id)
        counter.inc("fetched")
    except NoResultError:
        cache.add_no_result_id(prec_id)
        if no_result_ids is not None:
            no_result_ids.add(prec_id)
        counter.inc("no_result")
    except Exception as e:
        logger.error(f"Failed prec_id {prec_id}: {e}")
        counter.inc("errors")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and cache precedent detail responses"
    )
    parser.add_argument("--limit", type=int, help="Limit number of precedents to fetch")
    parser.add_argument(
        "--skip-list",
        action="store_true",
        help="Skip list pagination; load IDs from precedent_ids.json (for resuming)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=CONCURRENT_WORKERS,
        help=f"Number of concurrent workers (default: {CONCURRENT_WORKERS})",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.skip_list:
        if not _IDS_PATH.exists():
            logger.error(f"precedent_ids.json not found at {_IDS_PATH}. Run without --skip-list first.")
            raise SystemExit(1)
        data = json.loads(_IDS_PATH.read_text(encoding="utf-8"))
        all_ids = data["ids"]
        collected_at = data.get("collected_at", "unknown")
        logger.info(f"Loaded {len(all_ids)} IDs from {_IDS_PATH} (collected: {collected_at})")
    else:
        logger.info("Fetching precedent ID list...")
        all_ids = fetch_all_ids()
        logger.info(f"Total precedents found: {len(all_ids)}")

    if args.limit:
        all_ids = all_ids[:args.limit]

    workers = args.workers
    no_result_ids = cache.load_no_result_ids()
    logger.info(
        f"Fetching detail for {len(all_ids)} precedents "
        f"(workers={workers}, known_no_result={len(no_result_ids)})..."
    )

    counter = Counter()
    done = 0
    total = len(all_ids)

    def _snapshot_line(prefix: str) -> str:
        snap = counter.snapshot_all()
        return (
            f"{prefix}: {done}/{total} "
            f"(cached={snap.get('cached', 0)}, fetched={snap.get('fetched', 0)}, "
            f"no_result={snap.get('no_result', 0)}, errors={snap.get('errors', 0)})"
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_detail_task, prec_id, counter, no_result_ids): prec_id
            for prec_id in all_ids
        }
        for future in as_completed(futures):
            future.result()
            done += 1
            if done % 500 == 0:
                logger.info(_snapshot_line("Progress"))

    logger.info(_snapshot_line("Done"))


if __name__ == "__main__":
    main()
