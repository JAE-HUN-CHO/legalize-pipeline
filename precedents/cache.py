"""File-based cache for raw precedent detail API responses."""

import logging
import threading

from core.atomic_io import atomic_write_bytes

from .config import PREC_CACHE_DIR

logger = logging.getLogger(__name__)

_NO_RESULT_FILENAME = "_no_result_ids.txt"
_no_result_lock = threading.Lock()


def get_detail(prec_id: str) -> bytes | None:
    path = PREC_CACHE_DIR / f"{prec_id}.xml"
    if path.exists():
        return path.read_bytes()
    return None


def put_detail(prec_id: str, content: bytes) -> None:
    path = PREC_CACHE_DIR / f"{prec_id}.xml"
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(path, content)


def list_cached_ids() -> list[str]:
    """List all precedent IDs that have cached detail XML."""
    if not PREC_CACHE_DIR.exists():
        return []
    return [p.stem for p in PREC_CACHE_DIR.glob("*.xml")]


def _no_result_path():
    return PREC_CACHE_DIR / _NO_RESULT_FILENAME


def load_no_result_ids() -> set[str]:
    """Load the set of precedent IDs known to return no-result from the detail API.

    These are IDs the search API lists but the detail API cannot resolve (upstream
    returns ``<Law>일치하는 판례가 없습니다...</Law>``). Keeping them in a negative
    cache avoids re-requesting deterministically bad IDs on every run.
    """
    path = _no_result_path()
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def add_no_result_id(prec_id: str) -> None:
    """Append a precedent ID to the negative cache file (thread-safe)."""
    path = _no_result_path()
    with _no_result_lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{prec_id}\n")
