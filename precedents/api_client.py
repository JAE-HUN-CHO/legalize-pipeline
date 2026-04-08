"""Thin wrapper around law.go.kr OpenAPI for precedents."""

import logging
from xml.etree import ElementTree

import requests

from core.http import make_request
from core.throttle import Throttle

from . import cache
from .config import (
    BACKOFF_BASE_SECONDS,
    LAW_API_BASE,
    LAW_API_KEY,
    MAX_RETRIES,
    REQUEST_DELAY_SECONDS,
)

logger = logging.getLogger(__name__)

_throttle = Throttle(REQUEST_DELAY_SECONDS)


def _request(url: str, params: dict) -> requests.Response:
    """Make a throttled request with retry and exponential backoff."""
    return make_request(
        url, params,
        throttle=_throttle,
        api_key=LAW_API_KEY,
        max_retries=MAX_RETRIES,
        backoff_base=BACKOFF_BASE_SECONDS,
    )


def search_precedents(
    query: str = "",
    page: int = 1,
    display: int = 100,
    sort: str = "dasc",
    court: str = "",
    date_range: str = "",
) -> dict:
    """Search precedents via the search API.

    Returns dict with keys: totalCnt, page, precedents (list of precedent metadata dicts).
    """
    params = {
        "target": "prec",
        "type": "XML",
        "query": query,
        "page": str(page),
        "display": str(display),
        "sort": sort,
    }
    if court:
        params["curt"] = court
    if date_range:
        params["prncYd"] = date_range

    resp = _request(f"{LAW_API_BASE}/lawSearch.do", params)
    root = ElementTree.fromstring(resp.content)

    # Check for error response
    result = root.findtext("result")
    if result and "실패" in result:
        raise RuntimeError(f"API error (search page {page}): {result} - {root.findtext('msg', '')}")

    total = root.findtext("totalCnt", "0")
    page_num = root.findtext("page", "1")

    precedents = []
    for item in root.findall(".//prec"):
        precedents.append({
            "판례일련번호": item.findtext("판례일련번호", ""),
            "사건명": item.findtext("사건명", ""),
            "사건번호": item.findtext("사건번호", ""),
            "선고일자": item.findtext("선고일자", ""),
            "선고": item.findtext("선고", ""),
            "법원명": item.findtext("법원명", ""),
            "법원종류코드": item.findtext("법원종류코드", ""),
            "사건종류명": item.findtext("사건종류명", ""),
            "사건종류코드": item.findtext("사건종류코드", ""),
            "판결유형": item.findtext("판결유형", ""),
            "데이터출처명": item.findtext("데이터출처명", ""),
            "판례상세링크": item.findtext("판례상세링크", ""),
        })

    return {"totalCnt": int(total), "page": int(page_num), "precedents": precedents}


class NoResultError(RuntimeError):
    """Raised when the detail API returns a "no matching precedent" response.

    The upstream API returns HTTP 200 with ``<Law>일치하는 판례가 없습니다...</Law>``
    for certain IDs that the search API lists but the detail API cannot resolve.
    These responses must not be cached as valid precedent XML.
    """

    def __init__(self, prec_id: str, message: str) -> None:
        super().__init__(f"No precedent for ID {prec_id}: {message}")
        self.prec_id = prec_id
        self.message = message


def get_precedent_detail(prec_id: str | int) -> bytes:
    """Fetch raw precedent detail XML by ID.

    Checks cache first. Fetches from API if not cached, validates that the
    response root tag is ``PrecService``, stores to cache, and returns raw
    XML bytes. Raises :class:`NoResultError` for the "no matching precedent"
    error response so the caller can record the ID in the negative cache
    without polluting the positive cache.
    """
    prec_id = str(prec_id)

    cached = cache.get_detail(prec_id)
    if cached:
        logger.debug(f"Cache hit: detail prec_id={prec_id}")
        return cached

    params = {
        "target": "prec",
        "ID": prec_id,
        "type": "XML",
    }
    resp = _request(f"{LAW_API_BASE}/lawService.do", params)
    raw = resp.content

    root = ElementTree.fromstring(raw)
    result = root.findtext("result")
    if result and "실패" in result:
        raise RuntimeError(f"API error for prec_id {prec_id}: {result} - {root.findtext('msg', '')}")

    if root.tag != "PrecService":
        # Upstream returns <Law>일치하는 판례가 없습니다...</Law> for some IDs.
        # Treat any non-PrecService root as a no-result response and do not cache.
        raise NoResultError(prec_id, (root.text or "").strip() or f"unexpected root <{root.tag}>")

    cache.put_detail(prec_id, raw)
    return raw
