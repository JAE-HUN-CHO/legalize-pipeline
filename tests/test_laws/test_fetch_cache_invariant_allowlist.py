"""Unit tests for _assert_no_empty_history_cache with allowlist integration.

Cases
-----
G. allowlisted + unexpired -> passes (no exception)
H. allowlisted + expired -> raises with stem, original_name, tracking_issue
I. not allowlisted -> raises; if stem matches _<16hex> adds long-name hint
J. clean cache + allowlist -> passes
J2. main() with malformed YAML -> exits <1s at pre-flight (monkeypatched load_allowlist)
"""

import sys
import time
from pathlib import Path

import pytest
import yaml

import laws.cache as law_cache
import laws.fetch_cache as fetch_cache
import laws.history_allowlist as allowlist_mod
from laws.history_allowlist import AllowlistSchemaError


# ---------------------------------------------------------------------------
# Shared fixture: redirect cache dir to tmp_path
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_cache_dir(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(law_cache, "CACHE_DIR", tmp_path / ".cache")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FUTURE = "2099-01-01"
_PAST   = "2000-01-01"


def _write_allowlist(tmp_path: Path, entries: list[dict]) -> Path:
    p = tmp_path / "allowlist.yaml"
    p.write_text(yaml.dump({"entries": entries}, allow_unicode=True), encoding="utf-8")
    return p


def _entry(
    stem: str,
    original_name: str,
    expires_on: str = _FUTURE,
    tracking_issue: str = "owner/repo#1",
    reason: str = "test",
) -> dict:
    return {
        "stem": stem,
        "original_name": original_name,
        "reason": reason,
        "tracking_issue": tracking_issue,
        "expires_on": expires_on,
    }


def _write_raw_history(law_name: str, content: str) -> None:
    path = law_cache._history_path(law_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# G. allowlisted + unexpired -> passes
# ---------------------------------------------------------------------------

def test_g_allowlisted_unexpired_passes(tmp_path: Path, monkeypatch):
    stem = "테스트법"
    p = _write_allowlist(tmp_path, [_entry(stem=stem, original_name=stem, expires_on=_FUTURE)])
    monkeypatch.setattr(allowlist_mod, "_DEFAULT_PATH", p)

    # valid law present
    law_cache.put_history("민법", [{"법령일련번호": "1"}])
    # allowlisted law has empty history
    _write_raw_history(stem, "[]")

    # should not raise
    fetch_cache._assert_no_empty_history_cache()


# ---------------------------------------------------------------------------
# H. allowlisted + expired -> raises with stem, original_name, tracking_issue
# ---------------------------------------------------------------------------

def test_h_allowlisted_expired_raises(tmp_path: Path, monkeypatch):
    stem = "만료된법"
    original_name = "만료된법 원래이름"
    tracking = "legalize-kr/legalize-pipeline#42"
    p = _write_allowlist(tmp_path, [
        _entry(stem=stem, original_name=original_name, expires_on=_PAST, tracking_issue=tracking)
    ])
    monkeypatch.setattr(allowlist_mod, "_DEFAULT_PATH", p)

    law_cache.put_history("민법", [{"법령일련번호": "1"}])
    _write_raw_history(stem, "[]")

    with pytest.raises(RuntimeError) as exc:
        fetch_cache._assert_no_empty_history_cache()

    msg = str(exc.value)
    assert stem in msg
    assert original_name in msg
    assert tracking in msg


# ---------------------------------------------------------------------------
# I. not allowlisted -> raises; long-name stem gets hint
# ---------------------------------------------------------------------------

def test_i_not_allowlisted_raises(tmp_path: Path, monkeypatch):
    p = _write_allowlist(tmp_path, [])  # empty allowlist
    monkeypatch.setattr(allowlist_mod, "_DEFAULT_PATH", p)

    law_cache.put_history("민법", [{"법령일련번호": "1"}])
    _write_raw_history("알수없는법", "[]")

    with pytest.raises(RuntimeError) as exc:
        fetch_cache._assert_no_empty_history_cache()

    assert "알수없는법" in str(exc.value)


def test_i_long_name_stem_hint_in_error(tmp_path: Path, monkeypatch):
    long_stem = "대한민국법령_abcdef1234567890"
    p = _write_allowlist(tmp_path, [])
    monkeypatch.setattr(allowlist_mod, "_DEFAULT_PATH", p)

    law_cache.put_history("민법", [{"법령일련번호": "1"}])
    _write_raw_history(long_stem, "[]")

    with pytest.raises(RuntimeError) as exc:
        fetch_cache._assert_no_empty_history_cache()

    msg = str(exc.value)
    assert long_stem in msg
    assert "long-name law" in msg
    assert "search_laws" in msg


# ---------------------------------------------------------------------------
# J. clean cache + allowlist -> passes
# ---------------------------------------------------------------------------

def test_j_clean_cache_passes(tmp_path: Path, monkeypatch):
    stem = "허용법"
    p = _write_allowlist(tmp_path, [_entry(stem=stem, original_name=stem)])
    monkeypatch.setattr(allowlist_mod, "_DEFAULT_PATH", p)

    # Only valid (non-empty) history files in cache
    law_cache.put_history("민법", [{"법령일련번호": "1"}])
    law_cache.put_history("상법", [{"법령일련번호": "2"}])

    # should not raise
    fetch_cache._assert_no_empty_history_cache()


# ---------------------------------------------------------------------------
# J2. main() with malformed YAML -> pre-flight raises before crawl
# ---------------------------------------------------------------------------

def test_j2_malformed_yaml_preflight_raises_fast(tmp_path: Path, monkeypatch):
    """Malformed allowlist YAML causes main() to raise at pre-flight, not after fetch."""
    bad_yaml = tmp_path / "bad_allowlist.yaml"
    bad_yaml.write_text("entries: [: bad", encoding="utf-8")

    # Patch _DEFAULT_PATH so load_allowlist() in main()'s pre-flight reads our bad file
    monkeypatch.setattr(allowlist_mod, "_DEFAULT_PATH", bad_yaml)

    # Patch search_laws to ensure it is never called (pre-flight must happen before fetch)
    search_called = []

    def _no_search(**kw):
        search_called.append(True)
        return {"laws": [], "totalCnt": 0}

    monkeypatch.setattr(fetch_cache, "fetch_all_msts", lambda: search_called.append(True) or [])

    # Patch sys.argv so argparse sees no args
    monkeypatch.setattr(sys, "argv", ["fetch_cache"])

    start = time.monotonic()
    with pytest.raises(AllowlistSchemaError):
        fetch_cache.main()
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"Pre-flight took {elapsed:.2f}s, expected <1s"
    assert not search_called, "fetch_all_msts should not have been called before pre-flight"
