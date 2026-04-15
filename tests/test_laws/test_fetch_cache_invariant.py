"""Unit tests for _assert_no_empty_history_cache invariant check.

Covers the four cases described in the ralplan:
  A) one empty [] file -> RuntimeError names the offender
  B) one malformed JSON -> RuntimeError names the offender path
  C) mixed empty + malformed -> single RuntimeError names both
  D) all valid -> no exception
"""

from pathlib import Path

import pytest

import laws.cache as law_cache
import laws.fetch_cache as fetch_cache


@pytest.fixture(autouse=True)
def patch_cache_dir(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(law_cache, "CACHE_DIR", tmp_path / ".cache")


def _write_raw(law_name: str, content: str) -> Path:
    path = law_cache._history_path(law_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_a_single_empty_cache_raises_with_law_name():
    law_cache.put_history("민법", [{"법령일련번호": "1", "법령명한글": "민법"}])
    _write_raw("주택법", "[]")

    with pytest.raises(RuntimeError) as exc:
        fetch_cache._assert_no_empty_history_cache()

    msg = str(exc.value)
    assert "주택법" in msg
    assert "Empty (1)" in msg


def test_b_single_malformed_cache_raises_with_path():
    law_cache.put_history("민법", [{"법령일련번호": "1", "법령명한글": "민법"}])
    bad_path = _write_raw("상법", "{malformed json")

    with pytest.raises(RuntimeError) as exc:
        fetch_cache._assert_no_empty_history_cache()

    msg = str(exc.value)
    assert str(bad_path) in msg
    assert "Malformed (1)" in msg


def test_c_mixed_empty_and_malformed_raises_single_error_naming_both():
    law_cache.put_history("민법", [{"법령일련번호": "1", "법령명한글": "민법"}])
    _write_raw("주택법", "[]")
    bad_path = _write_raw("상법", "{malformed json")

    with pytest.raises(RuntimeError) as exc:
        fetch_cache._assert_no_empty_history_cache()

    msg = str(exc.value)
    assert "주택법" in msg
    assert str(bad_path) in msg
    assert "Empty (1)" in msg
    assert "Malformed (1)" in msg


def test_d_all_valid_does_not_raise():
    law_cache.put_history("민법", [{"법령일련번호": "1", "법령명한글": "민법"}])
    law_cache.put_history("상법", [{"법령일련번호": "2", "법령명한글": "상법"}])
    law_cache.put_history("형법", [{"법령일련번호": "3", "법령명한글": "형법"}])

    # Should not raise
    fetch_cache._assert_no_empty_history_cache()
