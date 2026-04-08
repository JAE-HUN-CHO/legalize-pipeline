"""Tests for precedents/cache.py."""

from pathlib import Path

import pytest

import precedents.cache as prec_cache


@pytest.fixture(autouse=True)
def patch_prec_cache_dir(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(prec_cache, "PREC_CACHE_DIR", tmp_path / "precedent")


def test_put_get_detail(tmp_path: Path):
    content = b"<prec><id>123456</id></prec>"
    prec_cache.put_detail("123456", content)
    result = prec_cache.get_detail("123456")
    assert result == content


def test_get_detail_miss():
    result = prec_cache.get_detail("nonexistent_99999")
    assert result is None


def test_list_cached_ids(tmp_path: Path):
    prec_cache.put_detail("111111", b"<a/>")
    prec_cache.put_detail("222222", b"<b/>")
    ids = prec_cache.list_cached_ids()
    assert set(ids) == {"111111", "222222"}


def test_list_cached_ids_empty():
    result = prec_cache.list_cached_ids()
    assert result == []


def test_no_result_ids_load_empty_when_missing():
    assert prec_cache.load_no_result_ids() == set()


def test_no_result_ids_add_and_load():
    prec_cache.add_no_result_id("111")
    prec_cache.add_no_result_id("222")
    prec_cache.add_no_result_id("111")  # duplicate lines are fine; set dedupes
    loaded = prec_cache.load_no_result_ids()
    assert loaded == {"111", "222"}
