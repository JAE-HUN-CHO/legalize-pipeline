"""Tests for images/manifest.py — ImageEntry dataclass and Manifest I/O."""

import json
from pathlib import Path

import pytest

import images.config as cfg
from images.manifest import ImageEntry, Manifest, load_manifest


@pytest.fixture(autouse=True)
def patch_manifest_path(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "MANIFEST_PATH", tmp_path / "manifest.json")


def _make_entry(**kwargs) -> ImageEntry:
    defaults = dict(
        doc_path="kr/민법/법률.md",
        image_id="12345",
        image_url="https://www.law.go.kr/LSW/flDownload.do?flSeq=12345",
        tag_format="src",
        original_tag='<img src="https://www.law.go.kr/LSW/flDownload.do?flSeq=12345">',
        line_number=10,
    )
    defaults.update(kwargs)
    return ImageEntry(**defaults)


# ---------------------------------------------------------------------------
# ImageEntry
# ---------------------------------------------------------------------------

def test_image_entry_defaults():
    e = _make_entry()
    assert e.status == "extracted"
    assert e.sha256 == ""
    assert e.converted_text == ""
    assert e.priority == 9999
    assert e.image_size is None


def test_image_entry_to_dict_roundtrip():
    e = _make_entry(status="downloaded", sha256="abc123", priority=365)
    d = e.to_dict()
    restored = ImageEntry.from_dict(d)
    assert restored == e


def test_image_entry_from_dict_ignores_unknown_keys():
    d = _make_entry().to_dict()
    d["unknown_field"] = "ignored"
    entry = ImageEntry.from_dict(d)
    assert entry.image_id == "12345"


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def test_manifest_save_and_load(tmp_path):
    e = _make_entry()
    m = Manifest(entries=[e])
    m.save()

    loaded = load_manifest()
    assert len(loaded.entries) == 1
    assert loaded.entries[0].image_id == "12345"


def test_load_manifest_returns_empty_when_missing():
    m = load_manifest()
    assert m.entries == []
    assert m.version == 1


def test_manifest_save_creates_parent_dirs(tmp_path, monkeypatch):
    deep_path = tmp_path / "a" / "b" / "manifest.json"
    monkeypatch.setattr(cfg, "MANIFEST_PATH", deep_path)
    Manifest(entries=[_make_entry()]).save()
    assert deep_path.exists()


def test_manifest_compute_stats():
    entries = [
        _make_entry(image_id="1", status="extracted"),
        _make_entry(image_id="2", status="downloaded"),
        _make_entry(image_id="3", status="downloaded"),
    ]
    m = Manifest(entries=entries)
    stats = m._compute_stats()
    assert stats["total"] == 3
    assert stats["extracted"] == 1
    assert stats["downloaded"] == 2


def test_manifest_entries_by_status():
    entries = [
        _make_entry(image_id="1", status="extracted"),
        _make_entry(image_id="2", status="approved"),
        _make_entry(image_id="3", status="approved"),
    ]
    m = Manifest(entries=entries)
    assert len(m.entries_by_status("approved")) == 2
    assert len(m.entries_by_status("extracted")) == 1
    assert m.entries_by_status("replaced") == []


def test_manifest_unique_image_ids():
    entries = [
        _make_entry(image_id="111", doc_path="kr/a.md", line_number=1),
        _make_entry(image_id="111", doc_path="kr/b.md", line_number=2),
        _make_entry(image_id="222", doc_path="kr/c.md", line_number=3),
    ]
    m = Manifest(entries=entries)
    assert m.unique_image_ids() == {"111", "222"}


def test_manifest_entries_by_image_id():
    entries = [
        _make_entry(image_id="111", doc_path="kr/a.md", line_number=1),
        _make_entry(image_id="111", doc_path="kr/b.md", line_number=2),
        _make_entry(image_id="999", doc_path="kr/c.md", line_number=3),
    ]
    m = Manifest(entries=entries)
    found = m.entries_by_image_id("111")
    assert len(found) == 2


def test_manifest_save_includes_stats():
    entries = [_make_entry(status="downloaded")]
    Manifest(entries=entries).save()

    raw = json.loads(cfg.MANIFEST_PATH.read_text(encoding="utf-8"))
    assert "stats" in raw
    assert raw["stats"]["total"] == 1
    assert raw["stats"]["downloaded"] == 1


def test_manifest_sorted_by_priority():
    entries = [
        _make_entry(image_id="1", priority=100),
        _make_entry(image_id="2", priority=10),
        _make_entry(image_id="3", priority=500),
    ]
    m = Manifest(entries=entries)
    sorted_entries = m.sorted_by_priority()
    assert [e.image_id for e in sorted_entries] == ["2", "1", "3"]
