"""Tests for images/download.py — extension detection, cache lookup, checksum verification."""

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import images.config as cfg
from images.download import (
    _cached_path_for,
    _detect_extension,
    _load_checksums,
    _save_checksums,
    verify_checksums,
)

# PNG magic bytes
_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 8
# JPEG magic bytes
_JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 8
# GIF magic bytes
_GIF = b"GIF89a" + b"\x00" * 8


@pytest.fixture(autouse=True)
def patch_cache_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "IMAGE_CACHE_DIR", tmp_path / "images")
    monkeypatch.setattr(cfg, "CHECKSUMS_PATH", tmp_path / "checksums.json")


# ---------------------------------------------------------------------------
# _detect_extension
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("content_type,expected", [
    ("image/png", ".png"),
    ("image/jpeg", ".jpg"),
    ("image/gif", ".gif"),
    ("image/bmp", ".bmp"),
    ("image/webp", ".webp"),
    ("image/tiff", ".tif"),
    ("image/png; charset=utf-8", ".png"),  # with extra params
])
def test_detect_extension_from_content_type(content_type, expected):
    assert _detect_extension(content_type, b"") == expected


@pytest.mark.parametrize("data,expected", [
    (_PNG, ".png"),
    (_JPEG, ".jpg"),
    (_GIF, ".gif"),
    (b"BM" + b"\x00" * 8, ".bmp"),
    (b"II\x2a\x00" + b"\x00" * 8, ".tif"),
    (b"MM\x00\x2a" + b"\x00" * 8, ".tif"),
])
def test_detect_extension_from_magic_bytes(data, expected):
    assert _detect_extension(None, data) == expected


def test_detect_extension_falls_back_to_png_for_unknown():
    assert _detect_extension(None, b"\x00unknown") == ".png"
    assert _detect_extension("application/octet-stream", b"\x00unknown") == ".png"


def test_detect_extension_prefers_content_type_over_magic():
    # Content-Type says PNG, but magic bytes say JPEG → Content-Type wins
    assert _detect_extension("image/png", _JPEG) == ".png"


# ---------------------------------------------------------------------------
# _cached_path_for
# ---------------------------------------------------------------------------

def test_cached_path_for_returns_path_when_exists(tmp_path):
    cache_dir = cfg.IMAGE_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    png_file = cache_dir / "99999.png"
    png_file.write_bytes(b"fake")

    result = _cached_path_for("99999")
    assert result == png_file


def test_cached_path_for_returns_none_when_missing():
    assert _cached_path_for("nonexistent_image_id") is None


def test_cached_path_for_finds_jpg(tmp_path):
    cache_dir = cfg.IMAGE_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "11111.jpg").write_bytes(b"fake jpg")
    assert _cached_path_for("11111") is not None


# ---------------------------------------------------------------------------
# _load_checksums / _save_checksums
# ---------------------------------------------------------------------------

def test_load_checksums_returns_empty_when_missing():
    assert _load_checksums() == {}


def test_save_and_load_checksums():
    data = {"11111": "abc123", "22222": "def456"}
    _save_checksums(data)
    loaded = _load_checksums()
    assert loaded == data


# ---------------------------------------------------------------------------
# verify_checksums
# ---------------------------------------------------------------------------

def test_verify_checksums_no_file():
    mismatches = verify_checksums()
    assert mismatches == []


def test_verify_checksums_all_match(tmp_path):
    cache_dir = cfg.IMAGE_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)

    data = b"image data"
    sha = hashlib.sha256(data).hexdigest()
    (cache_dir / "55555.png").write_bytes(data)
    _save_checksums({"55555": sha})

    assert verify_checksums() == []


def test_verify_checksums_detects_mismatch(tmp_path):
    cache_dir = cfg.IMAGE_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)

    (cache_dir / "66666.png").write_bytes(b"tampered")
    _save_checksums({"66666": "correct_hash_that_wont_match"})

    mismatches = verify_checksums()
    assert "66666" in mismatches


def test_verify_checksums_missing_file_is_mismatch():
    # checksums.json references a file that doesn't exist in cache
    _save_checksums({"77777": "somehash"})
    mismatches = verify_checksums()
    assert "77777" in mismatches
