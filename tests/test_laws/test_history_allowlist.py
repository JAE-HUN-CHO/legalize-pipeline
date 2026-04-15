"""Unit tests for laws/history_allowlist.py.

Cases
-----
A. missing YAML -> load_allowlist returns {}
B. malformed YAML -> AllowlistSchemaError
C. valid unexpired single entry -> filter_and_check returns ([], [], [])
D. expired entry -> filter_and_check returns ([], [expired], [])
E. unknown stem -> unallowlisted has 1 entry
F. mixed allowed + expired + unknown partition
F2. bad tracking_issue ("not-a-repo-slug") -> AllowlistSchemaError
F3. non-ISO expires_on ("next Tuesday") -> AllowlistSchemaError
F4. missing required field -> AllowlistSchemaError
F5. duplicate stem -> AllowlistSchemaError
P. allowlist entry stem absent from all_cached -> returned in orphaned
Q. match_mode: "original_name" -> loads ok; filter_and_check keys off original_name
R. filter_and_check(["foo_1234567890abcdef"], ...) unallowlisted -> hint says
   "long-name law; cross-reference via search_laws"
"""

from datetime import date
from pathlib import Path

import pytest
import yaml

from laws.history_allowlist import AllowlistSchemaError, filter_and_check, load_allowlist

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FUTURE = "2099-01-01"
_PAST = "2000-01-01"


def _write_yaml(tmp_path: Path, data: object, filename: str = "allowlist.yaml") -> Path:
    p = tmp_path / filename
    p.write_text(yaml.dump(data, allow_unicode=True), encoding="utf-8")
    return p


def _entry(
    stem: str = "테스트법",
    original_name: str = "테스트법",
    expires_on: str = _FUTURE,
    tracking_issue: str = "owner/repo#1",
    reason: str = "test",
    match_mode: str | None = None,
) -> dict:
    e: dict = {
        "stem": stem,
        "original_name": original_name,
        "reason": reason,
        "tracking_issue": tracking_issue,
        "expires_on": expires_on,
    }
    if match_mode is not None:
        e["match_mode"] = match_mode
    return e


# ---------------------------------------------------------------------------
# A. missing YAML
# ---------------------------------------------------------------------------

def test_a_missing_yaml_returns_empty_dict(tmp_path: Path):
    result = load_allowlist(tmp_path / "does_not_exist.yaml")
    assert result == {}


# ---------------------------------------------------------------------------
# B. malformed YAML
# ---------------------------------------------------------------------------

def test_b_malformed_yaml_raises_schema_error(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text("entries: [: bad", encoding="utf-8")
    with pytest.raises(AllowlistSchemaError):
        load_allowlist(p)


# ---------------------------------------------------------------------------
# C. valid unexpired single entry -> ([], [], [])
# ---------------------------------------------------------------------------

def test_c_valid_unexpired_entry_passes(tmp_path: Path, monkeypatch):
    stem = "민법"
    p = _write_yaml(tmp_path, {"entries": [_entry(stem=stem, original_name=stem)]})
    monkeypatch.setattr("laws.history_allowlist._DEFAULT_PATH", p)

    unallowed, expired, orphaned = filter_and_check(
        empty_stems=[stem],
        all_cached_stems=[stem],
        today=date(2025, 1, 1),
    )
    assert unallowed == []
    assert expired == []
    assert orphaned == []


# ---------------------------------------------------------------------------
# D. expired entry -> ([], [expired_entry], [])
# ---------------------------------------------------------------------------

def test_d_expired_entry_appears_in_expired(tmp_path: Path, monkeypatch):
    stem = "구법"
    p = _write_yaml(tmp_path, {"entries": [_entry(stem=stem, original_name=stem, expires_on=_PAST)]})
    monkeypatch.setattr("laws.history_allowlist._DEFAULT_PATH", p)

    unallowed, expired, orphaned = filter_and_check(
        empty_stems=[stem],
        all_cached_stems=[stem],
        today=date(2025, 1, 1),
    )
    assert unallowed == []
    assert len(expired) == 1
    assert expired[0]["stem"] == stem
    assert orphaned == []


# ---------------------------------------------------------------------------
# E. unknown stem -> unallowlisted has 1 entry
# ---------------------------------------------------------------------------

def test_e_unknown_stem_is_unallowlisted(tmp_path: Path, monkeypatch):
    # allowlist is empty
    p = _write_yaml(tmp_path, {"entries": []})
    monkeypatch.setattr("laws.history_allowlist._DEFAULT_PATH", p)

    unallowed, expired, orphaned = filter_and_check(
        empty_stems=["알수없는법"],
        all_cached_stems=["알수없는법"],
        today=date(2025, 1, 1),
    )
    assert len(unallowed) == 1
    assert unallowed[0]["stem"] == "알수없는법"
    assert expired == []
    assert orphaned == []


# ---------------------------------------------------------------------------
# F. mixed: allowed + expired + unknown
# ---------------------------------------------------------------------------

def test_f_mixed_partition(tmp_path: Path, monkeypatch):
    allowed_stem = "허용법"
    expired_stem = "만료법"
    unknown_stem = "미등록법"

    p = _write_yaml(tmp_path, {
        "entries": [
            _entry(stem=allowed_stem, original_name=allowed_stem, expires_on=_FUTURE),
            _entry(stem=expired_stem, original_name=expired_stem, expires_on=_PAST),
        ]
    })
    monkeypatch.setattr("laws.history_allowlist._DEFAULT_PATH", p)

    unallowed, expired, orphaned = filter_and_check(
        empty_stems=[allowed_stem, expired_stem, unknown_stem],
        all_cached_stems=[allowed_stem, expired_stem, unknown_stem],
        today=date(2025, 1, 1),
    )
    assert len(unallowed) == 1
    assert unallowed[0]["stem"] == unknown_stem
    assert len(expired) == 1
    assert expired[0]["stem"] == expired_stem
    assert orphaned == []


# ---------------------------------------------------------------------------
# F2. bad tracking_issue -> AllowlistSchemaError
# ---------------------------------------------------------------------------

def test_f2_bad_tracking_issue_raises(tmp_path: Path):
    p = _write_yaml(tmp_path, {"entries": [_entry(tracking_issue="not-a-repo-slug")]})
    with pytest.raises(AllowlistSchemaError, match="tracking_issue"):
        load_allowlist(p)


# ---------------------------------------------------------------------------
# F3. non-ISO expires_on -> AllowlistSchemaError
# ---------------------------------------------------------------------------

def test_f3_non_iso_expires_on_raises(tmp_path: Path):
    p = _write_yaml(tmp_path, {"entries": [_entry(expires_on="next Tuesday")]})
    with pytest.raises(AllowlistSchemaError, match="expires_on"):
        load_allowlist(p)


# ---------------------------------------------------------------------------
# F4. missing required field -> AllowlistSchemaError
# ---------------------------------------------------------------------------

def test_f4_missing_required_field_raises(tmp_path: Path):
    entry = _entry()
    del entry["tracking_issue"]
    p = _write_yaml(tmp_path, {"entries": [entry]})
    with pytest.raises(AllowlistSchemaError, match="tracking_issue"):
        load_allowlist(p)


# ---------------------------------------------------------------------------
# F5. duplicate stem -> AllowlistSchemaError
# ---------------------------------------------------------------------------

def test_f5_duplicate_stem_raises(tmp_path: Path):
    p = _write_yaml(tmp_path, {
        "entries": [
            _entry(stem="중복법", original_name="중복법"),
            _entry(stem="중복법", original_name="중복법 (2)"),
        ]
    })
    with pytest.raises(AllowlistSchemaError, match="duplicate"):
        load_allowlist(p)


# ---------------------------------------------------------------------------
# P. allowlist entry stem absent from all_cached -> orphaned
# ---------------------------------------------------------------------------

def test_p_stem_absent_from_all_cached_is_orphaned(tmp_path: Path, monkeypatch):
    stem = "사라진법"
    p = _write_yaml(tmp_path, {"entries": [_entry(stem=stem, original_name=stem)]})
    monkeypatch.setattr("laws.history_allowlist._DEFAULT_PATH", p)

    unallowed, expired, orphaned = filter_and_check(
        empty_stems=[],
        all_cached_stems=[],       # stem not present in cache
        today=date(2025, 1, 1),
    )
    assert unallowed == []
    assert expired == []
    assert len(orphaned) == 1
    assert orphaned[0]["stem"] == stem


# ---------------------------------------------------------------------------
# Q. match_mode: "original_name" -> loads ok; filter_and_check keys off original_name
# ---------------------------------------------------------------------------

def test_q_match_mode_original_name(tmp_path: Path, monkeypatch):
    stem = "법_abc1234567890abcd"
    original_name = "진짜이름법"
    p = _write_yaml(tmp_path, {
        "entries": [_entry(stem=stem, original_name=original_name, match_mode="original_name")]
    })
    monkeypatch.setattr("laws.history_allowlist._DEFAULT_PATH", p)

    result = load_allowlist(p)
    # key is original_name, not stem
    assert original_name in result
    assert stem not in result

    # filter_and_check: passing original_name as empty stem hits the allowlist
    unallowed, expired, orphaned = filter_and_check(
        empty_stems=[original_name],
        all_cached_stems=[original_name],
        today=date(2025, 1, 1),
    )
    assert unallowed == []
    assert expired == []
    # match_mode="original_name" entries are skipped for orphan detection
    assert orphaned == []


# ---------------------------------------------------------------------------
# R. long-name stem (_<16hex>) hint in unallowlisted
# ---------------------------------------------------------------------------

def test_r_long_name_stem_hint_in_unallowlisted(tmp_path: Path, monkeypatch):
    long_stem = "foo_1234567890abcdef"
    p = _write_yaml(tmp_path, {"entries": []})
    monkeypatch.setattr("laws.history_allowlist._DEFAULT_PATH", p)

    unallowed, expired, orphaned = filter_and_check(
        empty_stems=[long_stem],
        all_cached_stems=[long_stem],
        today=date(2025, 1, 1),
    )
    assert len(unallowed) == 1
    assert unallowed[0]["stem"] == long_stem
    hint = unallowed[0]["original_name_hint"]
    assert hint is not None
    assert "long-name law" in hint
    assert "search_laws" in hint
