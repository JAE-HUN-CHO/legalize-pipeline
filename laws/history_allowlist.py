"""Loader + invariant helper for the known-empty-history allowlist.

The allowlist is a hand-maintained YAML file (``known_empty_history.yaml``)
enumerating cache stems whose empty ``.cache/history/*.json`` files are
intractable for a named reason, each with a tracking issue and expiry
date. The history-cache invariant in ``laws/fetch_cache.py`` uses
:func:`filter_and_check` to decide whether to accept empty entries.

Schema (see also: the YAML file header):
  entries:
    - stem: str                      # Path.stem of .cache/history/*.json
                                     # (what ``cache.list_cached_history_names``
                                     # returns — see laws/cache.py:85-90)
      original_name: str             # human-readable law name
      match_mode: "stem" | "original_name"   # optional, default "stem"
      reason: str                    # free-form classification
      tracking_issue: "owner/repo#N" # regex ^[^/]+/[^/]+#\\d+$
      expires_on: "YYYY-MM-DD"       # ISO date

Public API:
  AllowlistSchemaError      — raised on any schema violation at load time.
  load_allowlist(path=None) — returns {effective_key: entry_dict}.
  filter_and_check(...)     — partitions empty cache stems into
                              (unallowlisted, expired, orphaned).
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).parent / "known_empty_history.yaml"

_TRACKING_ISSUE_RE = re.compile(r"^[^/]+/[^/]+#\d+$")
_LONG_NAME_SUFFIX_RE = re.compile(r"_[0-9a-f]{16}$")
_REQUIRED_FIELDS = ("stem", "original_name", "reason", "tracking_issue", "expires_on")
_ALLOWED_MATCH_MODES = frozenset({"stem", "original_name"})


class AllowlistSchemaError(RuntimeError):
    """Raised when the allowlist YAML is malformed or violates the schema."""


def _validate_entry(entry: object, idx: int) -> dict:
    if not isinstance(entry, dict):
        raise AllowlistSchemaError(f"entries[{idx}] is not a mapping: {entry!r}")
    for field in _REQUIRED_FIELDS:
        value = entry.get(field)
        if not isinstance(value, str) or not value.strip():
            raise AllowlistSchemaError(
                f"entries[{idx}]: field '{field}' is missing or not a non-empty string"
            )
    match_mode = entry.get("match_mode", "stem")
    if match_mode not in _ALLOWED_MATCH_MODES:
        raise AllowlistSchemaError(
            f"entries[{idx}]: match_mode must be one of {sorted(_ALLOWED_MATCH_MODES)}, "
            f"got {match_mode!r}"
        )
    if not _TRACKING_ISSUE_RE.match(entry["tracking_issue"]):
        raise AllowlistSchemaError(
            f"entries[{idx}]: tracking_issue {entry['tracking_issue']!r} "
            f"does not match 'owner/repo#N'"
        )
    try:
        date.fromisoformat(entry["expires_on"])
    except ValueError as e:
        raise AllowlistSchemaError(
            f"entries[{idx}]: expires_on {entry['expires_on']!r} is not a valid ISO date: {e}"
        ) from e
    return {**entry, "match_mode": match_mode}


def load_allowlist(path: Path | None = None) -> dict[str, dict]:
    """Return ``{effective_key: entry_dict}`` from the allowlist YAML.

    ``effective_key`` is ``entry['stem']`` when ``match_mode == 'stem'`` and
    ``entry['original_name']`` when ``match_mode == 'original_name'``. This
    keeps callers from having to re-derive the key.

    A missing YAML file returns ``{}`` (fail-safe: no allowlisting → strict
    invariant). Any schema violation raises :class:`AllowlistSchemaError`.
    """
    resolved = path if path is not None else _DEFAULT_PATH
    if not resolved.exists():
        return {}

    try:
        raw = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise AllowlistSchemaError(f"failed to parse {resolved}: {e}") from e

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise AllowlistSchemaError(f"{resolved}: top-level must be a mapping")

    entries = raw.get("entries")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        raise AllowlistSchemaError(f"{resolved}: 'entries' must be a list")

    result: dict[str, dict] = {}
    for idx, entry in enumerate(entries):
        validated = _validate_entry(entry, idx)
        key = validated["stem"] if validated["match_mode"] == "stem" else validated["original_name"]
        if key in result:
            raise AllowlistSchemaError(
                f"duplicate allowlist key {key!r} "
                f"(match_mode={validated['match_mode']}) at entries[{idx}]"
            )
        result[key] = validated
    return result


def _long_name_hint(stem: str) -> str | None:
    if _LONG_NAME_SUFFIX_RE.search(stem):
        return "long-name law; cross-reference via search_laws"
    return None


def filter_and_check(
    empty_stems: list[str],
    all_cached_stems: list[str],
    today: date | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Partition empty cache stems against the allowlist.

    Returns ``(unallowlisted, expired, orphaned)``:

    - ``unallowlisted``: ``[{stem, original_name_hint}]`` — empty cache
      entries that are not in the allowlist. ``original_name_hint`` is
      ``None`` unless the stem matches the ``_<16hex>`` hash-truncation
      suffix (see ``cache._safe_filename`` in laws/cache.py:23-33), in
      which case it is ``"long-name law; cross-reference via search_laws"``.
    - ``expired``: ``[{stem, original_name, tracking_issue, expires_on}]``
      — allowlist entries whose ``expires_on`` is on or before ``today``.
    - ``orphaned``: ``[{stem, original_name, tracking_issue}]`` —
      allowlist entries whose stem is no longer present in the cache
      directory. Informational only; the invariant does not fail on these.

    The invariant PASSES iff ``unallowlisted`` and ``expired`` are both empty.
    """
    allowlist = load_allowlist()
    today_ = today if today is not None else date.today()
    cached_set = set(all_cached_stems)

    unallowlisted: list[dict] = []
    expired: list[dict] = []

    for stem in empty_stems:
        entry = allowlist.get(stem)
        if entry is None:
            unallowlisted.append({"stem": stem, "original_name_hint": _long_name_hint(stem)})
            continue
        expires = date.fromisoformat(entry["expires_on"])
        if expires <= today_:
            expired.append(
                {
                    "stem": stem,
                    "original_name": entry["original_name"],
                    "tracking_issue": entry["tracking_issue"],
                    "expires_on": entry["expires_on"],
                }
            )

    orphaned: list[dict] = []
    for key, entry in allowlist.items():
        # Orphan detection is keyed off the cache stem; entries using
        # match_mode="original_name" are not eligible for stem-based
        # orphan detection and are skipped here.
        if entry["match_mode"] != "stem":
            continue
        if key not in cached_set:
            orphaned.append(
                {
                    "stem": key,
                    "original_name": entry["original_name"],
                    "tracking_issue": entry["tracking_issue"],
                }
            )

    return unallowlisted, expired, orphaned
