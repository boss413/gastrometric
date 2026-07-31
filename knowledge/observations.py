"""Idempotent helpers for the culinary_observations table.

Observations preserve provenance: every distinct piece of source text
becomes exactly one row, with the original wording untouched. Normalization
here is intentionally shallow (whitespace/comma spacing only) — it never
rewrites wording, corrects spelling, or changes case in the stored text.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Optional, Tuple

_WHITESPACE_RE = re.compile(r"\s+")
_COMMA_SPACING_RE = re.compile(r"\s*,\s*")


def normalize_modifier_text(raw_text: str) -> str:
    """Trim, collapse whitespace, and normalize comma spacing only.

    Case and wording are left exactly as-is; this is not a rewrite.
    """
    text = raw_text.strip()
    text = _WHITESPACE_RE.sub(" ", text)
    text = _COMMA_SPACING_RE.sub(", ", text)
    return text.strip()


def insert_observation(
    conn: sqlite3.Connection,
    raw_text: str,
    normalized_text: str,
    source_id: int,
    source_record_id: Optional[str],
    field_name: str,
    source_record_type: Optional[str] = None,
) -> Tuple[int, bool]:
    """Insert an observation if it doesn't already exist.

    Returns (observation_id, was_newly_inserted). Idempotent via the
    (source_id, normalized_text, field_name) unique constraint declared
    in db/init_db.py.
    """
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO culinary_observations
            (raw_text, normalized_text, source_id, source_record_id, field_name, source_record_type)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (raw_text, normalized_text, source_id, source_record_id, field_name, source_record_type),
    )
    inserted = bool(cur.rowcount)
    observation_id = get_observation_id(conn, source_id, normalized_text, field_name)
    assert observation_id is not None
    return observation_id, inserted


def get_observation_id(
    conn: sqlite3.Connection,
    source_id: int,
    normalized_text: str,
    field_name: str,
) -> Optional[int]:
    cur = conn.execute(
        """
        SELECT observation_id FROM culinary_observations
        WHERE source_id = ? AND normalized_text = ? AND field_name = ?
        """,
        (source_id, normalized_text, field_name),
    )
    row = cur.fetchone()
    return row[0] if row else None