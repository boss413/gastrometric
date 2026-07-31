"""Idempotent helpers for culinary_vocabulary / culinary_aliases.

No culinary vocabulary is hardcoded in this module. Which terms are
"obvious single concepts" and what class/plural aliases they have comes
entirely from JSON seed data (see data/seed/culinary_vocabulary.json),
loaded via load_known_vocabulary(). This module only knows the *shape* of
that data and the *rule* for what counts as a single-token candidate —
never the culinary content itself.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Dict, Optional, Set, Tuple

from .sources import load_json_seed

# A modifier normalizes to an "obvious single concept" candidate only when
# it is one bare alphabetic word (optionally hyphenated, e.g. "extra-large").
# Anything with whitespace, commas, parentheses, digits, etc. is a complex
# expression and is left as observation-only — decomposition is out of scope.
_SINGLE_TOKEN_RE = re.compile(r"^[A-Za-z]+(-[A-Za-z]+)*$")


def is_single_token_concept(normalized_text: str) -> bool:
    return bool(_SINGLE_TOKEN_RE.match(normalized_text.strip()))


def load_vocabulary_classes(path) -> Set[str]:
    data = load_json_seed(path)
    if isinstance(data, dict):
        return set(data["classes"])
    return set(data)


def load_known_vocabulary(path) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Load reference terms/aliases from JSON seed data.

    Returns:
        terms:   {term (lowercase) -> vocabulary_class}
        aliases: {alias (lowercase) -> canonical term (lowercase)}
    """
    data = load_json_seed(path)
    terms: Dict[str, str] = {}
    aliases: Dict[str, str] = {}
    for entry in data["vocabulary"]:
        term = entry["term"].strip().lower()
        terms[term] = entry["vocabulary_class"]
        for alias in entry.get("aliases", []):
            aliases[alias.strip().lower()] = term
    return terms, aliases


def ensure_vocabulary_entry(
    conn: sqlite3.Connection,
    term: str,
    vocabulary_class: str,
    observation_id: Optional[int],
    valid_classes: Set[str],
) -> Tuple[int, bool]:
    """Insert a vocabulary row if it doesn't already exist.

    Returns (vocabulary_id, was_newly_inserted). "unknown" is used in
    place of any class not in valid_classes, per the rule that unknown is
    preferred over guessing.
    """
    if vocabulary_class not in valid_classes:
        vocabulary_class = "unknown"

    cur = conn.execute(
        """
        INSERT OR IGNORE INTO culinary_vocabulary (term, vocabulary_class, observation_id)
        VALUES (?, ?, ?)
        """,
        (term, vocabulary_class, observation_id),
    )
    inserted = bool(cur.rowcount)
    vocabulary_id = get_vocabulary_id(conn, term)
    assert vocabulary_id is not None
    return vocabulary_id, inserted


def get_vocabulary_id(conn: sqlite3.Connection, term: str) -> Optional[int]:
    cur = conn.execute("SELECT vocabulary_id FROM culinary_vocabulary WHERE term = ?", (term,))
    row = cur.fetchone()
    return row[0] if row else None


def ensure_alias(conn: sqlite3.Connection, alias_text: str, vocabulary_id: int) -> Tuple[int, bool]:
    """Insert an alias if it doesn't already exist. Never creates duplicates.

    Returns (alias_id, was_newly_inserted).
    """
    cur = conn.execute("SELECT alias_id FROM culinary_aliases WHERE alias_text = ?", (alias_text,))
    existing = cur.fetchone()
    if existing:
        return existing[0], False

    conn.execute(
        "INSERT INTO culinary_aliases (alias_text, vocabulary_id) VALUES (?, ?)",
        (alias_text, vocabulary_id),
    )
    cur = conn.execute("SELECT alias_id FROM culinary_aliases WHERE alias_text = ?", (alias_text,))
    return cur.fetchone()[0], True