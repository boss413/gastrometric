# vocabulary_provider.py
#
# The seam between the parsing ALGORITHM (parse_ingredient_lines.py,
# parser_vocabulary.py) and CULINARY KNOWLEDGE (ingredient names/aliases,
# preparation/state/size/bone/skin/salt/sodium-level vocabulary,
# ingredient-specific portion terms).
#
# parse_ingredient_lines.py never imports culinary_vocabulary_bootstrap.py
# or touches the `ingredients` / `ingredient_aliases` / `attribute_type` /
# `attribute_value` tables directly. It only ever calls methods on a
# VocabularyProvider. That means swapping where the vocabulary comes from
# — bootstrap constants today, the database tomorrow, both at once during
# the transition — never requires touching the parsing algorithm.
#
# Two implementations:
#
#   StaticVocabularyProvider    Reads culinary_vocabulary_bootstrap.py.
#                                Used when there's no database connection
#                                (ad hoc calls, tests, tooling), and as
#                                the fallback inside DatabaseVocabularyProvider
#                                for any category the database can't yet
#                                answer (e.g. portion terms, before the
#                                USDA-ingestion table exists).
#
#   DatabaseVocabularyProvider  Reads `ingredients` + `ingredient_aliases`
#                                for protected_phrases(), and
#                                `attribute_type` + `attribute_value` for
#                                prep_patterns()/temperature_state_patterns().
#                                Falls back to StaticVocabularyProvider,
#                                per category, whenever its query returns
#                                nothing — so an unseeded or partially
#                                seeded database degrades gracefully
#                                instead of silently parsing worse.
#
# NOTE ON attribute_value.value FORMATTING
# ------------------------------------------
# attribute_value.value strings use underscores instead of spaces (e.g.
# "partially_cooked"), and multi-word values may appear in either word
# order in real recipe text ("partially cooked" AND "cooked partially").
# attribute_value_to_pattern() below is a PARSING-algorithm concern (it's
# about how to turn a piece of data into a regex that matches flexible
# English word order, not about what the data means) and is intentionally
# implemented here rather than baked into the bootstrap data or the DB
# rows themselves.

import re
import sqlite3
from abc import ABC, abstractmethod

import gastrometric.config.culinary_vocabulary_bootstrap as _bootstrap


def attribute_value_to_pattern(value):
    """
    Turn one attribute_value.value (underscore-separated, e.g.
    "partially_cooked") into a regex fragment that matches:
      - the value with underscores replaced by whitespace
        ("partially cooked" / "partially-cooked")
      - for exactly two tokens, the reversed order too
        ("cooked partially")

    This is pure text-matching flexibility (word order, separator
    choice) — it does not interpret what the value means, so it belongs
    in the parsing layer even though the values themselves are culinary
    data.
    """
    tokens = [t for t in re.split(r'[_\s]+', value.strip()) if t]
    if not tokens:
        return None
    escaped = [re.escape(t) for t in tokens]
    forward = r'\s+'.join(escaped)
    if len(tokens) == 2:
        reversed_pat = r'\s+'.join(escaped[::-1])
        return r'(?:%s|%s)' % (forward, reversed_pat)
    return forward


class VocabularyProvider(ABC):
    """Everything the parser needs that ISN'T pure grammar."""

    @abstractmethod
    def protected_phrases(self):
        """Multi-word ingredient names/aliases that must survive
        prep/unit extraction unsplit."""

    @abstractmethod
    def prep_patterns(self):
        """Ordered list of regex fragments for preparation techniques,
        most-specific/multi-word first."""

    @abstractmethod
    def temperature_state_patterns(self):
        """Ordered list of regex fragments for temperature/freshness
        state words."""

    @abstractmethod
    def portion_terms(self):
        """Ingredient-specific counting/portion nouns (clove, sprig,
        floweret, spear, medallion, ...). Open class; expected to grow
        via USDA + recipe ingestion."""


class StaticVocabularyProvider(VocabularyProvider):
    """Bootstrap fallback: reads culinary_vocabulary_bootstrap.py."""

    def protected_phrases(self):
        return list(_bootstrap.PROTECTED_PHRASES)

    def prep_patterns(self):
        return list(_bootstrap.PREP_PATTERNS)

    def temperature_state_patterns(self):
        return list(_bootstrap.TEMPERATURE_STATE_PATTERNS)

    def portion_terms(self):
        return set(_bootstrap.PORTION_TERMS)


class DatabaseVocabularyProvider(VocabularyProvider):
    """
    Reads culinary vocabulary from the runtime database where a table
    for it exists, falling back to StaticVocabularyProvider per-category
    whenever the corresponding table is missing, empty, or errors out.
    This is what makes it safe to switch this in as the default before
    every table is fully seeded.

    attribute_type.name values consulted for prep_patterns()/
    temperature_state_patterns() are configurable via
    PREP_ATTRIBUTE_TYPES / STATE_ATTRIBUTE_TYPES below — adjust these to
    match your actual attribute_type.name rows. They're deliberately not
    hardcoded assumptions buried in query logic.
    """

    # Which attribute_type.name rows feed which pattern list. Adjust to
    # match the actual seeded attribute_type names.
    PREP_ATTRIBUTE_TYPES = ("preparation",)
    STATE_ATTRIBUTE_TYPES = ("state", "temperature")

    def __init__(self, conn, fallback=None):
        self.conn = conn
        self.fallback = fallback or StaticVocabularyProvider()

    # -- internal helpers -------------------------------------------------

    def _table_exists(self, name):
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,)
        ).fetchone()
        return row is not None

    def _attribute_values_for(self, type_names):
        if not self._table_exists("attribute_type") or not self._table_exists("attribute_value"):
            return []
        placeholders = ",".join("?" for _ in type_names)
        try:
            rows = self.conn.execute(
                f"""
                SELECT av.value
                FROM attribute_value av
                JOIN attribute_type at ON at.id = av.attribute_type_id
                WHERE at.name IN ({placeholders})
                """,
                type_names
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        return [r[0] for r in rows if r[0]]

    # -- VocabularyProvider interface -------------------------------------

    def protected_phrases(self):
        if not self._table_exists("ingredients") or not self._table_exists("ingredient_aliases"):
            return self.fallback.protected_phrases()
        try:
            names = [r[0] for r in self.conn.execute(
                "SELECT ingredient_name FROM ingredients"
            ).fetchall() if r[0]]
            aliases = [r[0] for r in self.conn.execute(
                "SELECT alias FROM ingredient_aliases"
            ).fetchall() if r[0]]
        except sqlite3.OperationalError:
            return self.fallback.protected_phrases()

        phrases = {p.strip().lower() for p in (names + aliases) if p and " " in p.strip()}
        if not phrases:
            return self.fallback.protected_phrases()
        # Longest first, so multi-word greedy matching in _protect_phrases
        # prefers the more specific phrase (mirrors the bootstrap list's
        # existing convention, e.g. "freshly ground black pepper" before
        # "black pepper").
        return sorted(phrases, key=len, reverse=True)

    def prep_patterns(self):
        values = self._attribute_values_for(self.PREP_ATTRIBUTE_TYPES)
        if not values:
            return self.fallback.prep_patterns()
        patterns = [attribute_value_to_pattern(v) for v in values]
        patterns = [p for p in patterns if p]
        # Multi-word (more specific) patterns first, matching the
        # convention PREP_PATTERNS relies on.
        return sorted(patterns, key=lambda p: p.count(r'\s+'), reverse=True)

    def temperature_state_patterns(self):
        values = self._attribute_values_for(self.STATE_ATTRIBUTE_TYPES)
        if not values:
            return self.fallback.temperature_state_patterns()
        patterns = [attribute_value_to_pattern(v) for v in values]
        patterns = [p for p in patterns if p]
        return sorted(patterns, key=lambda p: p.count(r'\s+'), reverse=True)

    def portion_terms(self):
        # No portion-term table exists yet (future USDA ingestion).
        # Structured to pick it up automatically once one does: rename/
        # point this at the real table name when it's created.
        if not self._table_exists("ingredient_portion_terms"):
            return self.fallback.portion_terms()
        try:
            rows = self.conn.execute(
                "SELECT term FROM ingredient_portion_terms"
            ).fetchall()
        except sqlite3.OperationalError:
            return self.fallback.portion_terms()
        terms = {r[0].strip().lower() for r in rows if r[0]}
        return terms or self.fallback.portion_terms()