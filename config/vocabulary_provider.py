import re
import sqlite3
from abc import ABC, abstractmethod


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
    def ingredient_identities(self):
        """Every known ingredient name/alias, single- or multi-word.
        Used by the parser for longest-match ingredient-span recognition
        (see parse_ingredient_lines.py's Stage 2). Renamed from the old
        protected_phrases(), which filtered to multi-word terms only —
        that filter existed purely to shield phrases from being SPLIT by
        later regex passes under the old subtraction-based parser. Under
        the span-based parser every known name needs to be recognized
        and emitted, single-word included (e.g. "tuna"), so the filter
        is gone."""

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
    """No data source of its own — deliberately.

    This used to read gastrometric.config.culinary_vocabulary_bootstrap,
    a hand-maintained word list that had drifted into being a worse,
    stale duplicate of the `ingredients`/`ingredient_aliases` tables
    (exactly the data DatabaseVocabularyProvider.ingredient_identities()
    already reads directly). Keeping it around as a "fallback" meant
    that a missing/misconfigured DB connection would silently degrade
    into serving that stale duplicate instead of failing loudly or
    visibly returning nothing — the worst of both options, since it
    looks like real vocabulary while quietly being wrong.

    This class now returns empty results for everything instead. An
    empty result is honest and observable (parse_ingredient_lines.py's
    startup diagnostics will show "Ingredients has 0 foods", which is a
    plain signal to fix the DB wiring) rather than a plausible-looking
    but incorrect list. If a genuine offline/DB-less vocabulary is ever
    needed again, it should be built by reading directly from
    ingredients/ingredient_aliases (e.g. a small exported snapshot),
    not by hand-maintaining a second copy of that data here.
    """

    def ingredient_identities(self):
        return []

    def prep_patterns(self):
        return []

    def temperature_state_patterns(self):
        return []

    def portion_terms(self):
        return set()


class DatabaseVocabularyProvider(VocabularyProvider):
    """
    Reads culinary vocabulary from the runtime database where a table
    for it exists, falling back to StaticVocabularyProvider per-category
    whenever the corresponding table is missing, empty, or errors out.

    Since StaticVocabularyProvider now returns nothing, that fallback is
    purely a "degrade to empty rather than crash" safety net — not a
    second data source. If ingredient_identities()/prep_patterns()/etc.
    come back empty in practice, the fix is the DB/table, not this class.

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

    def ingredient_identities(self):
        if not self._table_exists("ingredients") or not self._table_exists("ingredient_aliases"):
            return self.fallback.ingredient_identities()
        try:
            names = [r[0] for r in self.conn.execute(
                "SELECT ingredient_name FROM ingredients"
            ).fetchall() if r[0]]
            aliases = [r[0] for r in self.conn.execute(
                "SELECT alias FROM ingredient_aliases"
            ).fetchall() if r[0]]
        except sqlite3.OperationalError:
            return self.fallback.ingredient_identities()
        # No multi-word-only filter (unlike the old protected_phrases()):
        # under the span-based parser, single-word names need recognizing
        # too — see the ABC docstring above.
        phrases = {p.strip().lower() for p in (names + aliases) if p and p.strip()}
        if not phrases:
            return self.fallback.ingredient_identities()
        # Longest first, so multi-word greedy matching in _protect_phrases
        # prefers the more specific phrase (e.g. "freshly ground black
        # pepper" before "black pepper").
        return sorted(phrases, key=len, reverse=True)

    def prep_patterns(self):
        values = self._attribute_values_for(self.PREP_ATTRIBUTE_TYPES)
        if not values:
            return self.fallback.prep_patterns()
        patterns = [attribute_value_to_pattern(v) for v in values]
        patterns = [p for p in patterns if p]
        # Multi-word (more specific) patterns first.
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