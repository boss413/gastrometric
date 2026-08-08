"""
Runtime culinary and grammar knowledge loader.

This module is the single runtime source of culinary and grammar knowledge
for lexical analysis. It is the last stop in the pipeline:

    JSON seed files
            |
        builders
            |
         SQLite
            |
    knowledge.loader   <- this module
            |
    immutable runtime indexes
            |
         lex.py

Vocabulary is read from SQLite exactly once, at load time. After that, every
runtime consumer (the future lexer included) works entirely off in-memory,
immutable indexes. This module is intentionally read-only:

    * No runtime component (this module included) reads JSON seed data.
      That is knowledge-building territory (the USDA/seed builders), not the
      runtime contract.
    * No runtime component queries SQLite after `__init__` returns. Schema
      creation/mutation belongs solely to db/init_db.py; this loader only
      ever issues SELECTs, and only during construction.
    * Runtime code never mutates culinary knowledge and never rebuilds
      indexes -- everything handed out by this module is a frozen/immutable
      view.

Expected schema (owned by the DB, not assumed by this loader):

    culinary_vocabulary(
        vocabulary_id     INTEGER PRIMARY KEY,
        term              TEXT NOT NULL,
        vocabulary_class  TEXT NOT NULL,
        ...              -- other provenance/description columns, not our concern
    )

    culinary_aliases(
        alias_id     INTEGER PRIMARY KEY,
        vocabulary_id INTEGER NOT NULL REFERENCES culinary_vocabulary(vocabulary_id),
        alias_text   TEXT NOT NULL,
        ...
    )

The alias -> canonical relationship is a foreign key (`vocabulary_id`), not a
text join on the term itself. The loader resolves it with a JOIN.

`vocabulary_class` is data, not schema: whatever class strings exist in the
DB today (ingredient, measurement, preparation, packaging, brand, ...) are
what get exposed. If a `grammar` class is added to the seed data tomorrow,
`knowledge.grammar_words` starts returning real vocabulary with zero code
changes here -- until then it is simply an empty set.

Runtime API
-----------

    from gastrometric.knowledge.loader import knowledge

    knowledge.ingredients            # frozenset[str] of canonical ingredient terms
    knowledge.aliases                # immutable mapping: normalized surface -> canonical term
    knowledge.measurements           # frozenset[str]
    knowledge.preparation_terms      # frozenset[str]
    knowledge.brand_names            # frozenset[str]
    knowledge.packaging_terms        # frozenset[str]
    knowledge.grammar_words          # frozenset[str] (empty until grammar tables exist)

    knowledge.find_phrases_starting_with("olive")
        -> tuple[PhraseMatch, ...], longest phrase (by token count) first

    knowledge.phrases_longest_first
        -> every known phrase (canonical terms AND aliases), longest first,
           precomputed once so the lexer never has to sort vocabulary itself

    knowledge.unicode_fractions
        -> immutable lookup table for Unicode fraction normalization
           (e.g. "\u00bd" -> 0.5). No parsing logic lives here; that stays
           the lexer's job.

The lexer should not need to know, or care, that any of this came from
SQLite.
"""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from types import MappingProxyType
from typing import DefaultDict, Mapping, NamedTuple

from gastrometric.config.paths import DB_PATH

_TOKEN_SPLIT_RE = re.compile(r"\s+")

# Immutable lookup table for Unicode vulgar-fraction normalization.
# This is data, not logic -- turning "1\u00bd" into 1.5 remains the lexer's
# job. The loader only hands out the character -> value mapping.
UNICODE_FRACTIONS: Mapping[str, float] = MappingProxyType(
    {
        "\u00bc": 0.25,   # ¼
        "\u00bd": 0.5,    # ½
        "\u00be": 0.75,   # ¾
        "\u2150": 1 / 9,  # ⅐
        "\u2151": 1 / 10,  # ⅑
        "\u2152": 1 / 10,  # ⅒
        "\u2153": 1 / 3,  # ⅓
        "\u2154": 2 / 3,  # ⅔
        "\u2155": 1 / 5,  # ⅕
        "\u2156": 2 / 5,  # ⅖
        "\u2157": 3 / 5,  # ⅗
        "\u2158": 4 / 5,  # ⅘
        "\u2159": 1 / 6,  # ⅙
        "\u215a": 5 / 6,  # ⅚
        "\u215b": 1 / 8,  # ⅛
        "\u215c": 3 / 8,  # ⅜
        "\u215d": 5 / 8,  # ⅝
        "\u215e": 7 / 8,  # ⅞
    }
)


class PhraseMatch(NamedTuple):
    """A single precomputed phrase in the runtime phrase index.

    `tokens` is the normalized, whitespace-split surface form -- this may be
    a canonical term ("extra virgin olive oil") or an alias ("bell pepper").
    `canonical` is what it resolves to and `vocabulary_class` is that
    canonical term's class (measurement, ingredient, preparation, ...).
    """

    tokens: tuple[str, ...]
    canonical: str
    vocabulary_class: str

    @property
    def token_count(self) -> int:
        return len(self.tokens)

    @property
    def surface(self) -> str:
        return " ".join(self.tokens)


class CulinaryVocabulary:
    """Loads culinary + grammar vocabulary from SQLite once, then exposes it
    as immutable, precomputed, in-memory indexes for lexical lookup.

    Nothing here queries the database after `__init__` returns, and nothing
    here reads JSON. It is the single runtime dependency of the future
    lexer (gastrometric/understanding/lex.py).
    """

    _VOCAB_TABLE = "culinary_vocabulary"
    _VOCAB_COLUMNS = ("vocabulary_id", "term", "vocabulary_class")

    _ALIAS_TABLE = "culinary_aliases"
    _ALIAS_COLUMNS = ("alias_id", "vocabulary_id", "alias_text")

    # Ingredients live in their own table pair, separate from the generic
    # culinary_vocabulary/culinary_aliases used for measurement, preparation,
    # packaging, brand, etc. There is no `vocabulary_class` column here --
    # every row is, definitionally, class "ingredient" -- and the alias FK
    # is `ingredient_id` rather than `vocabulary_id`.
    _INGREDIENT_TABLE = "ingredients"
    _INGREDIENT_COLUMNS = ("id", "ingredient_name")

    _INGREDIENT_ALIAS_TABLE = "ingredient_aliases"
    _INGREDIENT_ALIAS_COLUMNS = ("id", "ingredient_id", "alias")

    _INGREDIENT_VOCABULARY_CLASS = "ingredient"

    def __init__(self, db_path: Path = DB_PATH) -> None:
        # --- raw indexes, built directly off the DB rows ---
        self._canonical_by_alias: dict[str, str] = {}
        self._class_by_canonical: dict[str, str] = {}
        self._members_by_class: DefaultDict[str, set[str]] = defaultdict(set)

        self._load(db_path)

        # --- derived indexes, built once from the raw indexes above ---
        self._build_phrase_indexes()
        self._build_public_views()

    # -------------------------------------------------------------------------
    # Loading (SQLite access happens ONLY in these methods, and only once)
    # -------------------------------------------------------------------------

    def _load(self, db_path: Path) -> None:
        try:
            conn = sqlite3.connect(db_path)
        except sqlite3.Error as exc:
            raise RuntimeError(
                f"Unable to open culinary knowledge database: {db_path}"
            ) from exc

        try:
            conn.row_factory = sqlite3.Row

            self._validate_schema(conn)
            self._load_vocabulary(conn)
            self._load_aliases(conn)
            self._load_ingredients(conn)
            self._load_ingredient_aliases(conn)

        finally:
            conn.close()

    def _validate_schema(self, conn: sqlite3.Connection) -> None:
        """Fail fast if the DB doesn't have the tables/columns this loader
        depends on, rather than silently loading an empty vocabulary."""
        for table, required_columns in (
            (self._VOCAB_TABLE, self._VOCAB_COLUMNS),
            (self._ALIAS_TABLE, self._ALIAS_COLUMNS),
            (self._INGREDIENT_TABLE, self._INGREDIENT_COLUMNS),
            (self._INGREDIENT_ALIAS_TABLE, self._INGREDIENT_ALIAS_COLUMNS),
        ):
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            if cursor.fetchone() is None:
                raise RuntimeError(
                    "Culinary vocabulary schema is missing or incomplete: "
                    f"expected table '{table}' was not found."
                )

            columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
            missing = [c for c in required_columns if c not in columns]
            if missing:
                raise RuntimeError(
                    "Culinary vocabulary schema is missing or incomplete: "
                    f"table '{table}' is missing column(s) {missing}."
                )

    def _load_vocabulary(self, conn: sqlite3.Connection) -> None:
        """Populate canonical vocabulary structures from the vocabulary table.

        Also seeds each canonical term into `_canonical_by_alias` so that
        `resolve_alias`/`contains`/`vocabulary_class` work for the canonical
        spelling itself, even if no alias row happens to exist for it.
        """
        rows = conn.execute(
            f"SELECT term, vocabulary_class FROM {self._VOCAB_TABLE}"
        ).fetchall()

        for row in rows:
            term = self._normalize(row["term"])
            vocabulary_class = row["vocabulary_class"]

            existing_class = self._class_by_canonical.get(term)
            if existing_class is not None and existing_class != vocabulary_class:
                raise RuntimeError(
                    f"Culinary vocabulary conflict: canonical term '{term}' is "
                    f"classified as both '{existing_class}' and "
                    f"'{vocabulary_class}'."
                )

            self._class_by_canonical[term] = vocabulary_class
            self._members_by_class[vocabulary_class].add(term)
            self._canonical_by_alias.setdefault(term, term)

    def _load_aliases(self, conn: sqlite3.Connection) -> None:
        """Populate both lookup directions for each alias.

        The alias -> canonical relationship is a foreign key
        (`culinary_aliases.vocabulary_id` -> `culinary_vocabulary.vocabulary_id`),
        not a text join on the term itself, so this resolves it with an
        explicit JOIN. A LEFT JOIN is used (rather than an inner join) so
        that an alias row whose `vocabulary_id` doesn't match any vocabulary
        row shows up as a NULL canonical term and can be raised as an error,
        instead of silently vanishing from the result set.

        For every alias row this:
          - resolves the alias's canonical vocabulary entry and class via
            the FK,
          - adds the normalized alias -> canonical mapping, and
          - adds the normalized alias into `_members_by_class[class]` so
            that `measurements()`/`is_measurement()`/etc. recognize the
            surface forms the parser actually encounters (e.g. "cups"),
            not just the canonical spelling ("cup").
        """
        rows = conn.execute(
            """
            SELECT
                ca.alias_text   AS alias_text,
                ca.vocabulary_id AS vocabulary_id,
                cv.term         AS canonical_term
            FROM culinary_aliases ca
            LEFT JOIN culinary_vocabulary cv
                ON ca.vocabulary_id = cv.vocabulary_id
            """
        ).fetchall()

        for row in rows:
            alias = self._normalize(row["alias_text"])

            if row["canonical_term"] is None:
                raise RuntimeError(
                    f"Culinary vocabulary conflict: alias '{alias}' "
                    f"references vocabulary_id {row['vocabulary_id']!r}, "
                    "which does not exist in culinary_vocabulary (orphaned "
                    "alias)."
                )

            canonical = self._normalize(row["canonical_term"])
            vocabulary_class = self._class_by_canonical.get(canonical)
            if vocabulary_class is None:
                # Should be unreachable given the join above, but don't
                # trust it blindly.
                raise RuntimeError(
                    f"Culinary vocabulary conflict: alias '{alias}' points to "
                    f"unknown canonical term '{canonical}'."
                )

            existing_canonical = self._canonical_by_alias.get(alias)
            if existing_canonical is not None and existing_canonical != canonical:
                raise RuntimeError(
                    f"Culinary vocabulary conflict: alias '{alias}' maps to "
                    f"both '{existing_canonical}' and '{canonical}'."
                )

            self._canonical_by_alias[alias] = canonical
            self._members_by_class[vocabulary_class].add(alias)

    def _load_ingredients(self, conn: sqlite3.Connection) -> None:
        """Populate canonical ingredient structures from the `ingredients`
        table.

        Ingredients get folded into the exact same shared indexes
        (`_class_by_canonical`, `_members_by_class`, `_canonical_by_alias`)
        as everything loaded from `culinary_vocabulary`, all tagged with
        vocabulary_class "ingredient" -- there's no separate code path
        downstream (phrase indexes, public views, `phrase_index_for`, etc.)
        that needs to know ingredients came from a different table.
        """
        rows = conn.execute(
            f"SELECT ingredient_name FROM {self._INGREDIENT_TABLE}"
        ).fetchall()

        for row in rows:
            term = self._normalize(row["ingredient_name"])
            vocabulary_class = self._INGREDIENT_VOCABULARY_CLASS

            existing_class = self._class_by_canonical.get(term)
            if existing_class is not None and existing_class != vocabulary_class:
                raise RuntimeError(
                    f"Ingredient vocabulary conflict: canonical term '{term}' "
                    f"is classified as both '{existing_class}' and "
                    f"'{vocabulary_class}'."
                )

            self._class_by_canonical[term] = vocabulary_class
            self._members_by_class[vocabulary_class].add(term)
            self._canonical_by_alias.setdefault(term, term)

    def _load_ingredient_aliases(self, conn: sqlite3.Connection) -> None:
        """Populate both lookup directions for each ingredient alias.

        Mirrors `_load_aliases`, but resolves the FK via `ingredient_id`
        against `ingredients.id` rather than `vocabulary_id` against
        `culinary_vocabulary.vocabulary_id`. `confidence`/`source` on
        `ingredient_aliases` are provenance columns -- not this loader's
        concern -- so every alias row is loaded regardless of confidence.
        """
        rows = conn.execute(
            f"""
            SELECT
                ia.alias           AS alias_text,
                ia.ingredient_id   AS ingredient_id,
                i.ingredient_name  AS canonical_name
            FROM {self._INGREDIENT_ALIAS_TABLE} ia
            LEFT JOIN {self._INGREDIENT_TABLE} i
                ON ia.ingredient_id = i.id
            """
        ).fetchall()

        for row in rows:
            alias = self._normalize(row["alias_text"])

            if row["canonical_name"] is None:
                raise RuntimeError(
                    f"Ingredient vocabulary conflict: alias '{alias}' "
                    f"references ingredient_id {row['ingredient_id']!r}, "
                    "which does not exist in ingredients (orphaned alias)."
                )

            canonical = self._normalize(row["canonical_name"])
            vocabulary_class = self._class_by_canonical.get(canonical)
            if vocabulary_class is None:
                # Should be unreachable given the join above, but don't
                # trust it blindly.
                raise RuntimeError(
                    f"Ingredient vocabulary conflict: alias '{alias}' points "
                    f"to unknown canonical ingredient '{canonical}'."
                )

            existing_canonical = self._canonical_by_alias.get(alias)
            if existing_canonical is not None and existing_canonical != canonical:
                raise RuntimeError(
                    f"Ingredient vocabulary conflict: alias '{alias}' maps "
                    f"to both '{existing_canonical}' and '{canonical}'."
                )

            self._canonical_by_alias[alias] = canonical
            self._members_by_class[vocabulary_class].add(alias)

    # -------------------------------------------------------------------------
    # Derived index construction (in-memory only, runs once)
    # -------------------------------------------------------------------------

    def _build_phrase_indexes(self) -> None:
        """Precompute everything the lexer would otherwise have to derive
        itself: a token-split phrase for every known surface form (canonical
        term or alias), ordered longest-first, and bucketed by first token.

        This is what backs `find_phrases_starting_with` and
        `phrases_longest_first`. The lexer should never need to split or
        sort vocabulary on its own.
        """
        matches: list[PhraseMatch] = []
        seen_token_tuples: set[tuple[str, ...]] = set()

        for surface, canonical in self._canonical_by_alias.items():
            vocabulary_class = self._class_by_canonical.get(canonical)
            if vocabulary_class is None:
                continue  # unreachable in practice, but don't trust it blindly

            tokens = tuple(t for t in _TOKEN_SPLIT_RE.split(surface.strip()) if t)
            if not tokens or tokens in seen_token_tuples:
                continue
            seen_token_tuples.add(tokens)

            matches.append(
                PhraseMatch(tokens=tokens, canonical=canonical, vocabulary_class=vocabulary_class)
            )

        # Longest phrase (by token count) first; alphabetical as a stable
        # tiebreaker so iteration order doesn't depend on dict/set ordering
        # quirks between runs.
        matches.sort(key=lambda m: (-m.token_count, m.tokens))

        self._phrases_longest_first: tuple[PhraseMatch, ...] = tuple(matches)

        by_first_token: DefaultDict[str, list[PhraseMatch]] = defaultdict(list)
        for match in matches:
            by_first_token[match.tokens[0]].append(match)

        # Each bucket inherits the longest-first ordering established above.
        self._phrases_by_first_token: Mapping[str, tuple[PhraseMatch, ...]] = MappingProxyType(
            {token: tuple(phrases) for token, phrases in by_first_token.items()}
        )

        # Per-class views of the same data. lex.py's stages each need a
        # phrase index scoped to a single vocabulary_class (ingredient,
        # measurement, preparation, brand, grammar, ...) -- this is that,
        # precomputed once rather than filtered on every lookup.
        by_class_first_token: DefaultDict[str, DefaultDict[str, list[PhraseMatch]]] = defaultdict(
            lambda: defaultdict(list)
        )
        max_token_count_by_class: DefaultDict[str, int] = defaultdict(int)
        for match in matches:
            by_class_first_token[match.vocabulary_class][match.tokens[0]].append(match)
            if match.token_count > max_token_count_by_class[match.vocabulary_class]:
                max_token_count_by_class[match.vocabulary_class] = match.token_count

        self._phrases_by_class: Mapping[str, Mapping[str, tuple[PhraseMatch, ...]]] = MappingProxyType(
            {
                vocabulary_class: MappingProxyType(
                    {token: tuple(phrases) for token, phrases in first_token_map.items()}
                )
                for vocabulary_class, first_token_map in by_class_first_token.items()
            }
        )
        self._max_phrase_length_by_class: Mapping[str, int] = MappingProxyType(
            dict(max_token_count_by_class)
        )

    def _build_public_views(self) -> None:
        """Freeze the class-tagged vocabulary buckets into the named,
        immutable attributes the lexer is expected to use directly (e.g.
        `knowledge.measurements`, `knowledge.ingredients`).

        These are precomputed once here rather than recomputed on every
        access, in keeping with the "one-time startup cost, zero-cost
        repeated lookups" performance goal.
        """
        # Canonical name -> vocabulary class, immutable view.
        self.names: Mapping[str, str] = MappingProxyType(dict(self._class_by_canonical))

        # Normalized surface form (alias OR canonical spelling) -> canonical
        # term. This is the full alias index described in the design doc.
        self.aliases: Mapping[str, str] = MappingProxyType(dict(self._canonical_by_alias))

        # Names, not schema: the DB's vocabulary_class column drives which
        # classes exist. This is every distinct class currently loaded --
        # the five named ones above plus anything else in the data
        # (packaging, size, descriptor, modifier, state, seasoning, shape,
        # ingredient_form, natural_portion, temperature, and any future
        # class added to the seed data with zero loader changes required).
        # This is the enumeration hook for a stage that needs to sweep
        # "every category I don't already have a named attribute for."
        self.vocabulary_classes: frozenset[str] = frozenset(self._members_by_class.keys())

        self.ingredients: frozenset[str] = self._freeze_class("ingredient")
        self.measurements: frozenset[str] = self._freeze_class("measurement")
        self.natural_portions: frozenset[str] = self._freeze_class("natural_portion")
        self.preparation_terms: frozenset[str] = self._freeze_class("preparation")
        self.temperatures: frozenset[str] = self._freeze_class("temperature")
        self.packaging_terms: frozenset[str] = self._freeze_class("packaging")
        self.sizes: frozenset[str] = self._freeze_class("size")
        self.descriptors: frozenset[str] = self._freeze_class("descriptor")
        self.modifiers: frozenset[str] = self._freeze_class("modifier")
        self.brand_names: frozenset[str] = self._freeze_class("brand")
        self.states: frozenset[str] = self._freeze_class("state")
        self.seasonings: frozenset[str] = self._freeze_class("seasoning")
        self.shapes: frozenset[str] = self._freeze_class("shape")
        self.ingredient_forms: frozenset[str] = self._freeze_class("ingredient_form")

        # Grammar is deliberately not culinary knowledge (of/with/into/or/...).
        # If a "grammar" vocabulary_class doesn't exist in the DB yet, this
        # is simply an empty frozenset -- no lexer-side hardcoded list, and
        # no loader change required once grammar rows exist.
        self.grammar_words: frozenset[str] = self._freeze_class("grammar")

        self.unicode_fractions: Mapping[str, float] = UNICODE_FRACTIONS

        self.phrases_longest_first: tuple[PhraseMatch, ...] = self._phrases_longest_first

        # Per-class phrase indexes (see `_build_phrase_indexes`), exposed
        # directly for consumers that want to iterate a whole class rather
        # than go through `phrase_index_for`/`find_phrases_starting_with`.
        self.phrases_by_class: Mapping[str, Mapping[str, tuple[PhraseMatch, ...]]] = (
            self._phrases_by_class
        )
        self.max_phrase_length_by_class: Mapping[str, int] = self._max_phrase_length_by_class

    def _freeze_class(self, vocabulary_class: str) -> frozenset[str]:
        return frozenset(self._members_by_class.get(vocabulary_class, ()))

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize(term: str) -> str:
        return term.casefold()

    def _class_members(self, vocabulary_class: str) -> set[str]:
        return set(self._members_by_class.get(vocabulary_class, set()))

    # -------------------------------------------------------------------------
    # Phrase / lexical lookup API
    # -------------------------------------------------------------------------

    def find_phrases_starting_with(
        self, first_token: str, vocabulary_class: str | None = None
    ) -> tuple[PhraseMatch, ...]:
        """Every known phrase whose first token matches `first_token`,
        longest phrase first.

        Example: find_phrases_starting_with("olive") might return phrases
        for "olive oil", "olive tapenade", and "olive", in that order,
        letting the lexer try the longest candidate first without doing any
        sorting of its own.

        Pass `vocabulary_class` (e.g. "ingredient") to scope the lookup to
        a single stage's vocabulary instead of the whole mixed index.
        """
        token = self._normalize(first_token)
        if vocabulary_class is None:
            return self._phrases_by_first_token.get(token, ())
        return self._phrases_by_class.get(vocabulary_class, MappingProxyType({})).get(token, ())

    def phrase_index_for(
        self, vocabulary_class: str
    ) -> tuple[Mapping[str, tuple[PhraseMatch, ...]], int]:
        """`(phrase_index, max_words)` for a single vocabulary class, e.g.
        "ingredient", "measurement", "preparation", "brand", "grammar".

        `phrase_index` maps first-token -> that class's candidate phrases,
        longest-first. `max_words` is the token count of the longest phrase
        in that class (0 if the class has no vocabulary at all).

        This is precomputed at load time -- calling it does no work beyond
        a couple of dict lookups, and repeated calls are free. It exists so
        that per-stage lexer code (see gastrometric/understanding/lex.py)
        can get exactly the shape it needs without reaching into private
        loader internals or reconstructing anything itself.
        """
        return (
            self._phrases_by_class.get(vocabulary_class, MappingProxyType({})),
            self._max_phrase_length_by_class.get(vocabulary_class, 0),
        )

    # -------------------------------------------------------------------------
    # Canonical lookup
    # -------------------------------------------------------------------------

    def resolve_alias(self, term: str) -> str:
        return self._canonical_by_alias.get(self._normalize(term), term)

    def canonical(self, term: str) -> str:
        return self.resolve_alias(term)

    def vocabulary_class(self, term: str) -> str | None:
        canonical = self.resolve_alias(term)
        return self._class_by_canonical.get(canonical)

    def contains(self, term: str) -> bool:
        return self._normalize(term) in self._canonical_by_alias

    # -------------------------------------------------------------------------
    # Classification helpers (kept for existing runtime consumers)
    # -------------------------------------------------------------------------

    def _is_class(self, term: str, vocabulary_class: str) -> bool:
        return self.vocabulary_class(term) == vocabulary_class

    def is_measurement(self, term: str) -> bool:
        return self._is_class(term, "measurement")

    def is_natural_portion(self, term: str) -> bool:
        return self._is_class(term, "natural_portion")

    def is_preparation(self, term: str) -> bool:
        return self._is_class(term, "preparation")

    def is_temperature(self, term: str) -> bool:
        return self._is_class(term, "temperature")

    def is_packaging(self, term: str) -> bool:
        return self._is_class(term, "packaging")

    def is_size(self, term: str) -> bool:
        return self._is_class(term, "size")

    def is_descriptor(self, term: str) -> bool:
        return self._is_class(term, "descriptor")

    def is_modifier(self, term: str) -> bool:
        return self._is_class(term, "modifier")

    def is_brand(self, term: str) -> bool:
        return self._is_class(term, "brand")

    def is_state(self, term: str) -> bool:
        return self._is_class(term, "state")

    def is_seasoning(self, term: str) -> bool:
        return self._is_class(term, "seasoning")

    def is_ingredient(self, term: str) -> bool:
        return self._is_class(term, "ingredient")

    def is_tool(self, term: str) -> bool:
        return self._is_class(term, "tool")

    def is_component(self, term: str) -> bool:
        return self._is_class(term, "component")

    def is_grammar(self, term: str) -> bool:
        return self._is_class(term, "grammar")

    # -------------------------------------------------------------------------
    # BREAKING CHANGE / migration note
    # -------------------------------------------------------------------------
    # Prior to this refactor, class-tagged vocabulary was exposed as *methods*
    # that recomputed a fresh `set` on every call, e.g.:
    #
    #     knowledge.measurements()
    #     knowledge.preparations()
    #     knowledge.packaging()
    #     knowledge.descriptors()
    #     knowledge.modifier()
    #     knowledge.brand()
    #     knowledge.state()
    #     knowledge.seasoning()
    #     knowledge.shapes()
    #     knowledge.ingredient_forms()
    #
    # These have been replaced by precomputed, immutable attributes built
    # once in `_build_public_views` (zero-cost repeated access, per the
    # loader's performance goal):
    #
    #     knowledge.measurements
    #     knowledge.preparation_terms
    #     knowledge.packaging_terms
    #     knowledge.descriptors
    #     knowledge.modifiers
    #     knowledge.brand_names
    #     knowledge.states
    #     knowledge.seasonings
    #     knowledge.shapes
    #     knowledge.ingredient_forms
    #
    # Any existing call site using the old `()` method form needs to drop the
    # call and (for the renamed ones) update the name. The `is_*` predicate
    # methods above (is_measurement, is_preparation, ...) are unchanged and
    # continue to work exactly as before.


# ---------------------------------------------------------------------------
# The single runtime instance. Every downstream consumer -- lex.py included
# -- should import this rather than constructing its own CulinaryVocabulary,
# so there is exactly one in-memory copy of the vocabulary and exactly one
# load from SQLite per process.
# ---------------------------------------------------------------------------
knowledge = CulinaryVocabulary()

__all__ = ["CulinaryVocabulary", "PhraseMatch", "UNICODE_FRACTIONS", "knowledge"]