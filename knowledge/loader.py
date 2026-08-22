"""
Runtime culinary knowledge loader.

This module is the single runtime source of vocabulary, ingredient, and
relationship knowledge for lexical analysis. It is the last stop in the
pipeline:

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

Knowledge is read from SQLite exactly once, at load time. After that, every
runtime consumer (the future lexer included) works entirely off in-memory,
immutable indexes. This module is intentionally read-only:

    * No runtime component (this module included) reads JSON seed data.
      That is knowledge-building territory (the seed builders), not the
      runtime contract.
    * No runtime component queries SQLite after `__init__` returns. Schema
      creation/mutation belongs solely to db init/builder code; this loader
      only ever issues SELECTs, and only during construction.
    * Runtime code never mutates knowledge and never rebuilds indexes --
      everything handed out by this module is a frozen/immutable view.

Expected schema (owned by the DB, not assumed by this loader):

    vocabulary_terms(
        term_id     TEXT PRIMARY KEY,
        term        TEXT NOT NULL UNIQUE,
        created_at  TEXT NOT NULL DEFAULT (datetime('now'))
    )

    vocabulary_classes(
        class_id    TEXT PRIMARY KEY,
        class_name  TEXT NOT NULL UNIQUE
    )

    vocabulary_term_classes(
        term_id     TEXT NOT NULL REFERENCES vocabulary_terms(term_id),
        class_id    TEXT NOT NULL REFERENCES vocabulary_classes(class_id),
        PRIMARY KEY (term_id, class_id)
    )

    ingredients (
        id              STR PRIMARY KEY,
        ingredient_name TEXT UNIQUE NOT NULL,
        ...              -- notes/created_at, not our concern
    )

    ingredient_aliases(
        id             INTEGER PRIMARY KEY,
        ingredient_id  INTEGER NOT NULL REFERENCES ingredients(id),
        alias          TEXT NOT NULL,
        ...           -- confidence/source, not our concern
    )

    relationships(
        relationship_id  INTEGER PRIMARY KEY,
        subject_type     TEXT NOT NULL,
        subject_id       TEXT NOT NULL,
        predicate        TEXT NOT NULL,
        object_type      TEXT NOT NULL,
        object_id        TEXT NOT NULL,
        source           TEXT,
        confidence       REAL,
        created_at       TEXT NOT NULL
    )

`vocabulary_term_classes` is a many-to-many junction: a term can carry more
than one class simultaneously (e.g. a word that is both an ingredient and a
brand name). This loader does not pick a winner between them -- it exposes
every class a term is tagged with. Class membership is data, not schema:
whatever class names exist in `vocabulary_classes` today (measurement,
preparation, packaging, brand, ...) are what get exposed. If a `grammar`
class is added to the seed data tomorrow, `knowledge.grammar_words` starts
returning real vocabulary with zero code changes here -- until then it is
simply an empty set.

"Alias" is an ingredient-specific concept in this codebase: an alternate
name for an identical ingredient (e.g. "scallions" / "green onion"), sourced
from `ingredient_aliases`. Generic vocabulary terms (measurement,
preparation, brand, ...) have no alias concept -- a term is just a term with
class memberships, nothing resolves "through" it to something else.

Runtime API
-----------

    from gastrometric.knowledge.loader import knowledge

    knowledge.ingredients             # frozenset[str] of canonical ingredient names
    knowledge.ingredient_aliases      # immutable mapping: alias -> canonical ingredient name
    knowledge.measurements            # frozenset[str]
    knowledge.preparation_terms       # frozenset[str]
    knowledge.brand_names             # frozenset[str]
    knowledge.packaging_terms         # frozenset[str]
    knowledge.grammar_words           # frozenset[str] (empty until grammar-tagged terms exist)

    knowledge.classes_for("cinnamon")
        -> frozenset[str] of every class a term is tagged with. A term CAN
           belong to more than one class at once (e.g. {"ingredient", "brand"});
           this loader does not force a single answer.

    knowledge.find_phrases_starting_with("olive")
        -> tuple[PhraseMatch, ...], longest phrase (by token count) first

    knowledge.phrases_longest_first
        -> every known phrase (vocabulary terms, ingredient names, AND
           ingredient aliases), longest first, precomputed once so the
           lexer never has to sort vocabulary itself

    knowledge.unicode_fractions
        -> immutable lookup table for Unicode fraction normalization
           (e.g. "\u00bd" -> 0.5). No parsing logic lives here; that stays
           the lexer's job.

    knowledge.relationships
        -> tuple[Relationship, ...] of every persisted relationship
           assertion, exactly as loaded (no resolution, no inference, no
           inverses)

    knowledge.relationships_for_subject("vocabulary", "rib")
    knowledge.relationships_for_object("ingredient", "celery")
    knowledge.find_relationships(subject_type=..., subject_id=..., predicate=...,
                                  object_type=..., object_id=...)
        -> tuple[Relationship, ...], any argument to find_relationships is
           optional; a query that matches nothing returns ()

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
    a vocabulary term, a canonical ingredient name, or an ingredient alias.
    `canonical` is what it resolves to (itself, for anything that isn't an
    ingredient alias) and `vocabulary_class` is the specific class this
    phrase entry represents. Because a term can belong to more than one
    class at once, the SAME surface form can appear as more than one
    `PhraseMatch` -- one per class -- rather than being forced into a
    single answer.
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


class Relationship(NamedTuple):
    """A single persisted relationship assertion, exactly as the builder
    wrote it.

    Fields correspond directly to the `relationships` table row. An
    endpoint (`subject_type`/`subject_id` or `object_type`/`object_id`) is
    a typed identifier, not necessarily a currently-resolvable vocabulary
    or ingredient entry -- e.g. `subject_type="vocabulary", subject_id="grape"`
    is valid even if "grape" has no row in `vocabulary_terms`/`ingredients`.
    This loader does not resolve, validate, invert, or interpret that
    identifier; it only exposes the assertion that was persisted. Predicate
    semantics, inference, endpoint resolution, and inverse relationships
    are explicitly out of scope here and belong to later knowledge/query or
    analyzer layers.
    """

    relationship_id: int
    subject_type: str
    subject_id: str
    predicate: str
    object_type: str
    object_id: str
    source: str | None
    confidence: float | None
    created_at: str


class RuntimeKnowledge:
    """Loads vocabulary, ingredient, and relationship knowledge from SQLite
    once, then exposes it as immutable, precomputed, in-memory indexes.

    Nothing here queries the database after `__init__` returns, and nothing
    here reads JSON. It is the single runtime dependency of the future
    lexer (gastrometric/understanding/lex.py).
    """

    # Generic vocabulary: a many-to-many term <-> class relationship.
    # A term can legitimately carry more than one class at once (e.g. a
    # word that is both an ingredient and a brand name) -- this loader
    # does not force a single answer, unlike the single-class model this
    # replaced.
    _TERM_TABLE = "vocabulary_terms"
    _TERM_COLUMNS = ("term_id", "term")

    _CLASS_TABLE = "vocabulary_classes"
    _CLASS_COLUMNS = ("class_id", "class_name")

    _TERM_CLASS_TABLE = "vocabulary_term_classes"
    _TERM_CLASS_COLUMNS = ("term_id", "class_id")

    # Ingredients live in their own table pair, separate from the generic
    # vocabulary tables. There is no class column here -- every row is,
    # definitionally, class "ingredient" -- and the alias FK is
    # `ingredient_id` rather than a vocabulary term_id.
    _INGREDIENT_TABLE = "ingredients"
    _INGREDIENT_COLUMNS = ("id", "ingredient_name")

    _INGREDIENT_ALIAS_TABLE = "ingredient_aliases"
    _INGREDIENT_ALIAS_COLUMNS = ("id", "ingredient_id", "alias")

    _INGREDIENT_VOCABULARY_CLASS = "ingredient"

    # Relationships are a separate assertion table with no FK to vocabulary
    # or ingredients (deliberately -- see Relationship's docstring). This
    # loader reads it once, same as everything else, and never resolves
    # subject_id/object_id against any other table.
    _RELATIONSHIP_TABLE = "relationships"
    _RELATIONSHIP_COLUMNS = (
        "relationship_id",
        "subject_type",
        "subject_id",
        "predicate",
        "object_type",
        "object_id",
        "source",
        "confidence",
        "created_at",
    )

    def __init__(self, db_path: Path = DB_PATH) -> None:
        # --- raw indexes, built directly off the DB rows ---

        # term -> set of class names it carries. Many-to-many by design:
        # a term is not forced into a single class. Ingredient names/aliases
        # feed into this too, always tagged "ingredient".
        self._classes_by_term: DefaultDict[str, set[str]] = defaultdict(set)

        # class name -> member terms (inverse of the above). Every class
        # declared in `vocabulary_classes` is pre-registered here (even
        # with zero members) so `vocabulary_classes` reflects what's
        # actually declared, not just what happens to have data yet.
        self._members_by_class: DefaultDict[str, set[str]] = defaultdict(set)
        self._declared_classes: set[str] = {self._INGREDIENT_VOCABULARY_CLASS}

        # surface form -> canonical identity. For vocabulary terms and
        # canonical ingredient names this is a self-mapping; for ingredient
        # aliases it points at the real ingredient name. This is internal
        # plumbing for `PhraseMatch.canonical` and `resolve_ingredient_alias`
        # -- it is NOT publicly exposed as a generic "aliases" concept,
        # because generic vocabulary terms don't have aliases.
        self._canonical_by_surface: dict[str, str] = {}

        # alias -> canonical ingredient name, TRUE aliases only (surface !=
        # canonical). This is the ingredient-specific concept the public
        # `knowledge.ingredient_aliases` exposes.
        self._ingredient_alias_to_canonical: dict[str, str] = {}

        self._relationships_raw: tuple[Relationship, ...] = ()

        self._load(db_path)

        # --- derived indexes, built once from the raw indexes above ---
        self._build_phrase_indexes()
        self._build_relationship_indexes()
        self._build_public_views()

    # -------------------------------------------------------------------------
    # Loading (SQLite access happens ONLY in these methods, and only once)
    # -------------------------------------------------------------------------

    def _load(self, db_path: Path) -> None:
        try:
            conn = sqlite3.connect(db_path)
        except sqlite3.Error as exc:
            raise RuntimeError(
                f"Unable to open knowledge database: {db_path}"
            ) from exc

        try:
            conn.row_factory = sqlite3.Row

            self._validate_schema(conn)
            self._load_vocabulary_classes(conn)
            self._load_vocabulary_terms(conn)
            self._load_vocabulary_term_classes(conn)
            self._load_ingredients(conn)
            self._load_ingredient_aliases(conn)
            self._load_relationships(conn)

        finally:
            conn.close()

    def _validate_schema(self, conn: sqlite3.Connection) -> None:
        """Fail fast if the DB doesn't have the tables/columns this loader
        depends on, rather than silently loading empty knowledge."""
        for table, required_columns in (
            (self._TERM_TABLE, self._TERM_COLUMNS),
            (self._CLASS_TABLE, self._CLASS_COLUMNS),
            (self._TERM_CLASS_TABLE, self._TERM_CLASS_COLUMNS),
            (self._INGREDIENT_TABLE, self._INGREDIENT_COLUMNS),
            (self._INGREDIENT_ALIAS_TABLE, self._INGREDIENT_ALIAS_COLUMNS),
            (self._RELATIONSHIP_TABLE, self._RELATIONSHIP_COLUMNS),
        ):
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            if cursor.fetchone() is None:
                raise RuntimeError(
                    "Knowledge schema is missing or incomplete: "
                    f"expected table '{table}' was not found."
                )

            columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
            missing = [c for c in required_columns if c not in columns]
            if missing:
                raise RuntimeError(
                    "Knowledge schema is missing or incomplete: "
                    f"table '{table}' is missing column(s) {missing}."
                )

    def _load_vocabulary_classes(self, conn: sqlite3.Connection) -> None:
        """Register every declared class name, even ones with zero terms
        assigned yet. This is what lets `knowledge.vocabulary_classes`
        reflect what's actually declared in the DB rather than only what
        happens to have data.
        """
        rows = conn.execute(f"SELECT class_name FROM {self._CLASS_TABLE}").fetchall()
        for row in rows:
            self._declared_classes.add(row["class_name"])

    def _load_vocabulary_terms(self, conn: sqlite3.Connection) -> None:
        """Register every generic vocabulary term.

        This seeds `_classes_by_term` with an empty set for the term (so it
        shows up as "known" even before any class is attached) and seeds
        `_canonical_by_surface` with a self-mapping, matching how ingredient
        names behave.
        """
        rows = conn.execute(f"SELECT term FROM {self._TERM_TABLE}").fetchall()

        for row in rows:
            term = self._normalize(row["term"])
            self._classes_by_term[term]  # noqa: B018 -- touch to register via defaultdict
            self._canonical_by_surface.setdefault(term, term)

    def _load_vocabulary_term_classes(self, conn: sqlite3.Connection) -> None:
        """Populate the many-to-many term <-> class relationship.

        This is a straight JOIN across the junction table back to both
        `vocabulary_terms` and `vocabulary_classes`, resolving term_id/
        class_id FKs to their text values in one query rather than keeping
        separate id-keyed lookup tables around afterward.

        Unlike the old single-class model, this never raises on a term
        gaining a second class -- that's the whole point of the junction
        table. A term legitimately can be tagged both "ingredient" and
        "brand" (etc.); this loader exposes that, it doesn't pick a winner.
        """
        rows = conn.execute(
            f"""
            SELECT
                vt.term        AS term,
                vc.class_name  AS class_name
            FROM {self._TERM_CLASS_TABLE} vtc
            JOIN {self._TERM_TABLE} vt ON vtc.term_id = vt.term_id
            JOIN {self._CLASS_TABLE} vc ON vtc.class_id = vc.class_id
            """
        ).fetchall()

        for row in rows:
            term = self._normalize(row["term"])
            class_name = row["class_name"]

            self._classes_by_term[term].add(class_name)
            self._members_by_class[class_name].add(term)

    def _load_ingredients(self, conn: sqlite3.Connection) -> None:
        """Populate canonical ingredient structures from the `ingredients`
        table.

        Ingredients get folded into the exact same shared indexes
        (`_classes_by_term`, `_members_by_class`, `_canonical_by_surface`)
        as everything loaded from the generic vocabulary tables, always
        tagged with class "ingredient" -- there's no separate downstream
        code path (phrase indexes, public views, `phrase_index_for`, etc.)
        that needs to know ingredients came from a different table.

        Because `_classes_by_term` is many-to-many, an ingredient name that
        also happens to be tagged as, say, "brand" via
        `vocabulary_term_classes` is NOT a conflict -- it simply carries
        both classes. Nothing here raises on that.
        """
        rows = conn.execute(
            f"SELECT ingredient_name FROM {self._INGREDIENT_TABLE}"
        ).fetchall()

        for row in rows:
            term = self._normalize(row["ingredient_name"])

            self._classes_by_term[term].add(self._INGREDIENT_VOCABULARY_CLASS)
            self._members_by_class[self._INGREDIENT_VOCABULARY_CLASS].add(term)
            self._canonical_by_surface.setdefault(term, term)

    def _load_ingredient_aliases(self, conn: sqlite3.Connection) -> None:
        """Populate both lookup directions for each ingredient alias.

        This is the one place in the loader where "alias" means what it
        means in this codebase: an alternate name for an identical
        ingredient (e.g. "scallions" / "green onion"), never a generic
        vocabulary concept.

        Resolves the FK via `ingredient_id` against `ingredients.id`.
        `confidence`/`source` on `ingredient_aliases` are provenance
        columns -- not this loader's concern -- so every alias row is
        loaded regardless of confidence.
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
                    f"Ingredient alias conflict: alias '{alias}' "
                    f"references ingredient_id {row['ingredient_id']!r}, "
                    "which does not exist in ingredients (orphaned alias)."
                )

            canonical = self._normalize(row["canonical_name"])

            existing_canonical = self._ingredient_alias_to_canonical.get(alias)
            if existing_canonical is not None and existing_canonical != canonical:
                raise RuntimeError(
                    f"Ingredient alias conflict: alias '{alias}' maps "
                    f"to both '{existing_canonical}' and '{canonical}'."
                )

            self._ingredient_alias_to_canonical[alias] = canonical
            self._canonical_by_surface[alias] = canonical

            self._classes_by_term[alias].add(self._INGREDIENT_VOCABULARY_CLASS)
            self._members_by_class[self._INGREDIENT_VOCABULARY_CLASS].add(alias)

    def _load_relationships(self, conn: sqlite3.Connection) -> None:
        """Read every relationship row exactly once and freeze it into an
        immutable `Relationship`, with no interpretation whatsoever.

        This deliberately does NOT:
          - look up subject_id/object_id against vocabulary_terms,
            ingredients, or any other table,
          - decide a relationship is "invalid" because an endpoint isn't
            currently resolvable,
          - synthesize an inverse relationship,
          - or infer anything from the predicate string.

        A relationship endpoint is a typed identifier (subject_type +
        subject_id), not necessarily a currently-resolvable entity. The
        builder already made the persistence decision; this just loads the
        resulting rows as-is.
        """
        rows = conn.execute(
            f"""
            SELECT
                relationship_id, subject_type, subject_id, predicate,
                object_type, object_id, source, confidence, created_at
            FROM {self._RELATIONSHIP_TABLE}
            """
        ).fetchall()

        relationships = [
            Relationship(
                relationship_id=row["relationship_id"],
                subject_type=row["subject_type"],
                subject_id=row["subject_id"],
                predicate=row["predicate"],
                object_type=row["object_type"],
                object_id=row["object_id"],
                source=row["source"],
                confidence=row["confidence"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

        self._relationships_raw = tuple(relationships)

    # -------------------------------------------------------------------------
    # Derived index construction (in-memory only, runs once)
    # -------------------------------------------------------------------------

    def _build_phrase_indexes(self) -> None:
        """Precompute everything the lexer would otherwise have to derive
        itself: a token-split phrase for every known surface form (vocabulary
        term, ingredient name, or ingredient alias), ordered longest-first,
        and bucketed by first token.

        Because a surface form can carry more than one class (many-to-many),
        it can produce more than one `PhraseMatch` -- one per class -- all
        sharing the same tokens. This is what backs `find_phrases_starting_with`
        and `phrases_longest_first`. The lexer should never need to split or
        sort vocabulary on its own.
        """
        matches: list[PhraseMatch] = []
        seen: set[tuple[tuple[str, ...], str]] = set()

        for surface, classes in self._classes_by_term.items():
            if not classes:
                continue  # a registered term with no class tag isn't lexically taggable yet

            tokens = tuple(t for t in _TOKEN_SPLIT_RE.split(surface.strip()) if t)
            if not tokens:
                continue

            canonical = self._canonical_by_surface.get(surface, surface)

            for vocabulary_class in classes:
                key = (tokens, vocabulary_class)
                if key in seen:
                    continue
                seen.add(key)
                matches.append(
                    PhraseMatch(tokens=tokens, canonical=canonical, vocabulary_class=vocabulary_class)
                )

        # Longest phrase (by token count) first; alphabetical as a stable
        # tiebreaker so iteration order doesn't depend on dict/set ordering
        # quirks between runs.
        matches.sort(key=lambda m: (-m.token_count, m.tokens, m.vocabulary_class))

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

    def _build_relationship_indexes(self) -> None:
        """Precompute the four lookup shapes the required query patterns
        need, mirroring the database's own indexes -- but these are the
        runtime, in-memory versions; the DB's indexes are irrelevant once
        `_load` has closed the connection.

        Each index maps a structural key straight to a tuple of matching
        `Relationship` objects, so `relationships_for_subject`/
        `relationships_for_object`/`find_relationships` never scan the
        full relationship set for the patterns these indexes cover.
        """
        by_subject: DefaultDict[tuple[str, str], list[Relationship]] = defaultdict(list)
        by_object: DefaultDict[tuple[str, str], list[Relationship]] = defaultdict(list)
        by_subject_predicate: DefaultDict[tuple[str, str, str], list[Relationship]] = defaultdict(list)
        by_object_predicate: DefaultDict[tuple[str, str, str], list[Relationship]] = defaultdict(list)

        for relationship in self._relationships_raw:
            by_subject[(relationship.subject_type, relationship.subject_id)].append(relationship)
            by_object[(relationship.object_type, relationship.object_id)].append(relationship)
            by_subject_predicate[
                (relationship.subject_type, relationship.subject_id, relationship.predicate)
            ].append(relationship)
            by_object_predicate[
                (relationship.object_type, relationship.object_id, relationship.predicate)
            ].append(relationship)

        self._relationships_by_subject: Mapping[tuple[str, str], tuple[Relationship, ...]] = (
            MappingProxyType({key: tuple(rels) for key, rels in by_subject.items()})
        )
        self._relationships_by_object: Mapping[tuple[str, str], tuple[Relationship, ...]] = (
            MappingProxyType({key: tuple(rels) for key, rels in by_object.items()})
        )
        self._relationships_by_subject_predicate: Mapping[
            tuple[str, str, str], tuple[Relationship, ...]
        ] = MappingProxyType({key: tuple(rels) for key, rels in by_subject_predicate.items()})
        self._relationships_by_object_predicate: Mapping[
            tuple[str, str, str], tuple[Relationship, ...]
        ] = MappingProxyType({key: tuple(rels) for key, rels in by_object_predicate.items()})

    def _build_public_views(self) -> None:
        """Freeze the class-tagged vocabulary buckets into the named,
        immutable attributes the lexer is expected to use directly (e.g.
        `knowledge.measurements`, `knowledge.ingredients`).

        These are precomputed once here rather than recomputed on every
        access, in keeping with the "one-time startup cost, zero-cost
        repeated lookups" performance goal.
        """
        # term -> frozenset of every class it carries. Replaces the old
        # single-class `names` mapping; a term can legitimately have more
        # than one entry here.
        self.term_classes: Mapping[str, frozenset[str]] = MappingProxyType(
            {term: frozenset(classes) for term, classes in self._classes_by_term.items()}
        )

        # alias -> canonical ingredient name. TRUE ingredient aliases only
        # (e.g. "scallions" -> "green onion") -- not a generic vocabulary
        # concept, and does not include self-mapped canonical entries.
        self.ingredient_aliases: Mapping[str, str] = MappingProxyType(
            dict(self._ingredient_alias_to_canonical)
        )

        # Every class name actually declared in vocabulary_classes, plus
        # "ingredient" (which has no row there -- it's implicit), plus
        # anything that ended up with members some other way. This is the
        # enumeration hook for a stage that needs to sweep "every category
        # I don't already have a named attribute for."
        self.vocabulary_classes: frozenset[str] = frozenset(
            self._declared_classes | self._members_by_class.keys()
        )

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
        # If no term is tagged "grammar" yet, this is simply an empty
        # frozenset -- no lexer-side hardcoded list, and no loader change
        # required once grammar-tagged terms exist.
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

        # All loaded relationship assertions, exactly as persisted -- no
        # resolution, no inference, no inverses. See `Relationship`.
        self.relationships: tuple[Relationship, ...] = self._relationships_raw

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
    # Relationship lookup
    #
    # These expose persisted relationship assertions exactly as loaded.
    # There is no resolution of subject_id/object_id, no inverse-relationship
    # synthesis, and no interpretation of what a predicate means -- an
    # unresolved endpoint (e.g. an ingredient/vocabulary term with no
    # matching entry elsewhere) and "no relationships found" are both
    # ordinary outcomes, never errors.
    # -------------------------------------------------------------------------

    def relationships_for_subject(
        self, subject_type: str, subject_id: str
    ) -> tuple[Relationship, ...]:
        """Every relationship whose subject matches both fields, or `()` if
        none do. Subject existing/not-existing elsewhere is irrelevant --
        this is a lookup against loaded relationship rows only."""
        return self._relationships_by_subject.get((subject_type, subject_id), ())

    def relationships_for_object(
        self, object_type: str, object_id: str
    ) -> tuple[Relationship, ...]:
        """Every relationship whose object matches both fields, or `()` if
        none do."""
        return self._relationships_by_object.get((object_type, object_id), ())

    def find_relationships(
        self,
        *,
        subject_type: str | None = None,
        subject_id: str | None = None,
        predicate: str | None = None,
        object_type: str | None = None,
        object_id: str | None = None,
    ) -> tuple[Relationship, ...]:
        """Filter relationships by any combination of the five structural
        fields; every argument is optional, and calling this with none of
        them returns every loaded relationship.

        This is a structural filter only -- it matches fields exactly
        against what was persisted. It does not interpret predicates, does
        not search for inverse matches, and does not require any endpoint
        to be resolvable elsewhere. A call that matches nothing returns
        `()`, not an error.

        Picks whichever precomputed index (subject+predicate,
        subject-only, object+predicate, object-only) is most specific for
        the arguments given, to avoid a full scan for the common query
        shapes; the remaining criteria (if any) are then applied directly
        so the result is correct regardless of which index was used.
        """
        # Note: these branches deliberately re-check `is not None` inline
        # (rather than via a bool computed above) so static type checkers
        # can narrow subject_type/subject_id/object_type/object_id from
        # `str | None` to `str` right where each tuple key is built.
        if subject_type is not None and subject_id is not None and predicate is not None:
            candidates = self._relationships_by_subject_predicate.get(
                (subject_type, subject_id, predicate), ()
            )
        elif subject_type is not None and subject_id is not None:
            candidates = self._relationships_by_subject.get((subject_type, subject_id), ())
        elif object_type is not None and object_id is not None and predicate is not None:
            candidates = self._relationships_by_object_predicate.get(
                (object_type, object_id, predicate), ()
            )
        elif object_type is not None and object_id is not None:
            candidates = self._relationships_by_object.get((object_type, object_id), ())
        else:
            candidates = self.relationships

        def _matches(relationship: Relationship) -> bool:
            if subject_type is not None and relationship.subject_type != subject_type:
                return False
            if subject_id is not None and relationship.subject_id != subject_id:
                return False
            if predicate is not None and relationship.predicate != predicate:
                return False
            if object_type is not None and relationship.object_type != object_type:
                return False
            if object_id is not None and relationship.object_id != object_id:
                return False
            return True

        return tuple(relationship for relationship in candidates if _matches(relationship))

    # -------------------------------------------------------------------------
    # Canonical / classification lookup
    # -------------------------------------------------------------------------

    def resolve_ingredient_alias(self, term: str) -> str:
        """If `term` is a known ingredient alias, return the canonical
        ingredient name it points to. Otherwise return `term` unchanged.

        This is ingredient-specific by design -- generic vocabulary terms
        have no alias concept to resolve.
        """
        return self._ingredient_alias_to_canonical.get(self._normalize(term), term)

    def classes_for(self, term: str) -> frozenset[str]:
        """Every class `term` is tagged with, or an empty frozenset if the
        term is unknown or has no class tag yet.

        A term can belong to more than one class at once -- this does not
        collapse that down to a single answer the way the old single-class
        model did.
        """
        return frozenset(self._classes_by_term.get(self._normalize(term), ()))

    def contains(self, term: str) -> bool:
        return self._normalize(term) in self._classes_by_term

    def _is_class(self, term: str, vocabulary_class: str) -> bool:
        return vocabulary_class in self._classes_by_term.get(self._normalize(term), ())

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

    def is_grammar(self, term: str) -> bool:
        return self._is_class(term, "grammar")

    # -------------------------------------------------------------------------
    # BREAKING CHANGE / migration note (schema move: culinary_vocabulary /
    # culinary_aliases -> vocabulary_terms / vocabulary_classes /
    # vocabulary_term_classes)
    # -------------------------------------------------------------------------
    #
    # The single-class model is gone. A term can now carry more than one
    # class at once (that's what the vocabulary_term_classes junction table
    # is for), so anything that assumed "one term, one class" changed:
    #
    #     knowledge.vocabulary_class(term) -> str | None   REMOVED
    #     knowledge.classes_for(term) -> frozenset[str]     replacement
    #
    #     knowledge.names                                   REMOVED
    #         (was a single-class term -> class mapping; no longer valid)
    #         Use `classes_for(term)` per term, or `vocabulary_classes` for
    #         the set of all declared class names.
    #
    # "Alias" is now strictly an ingredient concept -- generic vocabulary
    # terms never had a real alias table backing them (culinary_aliases was
    # unused), so the old generic surface was misleading and is gone:
    #
    #     knowledge.aliases -> Mapping[str, str]             REMOVED
    #     knowledge.resolve_alias(term) -> str                REMOVED
    #     knowledge.canonical(term) -> str                    REMOVED
    #     knowledge.ingredient_aliases -> Mapping[str, str]    replacement
    #         (alias -> canonical ingredient name, TRUE aliases only)
    #     knowledge.resolve_ingredient_alias(term) -> str      replacement
    #
    # The class itself was renamed (the "culinary" prefix was redundant --
    # this is a culinary app, everything here is culinary):
    #
    #     CulinaryVocabulary  ->  RuntimeKnowledge
    #
    # `CulinaryVocabulary` is kept below as a deprecated alias so an import
    # by the old name doesn't hard-break, but new code should use
    # `RuntimeKnowledge` (or, in the overwhelming majority of cases, just
    # the `knowledge` singleton -- nothing should be constructing this
    # class directly).
    #
    # Everything from the prior migration note (`.measurements()` etc. ->
    # `.measurements` attributes) still applies unchanged.


# Deprecated alias -- see the migration note above. Prefer `RuntimeKnowledge`.
CulinaryVocabulary = RuntimeKnowledge


# ---------------------------------------------------------------------------
# The single runtime instance. Every downstream consumer -- lex.py included
# -- should import this rather than constructing its own RuntimeKnowledge,
# so there is exactly one in-memory copy of the knowledge and exactly one
# load from SQLite per process.
# ---------------------------------------------------------------------------
knowledge = RuntimeKnowledge()

__all__ = [
    "RuntimeKnowledge",
    "CulinaryVocabulary",
    "PhraseMatch",
    "Relationship",
    "UNICODE_FRACTIONS",
    "knowledge",
]