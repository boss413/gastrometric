"""
Runtime culinary vocabulary loader.

This module provides the single runtime API for accessing culinary vocabulary.
Vocabulary is loaded once from SQLite and all subsequent lookups occur entirely
in memory.

This module is intentionally read-only. It has no business reading JSON seed
data — that is knowledge-building territory (the USDA/seed builders), not the
runtime contract.

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
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict

from gastrometric.config.paths import DB_PATH


class CulinaryVocabulary:
    _VOCAB_TABLE = "culinary_vocabulary"
    _VOCAB_COLUMNS = ("vocabulary_id", "term", "vocabulary_class")

    _ALIAS_TABLE = "culinary_aliases"
    _ALIAS_COLUMNS = ("alias_id", "vocabulary_id", "alias_text")

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self._canonical_by_alias: dict[str, str] = {}
        self._class_by_canonical: dict[str, str] = {}
        self._members_by_class: DefaultDict[str, set[str]] = defaultdict(set)

        self._load(db_path)

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

        finally:
            conn.close()

    def _validate_schema(self, conn: sqlite3.Connection) -> None:
        """Fail fast if the DB doesn't have the tables/columns this loader
        depends on, rather than silently loading an empty vocabulary."""
        for table, required_columns in (
            (self._VOCAB_TABLE, self._VOCAB_COLUMNS),
            (self._ALIAS_TABLE, self._ALIAS_COLUMNS),
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

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize(term: str) -> str:
        return term.casefold()

    def _class_members(self, vocabulary_class: str) -> set[str]:
        return set(self._members_by_class.get(vocabulary_class, set()))

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
    # Classification helpers
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

    # -------------------------------------------------------------------------
    # Vocabulary accessors
    # -------------------------------------------------------------------------

    def measurements(self) -> set[str]:
        return self._class_members("measurement")

    def natural_portions(self) -> set[str]:
        return self._class_members("natural_portion")

    def preparations(self) -> set[str]:
        return self._class_members("preparation")

    def temperatuure(self) -> set[str]:
        return self._class_members("temperature")

    def packaging(self) -> set[str]:
        return self._class_members("packaging")

    def sizes(self) -> set[str]:
        return self._class_members("size")

    def descriptors(self) -> set[str]:
        return self._class_members("descriptor")
    
    def modifier(self) -> set[str]:
        return self._class_members("modifier")

    def brand(self) -> set[str]:
        return self._class_members("brand")
    
    def state(self) -> set[str]:
        return self._class_members("state")

    def seasoning(self) -> set[str]:
        return self._class_members("seasoning")
    
    def shapes(self) -> set[str]:
        return self._class_members("shape")
    
    def ingredient_forms(self) -> set[str]:
        return self._class_members("ingredient_form")