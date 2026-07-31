"""
USDA Culinary Observation Recorder.

Records distinct USDA modifiers as culinary_observations rows,
provenance preserved, wording untouched.

This module does NOT normalize meaning, decompose compound text, or
promote anything to culinary_vocabulary/culinary_aliases. That was a
prior design of this file and has been removed: this builder's only
job is observation, and classification is a separate concern for a
separate builder to own later.

Filtering: usda_food_portions.modifier is dominated by noise (candy
brand names, novelty products, packaging text) that has nothing to do
with cooking. Rather than record all ~1,800 distinct modifiers
indiscriminately, this builder only records a modifier if it's
attached to at least one food (usda_foods, joined on fdc_id) whose
description contains a known ingredient name or alias (dim.ingredients
/ dim.ingredient_aliases). This is a relevance filter, not a
classification step — it answers "is this food-related at all", not
"what culinary concept is this."

The representative source_record_id recorded for a modifier is drawn
from a food row that actually matched the filter, not an arbitrary
MIN(id) across all rows sharing that modifier text (which could easily
land on a candy product for a modifier like "chopped" or "sliced").

Idempotent: running against the same usda_food_portions data twice
inserts the same rows once and reports zero new rows the second time.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Dict, Optional, Set

from gastrometric.config.paths import DATA_DIR

from ..builder import KnowledgeBuilder
from ..models import BuildResult
from ..observations import insert_observation, normalize_modifier_text
from ..sources import ensure_sources, get_source_id

SOURCE_NAME = "USDA FoodData Central"
FIELD_NAME = "modifier"
SOURCE_RECORD_TYPE = "usda_food_portions"

_SEED_DIR = DATA_DIR / "seed"
DEFAULT_SOURCES_SEED = _SEED_DIR / "culinary_sources.json"


def _load_ingredient_terms(conn: sqlite3.Connection) -> Set[str]:
    """Union of known ingredient names and aliases, lowercased, used
    only as a relevance filter against food descriptions — never
    written back to the database from here."""
    terms: Set[str] = set()

    cur = conn.execute("SELECT ingredient_name FROM ingredients WHERE ingredient_name IS NOT NULL")
    for (name,) in cur.fetchall():
        stripped = name.strip().lower()
        if stripped:
            terms.add(stripped)

    cur = conn.execute("SELECT alias FROM ingredient_aliases WHERE alias IS NOT NULL")
    for (alias,) in cur.fetchall():
        stripped = alias.strip().lower()
        if stripped:
            terms.add(stripped)

    return terms


def _build_ingredient_pattern(terms: Set[str]) -> Optional[re.Pattern]:
    """One compiled alternation over all known ingredient terms, longest
    first so multi-word ingredients aren't shadowed by a shorter
    substring also present in the term set."""
    if not terms:
        return None
    ordered = sorted(terms, key=len, reverse=True)
    alternation = "|".join(re.escape(term) for term in ordered)
    return re.compile(r"\b(?:" + alternation + r")\b", re.IGNORECASE)


class UsdaVocabularyBuilder(KnowledgeBuilder):
    name = "USDA Culinary Observation Recorder"

    def __init__(self, sources_seed=DEFAULT_SOURCES_SEED):
        self.sources_seed = sources_seed

    def run(self, conn: sqlite3.Connection) -> BuildResult:
        result = BuildResult(builder_name=self.name)

        ensure_sources(conn, self.sources_seed)
        source_id = get_source_id(conn, SOURCE_NAME)

        ingredient_terms = _load_ingredient_terms(conn)
        pattern = _build_ingredient_pattern(ingredient_terms)

        representative_ids = self._find_representative_matches(conn, pattern)
        result.distinct_inputs = len(representative_ids)

        for modifier, representative_portion_id in representative_ids.items():
            normalized_text = normalize_modifier_text(modifier)

            _, inserted = insert_observation(
                conn,
                raw_text=modifier,
                normalized_text=normalized_text,
                source_id=source_id,
                source_record_id=str(representative_portion_id),
                field_name=FIELD_NAME,
                source_record_type=SOURCE_RECORD_TYPE,
            )
            if inserted:
                result.observations_inserted += 1

        conn.commit()
        return result

    @staticmethod
    def _find_representative_matches(
        conn: sqlite3.Connection, pattern: Optional[re.Pattern]
    ) -> Dict[str, int]:
        """For each distinct modifier, find the lowest-id portion row
        whose associated food description matches a known ingredient
        term. Modifiers with no matching food anywhere are dropped
        entirely — they never reach observation.
        """
        if pattern is None:
            # No ingredient vocabulary loaded at all — nothing can be
            # judged relevant, so record nothing rather than falling
            # back to "record everything".
            return {}

        cur = conn.execute(
            """
            SELECT p.modifier, p.usda_portion_id, f.description
            FROM usda_food_portions p
            JOIN usda_foods f ON f.fdc_id = p.fdc_id
            WHERE p.modifier IS NOT NULL AND TRIM(p.modifier) != ''
            ORDER BY p.modifier, p.usda_portion_id
            """
        )

        representative_ids: Dict[str, int] = {}
        for modifier, portion_id, description in cur.fetchall():
            if modifier in representative_ids:
                continue  # already found this modifier's lowest matching id
            if description and pattern.search(description):
                representative_ids[modifier] = portion_id

        return representative_ids


def build(conn: sqlite3.Connection) -> BuildResult:
    """Convenience entry point for callers that already have a connection."""
    return UsdaVocabularyBuilder().run(conn)


def build_usda_vocabulary() -> BuildResult:
    """Self-contained entry point matching the other pipeline steps in
    orchestration/rebuild_db.py (init_db, ingest_markdown, ...), none of
    which take a conn argument — this opens and closes its own connection
    against the project's configured DB_PATH."""
    from gastrometric.config.paths import DB_PATH

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        result = UsdaVocabularyBuilder().run(conn)
        print(result.render())
        return result
    finally:
        conn.close()


if __name__ == "__main__":
    build_usda_vocabulary()