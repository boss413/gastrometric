"""Shared builder interface.

Every ingestion/builders/*.py module implements this so rebuild_db.py can
orchestrate them uniformly without knowing their internals.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable, List

from .models import BuildResult


class KnowledgeBuilder:
    name: str = "base"

    def run(self, conn: sqlite3.Connection) -> BuildResult:
        raise NotImplementedError


def run_builders(conn: sqlite3.Connection, builders: Iterable[KnowledgeBuilder]) -> List[BuildResult]:
    return [builder.run(conn) for builder in builders]


def _all_builders() -> List[KnowledgeBuilder]:
    """The full set of knowledge builders, in the order they should run.

    Seed builders come first since they're the authoritative baseline;
    evidence builders (once they exist) run after and layer observations
    on top. Add new builders here as they're implemented -- this is the
    single place rebuild_db.py's entry point needs to know about.
    """
    from .builders.seed_culinary_vocabulary_builder import SeedCulinaryVocabularyBuilder

    return [
        SeedCulinaryVocabularyBuilder(),
        # SeedIngredientsBuilder(),
        # SeedRelationshipsBuilder(),
        # UsdaObservationsBuilder(),
        # RecipeObservationsBuilder(),
        # ParserFailureObservationsBuilder(),
    ]


def rebuild_knowledge() -> List[BuildResult]:
    """Self-contained entry point for rebuild_db.py.

    Opens its own connection, runs every registered knowledge builder in
    order, prints each one's summary, and returns the results. This is
    the only knowledge-layer symbol rebuild_db.py should need to import --
    individual builders stay internal to this package.
    """
    from gastrometric.config.paths import DB_PATH

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        results = run_builders(conn, _all_builders())
        for result in results:
            print(result.render())
        return results
    finally:
        conn.close()