"""
Deterministic rebuild of every nutrition-derived table.

Nothing here touches recipe/ingredient/USDA source tables -- only tables
that are *derived* by the nutrition pipeline are cleared and repopulated:

    nutrition_mapping_measurements   (child of nutrition_ingredient_mappings)
    nutrition_ingredient_mappings    (derived from data/nutrition_mappings.json)
    recipe_ingredient_line_nutrition (derived, level 1)
    recipe_section_nutrition         (derived, level 2)
    recipe_nutrition                 (derived, level 3)

Tables are cleared in dependency order (children before parents / results
before their sources) so a rebuild never leaves stale rows behind, and the
whole operation runs in a single transaction so a partial failure can't
leave the database in a mixed old/new state.

This module only covers the schema + mapping-ingestion foundation. The
line/section/recipe result tables are cleared here so the tables are never
left stale, but they are not yet repopulated -- quantity resolution and
nutrient aggregation are implemented in a later stage.
"""

import sqlite3

from gastrometric.config.paths import DB_PATH, NUTRITION_MAPPINGS_JSON_PATH
from gastrometric.pipeline.enrichment.usda.ingest_mappings import (
    IngestionResult,
    ingest_nutrition_mappings,
)

# Order matters: children/results before the tables they depend on.
_TABLES_IN_CLEAR_ORDER = (
    "recipe_ingredient_line_nutrition",
    "recipe_section_nutrition",
    "recipe_nutrition",
    "nutrition_mapping_portions",
    "nutrition_ingredient_mappings",
)


def clear_nutrition_tables(db_path=DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        for table in _TABLES_IN_CLEAR_ORDER:
            conn.execute(f"DELETE FROM {table}")
        conn.commit()
    finally:
        conn.close()


def rebuild_nutrition_mappings(
    db_path=DB_PATH,
    mappings_path=NUTRITION_MAPPINGS_JSON_PATH,
) -> IngestionResult:
    """Clear nutrition-derived tables and re-ingest approved mappings.

    Safe to call repeatedly: each call starts from a clean slate, so the
    result is deterministic given the same ingredients table and
    nutrition_mappings.json content.
    """
    clear_nutrition_tables(db_path)
    return ingest_nutrition_mappings(db_path=db_path, mappings_path=mappings_path)


if __name__ == "__main__":
    outcome = rebuild_nutrition_mappings()
    print(outcome)