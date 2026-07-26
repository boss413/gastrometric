"""
Ingest data/nutrition_mappings.json into the nutrition mapping tables.

Usage (consistent with other Gastrometric pipeline stages, e.g.
gastrometric.db.create_views.create_views):

    from gastrometric.pipeline.enrichment.ingest_mappings import ingest_nutrition_mappings
    ingest_nutrition_mappings()

Only mappings with status == "approved" are persisted. Mappings that are
"unresolved", carry any other status, or are absent from the JSON file
entirely are skipped. This module does not resolve quantities or compute
nutrition -- it only persists the approved ingredient -> USDA mapping and
its declared portions.
"""

import json
import sqlite3
from dataclasses import dataclass, field

from gastrometric.config.paths import DB_PATH, NUTRITION_MAPPINGS_JSON_PATH

APPROVED_STATUS = "approved"


@dataclass
class IngestionResult:
    approved_ingested: int = 0
    skipped_unresolved: int = 0
    skipped_unknown_status: int = 0
    skipped_ingredient_not_found: int = 0
    skipped_no_fdc_id: int = 0
    skipped_malformed_entry: int = 0
    skipped_portion_not_found: int = 0
    skipped_portion_fdc_mismatch: int = 0
    portions_ingested: int = 0
    diagnostics: list = field(default_factory=list)


def _connect(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _resolve_ingredient_id(conn: sqlite3.Connection, mapping_key: str):
    """Resolve a nutrition_mappings.json key to a canonical ingredients.id.

    Tries an exact match against ingredients.ingredient_name first (the
    mapping keys are written as canonical ingredient names), then falls
    back to ingredient_aliases. Returns None if no ingredient can be
    resolved.
    """
    row = conn.execute(
        "SELECT id FROM ingredients WHERE ingredient_name = ?",
        (mapping_key,),
    ).fetchone()
    if row:
        return row[0]

    row = conn.execute(
        "SELECT ingredient_id FROM ingredient_aliases WHERE alias = ?",
        (mapping_key,),
    ).fetchone()
    if row:
        return row[0]

    return None


def _get_portion_fdc_id(conn: sqlite3.Connection, usda_portion_id):
    """Look up the fdc_id a usda_food_portions row belongs to.

    Returns None if the portion id doesn't exist at all.
    """
    row = conn.execute(
        "SELECT fdc_id FROM usda_food_portions WHERE usda_portion_id = ?",
        (usda_portion_id,),
    ).fetchone()
    return row[0] if row else None


def _load_mappings(mappings_path) -> dict:
    """Load nutrition_mappings.json.

    The file may be a flat dict of ingredient entries, or a versioned
    envelope of the form {"schema_version": ..., "description": ...,
    "mappings": {...}}. If a top-level "mappings" key is present and is
    itself a dict, treat that as the entries to ingest; otherwise treat
    the whole document as the flat entries dict.
    """
    with open(mappings_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and isinstance(data.get("mappings"), dict):
        return data["mappings"]
    return data


def ingest_nutrition_mappings(
    db_path=DB_PATH,
    mappings_path=NUTRITION_MAPPINGS_JSON_PATH,
) -> IngestionResult:
    """Ingest approved entries from nutrition_mappings.json.

    Assumes gastrometric.db.init_db.init_db() has already been run against
    db_path, and that nutrition_ingredient_mappings /
    nutrition_mapping_portions are empty or being rebuilt (see
    gastrometric.db.rebuild_nutrition). This function does not clear
    tables itself so it can also be used for incremental/test scenarios.
    """
    mappings = _load_mappings(mappings_path)
    result = IngestionResult()

    conn = _connect(db_path)
    try:
        seen_ingredient_ids = {}  # ingredient_id -> mapping_key, for dedup diagnostics

        for mapping_key, entry in mappings.items():
            if not isinstance(entry, dict):
                result.skipped_malformed_entry += 1
                result.diagnostics.append(
                    f"'{mapping_key}': expected a mapping object, got "
                    f"{type(entry).__name__} ({entry!r}); skipped"
                )
                continue

            status = entry.get("status")

            if status != APPROVED_STATUS:
                if status == "unresolved":
                    result.skipped_unresolved += 1
                else:
                    result.skipped_unknown_status += 1
                continue

            ingredient_id = _resolve_ingredient_id(conn, mapping_key)
            if ingredient_id is None:
                result.skipped_ingredient_not_found += 1
                result.diagnostics.append(
                    f"'{mapping_key}': approved mapping but no matching "
                    f"ingredient found in ingredients/ingredient_aliases"
                )
                continue

            source = entry.get("source")
            default_fdc_id = entry.get("source_id")
            state_mappings = entry.get("state_mappings", {}) or {}
            raw_state = state_mappings.get("raw") or {}
            cooked_state = state_mappings.get("cooked") or {}
            raw_fdc_id = raw_state.get("source_id")
            cooked_fdc_id = cooked_state.get("source_id")

            if default_fdc_id is None and raw_fdc_id is None:
                # Nothing usable as a raw/default state -- can't be used
                # by the nutrition pipeline even though it was "approved".
                result.skipped_no_fdc_id += 1
                result.diagnostics.append(
                    f"'{mapping_key}': approved but has neither source_id "
                    f"nor state_mappings.raw"
                )
                continue

            if ingredient_id in seen_ingredient_ids:
                result.diagnostics.append(
                    f"'{mapping_key}': ingredient_id {ingredient_id} was "
                    f"already mapped by '{seen_ingredient_ids[ingredient_id]}'; "
                    f"keeping the first mapping and skipping this one"
                )
                continue
            seen_ingredient_ids[ingredient_id] = mapping_key

            cur = conn.execute(
                """
                INSERT INTO nutrition_ingredient_mappings (
                    ingredient_id, mapping_key, status, source,
                    default_fdc_id, raw_fdc_id, cooked_fdc_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingredient_id,
                    mapping_key,
                    APPROVED_STATUS,
                    source,
                    default_fdc_id,
                    raw_fdc_id,
                    cooked_fdc_id,
                ),
            )
            mapping_id = cur.lastrowid
            result.approved_ingested += 1

            # A portion is valid for this mapping only if it belongs to
            # one of the fdc_ids this mapping actually resolves to (its
            # default/raw/cooked food). This catches a wrong
            # usda_portion_id in the JSON (e.g. copied from a different
            # food) as a diagnostic instead of silently linking it.
            valid_fdc_ids = {
                fdc_id
                for fdc_id in (default_fdc_id, raw_fdc_id, cooked_fdc_id)
                if fdc_id is not None
            }

            # Default-style mappings declare `portions` at the top level;
            # state-specific mappings declare them nested under each state
            # instead (there is no top-level `portions` key in that shape).
            # Read from wherever is applicable -- exactly one of these will
            # be non-empty for any given entry.
            portions = list(entry.get("portions", []) or [])
            portions += list(raw_state.get("portions", []) or [])
            portions += list(cooked_state.get("portions", []) or [])

            for portion in portions:
                usda_portion_id = portion.get("usda_portion_id")
                portion_fdc_id = _get_portion_fdc_id(conn, usda_portion_id)

                if portion_fdc_id is None:
                    result.skipped_portion_not_found += 1
                    result.diagnostics.append(
                        f"'{mapping_key}': usda_portion_id "
                        f"{usda_portion_id!r} not found in "
                        f"usda_food_portions; portion skipped"
                    )
                    continue

                if portion_fdc_id not in valid_fdc_ids:
                    result.skipped_portion_fdc_mismatch += 1
                    result.diagnostics.append(
                        f"'{mapping_key}': usda_portion_id {usda_portion_id} "
                        f"belongs to fdc_id {portion_fdc_id}, which is not "
                        f"one of this mapping's fdc_ids {sorted(valid_fdc_ids)}; "
                        f"portion skipped"
                    )
                    continue

                conn.execute(
                    """
                    INSERT INTO nutrition_mapping_portions (
                        mapping_id, usda_portion_id, modifier, notes
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        mapping_id,
                        usda_portion_id,
                        portion.get("modifier"),
                        portion.get("notes"),
                    ),
                )
                result.portions_ingested += 1

        conn.commit()
    finally:
        conn.close()

    return result


if __name__ == "__main__":
    outcome = ingest_nutrition_mappings()
    print(outcome)