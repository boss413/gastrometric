"""
gastrometric.pipeline.enrichment.usda.calculate_nutrition

Prompt 3: Nutrition calculation and aggregation.

Reads the ingredient-line quantity resolution produced by Prompt 2
(`recipe_ingredient_line_nutrition`) together with the USDA nutrient
source data (`usda_nutrients`), and produces:

    * recipe_ingredient_line_nutrients  (line-level, all USDA nutrients)
    * recipe_section_nutrition / recipe_section_nutrients (section totals)
    * recipe_nutrition / recipe_nutrients (whole-recipe totals)

Design notes
------------
* The normalized `*_nutrients` child tables are the authoritative
  nutrition data store — every USDA nutrient, keyed by `nutrient_id`,
  is preserved there regardless of whether it feeds a fixed column.
* The fixed summary columns (calories, protein, ...) are populated as a
  convenience projection, but the projection is *derived at runtime* by
  resolving each summary field to a `nutrient_id` via an exact,
  case-insensitive match against the `nutrient_name` values already
  present in this database's own `usda_nutrients` table (see
  `_resolve_summary_nutrient_ids`). No numeric USDA nutrient ID is
  hardcoded: hardcoding a literal ID risks silently mismatching if the
  ingested USDA dataset uses different IDs than assumed. If a field's
  expected name isn't found in the data, or matches more than one
  distinct nutrient_id, that column is left NULL and the mismatch is
  reported in the run summary rather than guessed.
* `nutrient_id` is the sole stable identity used for grouping/aggregation
  once resolved; the one-time name lookup above is only used to locate
  the ID for the fixed convenience columns, never for the normalized
  nutrient tables or the core aggregation logic.
* NULL is never coerced to 0. A NULL means "unavailable", 0 means
  "known to be zero". This distinction is preserved through every
  aggregation step.
* This module performs a full deterministic rebuild of all *derived*
  tables on every run. Source/parent tables owned by Prompts 1 and 2
  (`recipe_ingredient_lines_parsed`, `recipe_ingredients`, `ingredients`,
  `nutrition_ingredient_mappings`, `nutrition_mapping_measurements`,
  `usda_foods`, `usda_food_portions`, `usda_nutrients`) are never
  modified. The only Prompt-2-owned table this module writes to is
  `recipe_ingredient_line_nutrition`, and only its fixed summary
  columns (calories, protein, ...) — never `status`, `source`,
  `diagnostic_notes`, or any of the resolution columns.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, Union

from pathlib import Path

from gastrometric.config.paths import DB_PATH

STATUS_CALCULATED = "calculated"
STATUS_PARTIAL = "partial"
STATUS_NO_NUTRITION_DATA = "no_nutrition_data"
STATUS_UNRESOLVED = "unresolved"

MAX_EXAMPLES = 8


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create all tables this pipeline stage depends on, if missing.

    Parent tables use exactly the schemas given in the task contract.
    Nothing here alters or drops pre-existing tables.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS recipe_ingredient_line_nutrition (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_ingredient_id INTEGER NOT NULL,
            recipe_id INTEGER NOT NULL,
            recipe_section_id INTEGER,
            ingredient_id INTEGER,
            mapping_id INTEGER,
            resolved_fdc_id INTEGER,
            resolved_state TEXT CHECK (resolved_state IN ('raw', 'cooked')),
            resolved_grams REAL,
            calories REAL,
            protein REAL,
            total_fat REAL,
            saturated_fat REAL,
            monounsaturated_fat REAL,
            polyunsaturated_fat REAL,
            carbohydrates REAL,
            sugars REAL,
            fiber REAL,
            sodium REAL,
            status TEXT NOT NULL,
            source TEXT,
            diagnostic_notes TEXT,
            calculated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS recipe_ingredient_line_nutrients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_ingredient_line_nutrition_id INTEGER NOT NULL
                REFERENCES recipe_ingredient_line_nutrition(id)
                ON DELETE CASCADE,
            nutrient_id INTEGER NOT NULL,
            nutrient_name TEXT NOT NULL,
            unit TEXT NOT NULL,
            amount REAL,
            amount_per_100g REAL,
            UNIQUE (recipe_ingredient_line_nutrition_id, nutrient_id)
        );

        CREATE TABLE IF NOT EXISTS recipe_section_nutrition (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id INTEGER NOT NULL,
            recipe_section_id INTEGER NOT NULL,
            section_name TEXT,
            total_grams REAL,
            calories REAL,
            protein REAL,
            total_fat REAL,
            saturated_fat REAL,
            monounsaturated_fat REAL,
            polyunsaturated_fat REAL,
            carbohydrates REAL,
            sugars REAL,
            fiber REAL,
            sodium REAL,
            status TEXT NOT NULL,
            calculated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS recipe_section_nutrients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_section_nutrition_id INTEGER NOT NULL
                REFERENCES recipe_section_nutrition(id)
                ON DELETE CASCADE,
            nutrient_id INTEGER NOT NULL,
            nutrient_name TEXT NOT NULL,
            unit TEXT NOT NULL,
            amount REAL,
            UNIQUE (recipe_section_nutrition_id, nutrient_id)
        );

        CREATE TABLE IF NOT EXISTS recipe_nutrition (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id INTEGER NOT NULL,
            total_grams REAL,
            calories REAL,
            protein REAL,
            total_fat REAL,
            saturated_fat REAL,
            monounsaturated_fat REAL,
            polyunsaturated_fat REAL,
            carbohydrates REAL,
            sugars REAL,
            fiber REAL,
            sodium REAL,
            status TEXT NOT NULL,
            calculated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS recipe_nutrients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_nutrition_id INTEGER NOT NULL
                REFERENCES recipe_nutrition(id)
                ON DELETE CASCADE,
            nutrient_id INTEGER NOT NULL,
            nutrient_name TEXT NOT NULL,
            unit TEXT NOT NULL,
            amount REAL,
            UNIQUE (recipe_nutrition_id, nutrient_id)
        );
        """
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class NutrientAgg:
    nutrient_name: str
    unit: str
    known_values: list = field(default_factory=list)  # non-NULL contributions
    saw_any_row: bool = False  # True if at least one contributing row existed

    def resolved_amount(self) -> Optional[float]:
        if not self.known_values:
            return None
        return sum(self.known_values)


def _fetch_usda_nutrients_for_fdc(conn: sqlite3.Connection, fdc_id: int):
    """Return deduped USDA nutrient rows for an fdc_id, keyed by nutrient_id.

    Deduplication rule: if usda_nutrients contains multiple rows for the
    same (fdc_id, nutrient_id), the row with the smallest `id` is kept
    (earliest-inserted record treated as authoritative). This is
    deterministic and avoids double-counting. A diagnostic is returned
    for any nutrient_id where duplicate rows disagreed on amount.
    """
    rows = conn.execute(
        """
        SELECT id, nutrient_id, nutrient_name, unit, amount
        FROM usda_nutrients
        WHERE fdc_id = ?
        ORDER BY nutrient_id, id
        """,
        (fdc_id,),
    ).fetchall()

    by_nutrient: dict[int, sqlite3.Row] = {}
    duplicate_conflicts: list[int] = []
    for row in rows:
        nid = row["nutrient_id"]
        if nid not in by_nutrient:
            by_nutrient[nid] = row
        else:
            existing = by_nutrient[nid]
            if existing["amount"] != row["amount"]:
                duplicate_conflicts.append(nid)
            # keep the first (smallest id) row; do not overwrite

    return list(by_nutrient.values()), duplicate_conflicts


def _calc_amount(amount_per_100g: Optional[float], grams: float) -> Optional[float]:
    if amount_per_100g is None:
        return None
    return amount_per_100g * grams / 100.0


# Fixed summary column -> ordered list of candidates. Each candidate is
# either a bare name string, or a (name, unit) tuple when the name alone
# is known to be ambiguous in USDA data (e.g. "Energy" commonly exists in
# both kcal and kJ variants). All matching happens against the actual
# `nutrient_name` / `unit` values already present in this database's own
# `usda_nutrients` table — nothing here is a hardcoded nutrient_id.
# First candidate that resolves to exactly one distinct nutrient_id wins.
SUMMARY_COLUMN_CANDIDATES: dict[str, list] = {
    "calories": [("Energy", "kcal"), "Energy"],
    "protein": ["Protein"],
    "total_fat": ["Total lipid (fat)"],
    "saturated_fat": ["Fatty acids, total saturated"],
    "monounsaturated_fat": ["Fatty acids, total monounsaturated"],
    "polyunsaturated_fat": ["Fatty acids, total polyunsaturated"],
    "carbohydrates": ["Carbohydrate, by difference"],
    "sugars": [
        "Sugars, total including NLEA",
        "Sugars, total",
        "Sugar, total",
        "Total Sugars",
        "Sugars",
    ],
    "fiber": ["Fiber, total dietary"],
    "sodium": ["Sodium, Na"],
}


def _resolve_summary_nutrient_ids(conn: sqlite3.Connection) -> tuple[dict[str, Optional[int]], list[str]]:
    """Resolve each fixed summary column to a nutrient_id using the
    nutrient names (and, where a candidate specifies one, units) actually
    present in this database's usda_nutrients table. Returns
    (column -> nutrient_id_or_None, diagnostic_notes).

    A column is left unresolved (None) if none of its candidates appear
    in the data, or if a name-only candidate maps to more than one
    distinct nutrient_id (ambiguous/inconsistent source data) — in
    either case the column stays NULL rather than guessing.
    """
    rows = conn.execute(
        "SELECT DISTINCT nutrient_id, nutrient_name, unit FROM usda_nutrients"
    ).fetchall()

    name_to_ids: dict[str, set] = defaultdict(set)
    name_unit_to_ids: dict[tuple, set] = defaultdict(set)
    for r in rows:
        name_key = r["nutrient_name"].strip().lower()
        name_to_ids[name_key].add(r["nutrient_id"])
        name_unit_to_ids[(name_key, r["unit"].strip().lower())].add(r["nutrient_id"])

    resolved: dict[str, Optional[int]] = {}
    notes: list[str] = []

    for column, candidates in SUMMARY_COLUMN_CANDIDATES.items():
        found_id = None
        any_name_seen = False
        for candidate in candidates:
            if isinstance(candidate, tuple):
                name, unit = candidate
                key = (name.strip().lower(), unit.strip().lower())
                ids = name_unit_to_ids.get(key)
                label = f"{name!r} (unit={unit!r})"
            else:
                name = candidate
                ids = name_to_ids.get(name.strip().lower())
                label = f"{name!r}"

            if not ids:
                continue
            any_name_seen = True
            if len(ids) > 1:
                notes.append(
                    f"summary column '{column}': candidate {label} "
                    f"maps to multiple nutrient_ids {sorted(ids)} in usda_nutrients; "
                    f"left unresolved"
                )
                continue
            found_id = next(iter(ids))
            break

        resolved[column] = found_id
        if found_id is None and not any_name_seen:
            notes.append(
                f"summary column '{column}': none of {candidates!r} found in "
                f"usda_nutrients; left unresolved (NULL)"
            )

    return resolved, notes


def _project_summary_columns(
    summary_ids: dict[str, Optional[int]], amounts_by_nutrient_id: dict[int, Optional[float]]
) -> dict[str, Optional[float]]:
    """Build a {column: amount} dict for the fixed summary columns from
    an already-computed {nutrient_id: amount} mapping. Never invents a
    value: a column is None unless its resolved nutrient_id is present
    in amounts_by_nutrient_id.
    """
    result: dict[str, Optional[float]] = {}
    for column, nid in summary_ids.items():
        if nid is not None and nid in amounts_by_nutrient_id:
            result[column] = amounts_by_nutrient_id[nid]
        else:
            result[column] = None
    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def calculate_nutrition(db_path: Optional[Union[str, Path]] = None) -> None:
    """Run the full nutrition calculation and aggregation stage.

    Deterministic: wipes and rebuilds all derived nutrition tables from
    the current state of `recipe_ingredient_line_nutrition` and
    `usda_nutrients`, then commits and closes the connection.
    """
    path = db_path if db_path is not None else DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        ensure_schema(conn)

        summary_ids, summary_notes = _resolve_summary_nutrient_ids(conn)

        stats = {
            "lines_processed": 0,
            "lines_resolved": 0,
            "lines_no_usda_data": 0,
            "lines_skipped_unresolved": 0,
            "nutrient_values_calculated": 0,
            "nutrient_values_null_source": 0,
            "sections_calculated": 0,
            "sections_partial": 0,
            "recipes_calculated": 0,
            "recipes_partial": 0,
        }
        examples: list[str] = []

        # Full deterministic rebuild of derived tables. Child rows cascade
        # on delete of their parents.
        conn.execute("DELETE FROM recipe_ingredient_line_nutrients")
        conn.execute("DELETE FROM recipe_section_nutrition")
        conn.execute("DELETE FROM recipe_nutrition")

        all_lines = conn.execute(
            """
            SELECT id, recipe_id, recipe_section_id, resolved_fdc_id, resolved_grams
            FROM recipe_ingredient_line_nutrition
            ORDER BY id
            """
        ).fetchall()
        stats["lines_processed"] = len(all_lines)

        # line_id -> list of (nutrient_id, nutrient_name, unit, amount)
        line_nutrients: dict[int, list[tuple]] = {}
        # line_id -> recipe_id, recipe_section_id, resolved_grams (eligible only)
        eligible_lines: dict[int, sqlite3.Row] = {}

        for line in all_lines:
            fdc_id = line["resolved_fdc_id"]
            grams = line["resolved_grams"]
            eligible = fdc_id is not None and grams is not None and grams >= 0
            if not eligible:
                stats["lines_skipped_unresolved"] += 1
                continue

            eligible_lines[line["id"]] = line
            stats["lines_resolved"] += 1

            usda_rows, conflicts = _fetch_usda_nutrients_for_fdc(conn, fdc_id)
            if conflicts:
                for nid in conflicts[:1]:
                    if len(examples) < MAX_EXAMPLES:
                        examples.append(
                            f"line_id={line['id']} recipe_id={line['recipe_id']}: "
                            f"duplicate usda_nutrients rows for nutrient_id={nid} "
                            f"disagreed on amount; kept lowest-id row"
                        )

            if not usda_rows:
                stats["lines_no_usda_data"] += 1
                if len(examples) < MAX_EXAMPLES:
                    examples.append(
                        f"line_id={line['id']} recipe_id={line['recipe_id']} "
                        f"section_id={line['recipe_section_id']}: "
                        f"no USDA nutrient data for fdc_id={fdc_id}"
                    )
                line_nutrients[line["id"]] = []
                conn.execute(
                    """
                    UPDATE recipe_ingredient_line_nutrition
                    SET calories = NULL, protein = NULL, total_fat = NULL,
                        saturated_fat = NULL, monounsaturated_fat = NULL,
                        polyunsaturated_fat = NULL, carbohydrates = NULL,
                        sugars = NULL, fiber = NULL, sodium = NULL
                    WHERE id = ?
                    """,
                    (line["id"],),
                )
                continue

            computed = []
            for r in usda_rows:
                amount_per_100g = r["amount"]
                amount = _calc_amount(amount_per_100g, grams)
                if amount_per_100g is None:
                    stats["nutrient_values_null_source"] += 1
                else:
                    stats["nutrient_values_calculated"] += 1
                computed.append(
                    (r["nutrient_id"], r["nutrient_name"], r["unit"], amount, amount_per_100g)
                )

            conn.executemany(
                """
                INSERT INTO recipe_ingredient_line_nutrients
                    (recipe_ingredient_line_nutrition_id, nutrient_id,
                     nutrient_name, unit, amount, amount_per_100g)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [(line["id"], nid, name, unit, amt, amt100) for nid, name, unit, amt, amt100 in computed],
            )
            line_nutrients[line["id"]] = [(nid, name, unit, amt) for nid, name, unit, amt, _ in computed]

            amounts_by_nid = {nid: amt for nid, name, unit, amt, amt100 in computed}
            summary_values = _project_summary_columns(summary_ids, amounts_by_nid)
            conn.execute(
                """
                UPDATE recipe_ingredient_line_nutrition
                SET calories = :calories, protein = :protein, total_fat = :total_fat,
                    saturated_fat = :saturated_fat, monounsaturated_fat = :monounsaturated_fat,
                    polyunsaturated_fat = :polyunsaturated_fat, carbohydrates = :carbohydrates,
                    sugars = :sugars, fiber = :fiber, sodium = :sodium
                WHERE id = :line_id
                """,
                {**summary_values, "line_id": line["id"]},
            )

        # -------------------------------------------------------------
        # Section aggregation
        # -------------------------------------------------------------
        sections: dict[tuple, list] = defaultdict(list)  # (recipe_id, section_id) -> line rows (all)
        for line in all_lines:
            key = (line["recipe_id"], line["recipe_section_id"])
            sections[key].append(line)

        # section_key -> recipe_nutrition contribution aggregator
        recipe_section_agg: dict[int, dict[int, NutrientAgg]] = defaultdict(dict)

        for (recipe_id, section_id), lines_in_section in sections.items():
            total_grams = 0.0
            any_mass = False
            has_eligible = False
            has_unresolved = False
            has_usda_data = False
            has_no_usda_data = False

            nutrient_aggs: dict[int, NutrientAgg] = {}

            for line in lines_in_section:
                if line["id"] in eligible_lines:
                    has_eligible = True
                    total_grams += line["resolved_grams"]
                    any_mass = True
                    lns = line_nutrients.get(line["id"], [])
                    if lns:
                        has_usda_data = True
                    else:
                        has_no_usda_data = True
                    for nid, name, unit, amount in lns:
                        agg = nutrient_aggs.setdefault(nid, NutrientAgg(name, unit))
                        agg.saw_any_row = True
                        if amount is not None:
                            agg.known_values.append(amount)
                else:
                    has_unresolved = True

            if not has_eligible:
                section_status = STATUS_UNRESOLVED
                stats["sections_partial"] += 1
                if len(examples) < MAX_EXAMPLES:
                    examples.append(
                        f"recipe_id={recipe_id} section_id={section_id}: "
                        f"no resolved ingredient lines"
                    )
            elif not has_usda_data:
                section_status = STATUS_NO_NUTRITION_DATA
                stats["sections_partial"] += 1
                if len(examples) < MAX_EXAMPLES:
                    examples.append(
                        f"recipe_id={recipe_id} section_id={section_id}: "
                        f"no USDA nutrient data for any resolved ingredient"
                    )
            elif has_unresolved or has_no_usda_data:
                section_status = STATUS_PARTIAL
                stats["sections_partial"] += 1
            else:
                section_status = STATUS_CALCULATED
                stats["sections_calculated"] += 1

            section_amounts_by_nid = {nid: agg.resolved_amount() for nid, agg in nutrient_aggs.items()}
            section_summary_values = _project_summary_columns(summary_ids, section_amounts_by_nid)

            cur = conn.execute(
                """
                INSERT INTO recipe_section_nutrition
                    (recipe_id, recipe_section_id, total_grams, status,
                     calories, protein, total_fat, saturated_fat, monounsaturated_fat,
                     polyunsaturated_fat, carbohydrates, sugars, fiber, sodium)
                VALUES (:recipe_id, :section_id, :total_grams, :status,
                        :calories, :protein, :total_fat, :saturated_fat, :monounsaturated_fat,
                        :polyunsaturated_fat, :carbohydrates, :sugars, :fiber, :sodium)
                """,
                {
                    "recipe_id": recipe_id,
                    "section_id": section_id,
                    "total_grams": total_grams if any_mass else None,
                    "status": section_status,
                    **section_summary_values,
                },
            )
            section_nutrition_id = cur.lastrowid

            for nid, agg in nutrient_aggs.items():
                resolved = agg.resolved_amount()
                conn.execute(
                    """
                    INSERT INTO recipe_section_nutrients
                        (recipe_section_nutrition_id, nutrient_id, nutrient_name, unit, amount)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (section_nutrition_id, nid, agg.nutrient_name, agg.unit, resolved),
                )
                # feed up to recipe-level aggregation
                recipe_agg_for_recipe = recipe_section_agg[recipe_id]
                r_agg = recipe_agg_for_recipe.setdefault(nid, NutrientAgg(agg.nutrient_name, agg.unit))
                r_agg.saw_any_row = True
                if resolved is not None:
                    r_agg.known_values.append(resolved)

        # -------------------------------------------------------------
        # Recipe aggregation
        # -------------------------------------------------------------
        recipe_ids = sorted({line["recipe_id"] for line in all_lines})

        for recipe_id in recipe_ids:
            recipe_lines = [l for l in all_lines if l["recipe_id"] == recipe_id]
            eligible_recipe_lines = [l for l in recipe_lines if l["id"] in eligible_lines]
            has_unresolved = len(eligible_recipe_lines) < len(recipe_lines)
            has_eligible = len(eligible_recipe_lines) > 0
            has_usda_data = any(line_nutrients.get(l["id"]) for l in eligible_recipe_lines)
            has_no_usda_data = any(
                l["id"] in eligible_lines and not line_nutrients.get(l["id"])
                for l in eligible_recipe_lines
            )

            recipe_total_grams: Optional[float] = (
                sum(l["resolved_grams"] for l in eligible_recipe_lines) if has_eligible else None
            )

            if not has_eligible:
                recipe_status = STATUS_UNRESOLVED
                stats["recipes_partial"] += 1
                if len(examples) < MAX_EXAMPLES:
                    examples.append(f"recipe_id={recipe_id}: no resolved ingredient lines")
            elif not has_usda_data:
                recipe_status = STATUS_NO_NUTRITION_DATA
                stats["recipes_partial"] += 1
                if len(examples) < MAX_EXAMPLES:
                    examples.append(f"recipe_id={recipe_id}: no USDA nutrient data for any ingredient")
            elif has_unresolved or has_no_usda_data:
                recipe_status = STATUS_PARTIAL
                stats["recipes_partial"] += 1
                if len(examples) < MAX_EXAMPLES:
                    examples.append(
                        f"recipe_id={recipe_id}: partial calculation "
                        f"(unresolved_lines={has_unresolved}, missing_usda_data={has_no_usda_data})"
                    )
            else:
                recipe_status = STATUS_CALCULATED
                stats["recipes_calculated"] += 1

            recipe_amounts_by_nid = {
                nid: agg.resolved_amount() for nid, agg in recipe_section_agg.get(recipe_id, {}).items()
            }
            recipe_summary_values = _project_summary_columns(summary_ids, recipe_amounts_by_nid)

            cur = conn.execute(
                """
                INSERT INTO recipe_nutrition
                    (recipe_id, total_grams, status,
                     calories, protein, total_fat, saturated_fat, monounsaturated_fat,
                     polyunsaturated_fat, carbohydrates, sugars, fiber, sodium)
                VALUES (:recipe_id, :total_grams, :status,
                        :calories, :protein, :total_fat, :saturated_fat, :monounsaturated_fat,
                        :polyunsaturated_fat, :carbohydrates, :sugars, :fiber, :sodium)
                """,
                {
                    "recipe_id": recipe_id,
                    "total_grams": recipe_total_grams,
                    "status": recipe_status,
                    **recipe_summary_values,
                },
            )
            recipe_nutrition_id = cur.lastrowid

            for nid, agg in recipe_section_agg.get(recipe_id, {}).items():
                resolved = agg.resolved_amount()
                conn.execute(
                    """
                    INSERT INTO recipe_nutrients
                        (recipe_nutrition_id, nutrient_id, nutrient_name, unit, amount)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (recipe_nutrition_id, nid, agg.nutrient_name, agg.unit, resolved),
                )

        conn.commit()
        _print_summary(stats, examples, summary_ids, summary_notes)
    finally:
        conn.close()


def _print_summary(
    stats: dict,
    examples: list[str],
    summary_ids: Optional[dict[str, Optional[int]]] = None,
    summary_notes: Optional[list[str]] = None,
) -> None:
    print("Recipe nutrition calculation complete")
    print()
    print(f"Ingredient lines processed: {stats['lines_processed']}")
    print(f"Ingredient lines with resolved quantities: {stats['lines_resolved']}")
    print(f"Ingredient lines with no USDA nutrient data: {stats['lines_no_usda_data']}")
    print(f"Ingredient lines skipped/unresolved: {stats['lines_skipped_unresolved']}")
    print()
    print(f"Nutrient values calculated: {stats['nutrient_values_calculated']}")
    print(f"Nutrient values with NULL source amounts: {stats['nutrient_values_null_source']}")
    print()
    print(f"Sections calculated: {stats['sections_calculated']}")
    print(f"Partial sections: {stats['sections_partial']}")
    print()
    print(f"Recipes calculated: {stats['recipes_calculated']}")
    print(f"Partial recipes: {stats['recipes_partial']}")

    if summary_ids is not None:
        print()
        resolved = {k: v for k, v in summary_ids.items() if v is not None}
        unresolved = [k for k, v in summary_ids.items() if v is None]
        print(f"Summary columns resolved to nutrient_id: {resolved}")
        if unresolved:
            print(f"Summary columns left NULL (no unambiguous match in usda_nutrients): {unresolved}")

    if summary_notes:
        print()
        print("Summary column resolution notes:")
        for note in summary_notes:
            print(f"  - {note}")

    if examples:
        print()
        print("Representative examples:")
        for ex in examples[:MAX_EXAMPLES]:
            print(f"  - {ex}")


if __name__ == "__main__":
    calculate_nutrition()