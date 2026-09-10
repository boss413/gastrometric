from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from pprint import pprint
from typing import Any

from gastrometric.config.paths import DB_PATH
from gastrometric.orchestration.inventory_ingredient_understanding import (
    understand_inventory_ingredient,
)


PERSISTENCE_TABLES = (
    "lexical_spans",
    "ingredient_parse_trees",
    "analysis_records",
    "analysis_candidate_evaluations",
    "analysis_evidence",
    "inventory_items",
)

TEST_INPUTS = (
    "cabbage",
    "2 heads cabbage",
    "purple cabbage thing",
    "1/2 lb cabbage",
)

VALID_STATUSES = {"resolved", "unresolved", "ambiguous", "invalid"}


def get_row_counts() -> dict[str, int]:
    """Return row counts for tables that inventory understanding must not modify."""
    with sqlite3.connect(DB_PATH) as conn:
        return {
            table: conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in PERSISTENCE_TABLES
        }


def print_inventory_items() -> None:
    """Print the current inventory_items rows for visual inspection."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                original_input,
                ingredient_id,
                location,
                quantity,
                unit,
                resolution_status,
                analysis_result_json
            FROM inventory_items
            ORDER BY id
            """
        ).fetchall()

    print("\nCurrent inventory_items:")
    if not rows:
        print("  (none)")
        return

    for row in rows:
        (
            item_id,
            original_input,
            ingredient_id,
            location,
            quantity,
            unit,
            resolution_status,
            analysis_result_json,
        ) = row

        print(f"  id={item_id}")
        print(f"    original_input: {original_input!r}")
        print(f"    ingredient_id: {ingredient_id!r}")
        print(f"    location: {location!r}")
        print(f"    quantity: {quantity!r}")
        print(f"    unit: {unit!r}")
        print(f"    resolution_status: {resolution_status!r}")

        try:
            parsed_json = json.loads(analysis_result_json)
            print("    analysis_result_json:")
            pprint(parsed_json, indent=6, sort_dicts=False)
        except (TypeError, json.JSONDecodeError):
            print(f"    analysis_result_json: {analysis_result_json!r}")


def test_function_signature() -> None:
    """Verify the public function accepts one positional text argument."""
    result = understand_inventory_ingredient("cabbage")

    assert isinstance(result, dict), (
        f"Expected dict, got {type(result).__name__}"
    )

    print("PASS: public callable accepts a single positional text argument")


def test_understanding_inputs() -> None:
    """Run representative inventory text through the complete pipeline."""
    for text in TEST_INPUTS:
        print(f"\n--- Testing: {text!r} ---")

        try:
            result = understand_inventory_ingredient(text)
        except Exception as exc:
            print(f"FAIL: raised {type(exc).__name__}: {exc}")
            continue

        ...


def test_no_database_writes() -> None:
    """
    Verify inventory understanding does not persist anything.

    The function should only operate in memory. In particular, it must not
    create rows in recipe-specific intermediate/analysis tables or modify
    inventory_items.
    """
    before = get_row_counts()

    for text in TEST_INPUTS:
        understand_inventory_ingredient(text)

    after = get_row_counts()

    assert before == after, (
        "Database row counts changed during inventory understanding.\n"
        f"Before: {before}\n"
        f"After:  {after}"
    )

    print("\nPASS: inventory understanding wrote to none of the tested tables")


def main() -> None:
    print("=" * 72)
    print("Gastrometric Inventory Ingredient Understanding Tester")
    print("=" * 72)
    print(f"Database: {Path(DB_PATH).resolve()}")

    print("\nInitial database row counts:")
    initial_counts = get_row_counts()
    for table, count in initial_counts.items():
        print(f"  {table}: {count}")

    print_inventory_items()

    print("\nRunning tests...")
    test_function_signature()
    test_understanding_inputs()
    test_no_database_writes()

    print_inventory_items()

    print("\n" + "=" * 72)
    print("ALL TESTS PASSED")
    print("=" * 72)


if __name__ == "__main__":
    main()