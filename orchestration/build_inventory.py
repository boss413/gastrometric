"""
Orchestration for the inventory-ingredient workflow: the persistence
boundary between `inventory_ingredient_understanding.py` (pure lex/parse/
analyze, no database access) and the `inventory_items` table.

Mirrors the separation the recipe pipeline draws between
`ingredient_parser.py`/`analyzer.py` (source-agnostic, pure) and
`recipe_ingredient_understanding.py` (source-specific: SQLite, table
names, lineage). This module is that same "source-specific" layer for
inventory: it knows about `inventory_items`, the `fridge`/`pantry`
location constraint, and how to turn one inventory observation into a
persisted row. It contains no Lexer/Parser/Analyzer logic of its own --
every semantic decision about the ingredient text comes from
`understand_inventory_ingredient()`, unmodified and unreparsed.

Call chain this module sits in the middle of:

    application/inventory_editor.py
            |
            v
    orchestration/inventory_ingredient_orchestrator.py   <- this module
            |
            v
    orchestration/inventory_ingredient_understanding.py
            |
            v
    lex() -> IngredientParser.parse() -> analyze_parse_result()

This module never imports `lex`, `IngredientParser`, or `analyzer`
directly, and never calls back down past
`understand_inventory_ingredient()` -- if it needs something the analyzer
result doesn't already contain, that's a signal the understanding layer
is missing a field, not a reason to reimplement analyzer logic here.
"""

import json
import sqlite3
from typing import Any, Dict, Optional

from gastrometric.config.paths import DB_PATH
from gastrometric.orchestration.inventory_ingredient_understanding import (
    understand_inventory_ingredient,
)

# Matches the `CHECK (location IN ('fridge', 'pantry'))` constraint on
# `inventory_items` verbatim -- validated here too so an invalid location
# is rejected before `understand_inventory_ingredient()` does any work
# that would just be discarded, and so the caller gets a clear ValueError
# instead of an sqlite3.IntegrityError from deep inside the INSERT.
VALID_LOCATIONS = frozenset({"fridge", "pantry"})


# ---------------------------------------------------------------------------
# Analyzer-result projection
# ---------------------------------------------------------------------------
#
# Reads fields the analyzer already computed -- never reparses the
# original text, never queries the vocabulary/knowledge graph again. Per
# the work order's example:
#
#   {"status": "resolved", "selected_interpretation": "interp_1",
#    "interpretations": [{"id": "interp_1",
#                          "references": [{"ingredient": {"id": "cabbage"}}]}]}
#   -> ingredient_id = "cabbage"
#
# and, critically, this must still find "cabbage" for an *unresolved*
# top-level result ("purple cabbage thing"): `resolution_status` and
# `ingredient_id` are independent projections of the same result, one is
# never derived from the other.


def _find_selected_interpretation(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Returns the interpretation dict whose `id` matches
    `result["selected_interpretation"]`, or None if there's no selection
    (e.g. a fully unresolved result with no viable candidate) or no
    interpretation with a matching id.
    """
    selected_id = result.get("selected_interpretation")
    if selected_id is None:
        return None
    for interpretation in result.get("interpretations", []):
        if interpretation.get("id") == selected_id:
            return interpretation
    return None


def _derive_ingredient_id(result: Dict[str, Any]) -> Optional[str]:
    """Derives `inventory_items.ingredient_id` from the selected
    interpretation's first reference that carries a resolved ingredient.

    Returns None if there is no selected interpretation, or none of its
    references identify an ingredient. Deliberately independent of
    `result["status"]` -- "purple cabbage thing" has a resolved
    `ingredient_id` ("cabbage") despite an overall `unresolved` status,
    because its selected interpretation still has a reference identifying
    that ingredient; the unresolved status comes from the leftover
    "thing" material, not from ingredient identification failing.
    """
    interpretation = _find_selected_interpretation(result)
    if interpretation is None:
        return None
    for reference in interpretation.get("references") or []:
        ingredient = reference.get("ingredient")
        if ingredient and ingredient.get("id"):
            return ingredient["id"]
    return None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _connect(db_path: Any) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _insert_inventory_item(
    conn: sqlite3.Connection,
    original_input: str,
    ingredient_id: Optional[str],
    location: str,
    quantity: Optional[str],
    unit: Optional[str],
    resolution_status: str,
    analysis_result_json: str,
) -> sqlite3.Row:
    """Inserts one `inventory_items` row and returns it as persisted
    (including DB-assigned `id` and default `created_at`/`updated_at`),
    rather than reconstructing the row from the arguments passed in.
    """
    cursor = conn.execute(
        """
        INSERT INTO inventory_items (
            original_input,
            ingredient_id,
            location,
            quantity,
            unit,
            resolution_status,
            analysis_result_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            original_input,
            ingredient_id,
            location,
            quantity,
            unit,
            resolution_status,
            analysis_result_json,
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM inventory_items WHERE id = ?", (cursor.lastrowid,)
    ).fetchone()
    return row


def _row_to_item(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def add_inventory_item(
    original_input: str,
    location: str,
    quantity: Optional[str] = None,
    unit: Optional[str] = None,
    db_path: Optional[Any] = None,
) -> Dict[str, Any]:
    """Adds one inventory observation end-to-end.

    Validates `location`, runs `original_input` through
    `understand_inventory_ingredient()`, derives `ingredient_id` and
    `resolution_status` from the complete, unmodified analyzer result,
    and persists everything -- including the full analyzer result -- as
    one `inventory_items` row.

    `quantity`/`unit` are stored as inventory metadata exactly as
    supplied; for this PoC they play no part in ingredient understanding
    or resolution -- `understand_inventory_ingredient()` is called with
    `original_input` alone, never with `quantity`/`unit` folded in.

    Multiple calls with identical arguments are NOT deduplicated: each
    call inserts a new row. "fridge: cabbage" logged twice is two
    separate observations, both preserved.

    Args:
        original_input: The raw, human-authored inventory text, stored
            verbatim -- never replaced with the canonical ingredient name.
        location: Must be one of `VALID_LOCATIONS` ("fridge", "pantry").
        quantity: Optional inventory metadata, stored as-is.
        unit: Optional inventory metadata, stored as-is.
        db_path: Optional override of `DB_PATH`, for tests.

    Raises:
        ValueError: `location` is not one of `VALID_LOCATIONS`. Raised
            before `understand_inventory_ingredient()` is called, so an
            invalid location never triggers understanding work that would
            just be discarded.
        Exception: whatever `understand_inventory_ingredient()` raises,
            propagated unchanged and un-wrapped. This function never
            substitutes a placeholder analyzer result for a failed
            understanding call -- if understanding fails, no row is
            written at all.

    Returns:
        A dict reflecting the row exactly as persisted: at minimum `id`,
        `original_input`, `ingredient_id`, `location`, `quantity`, `unit`,
        `resolution_status`, `analysis_result_json`, plus any other
        `inventory_items` columns (e.g. `created_at`/`updated_at`).
    """
    if location not in VALID_LOCATIONS:
        raise ValueError(
            f"Invalid location {location!r}; must be one of "
            f"{sorted(VALID_LOCATIONS)}."
        )

    # Step 2: understand. Not wrapped in try/except -- failures here must
    # propagate unchanged (see docstring and module-level "Error
    # behavior" contract); no row is written if this raises.
    result = understand_inventory_ingredient(original_input)

    # Steps 3-5: derive + serialize. The complete `result` dict is stored
    # unmodified -- these two derivations read it, they don't reduce it.
    ingredient_id = _derive_ingredient_id(result)
    resolution_status = result["status"]
    analysis_result_json = json.dumps(result)

    # Steps 6-7: persist and return.
    conn = _connect(db_path or DB_PATH)
    try:
        row = _insert_inventory_item(
            conn,
            original_input=original_input,
            ingredient_id=ingredient_id,
            location=location,
            quantity=quantity,
            unit=unit,
            resolution_status=resolution_status,
            analysis_result_json=analysis_result_json,
        )
    finally:
        conn.close()

    return _row_to_item(row)