"""
Workflow/persistence coordinator for the inventory-ingredient pipeline.

Architecture:

    application/inventory_editor.py
            |
            v
    orchestration/inventory_ingredient_orchestrator.py     <- this module
            |
            v
    orchestration/inventory_ingredient_understanding.py
            |
            v
    complete analyzer result
            |
            v
    derive ingredient_id / resolution_status
            |
            v
    persist inventory_items

This module owns exactly two things: calling
`understand_inventory_ingredient()`, and turning its complete, unmodified
result into the fields `inventory_items` needs (`ingredient_id`,
`resolution_status`, `analysis_result_json`). It contains no
Lexer/Parser/Analyzer logic, and it never reduces the analyzer result to
a smaller DTO before persistence -- the complete result is exactly what
gets serialized into `analysis_result_json`.

Persistence itself is delegated to the existing
`gastrometric.db.inventory_repository` rather than duplicated here with
raw SQL: that module is already the single source of truth for
`inventory_items` SQL and already has correct create/read/update/delete
behavior. This orchestrator has no reason to compete with it or
reimplement it.

`application/inventory_editor.py` remains the application-facing
boundary: it owns input validation and translating a persisted record
into the application-level item shape (parsing `analysis_result_json`
back into `analysis_result`), and it still calls `inventory_repository`
directly for reads/list/delete/metadata-only updates. It calls into this
module only for the two operations that require running text through
understanding: creating a new item, and updating one whose
`original_input` changed.
"""

import json
from typing import Any, Callable, Dict, Optional

from gastrometric.db import inventory_repository as repo
from gastrometric.orchestration.inventory_ingredient_understanding import (
    understand_inventory_ingredient,
)

# Matches the `CHECK (location IN ('fridge', 'pantry'))` constraint on
# `inventory_items` verbatim.
VALID_LOCATIONS = frozenset({"fridge", "pantry"})


# ---------------------------------------------------------------------------
# Analyzer-result projection
# ---------------------------------------------------------------------------
#
# Reads fields the analyzer already computed -- never reparses
# original_input, never re-queries the vocabulary/knowledge graph. Per
# the real, repo-verified behavior: "purple cabbage thing" ->
# status="unresolved", and the selected interpretation still has a
# reference identifying ingredient "cabbage". ingredient_id and
# resolution_status are independent projections of the same analyzer
# result -- neither is derived from the other.


def _find_selected_interpretation(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Returns the interpretation dict whose `id` matches
    `result["selected_interpretation"]`, or None if there's no selection
    or no interpretation with a matching id.
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

    Deliberately independent of `result["status"]`: "purple cabbage
    thing" must still yield "cabbage" here despite an overall
    "unresolved" status, because the leftover "thing" material is what
    makes the overall result unresolved, not a failure to identify the
    ingredient.
    """
    interpretation = _find_selected_interpretation(result)
    if interpretation is None:
        return None
    for reference in interpretation.get("references") or []:
        ingredient = reference.get("ingredient")
        if ingredient and ingredient.get("id"):
            return ingredient["id"]
    return None


def derive_inventory_fields(
    original_input: str,
    *,
    understand: Callable[[str], Dict[str, Any]] = understand_inventory_ingredient,
) -> Dict[str, Any]:
    """
    Runs `original_input` through understanding and projects the
    complete, unmodified analyzer result down to exactly the fields
    `inventory_items` needs.

    This is the single place `ingredient_id`/`resolution_status` are
    derived -- both `add_inventory_observation` (create path, below) and
    `application.inventory_editor.update_inventory_item` (update path,
    when `original_input` changes) call this instead of each
    implementing their own projection.

    Returns a dict with:
        ingredient_id: Optional[str] -- see `_derive_ingredient_id`.
        resolution_status: str -- the analyzer's top-level `status`,
            unchanged ("resolved" / "ambiguous" / "unresolved" /
            "invalid").
        analysis_result: Dict[str, Any] -- the COMPLETE analyzer result,
            unmodified. Callers `json.dumps` this verbatim into
            `analysis_result_json`; it is never reduced before that
            point.

    Any exception from `understand` propagates unchanged; this function
    never substitutes a placeholder result for a failed understanding
    call.
    """
    result = understand(original_input)
    return {
        "ingredient_id": _derive_ingredient_id(result),
        "resolution_status": result["status"],
        "analysis_result": result,
    }


# ---------------------------------------------------------------------------
# Public entry point: create
# ---------------------------------------------------------------------------


def add_inventory_observation(
    original_input: str,
    location: str,
    quantity: Optional[str] = None,
    unit: Optional[str] = None,
    *,
    understand: Callable[[str], Dict[str, Any]] = understand_inventory_ingredient,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Adds one inventory observation end-to-end: understands
    `original_input` via `derive_inventory_fields`, and persists
    everything -- including the complete analyzer result -- as one
    `inventory_items` row via `inventory_repository.create_inventory_record`.

    `quantity`/`unit` are stored as inventory metadata exactly as
    supplied; they play no part in understanding -- `understand` is
    called with `original_input` alone.

    Multiple calls with identical arguments are NOT deduplicated: each
    call inserts a new row.

    Raises:
        ValueError: `location` is not one of `VALID_LOCATIONS` (`fridge`,
            `pantry`). Raised before understanding runs, so an invalid
            location never triggers work that would just be discarded.
        Exception: whatever `understand` raises, propagated unchanged --
            no row is written if understanding fails.

    Returns:
        The persisted record exactly as `inventory_repository` returns
        it -- `analysis_result_json` is still a JSON string at this
        layer; parsing it back into `analysis_result` for
        application-level callers is `inventory_editor.py`'s job, not
        this module's.
    """
    if location not in VALID_LOCATIONS:
        raise ValueError(
            f"Invalid location {location!r}; must be one of "
            f"{sorted(VALID_LOCATIONS)}."
        )

    fields = derive_inventory_fields(original_input, understand=understand)

    return repo.create_inventory_record(
        original_input=original_input,
        ingredient_id=fields["ingredient_id"],
        location=location,
        quantity=quantity,
        unit=unit,
        resolution_status=fields["resolution_status"],
        analysis_result_json=json.dumps(fields["analysis_result"]),
        db_path=db_path,
    )