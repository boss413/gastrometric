"""
Generic inventory application.

Application-facing boundary for inventory operations. Owns input
validation and the record<->item shape translation
(`analysis_result_json` <-> `analysis_result`). Delegates anything that
requires running text through understanding -- creating a new item, or
updating one whose `original_input` changed -- to
`gastrometric.orchestration.inventory_ingredient_orchestrator`, which is
the sole place that calls `understand_inventory_ingredient()` and derives
`ingredient_id`/`resolution_status` from its result. This module contains
no understanding logic of its own; it calls `inventory_repository`
directly only for operations that don't require re-understanding
(read/list/delete, and quantity/unit/location-only updates).
"""

import json
from typing import Any, Dict, List, Optional

from gastrometric.db import inventory_repository as repo
from gastrometric.orchestration.inventory_ingredient_orchestrator import (
    add_inventory_observation,
    derive_inventory_fields,
)

VALID_LOCATIONS = ("fridge", "pantry")

# Sentinel distinguishing "caller did not pass this argument" from
# "caller explicitly wants to clear this field to None" on update.
_UNSET = object()


class InventoryValidationError(ValueError):
    """Raised when caller-supplied inventory input fails application-level validation."""


def _validate_original_input(original_input: str) -> None:
    if not isinstance(original_input, str) or not original_input.strip():
        raise InventoryValidationError("original_input must be a non-empty string")


def _validate_location(location: str) -> None:
    if location not in VALID_LOCATIONS:
        raise InventoryValidationError(
            f"location must be one of {VALID_LOCATIONS!r}, got {location!r}"
        )


def _record_to_item(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a persistence-layer record into the application-level
    representation returned to callers: analysis_result_json is
    parsed back into a dict (analysis_result) so callers never see
    raw JSON text.
    """
    item = dict(record)
    item["analysis_result"] = json.loads(item.pop("analysis_result_json"))
    return item


def create_inventory_item(
    original_input: str,
    location: str,
    quantity: Optional[str] = None,
    unit: Optional[str] = None,
    *,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create one inventory item from raw human input.

    Validation happens here. Understanding, deriving `ingredient_id` /
    `resolution_status`, and persisting all happen in
    `inventory_ingredient_orchestrator.add_inventory_observation` -- this
    function does not touch the analyzer result itself.
    """
    _validate_original_input(original_input)
    _validate_location(location)

    record = add_inventory_observation(
        original_input,
        location,
        quantity=quantity,
        unit=unit,
        db_path=db_path,
    )
    return _record_to_item(record)


def get_inventory_item(item_id: int, *, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Retrieve one inventory item by ID, or None if it does not exist."""
    record = repo.get_inventory_record(item_id, db_path=db_path)
    if record is None:
        return None
    return _record_to_item(record)


def list_inventory_items(*, db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retrieve all inventory items."""
    return [_record_to_item(record) for record in repo.list_inventory_records(db_path=db_path)]


def update_inventory_item(
    item_id: int,
    *,
    original_input: Any = _UNSET,
    location: Any = _UNSET,
    quantity: Any = _UNSET,
    unit: Any = _UNSET,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update an existing inventory item. Only fields explicitly passed
    are changed; omitted keyword arguments retain their current value.

    Changing location/quantity/unit alone retains the existing semantic
    result (no re-understanding call).

    Changing original_input re-derives ingredient_id/resolution_status/
    analysis_result_json via
    `inventory_ingredient_orchestrator.derive_inventory_fields` -- the
    same derivation logic the create path uses, not a separate copy of
    it here.

    Returns None if item_id does not exist.
    """
    existing = repo.get_inventory_record(item_id, db_path=db_path)
    if existing is None:
        return None

    new_original_input = existing["original_input"] if original_input is _UNSET else original_input
    new_location = existing["location"] if location is _UNSET else location
    new_quantity = existing["quantity"] if quantity is _UNSET else quantity
    new_unit = existing["unit"] if unit is _UNSET else unit

    if original_input is not _UNSET:
        _validate_original_input(new_original_input)
    if location is not _UNSET:
        _validate_location(new_location)

    input_changed = original_input is not _UNSET and new_original_input != existing["original_input"]

    if input_changed:
        fields = derive_inventory_fields(new_original_input)
        new_ingredient_id = fields["ingredient_id"]
        new_resolution_status = fields["resolution_status"]
        new_analysis_result_json = json.dumps(fields["analysis_result"])
    else:
        new_ingredient_id = existing["ingredient_id"]
        new_resolution_status = existing["resolution_status"]
        new_analysis_result_json = existing["analysis_result_json"]

    record = repo.update_inventory_record(
        item_id,
        original_input=new_original_input,
        ingredient_id=new_ingredient_id,
        location=new_location,
        quantity=new_quantity,
        unit=new_unit,
        resolution_status=new_resolution_status,
        analysis_result_json=new_analysis_result_json,
        db_path=db_path,
    )
    if record is None:
        return None
    return _record_to_item(record)


def delete_inventory_item(item_id: int, *, db_path: Optional[str] = None) -> bool:
    """Delete one inventory item. Returns True if it existed and was deleted."""
    return repo.delete_inventory_record(item_id, db_path=db_path)