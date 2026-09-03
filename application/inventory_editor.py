"""
Generic inventory application (BE-01).

This is the application-level boundary for inventory management. It
is the only thing callers (seed_kitchen.py today, the future web API
later) should talk to for inventory operations. Callers do not need
to know:
  * SQLite table names or SQL,
  * how analyzer/understanding results are serialized,
  * where the database lives,
  * how semantic understanding is orchestrated.

Persistence lives in gastrometric.db.inventory_repository.
The understanding seam lives in gastrometric.application.inventory_understanding
and is a BE-01 placeholder pending BE-02A/BE-02C.
"""

import json
from typing import Any, Callable, Dict, List, Optional

from gastrometric.db import inventory_repository as repo
from gastrometric.application.inventory_understanding import (
    UnderstandingResult,
    understand_inventory_input,
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

    Callers are responsible for handling the None case themselves
    (record-not-found) so this helper's type stays precise instead of
    forcing every return type in this module to be Optional.
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
    understand: Callable[[str], UnderstandingResult] = understand_inventory_input,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create one inventory item from raw human input.

    original_input is passed through `understand` (the BE-02C seam).
    If the result is not "resolved", ingredient_id is persisted as
    NULL — the application never invents a canonical ingredient.
    """
    _validate_original_input(original_input)
    _validate_location(location)

    result = understand(original_input)
    ingredient_id = result.ingredient_id if result.status == "resolved" else None

    record = repo.create_inventory_record(
        original_input=original_input,
        ingredient_id=ingredient_id,
        location=location,
        quantity=quantity,
        unit=unit,
        resolution_status=result.status,
        analysis_result_json=json.dumps(result.raw_result),
        db_path=db_path,
    )
    # repo.create_inventory_record always returns a record (never None)
    # on success, so this is safe without an explicit None-check.
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
    understand: Callable[[str], UnderstandingResult] = understand_inventory_input,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update an existing inventory item. Only fields explicitly passed
    are changed; omitted keyword arguments retain their current value.

    Changing location/quantity/unit alone retains the existing
    semantic result (no re-understanding call).

    Changing original_input re-runs it through `understand` and the
    resulting ingredient_id/resolution_status/analysis_result_json
    replace the previous values.

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
        result = understand(new_original_input)
        new_ingredient_id = result.ingredient_id if result.status == "resolved" else None
        new_resolution_status = result.status
        new_analysis_result_json = json.dumps(result.raw_result)
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