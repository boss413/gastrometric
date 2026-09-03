"""
Development kitchen seed operation (BE-01).

Responsibility: read data/seed/kitchen.json and submit each entry to
the generic inventory application (gastrometric.application.inventory_editor).

This module must NOT:
  * contain inventory persistence logic (no SQL against inventory_items),
  * contain the semantic-understanding implementation,
  * become the future web API's inventory entry point,
  * write to fridge_items / pantry_items.

It is invoked by rebuild_db.py for development database rebuilds only.
"""

import json
from typing import Any, Dict, List, Optional

from gastrometric.config.paths import SEED_DIR
from gastrometric.application.inventory_editor import (
    InventoryValidationError,
    create_inventory_item,
)

KITCHEN_SEED_FILE = SEED_DIR / "kitchen.json"

_MAX_FAILURE_EXAMPLES = 8


class SeedEntryError(Exception):
    """Raised for a malformed seed entry; identifies which entry failed."""

    def __init__(self, index: int, entry: Any, reason: str):
        self.index = index
        self.entry = entry
        self.reason = reason
        super().__init__(f"seed entry #{index} ({entry!r}): {reason}")


def _load_seed_entries(seed_file) -> List[Dict[str, Any]]:
    with open(seed_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SeedEntryError(-1, data, "seed file must contain a JSON array of entries")
    return data


def _validate_entry(index: int, entry: Any) -> None:
    if not isinstance(entry, dict):
        raise SeedEntryError(index, entry, "entry must be a JSON object")

    original_input = entry.get("input")
    if not isinstance(original_input, str) or not original_input.strip():
        raise SeedEntryError(index, entry, "'input' is required and must be a non-empty string")

    location = entry.get("location")
    if location not in ("fridge", "pantry"):
        raise SeedEntryError(index, entry, "'location' is required and must be 'fridge' or 'pantry'")


def seed_kitchen(seed_file=None, db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load data/seed/kitchen.json and create one inventory item per entry
    via the generic inventory application.

    Returns a summary dict: {"successes": int, "failures": [str, ...]}.
    """
    seed_file = seed_file or KITCHEN_SEED_FILE
    entries = _load_seed_entries(seed_file)

    successes = 0
    failures: List[str] = []

    for index, entry in enumerate(entries):
        try:
            _validate_entry(index, entry)
        except SeedEntryError as exc:
            failures.append(str(exc))
            continue

        try:
            create_inventory_item(
                entry["input"],
                entry["location"],
                quantity=entry.get("quantity"),
                unit=entry.get("unit"),
                db_path=db_path,
            )
            successes += 1
        except InventoryValidationError as exc:
            failures.append(f"seed entry #{index} ({entry!r}) rejected by inventory application: {exc}")

    print(f"Kitchen seeded: {successes} inventory items created from {len(entries)} seed entries")
    print(f"Failed seed entries: {len(failures)}")
    if failures:
        print("Failure examples:")
        for message in failures[:_MAX_FAILURE_EXAMPLES]:
            print(f"  - {message}")
        if len(failures) > _MAX_FAILURE_EXAMPLES:
            print(f"  ... and {len(failures) - _MAX_FAILURE_EXAMPLES} more")

    return {"successes": successes, "failures": failures}