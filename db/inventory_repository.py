"""
Inventory persistence (BE-01).

This module is responsible ONLY for storing and retrieving
``inventory_items`` rows in SQLite. It has no knowledge of ingredient
text, semantic understanding, or application-level validation — that
lives in ``gastrometric.application.inventory_editor``.

Schema (owned by init_db.py, not this module):

    inventory_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_input TEXT NOT NULL,
        ingredient_id TEXT,
        location TEXT NOT NULL,
        quantity TEXT,
        unit TEXT,
        resolution_status TEXT NOT NULL,
        analysis_result_json TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CHECK (location IN ('fridge', 'pantry'))
    )

Every function accepts an optional ``db_path`` override (following the
convention already used in ``gastrometric.understanding.analyzer``),
defaulting to ``gastrometric.config.paths.DB_PATH``.
"""

import sqlite3
from typing import Any, Dict, List, Optional

from gastrometric.config.paths import DB_PATH

_COLUMNS = (
    "id",
    "original_input",
    "ingredient_id",
    "location",
    "quantity",
    "unit",
    "resolution_status",
    "analysis_result_json",
    "created_at",
    "updated_at",
)


def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {column: row[column] for column in _COLUMNS}


def create_inventory_record(
    *,
    original_input: str,
    ingredient_id: Optional[str],
    location: str,
    quantity: Optional[str],
    unit: Optional[str],
    resolution_status: str,
    analysis_result_json: str,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Insert one inventory record and return its persisted representation."""
    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO inventory_items (
                original_input, ingredient_id, location,
                quantity, unit, resolution_status, analysis_result_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
        new_id = cursor.lastrowid
        if new_id is None:
            # Cannot happen after a successful INSERT on a table with an
            # AUTOINCREMENT primary key; guarded for the type checker.
            raise RuntimeError("insert succeeded but no lastrowid was returned")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    record = get_inventory_record(new_id, db_path=db_path)
    assert record is not None  # just inserted; must exist
    return record


def get_inventory_record(item_id: int, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Fetch one inventory record by ID, or None if it does not exist."""
    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM inventory_items WHERE id = ?",
            (item_id,),
        )
        row = cursor.fetchone()
        return _row_to_dict(row) if row is not None else None
    finally:
        conn.close()


def list_inventory_records(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch all inventory records."""
    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT {', '.join(_COLUMNS)} FROM inventory_items ORDER BY id")
        return [_row_to_dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def update_inventory_record(
    item_id: int,
    *,
    original_input: str,
    ingredient_id: Optional[str],
    location: str,
    quantity: Optional[str],
    unit: Optional[str],
    resolution_status: str,
    analysis_result_json: str,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Overwrite one inventory record's fields. Returns None if it does not exist."""
    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE inventory_items
            SET original_input = ?,
                ingredient_id = ?,
                location = ?,
                quantity = ?,
                unit = ?,
                resolution_status = ?,
                analysis_result_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                original_input,
                ingredient_id,
                location,
                quantity,
                unit,
                resolution_status,
                analysis_result_json,
                item_id,
            ),
        )
        conn.commit()
        if cursor.rowcount == 0:
            return None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return get_inventory_record(item_id, db_path=db_path)


def delete_inventory_record(item_id: int, db_path: Optional[str] = None) -> bool:
    """Delete one inventory record. Returns True if a row was deleted."""
    conn = _connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM inventory_items WHERE id = ?", (item_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()