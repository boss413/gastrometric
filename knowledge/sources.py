"""Idempotent helpers for the culinary_sources table.

culinary_sources is one of the four New Tables and must never be created
or altered here — only populated, using INSERT OR IGNORE, from JSON seed
data supplied by the caller.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict

def load_json_seed(path: Path | str) -> Any:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Seed file not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def ensure_sources(conn: sqlite3.Connection, seed_path: Path | str) -> Dict[str, int]:
    """Insert any missing culinary_sources rows from seed_path.

    Returns a mapping of source name -> source_id for every source that
    now exists in the table (not just the ones this call inserted).
    """
    rows = load_json_seed(seed_path)
    cur = conn.cursor()
    for row in rows:
        cur.execute(
            "INSERT OR IGNORE INTO culinary_sources (name, description) VALUES (?, ?)",
            (row["name"], row.get("description")),
        )
    conn.commit()

    cur.execute("SELECT source_id, name FROM culinary_sources")
    return {name: source_id for source_id, name in cur.fetchall()}


def get_source_id(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute("SELECT source_id FROM culinary_sources WHERE name = ?", (name,))
    row = cur.fetchone()
    if row is None:
        raise ValueError(f"Unknown culinary source: {name!r}. Was ensure_sources() called first?")
    return row[0]
