"""
Load parsed USDA SR Legacy parquet files (foods, nutrients, portions)
into gastrometric.db as raw reference tables.

This does NOT touch `ingredients` or `ingredient_aliases` -- those are
owned by ingredients.json. It does NOT canonicalize or match USDA
entries to `ingredients` at all; it just gets the USDA data into the
db so a separate mapping step can populate `usda_source_map`.

Re-running this script is safe/idempotent: usda_foods is keyed on
fdc_id, and usda_nutrients / usda_food_portions have UNIQUE constraints,
so re-running with the same parquet files just no-ops via INSERT OR
IGNORE. If the upstream USDA data changes and you want it reloaded,
delete the relevant rows first (this script won't overwrite existing
rows for you).
"""
import re
import sqlite3

import pandas as pd

from gastrometric.config.paths import DB_PATH, DATA_DIR
from gastrometric.pipeline.enrichment.usda.schema_usda import create_usda_tables
from gastrometric.data.exclusions import BRAND_TERMS, ULTRA_PROCESSED_KEYWORDS, ALLOWLIST

PROCESSED_DIR = DATA_DIR / "usda" / "processed"


def is_allowed(description: str) -> bool:
    """Filter out obviously branded / ultra-processed entries. SR Legacy
    is mostly generic (non-branded) foods already, so this should reject
    very little -- it's a cheap safety net, not your primary QA step."""
    d = description.lower()

    if any(term in d for term in ALLOWLIST):
        return True
    if any(term in d for term in BRAND_TERMS):
        return False
    if any(term in d for term in ULTRA_PROCESSED_KEYWORDS):
        return False
    if re.search(r"\b(inc|llc|co\.|company|foods)\b", d):
        return False

    return True


def load_foods(conn, foods_df):
    df = foods_df.dropna(subset=["fdc_id"]).copy()
    df["description"] = df["description"].fillna("").astype(str)
    df = df[df["description"] != ""]
    df = df[df["description"].map(is_allowed)]

    rows = [
        (int(r.fdc_id), r.description, r.data_type, r.category)
        for r in df.itertuples(index=False)
    ]
    skipped = len(foods_df) - len(rows)

    cur = conn.cursor()
    cur.executemany("""
        INSERT OR IGNORE INTO usda_foods (fdc_id, description, data_type, category)
        VALUES (?, ?, ?, ?)
    """, rows)
    conn.commit()
    print(f"usda_foods: attempted={len(rows)}, skipped={skipped}")


def load_nutrients(conn, nutrients_df):
    df = nutrients_df.dropna(subset=["fdc_id", "nutrient_id"])

    rows = [
        (
            int(r.fdc_id),
            int(r.nutrient_id),
            r.nutrient_name,
            r.unit,
            float(r.amount) if pd.notna(r.amount) else None,
        )
        for r in df.itertuples(index=False)
    ]
    skipped = len(nutrients_df) - len(rows)

    cur = conn.cursor()
    cur.executemany("""
        INSERT OR IGNORE INTO usda_nutrients
        (fdc_id, nutrient_id, nutrient_name, unit, amount)
        VALUES (?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    print(f"usda_nutrients: attempted={len(rows)}, skipped={skipped}")


def load_portions(conn, portions_df):
    df = portions_df.dropna(subset=["usda_portion_id", "fdc_id"])

    rows = [
        (
            int(r.usda_portion_id),
            int(r.fdc_id),
            float(r.amount) if pd.notna(r.amount) else None,
            float(r.gram_weight) if pd.notna(r.gram_weight) else None,
            r.modifier if pd.notna(r.modifier) else None,
            r.portion_description if pd.notna(r.portion_description) else None,
            int(r.measure_unit_id) if pd.notna(r.measure_unit_id) else None,
            r.measure_unit_name if pd.notna(r.measure_unit_name) else None,
            r.measure_unit_abbr if pd.notna(r.measure_unit_abbr) else None,
        )
        for r in df.itertuples(index=False)
    ]
    skipped = len(portions_df) - len(rows)

    cur = conn.cursor()
    cur.executemany("""
        INSERT OR IGNORE INTO usda_food_portions
        (usda_portion_id, fdc_id, amount, gram_weight, modifier,
         portion_description, measure_unit_id, measure_unit_name, measure_unit_abbr)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    print(f"usda_food_portions: attempted={len(rows)}, skipped={skipped}")


def ingest_usda_legacy():
    conn = sqlite3.connect(DB_PATH)

    # These trade durability for speed (no fsync per transaction, no WAL
    # rollback journal safety). Fine here because this db is fully
    # rebuildable from rebuild_db.py -- don't carry these into code paths
    # that write data you can't regenerate.
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")

    create_usda_tables(conn)

    foods_df = pd.read_parquet(PROCESSED_DIR / "legacy_foods.parquet")
    nutrients_df = pd.read_parquet(PROCESSED_DIR / "legacy_nutrients.parquet")
    portions_df = pd.read_parquet(PROCESSED_DIR / "legacy_portions.parquet")

    load_foods(conn, foods_df)
    load_nutrients(conn, nutrients_df)
    load_portions(conn, portions_df)

    conn.close()


if __name__ == "__main__":
    ingest_usda_legacy()