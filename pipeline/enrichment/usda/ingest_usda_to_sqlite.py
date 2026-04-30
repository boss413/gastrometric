import os
import re
import glob
import sqlite3
import pandas as pd
from hashlib import sha1

# this next two lines was a quick hack to get the USDA SR Legacy data into the db for testing purposes. 
# It's not meant to be a robust long term solution, just a way to get the data in there so we 
# can iterate on canonicalization and mapping logic.

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from data.exclusions import (
    BRAND_TERMS,
    ULTRA_PROCESSED_KEYWORDS,
    ALLOWLIST
)

DB_PATH = os.path.abspath("data/gastrometric.db")
PARQUET_DIR = os.path.abspath("data/usda/processed/")

# -----------------------------
# DB setup
# -----------------------------
def init_db(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS canonical_ingredients (
        id TEXT PRIMARY KEY,
        name TEXT,
        base_food TEXT,
        state TEXT,
        form TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ingredient_aliases (
        alias TEXT,
        canonical_id TEXT,
        source TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS usda_source_map (
        fdc_id INTEGER,
        canonical_id TEXT
    )
    """)

    conn.commit()


# -----------------------------
# Filtering logic
# -----------------------------
def is_allowed(description: str) -> bool:
    d = description.lower()

    if any(term in d for term in ALLOWLIST):
        return True

    if any(term in d for term in BRAND_TERMS):
        return False

    if any(term in d for term in ULTRA_PROCESSED_KEYWORDS):
        return False

    # hard reject obvious branded style strings
    if re.search(r"\b(inc|llc|co\.|company|foods)\b", d):
        return False

    return True


# -----------------------------
# Canonicalization (v1 heuristic)
# -----------------------------
def canonicalize(description: str):
    d = description.lower()

    parts = [p.strip() for p in d.split(",")]

    base = parts[0]

    state = "unknown"
    form = []

    STATE_MAP = {
        "raw": "raw",
        "fresh": "raw",
        "roasted": "cooked",
        "boiled": "cooked",
        "fried": "cooked",
        "baked": "cooked",
        "dried": "dried"
    }

    for p in parts:
        for k, v in STATE_MAP.items():
            if k in p:
                state = v

    # form detection
    FORM_KEYWORDS = [
        "skinless", "skin on", "boneless", "ground",
        "whole", "juice", "concentrate", "oil",
        "lean", "extra lean"
    ]

    for fk in FORM_KEYWORDS:
        if fk in d:
            form.append(fk)

    form = sorted(set(form))

    canonical_name = f"{base}, {state}"
    if form:
        canonical_name += ", " + ", ".join(form)

    return base.strip(), state, ",".join(form), canonical_name


def make_id(text: str) -> str:
    return sha1(text.encode("utf-8")).hexdigest()[:16]


# -----------------------------
# Processing pipeline
# -----------------------------
def process_file(path, conn):
    df = pd.read_parquet(path)

    cur = conn.cursor()

    kept = 0
    skipped = 0

    for _, row in df.iterrows():
        desc = str(row.get("description", ""))

        if not desc:
            continue

        if not is_allowed(desc):
            skipped += 1
            continue

        base, state, form, canonical_name = canonicalize(desc)
        cid = make_id(canonical_name)

        cur.execute("""
        INSERT OR IGNORE INTO ingredients (ingredient_name, canonical_group)
        VALUES (?, ?)
        """, (canonical_name, base))

        # insert canonical ingredient (ignore duplicates)
        cur.execute("""
            INSERT OR IGNORE INTO canonical_ingredients
            (id, name, base_food, state, form)
            VALUES (?, ?, ?, ?, ?)
        """, (cid, canonical_name, base, state, form))

        # alias mapping
        cur.execute("""
            INSERT INTO ingredient_aliases
            (alias, canonical_id, source)
            VALUES (?, ?, ?)
        """, (desc, cid, "usda_sr_legacy"))

        # source mapping
        if "fdc_id" in row:
            cur.execute("""
                INSERT INTO usda_source_map
                (fdc_id, canonical_id)
                VALUES (?, ?)
            """, (int(row["fdc_id"]), cid))

        kept += 1

    conn.commit()

    print(f"{os.path.basename(path)} -> kept={kept}, skipped={skipped}")


# -----------------------------
# Main
# -----------------------------
def main():
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    files = glob.glob(os.path.join(PARQUET_DIR, "*.parquet"))

    print(f"Found {len(files)} parquet files")

    for f in files:
        process_file(f, conn)

    conn.close()


if __name__ == "__main__":
    main()