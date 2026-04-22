import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.executescript("""
CREATE TABLE IF NOT EXISTS canonical_ingredients (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS usda_foods (
    id INTEGER PRIMARY KEY,
    name TEXT,
    usda_id TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS ingredient_usda_map (
    id INTEGER PRIMARY KEY,
    canonical_ingredient_id INTEGER,
    usda_food_id INTEGER,
    is_primary BOOLEAN DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ingredient_relationships (
    id INTEGER PRIMARY KEY,
    from_ingredient_id INTEGER,
    to_ingredient_id INTEGER,
    relationship_type TEXT
);

CREATE TABLE IF NOT EXISTS ingredient_conversions (
    id INTEGER PRIMARY KEY,
    canonical_ingredient_id INTEGER,
    unit TEXT,
    grams REAL,
    source TEXT,
    confidence REAL
);

CREATE TABLE IF NOT EXISTS recipe_ingredients_new (
    id INTEGER PRIMARY KEY,
    recipe_id INTEGER,
    recipe_row_id INTEGER,
    line_index INTEGER,
    raw_text TEXT,
    canonical_ingredient_id INTEGER,
    quantity_value REAL,
    quantity_unit TEXT,
    quantity_grams REAL,
    preparation TEXT,
    parsing_confidence REAL,
    needs_review BOOLEAN DEFAULT 0
);

ALTER TABLE recipe_rows ADD COLUMN referenced_recipe_id INTEGER;

CREATE TABLE IF NOT EXISTS pantry_items_new (
    id INTEGER PRIMARY KEY,
    canonical_ingredient_id INTEGER
);

CREATE TABLE IF NOT EXISTS fridge_items_new (
    id INTEGER PRIMARY KEY,
    canonical_ingredient_id INTEGER
);

CREATE TABLE IF NOT EXISTS substitution_events (
    id INTEGER PRIMARY KEY,
    original_ingredient_id INTEGER,
    substituted_ingredient_id INTEGER,
    context TEXT,
    accepted BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

conn.commit()
conn.close()

print("001_create_canonical_tables complete")