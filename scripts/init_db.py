import sqlite3

conn = sqlite3.connect("data/gastrometric.db")
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS recipes (
    id INTEGER PRIMARY KEY,
    name TEXT,
    alt_names TEXT,
    author TEXT,
    attribution TEXT,
    source TEXT,
    url TEXT,
    video TEXT,
    notes TEXT,
    yield TEXT,
    state TEXT,
    parent_recipe_id INTEGER,
    ingestion_method TEXT,
    raw_hash
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS recipe_rows (
    id INTEGER PRIMARY KEY,
    recipe_id INTEGER,
    recipe_name TEXT,
    source_row_ref TEXT,
    section_name TEXT,
    ingredient_block TEXT,
    instruction_block TEXT,
    FOREIGN KEY(recipe_id) REFERENCES recipes(id)
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS ingredients (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,
    canonical_group TEXT
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS ingredient_aliases (
    id INTEGER PRIMARY KEY,
    raw_text TEXT UNIQUE,
    canonical_group TEXT,
    confidence INTEGER, -- 1–3 (optional but useful)
    source TEXT -- 'rule', 'manual', 'auto'
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS relationships (
    id INTEGER PRIMARY KEY,
    source_id INTEGER,
    target_id INTEGER,
    score INTEGER,
    source TEXT,  -- 'flavor_bible'
    FOREIGN KEY(source_id) REFERENCES ingredients(id),
    FOREIGN KEY(target_id) REFERENCES ingredients(id)
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS recipe_ingredients (
    id INTEGER PRIMARY KEY,
    recipe_id INTEGER,
    recipe_row_id INTEGER,
    line_index INTEGER,
    raw_text TEXT,
    section TEXT,
    ingredient_id INTEGER,
    ingredient_name TEXT,
    quantity_value REAL,
    quantity_unit TEXT,
    imperial_volume_value REAL,
    imperial_volume_unit TEXT,
    imperial_weight_value REAL,
    imperial_weight_unit TEXT,
    preparation TEXT,
    grams REAL,
    ml REAL,
    scaling REAL,
    optional BOOLEAN DEFAULT 0,

    FOREIGN KEY(recipe_id) REFERENCES recipes(id),
    FOREIGN KEY(recipe_row_id) REFERENCES recipe_rows(id)
)
""")

c.execute("""
CREATE TABLE flavor_bible_raw (
    id INTEGER PRIMARY KEY,
    source_text TEXT,
    target_text TEXT,
    score INTEGER
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS pantry_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingredient_id INTEGER,
    quantity TEXT,
    unit TEXT,
    FOREIGN KEY(ingredient_id) REFERENCES ingredients(id)
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS fridge_items (
    id INTEGER PRIMARY KEY,
    ingredient_id INTEGER,
    name TEXT,
    FOREIGN KEY(ingredient_id) REFERENCES ingredients(id)
)
""")

conn.commit()
conn.close()