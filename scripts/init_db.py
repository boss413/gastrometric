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
    notes TEXT,
    yield TEXT
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
    alias TEXT UNIQUE,
    ingredient_id INTEGER,
    FOREIGN KEY(ingredient_id) REFERENCES ingredients(id)
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

    -- future parsed fields (nullable)
    ingredient_name TEXT,
    quantity_value TEXT,
    quantity_unit TEXT,
    preparation TEXT,

    FOREIGN KEY(recipe_id) REFERENCES recipes(id),
    FOREIGN KEY(recipe_row_id) REFERENCES recipe_rows(id)
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