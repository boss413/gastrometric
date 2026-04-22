import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.executescript("""
-- populate canonical ingredients
INSERT OR IGNORE INTO canonical_ingredients (name)
SELECT DISTINCT name FROM ingredients WHERE name IS NOT NULL;

-- temp mapping
DROP TABLE IF EXISTS ingredient_to_canonical_map;

CREATE TABLE ingredient_to_canonical_map AS
SELECT 
    i.id AS old_ingredient_id,
    c.id AS canonical_ingredient_id
FROM ingredients i
JOIN canonical_ingredients c
  ON i.name = c.name;

-- migrate recipe ingredients
INSERT INTO recipe_ingredients_new (
    id,
    recipe_id,
    recipe_row_id,
    line_index,
    raw_text,
    canonical_ingredient_id,
    quantity_value,
    quantity_unit,
    preparation,
    parsing_confidence,
    needs_review
)
SELECT
    ri.id,
    ri.recipe_id,
    ri.recipe_row_id,
    ri.line_index,
    ri.raw_text,
    m.canonical_ingredient_id,
    CAST(ri.quantity_value AS REAL),
    ri.quantity_unit,
    ri.preparation,
    CASE WHEN ri.ingredient_name IS NOT NULL THEN 0.8 ELSE 0.2 END,
    CASE WHEN m.canonical_ingredient_id IS NULL THEN 1 ELSE 0 END
FROM recipe_ingredients ri
LEFT JOIN ingredient_to_canonical_map m
  ON ri.ingredient_id = m.old_ingredient_id;

-- pantry
INSERT INTO pantry_items_new (canonical_ingredient_id)
SELECT DISTINCT m.canonical_ingredient_id
FROM pantry_items p
JOIN ingredient_to_canonical_map m
  ON p.ingredient_id = m.old_ingredient_id;

-- fridge
INSERT INTO fridge_items_new (canonical_ingredient_id)
SELECT DISTINCT m.canonical_ingredient_id
FROM fridge_items f
JOIN ingredient_to_canonical_map m
  ON f.ingredient_id = m.old_ingredient_id;
""")

conn.commit()
conn.close()

print("002_migrate_to_canonical complete")