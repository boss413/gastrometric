# scripts/link_ingredients.py

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""
SELECT id, ingredient_name
FROM recipe_ingredients
WHERE ingredient_name IS NOT NULL
""")

rows = c.fetchall()

for row_id, name in rows:
    normalized = name.strip().lower()

    c.execute("""
    SELECT ingredient_id FROM ingredient_aliases WHERE alias = ?
    """, (normalized,))
    result = c.fetchone()

    if result:
        ingredient_id = result[0]

        c.execute("""
        UPDATE recipe_ingredients
        SET ingredient_id = ?
        WHERE id = ?
        """, (ingredient_id, row_id))

conn.commit()
conn.close()

print("Recipe ingredients linked to canonical ingredients")