# scripts/build_ingredients.py

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# clear ingredients
c.execute("DELETE FROM ingredients")

# insert unique ingredient names
c.execute("""
INSERT INTO ingredients (name)
SELECT DISTINCT ingredient_name
FROM recipe_ingredients
WHERE ingredient_name IS NOT NULL
""")

conn.commit()
conn.close()

print("Ingredients table rebuilt")