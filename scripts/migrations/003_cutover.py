import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

print("Old count:", c.execute("SELECT COUNT(*) FROM recipe_ingredients").fetchone()[0])
print("New count:", c.execute("SELECT COUNT(*) FROM recipe_ingredients_new").fetchone()[0])

print("Unresolved:",
      c.execute("""
      SELECT COUNT(*) FROM recipe_ingredients_new
      WHERE canonical_ingredient_id IS NULL
      """).fetchone()[0])

conn.close()