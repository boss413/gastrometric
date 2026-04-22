# scripts/load_csv.py
import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")
BASE_DIR = os.path.dirname(__file__)  # scripts/
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

RECIPES_CSV = os.path.join(DATA_DIR, "recipes.csv")
ROWS_CSV = os.path.join(DATA_DIR, "recipe_rows.csv")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Load recipes
recipes_df = pd.read_csv(RECIPES_CSV, engine='python')
for _, row in recipes_df.iterrows():
    c.execute("""
        INSERT OR IGNORE INTO recipes (id, name, alt_names, author, source, url, notes, yield, state, parent_recipe_id, ingestion_method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (row['id'], row['recipe_name'], row.get('alt_names', None),
          row.get('recipe_author', None), row.get('recipe_attribution', None),
          row.get('recipe_source', None), row.get('notes', None),
          row.get('yield', None), row.get('state', None), row.get('parent_recipe_id', None), row.get('ingestion_method', None)))

# Load recipe_rows
rows_df = pd.read_csv(ROWS_CSV)
for _, row in rows_df.iterrows():
    c.execute("""
        INSERT OR IGNORE INTO recipe_rows (id, recipe_id, recipe_name, source_row_ref, section_name, ingredient_block, instruction_block)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (row['id'], row['recipe_id'], row.get('recipe_name', None),
          row.get('source_row_ref', None), row.get('section_name', None),
          row.get('ingredient_block', None), row.get('instruction_block', None)))

conn.commit()
conn.close()

print("CSV data loaded successfully into gastrometric.db")