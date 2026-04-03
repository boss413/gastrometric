# scripts/parse_ingredient_blocks.py

import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

def split_ingredient_block(block):
    if not block or not isinstance(block, str):
        return []

    # Normalize line breaks
    block = block.replace('\r\n', '\n').replace('\r', '\n')

    # Split on common delimiters
    lines = re.split(r'\n|•|- ', block)

    # Clean
    cleaned = []
    for line in lines:
        line = line.strip()
        if line:
            cleaned.append(line)

    return cleaned


conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Pull all rows with ingredient blocks
c.execute("""
SELECT id, recipe_id, section_name, ingredient_block
FROM recipe_rows
WHERE ingredient_block IS NOT NULL
""")

rows = c.fetchall()

ingredient_id_counter = 0

for row_id, recipe_id, section_name, block in rows:
    lines = split_ingredient_block(block)

    for i, line in enumerate(lines):
        c.execute("""
        INSERT INTO recipe_ingredients
        (recipe_id, recipe_row_id, line_index, raw_text, section)
        VALUES (?, ?, ?, ?, ?)
        """, (
            recipe_id,
            row_id,
            i,
            line,
            section_name if section_name else "default"
        ))

conn.commit()
conn.close()

print("Ingredient blocks parsed into recipe_ingredients")