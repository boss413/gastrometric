import sqlite3
import os

from parse_markdown_recipes import parse_markdown_file

DB_PATH = os.path.abspath("data/gastrometric.db")
MD_PATH = os.path.abspath("data/seed/recipes.md")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

recipes = parse_markdown_file(MD_PATH)

for recipe in recipes:
    c.execute("""
    INSERT INTO recipes (
        name,
        author,
        attribution,
        source,
        url,
        video,
        notes,
        yield,
        state,
        ingestion_method
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'raw', 'manual')
""", (
    recipe["name"],
    recipe["metadata"].get("author"),
    recipe["metadata"].get("attribution"),
    recipe["metadata"].get("source"),
    recipe["metadata"].get("url"),
    recipe["metadata"].get("video"),
    recipe["metadata"].get("notes") or recipe["metadata"].get("note"),
    recipe["metadata"].get("yield"),
))

    recipe_id = c.lastrowid

    for section in recipe["sections"]:
        c.execute("""
            INSERT INTO recipe_rows (recipe_id, section_name, ingredient_block, instruction_block)
            VALUES (?, ?, ?, ?)
        """, (
            recipe_id,
            section["name"],
            "\n".join(section["ingredients"]),
            "\n".join(section["instructions"]),
        ))

        row_id = c.lastrowid

        # ingredients
        for i, ing in enumerate(section["ingredients"]):
            c.execute("""
                INSERT INTO recipe_ingredients (
                    recipe_id, recipe_row_id, line_index, raw_text
                )
                VALUES (?, ?, ?, ?)
            """, (
                recipe_id,
                row_id,
                i,
                ing
            ))

        # instructions
        for step in section["instructions"]:
            c.execute("""
                UPDATE recipe_rows
                SET instruction_block = instruction_block || ? || '\n'
                WHERE id = ?
            """, (step, row_id))

conn.commit()
conn.close()

recipes = parse_markdown_file(MD_PATH)

print("Total recipes:", len(recipes))

section_total = sum(len(r["sections"]) for r in recipes)
print("Total sections:", section_total)