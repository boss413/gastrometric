# pipeline/parse/parse_ingredient_blocks.py

# this file is deprecated and will be removed in favor of parse_ingredient_lines.py, 
# which performs the same function but with a more robust and flexible implementation.

import sqlite3
import re

from gastrometric.config.paths import DB_PATH


def split_block(raw_text: str) -> list[str]:
    """
    Split a raw ingredient or instruction block into individual lines.

    Handles:
      - Unix / Windows / bare-CR line endings
      - Bullet characters (•) and leading dashes as list markers

    Returns a list of non-empty stripped strings.
    """
    if not raw_text or not isinstance(raw_text, str):
        return []

    raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = re.split(r"\n|•|(?<!\w)- ", raw_text)
    return [line.strip() for line in lines if line.strip()]


def parse_ingredient_blocks():
    """
    Stage 2 — Ingredient line splitting.

    Reads every row from recipe_ingredient_blocks and splits raw_text into
    individual lines, writing one row per line to recipe_ingredient_lines_raw.

    Metadata carried forward on every row:
        recipe_id            FK → recipes
        recipe_section_id    FK → recipe_sections
        ingredient_block_id  FK → recipe_ingredient_blocks
        recipe_name          denormalised for readability
        section_name
        line_index           position of this line within its block
        raw_text             the individual line, untouched

    All enrichment columns start NULL and are filled by downstream stages.

    Re-run safe: rows already present in recipe_ingredient_lines_raw for a
    given (ingredient_block_id, line_index) pair are skipped.
    """
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        c.execute(
            """
            SELECT
                ib.id,
                ib.recipe_id,
                ib.recipe_section_id,
                ib.recipe_name,
                ib.section_name,
                ib.raw_text
            FROM recipe_ingredient_blocks ib
            WHERE ib.raw_text IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM recipe_ingredient_lines_raw ril
                  WHERE ril.ingredient_block_id = ib.id
              )
            ORDER BY ib.recipe_id, ib.recipe_section_id
            """
        )
        blocks = c.fetchall()

        rows_written = 0

        for ingredient_block_id, recipe_id, recipe_section_id, recipe_name, section_name, raw_text in blocks:
            lines = split_block(raw_text)

            for i, line in enumerate(lines):
                c.execute(
                    """
                    INSERT INTO recipe_ingredient_lines_raw (
                        recipe_id,
                        recipe_section_id,
                        ingredient_block_id,
                        recipe_name,
                        section_name,
                        line_index,
                        raw_text
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        recipe_id,
                        recipe_section_id,
                        ingredient_block_id,
                        recipe_name,
                        section_name if section_name else "default",
                        i,
                        line,
                    ),
                )
                rows_written += 1

        conn.commit()

    return rows_written


def main():
    try:
        rows_written = parse_ingredient_blocks()
        print(f"recipe_ingredient_lines_raw populated with {rows_written} ingredient lines")
    except Exception:
        print("Failed to parse ingredient blocks")
        raise


if __name__ == "__main__":
    main()