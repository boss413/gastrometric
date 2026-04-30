import sqlite3

from gastrometric.pipeline.parse.parse_markdown_recipes import parse_markdown_file
from gastrometric.config.paths import DB_PATH, DATA_DIR

MD_PATH = DATA_DIR / "seed" / "recipes.md"


def ingest_markdown():
    """
    Stage 1 — Ingestion.

    Reads recipes.md and populates four tables:

        recipes
            One row per recipe, all metadata fields.

        recipe_sections
            One row per named section (e.g. "Cook the aromatics").
            ingredient_block and instruction_block are stored whole — no
            splitting or interpretation occurs here.

        recipe_ingredient_blocks
            One row per section.  Holds the raw ingredient blob so the
            ingredient parse stage has a clean, dedicated source table.

        recipe_instruction_blocks
            One row per section.  Mirrors recipe_ingredient_blocks for
            the instruction pipeline.
    """
    recipes = parse_markdown_file(MD_PATH)

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        for recipe in recipes:
            recipe_name = recipe["name"]

            # ----------------------------------------------------------------
            # recipes
            # ----------------------------------------------------------------
            c.execute(
                """
                INSERT INTO recipes (
                    recipe_name,
                    recipe_author,
                    recipe_attribution,
                    recipe_source,
                    recipe_url,
                    recipe_video,
                    recipe_notes,
                    recipe_yield,
                    recipe_state,
                    recipe_ingestion_method
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'raw', 'manual')
                """,
                (
                    recipe_name,
                    recipe["metadata"].get("author"),
                    recipe["metadata"].get("attribution"),
                    recipe["metadata"].get("source"),
                    recipe["metadata"].get("url"),
                    recipe["metadata"].get("video"),
                    recipe["metadata"].get("notes") or recipe["metadata"].get("note"),
                    recipe["metadata"].get("yield"),
                ),
            )
            recipe_id = c.lastrowid

            for section in recipe["sections"]:
                section_name = section["name"]
                ingredient_block = "\n".join(section["ingredients"])
                instruction_block = "\n".join(section["instructions"])
                source_section_ref = (
                    f"{recipe_name}::{section_name}" if section_name else recipe_name
                )

                # ------------------------------------------------------------
                # recipe_sections
                # ------------------------------------------------------------
                c.execute(
                    """
                    INSERT INTO recipe_sections (
                        recipe_id,
                        recipe_name,
                        section_name,
                        source_section_ref,
                        ingredient_block,
                        instruction_block
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        recipe_id,
                        recipe_name,
                        section_name,
                        source_section_ref,
                        ingredient_block,
                        instruction_block,
                    ),
                )
                recipe_section_id = c.lastrowid

                # ------------------------------------------------------------
                # recipe_ingredient_blocks — one row per section, blob stored whole
                # ------------------------------------------------------------
                c.execute(
                    """
                    INSERT INTO recipe_ingredient_blocks (
                        recipe_id,
                        recipe_section_id,
                        recipe_name,
                        section_name,
                        raw_text
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        recipe_id,
                        recipe_section_id,
                        recipe_name,
                        section_name,
                        ingredient_block,
                    ),
                )

                # ------------------------------------------------------------
                # recipe_instruction_blocks — one row per section, blob stored whole
                # ------------------------------------------------------------
                c.execute(
                    """
                    INSERT INTO recipe_instruction_blocks (
                        recipe_id,
                        recipe_section_id,
                        recipe_name,
                        section_name,
                        raw_text
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        recipe_id,
                        recipe_section_id,
                        recipe_name,
                        section_name,
                        instruction_block,
                    ),
                )

        conn.commit()

    recipe_count = len(recipes)
    section_count = sum(len(r["sections"]) for r in recipes)

    print(f"Ingested {recipe_count} recipes")
    print(f"Ingested {section_count} sections")
    print(f"Staged {section_count} ingredient blocks")
    print(f"Staged {section_count} instruction blocks")


def main():
    ingest_markdown()


if __name__ == "__main__":
    main()