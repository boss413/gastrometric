"""
Ingredient knowledge builder.

Loads canonical ingredient identities and aliases from
data/seed/ingredients.json into the SQLite knowledge database.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from gastrometric.config.paths import DB_PATH, SEED_DIR
from gastrometric.knowledge.builder import KnowledgeBuilder
from gastrometric.knowledge.models import BuildResult


class IngredientBuilder(KnowledgeBuilder):
    """Populate ingredient identities and aliases."""

    name = "ingredients"

    def __init__(self, seed_path: Path | None = None) -> None:
        self.seed_path = seed_path or (SEED_DIR / "ingredients.json")

    def run(self, conn: sqlite3.Connection) -> BuildResult:
        print("Building ingredients and ingredient aliases to gastrometric.db")

        try:
            with self.seed_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            identities = data.get("identities")
            if not isinstance(identities, list):
                raise ValueError(
                    "ingredients.json must contain an 'identities' array."
                )

            cursor = conn.cursor()

            cursor.execute("DELETE FROM ingredient_aliases")
            cursor.execute("DELETE FROM ingredients")

            ingredient_count = 0
            alias_count = 0
            seen_aliases: dict[str, str] = {}

            for identity in identities:
                ingredient_id = identity["id"]
                ingredient_name = identity["name"]
                notes = identity.get("notes")

                cursor.execute(
                    """
                    INSERT INTO ingredients (
                        id,
                        ingredient_name,
                        notes
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        ingredient_id,
                        ingredient_name,
                        notes,
                    ),
                )
                ingredient_count += 1

                aliases = identity.get("aliases", [])

                if not isinstance(aliases, list):
                    raise ValueError(
                        f"Ingredient '{ingredient_id}' aliases must be a list."
                    )

                for alias in aliases:
                    normalized = alias.strip().lower()

                    if normalized in seen_aliases:
                        previous = seen_aliases[normalized]
                        raise ValueError(
                            f"Duplicate alias '{alias}' claimed by "
                            f"'{previous}' and '{ingredient_id}'."
                        )

                    seen_aliases[normalized] = ingredient_id

                    cursor.execute(
                        """
                        INSERT INTO ingredient_aliases (
                            ingredient_id,
                            alias,
                            confidence,
                            source
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            ingredient_id,
                            normalized,
                            1.0,
                            "ingredients.json",
                        ),
                    )
                    alias_count += 1

            conn.commit()

            print(f"{ingredient_count:,} ingredients added to ingredients")
            print(f"{alias_count:,} ingredient aliases added")

            return BuildResult(
                builder_name=self.name,
                distinct_inputs=ingredient_count,
                vocabulary_created=ingredient_count,
                aliases_created=alias_count,
            )

        except FileNotFoundError:
            conn.rollback()
            print(f"ERROR: Could not find {self.seed_path}")
            raise

        except json.JSONDecodeError as e:
            conn.rollback()
            print(f"ERROR: Failed to parse {self.seed_path.name}: {e}")
            raise

        except sqlite3.Error as e:
            conn.rollback()
            print(f"ERROR: Failed to write to database: {e}")
            raise

        except Exception as e:
            conn.rollback()
            print(f"ERROR: {e}")
            raise

def build_ingredients() -> BuildResult:
    """Build ingredient identities and aliases into gastrometric.db."""

    conn = sqlite3.connect(DB_PATH)
    try:
        return IngredientBuilder().run(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    build_ingredients()
    connection = sqlite3.connect(DB_PATH)

    try:
        IngredientBuilder().run(connection)
    finally:
        connection.close()