"""
Application-layer recipe read service (BE-04).

`get_recipe(recipe_id, db_path=None)` is the one public operation: given
a `recipe_id`, assembles and returns the complete application-facing
`Recipe` representation (`recipe_models.Recipe`) from the existing
recipe persistence artifacts (`recipes`, `recipe_sections`,
`recipe_ingredient_lines_raw`, `recipe_ingredient_lines_parsed`,
`recipe_instruction_blocks`). Strictly read-only -- no
INSERT/UPDATE/DELETE statement appears anywhere in this module -- and
performs no semantic reasoning of its own: every `ingredient_id`,
quantity dimension, and preparation phrase already came from the
analyzer: this module only assembles and renders what's already
persisted (see `recipe_presentation.py` for the actual name/quantity
rendering).

No FastAPI or other HTTP-framework dependency (work order section 19):
this is callable directly by tests, a future CLI, and eventually the
recipe HTTP API, without any of them needing to construct an HTTP
request/response to use it.

--------------------------------------------------------------------
SCHEMA CONFIDENCE NOTE
--------------------------------------------------------------------
`recipes`, `recipe_instruction_blocks`, and `recipe_ingredient_lines_parsed`
below use exact, confirmed column names (from the real CREATE TABLE
statements). `recipe_sections`'s columns are also confirmed (from a real
header row: id, recipe_id, recipe_name, section_name, source_section_ref,
ingredient_block, instruction_block -- the last two are raw
ingestion-stage blobs this module deliberately does NOT read from; BE-04
reads the already-split `recipe_ingredient_lines_raw` /
`recipe_instruction_blocks` rows instead, per the work order's own list
of persistence sources).

`recipe_ingredient_lines_raw`'s columns, by contrast, are RECONSTRUCTED
from a single example data row, not confirmed against a real CREATE
TABLE statement -- I'm inferring:

    id, recipe_id, recipe_section_id, line_index,
    recipe_name, section_name, ingredient_block_id, raw_text

`line_index` (needed for `ORDER BY` below, work order section 12) and
`ingredient_block_id` (unused here, but presumably the FK counterpart to
`recipe_ingredient_lines_parsed.ingredient_block_id`) are my best guess
at those two column names specifically. If either is wrong, the queries
below fail loudly with a clear `sqlite3.OperationalError` naming the bad
column -- not a silent misordering -- but this is still worth a quick
confirmation against the actual schema before relying on it.
"""

import sqlite3
from typing import Any, Dict, List, Optional

from gastrometric.application.recipe_models import (
    Recipe,
    RecipeIngredient,
    RecipeInstruction,
    RecipeNotFoundError,
    RecipeSection,
)
from gastrometric.application.recipe_presentation import render_ingredient_name, render_quantity
from gastrometric.config.paths import DB_PATH


def _connect(db_path: Optional[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _fetch_recipe_row(conn: sqlite3.Connection, recipe_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchone()


def _fetch_sections(conn: sqlite3.Connection, recipe_id: int) -> List[sqlite3.Row]:
    """Ordered by `recipe_sections.id` ascending -- section IDs are
    autoincremented in processing order (work order section 13). Never
    alphabetized, never inferred from `section_name`.
    """
    return conn.execute(
        "SELECT * FROM recipe_sections WHERE recipe_id = ? ORDER BY id ASC",
        (recipe_id,),
    ).fetchall()


def _fetch_parsed_ingredient_rows(conn: sqlite3.Connection, recipe_id: int) -> List[sqlite3.Row]:
    """
    Joins `recipe_ingredient_lines_parsed` to `recipe_ingredient_lines_raw`
    to get `line_index`, the current source-ordering column (work order
    section 12) -- explicitly NOT `recipe_ingredient_lines_parsed.id`,
    which that same section says must not be treated as the ordering
    rule.
    """
    return conn.execute(
        """
        SELECT parsed.*, raw.line_index AS _line_index
        FROM recipe_ingredient_lines_parsed AS parsed
        JOIN recipe_ingredient_lines_raw AS raw
            ON raw.id = parsed.recipe_ingredient_line_id
        WHERE parsed.recipe_id = ?
        ORDER BY parsed.recipe_section_id ASC, raw.line_index ASC
        """,
        (recipe_id,),
    ).fetchall()


def _fetch_instruction_rows(conn: sqlite3.Connection, recipe_id: int) -> List[sqlite3.Row]:
    """Ordered by `recipe_instruction_blocks.id` ascending -- the
    database id establishes order but is NOT the cook-facing instruction
    number (work order section 14); presentation numbers are assigned
    separately below, starting at 1 within each section.
    """
    return conn.execute(
        "SELECT * FROM recipe_instruction_blocks WHERE recipe_id = ? ORDER BY id ASC",
        (recipe_id,),
    ).fetchall()


def _resolve_display_name_base(row: Dict[str, Any]) -> str:
    """
    The base ingredient identity string to build `name` from:
    `ingredient_name_original` when present (the normal case), falling
    back to the raw `ingredient_phrase` for a line the analyzer couldn't
    identify at all, and finally an empty string rather than raising if
    even that's absent.

    This fallback lives here, in the reader, rather than in
    `recipe_presentation.py`, because it's about what to do when
    expected data is missing -- an assembly concern -- not about
    rendering already-present data, which is what that module does.
    """
    if not _is_blank(row.get("ingredient_name_original")):
        return row["ingredient_name_original"]
    if not _is_blank(row.get("ingredient_phrase")):
        return row["ingredient_phrase"]
    return ""


def _row_to_ingredient(row: sqlite3.Row) -> RecipeIngredient:
    data = dict(row)
    name_base = _resolve_display_name_base(data)
    return RecipeIngredient(
        ingredient_id=data.get("ingredient_id"),
        name=render_ingredient_name(name_base, data.get("preparation")),
        quantity=render_quantity(data),
        grams=data.get("grams"),
        notes=data.get("notes"),
        optional=bool(data.get("optional")),
        alt_group_id=data.get("alt_group_id"),
        alt_kind=data.get("alt_kind"),
    )


def _row_to_instruction(row: sqlite3.Row, number: int) -> RecipeInstruction:
    return RecipeInstruction(number=number, text=row["raw_text"])


def get_recipe(recipe_id: int, db_path: Optional[str] = None) -> Recipe:
    """
    Retrieves and assembles the complete application-facing
    representation of one recipe.

    Raises `RecipeNotFoundError` if `recipe_id` does not correspond to a
    persisted recipe (work order section 18) -- never returns an empty
    recipe object for a missing id.

    Performs no writes, no re-parsing, and no semantic reasoning -- see
    the module docstring.
    """
    conn = _connect(db_path)
    try:
        recipe_row = _fetch_recipe_row(conn, recipe_id)
        if recipe_row is None:
            raise RecipeNotFoundError(recipe_id)

        section_rows = _fetch_sections(conn, recipe_id)
        ingredient_rows = _fetch_parsed_ingredient_rows(conn, recipe_id)
        instruction_rows = _fetch_instruction_rows(conn, recipe_id)
    finally:
        conn.close()

    ingredients_by_section: Dict[int, List[RecipeIngredient]] = {}
    for row in ingredient_rows:
        section_id = row["recipe_section_id"]
        ingredients_by_section.setdefault(section_id, []).append(_row_to_ingredient(row))

    # Presentation numbers start at 1 WITHIN each section (work order
    # section 14) -- not a single running count across the whole recipe.
    instructions_by_section: Dict[int, List[RecipeInstruction]] = {}
    for row in instruction_rows:
        section_id = row["recipe_section_id"]
        bucket = instructions_by_section.setdefault(section_id, [])
        bucket.append(_row_to_instruction(row, number=len(bucket) + 1))

    sections = [
        RecipeSection(
            id=section_row["id"],
            name=section_row["section_name"],
            ingredients=ingredients_by_section.get(section_row["id"], []),
            instructions=instructions_by_section.get(section_row["id"], []),
        )
        for section_row in section_rows
    ]

    recipe_data = dict(recipe_row)
    return Recipe(
        id=recipe_data["id"],
        recipe_name=recipe_data["recipe_name"],
        recipe_author=recipe_data.get("recipe_author"),
        recipe_attribution=recipe_data.get("recipe_attribution"),
        recipe_source=recipe_data.get("recipe_source"),
        recipe_url=recipe_data.get("recipe_url"),
        recipe_video=recipe_data.get("recipe_video"),
        recipe_notes=recipe_data.get("recipe_notes"),
        recipe_yield=recipe_data.get("recipe_yield"),
        recipe_state=recipe_data.get("recipe_state"),
        recipe_ingestion_method=recipe_data.get("recipe_ingestion_method"),
        sections=sections,
    )