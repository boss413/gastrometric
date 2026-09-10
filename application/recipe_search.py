"""
Application-level recipe search (BE-06).

Two independent search modes:

  - search_recipes_by_name: textual match against `recipes.recipe_name`.
      Never touches the ingredient matcher.
  - search_recipes_by_ingredient: query -> MATCH-01
      (`find_ingredient_matches`) -> canonical ingredient name
      candidate(s) -> recipes whose parsed ingredient identity
      (`recipe_ingredient_lines_parsed.ingredient_id`) matches one of
      those candidates.

Both return lightweight `RecipeSearchResult` candidates -- recipe_id +
recipe_name (+ matcher-derived match info for ingredient search) --
never full recipe sections/ingredients/instructions. When a caller wants
the complete recipe, `application.recipe_reader.get_recipe()` remains
the one place that assembles it; this module never duplicates that
logic.

--------------------------------------------------------------------
Canonical name -> persisted ingredient_id translation
--------------------------------------------------------------------
Per MATCH-01's contract, the matcher returns a canonical ingredient
NAME (e.g. "chicken breast") -- there is no separate id space at the
knowledge layer. This module is responsible for using that name as the
query value against `recipe_ingredient_lines_parsed.ingredient_id`
directly (see `_query_recipes_by_ingredient_name` below).

ASSUMPTION, flagged rather than silently relied on: this treats the
knowledge layer's canonical-name space and
`recipe_ingredient_lines_parsed.ingredient_id`'s persisted value space
as the same strings. The one confirmed real example I have --
`ingredient_id: "chuck roast"` from an actual persisted row -- is
consistent with this (a plain canonical name, not a hyphenated slug),
but I have not been shown a case that would prove or disprove exact
equivalence for every ingredient. If the two spaces ever diverge (e.g.
different casing, punctuation, or a real id scheme introduced later),
this function is the one place that needs to change -- MATCH-01 itself
has no persistence awareness to update.

No SQL against `recipe_sections`, `recipe_ingredient_lines_raw`, or
`recipe_instruction_blocks` is needed here -- search only touches
`recipes` and `recipe_ingredient_lines_parsed`, both of which have
schemas confirmed directly (unlike the other three tables -- see
`recipe_reader.py`'s module docstring for that separate, still-open
caveat, which this module doesn't inherit since it never queries those
tables).
"""

import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from gastrometric.application.ingredient_matcher import (
    RuntimeKnowledgeLike,
    find_ingredient_matches,
)
from gastrometric.config.paths import DB_PATH


@dataclass(frozen=True)
class RecipeSearchResult:
    """
    One lightweight recipe candidate.

    `matched_ingredient` through `matched_term` are populated only for
    ingredient-search results -- always `None` for name-search results,
    since a name search never involves the matcher and has nothing to
    report.
    """

    recipe_id: int
    recipe_name: str
    matched_ingredient: Optional[str] = None
    match_type: Optional[str] = None
    match_reason: Optional[Dict[str, Any]] = None
    matched_term: Optional[str] = None


def _connect(db_path: Optional[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _escape_like(text: str) -> str:
    """Escapes SQLite LIKE wildcards (%, _) in user input so a query
    containing them is matched literally rather than as a pattern."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# ---------------------------------------------------------------------------
# Recipe-name search
# ---------------------------------------------------------------------------


def search_recipes_by_name(
    query: str, db_path: Optional[str] = None
) -> Tuple[RecipeSearchResult, ...]:
    """
    Textual recipe-name search: a case-insensitive substring match
    against `recipes.recipe_name`, using ordinary SQL (`LIKE`) -- no
    FTS5, per the work order's explicit preference not to introduce it
    without first establishing that ordinary SQL is inadequate, which
    hasn't been shown for this PoC's data volume.

    Deterministic: results ordered by `recipe_name` ascending, then `id`
    ascending as a stable tie-break for recipes sharing a name.

    An empty/whitespace-only query returns `()` -- never "every recipe."

    Never touches the ingredient matcher.
    """
    normalized_query = query.strip()
    if not normalized_query:
        return ()

    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT id, recipe_name
            FROM recipes
            WHERE recipe_name LIKE ? ESCAPE '\\'
            ORDER BY recipe_name ASC, id ASC
            """,
            (f"%{_escape_like(normalized_query)}%",),
        ).fetchall()
    finally:
        conn.close()

    return tuple(
        RecipeSearchResult(recipe_id=row["id"], recipe_name=row["recipe_name"]) for row in rows
    )


# ---------------------------------------------------------------------------
# Ingredient search
# ---------------------------------------------------------------------------


def _query_recipes_by_ingredient_name(
    conn: sqlite3.Connection, ingredient_name: str
) -> List[sqlite3.Row]:
    """
    The one place this module translates a canonical ingredient name
    into a `recipe_ingredient_lines_parsed.ingredient_id` query -- see
    the module docstring's "Canonical name -> persisted ingredient_id
    translation" section for the assumption this rests on.

    `DISTINCT` collapses multiple parsed-line matches within the SAME
    recipe (e.g. an ingredient appearing twice) into one row.
    """
    return conn.execute(
        """
        SELECT DISTINCT recipes.id AS recipe_id, recipes.recipe_name AS recipe_name
        FROM recipe_ingredient_lines_parsed AS parsed
        JOIN recipes ON recipes.id = parsed.recipe_id
        WHERE parsed.ingredient_id = ?
        ORDER BY recipes.recipe_name ASC, recipes.id ASC
        """,
        (ingredient_name,),
    ).fetchall()


def search_recipes_by_ingredient(
    query: str,
    knowledge: RuntimeKnowledgeLike,
    db_path: Optional[str] = None,
) -> Tuple[RecipeSearchResult, ...]:
    """
    Ingredient recipe search: query -> MATCH-01 -> canonical ingredient
    name candidate(s) -> distinct recipes whose parsed ingredient
    identity matches one of those candidates.

    Contains no ingredient-identity logic of its own -- every decision
    about what `query` resolves to happens inside
    `find_ingredient_matches`; this function only consumes its output.
    Iterates over however many `IngredientMatch` candidates the matcher
    returns (currently at most one, under MATCH-01's rules) and
    de-duplicates recipes ACROSS candidates too (via `seen_recipe_ids`,
    not just within one candidate's own `DISTINCT` query) -- so a future
    matcher returning multiple candidates (e.g. component matches)
    requires no change here.

    An unknown ingredient (matcher returns no candidates) yields an
    empty result tuple, not an error. An empty/whitespace-only query
    returns `()` without even calling the matcher.
    """
    normalized_query = query.strip()
    if not normalized_query:
        return ()

    matches = find_ingredient_matches(normalized_query, knowledge)
    if not matches:
        return ()

    conn = _connect(db_path)
    try:
        results: List[RecipeSearchResult] = []
        seen_recipe_ids: Set[int] = set()
        for match in matches:
            for row in _query_recipes_by_ingredient_name(conn, match.ingredient_name):
                if row["recipe_id"] in seen_recipe_ids:
                    continue
                seen_recipe_ids.add(row["recipe_id"])
                results.append(
                    RecipeSearchResult(
                        recipe_id=row["recipe_id"],
                        recipe_name=row["recipe_name"],
                        matched_ingredient=match.ingredient_name,
                        match_type=match.match_type,
                        match_reason=match.reason,
                        matched_term=match.matched_term,
                    )
                )
    finally:
        conn.close()

    # NOTE: each candidate's own rows are individually ordered by
    # recipe_name/id (see _query_recipes_by_ingredient_name), but the
    # OVERALL list across multiple candidates is only ordered
    # candidate-by-candidate, not globally re-sorted. A non-issue today
    # since MATCH-01 never returns more than one candidate; worth
    # revisiting if/when it does.
    return tuple(results)