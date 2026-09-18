"""
Recipe-level matching against inventory (BE-07).

Architecture this module sits in:

                    Runtime Knowledge
                           |
                           v
    Inventory ───────→ Ingredient Matcher ←────── Recipe Ingredients
                           |
                           v
                    Recipe Matcher              <- this module
                           |
                           v
                    Match classification

This module answers: "given all ingredient matches for a recipe and the
user's inventory, what recipe-match category does this produce?" It does
NOT answer "do these two ingredient identities match, and why?" -- that
question belongs entirely to `ingredient_matcher.py`
(`ingredients_match`), which this module calls rather than reimplements.
It also does not own persistence mechanics -- recipe ingredient reads go
through `recipe_search.py`'s existing functions, and inventory is
supplied by the caller (the HTTP route), already fetched via
`inventory_editor.list_inventory_items()`. This module has no import of
`inventory_repository`, `sqlite3` connection handling for inventory, or
any recipe-understanding/analyzer code.

Quantities are intentionally ignored throughout, per the work order's
explicit scope limitation -- "1 onion" in inventory satisfies "3 onions"
in a recipe; no sufficiency calculation is attempted anywhere here.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from gastrometric.application.ingredient_matcher import (
    IngredientMatch,
    RuntimeKnowledgeLike,
    find_ingredient_matches,
    ingredients_match,
)
from gastrometric.application.recipe_search import (
    RecipeCandidate,
    find_recipes_containing_ingredient,
    get_recipe_ingredient_lines,
)


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchedIngredient:
    """
    One recipe ingredient satisfied by inventory.

    recipe_ingredient_id: the recipe's canonical ingredient identity
        (`recipe_ingredient_lines_parsed.ingredient_id`) that was
        satisfied. An identity field -- never treat this as text safe to
        display or concatenate with anything.
    ingredient_name_original: the recipe's own raw, human-facing wording
        for this ingredient (`recipe_ingredient_lines_parsed.ingredient_name_original`),
        read directly -- NOT run through `render_ingredient_name()`
        (which would additionally concatenate preparation phrases), and
        NOT derived from `ingredient_phrase`. Deliberately a distinct
        contract from `recipe_ingredient_id`: two conceptually different
        strings that may coincidentally look similar (e.g. "tomato" vs
        "tomatoes") should never be assumed interchangeable by a
        consumer.
    inventory_item_id: the specific inventory row (`id`) that satisfied
        it. When multiple eligible inventory rows share the same
        canonical identity (duplicates are explicitly permitted -- see
        module docstring), the lowest `id` among them is used as the
        representative, chosen only for determinism; which specific
        duplicate row gets referenced has no other significance.
    location: the fridge/pantry location of the specific inventory item
        referenced by `inventory_item_id` above, read directly from that
        matched inventory row's own `location` field (BE-06-CO1) -- not
        inferred from `fridge_match_count`/`pantry_match_count`, not
        re-derived, not defaulted. Always one of the existing inventory
        locations ("fridge"/"pantry"); no new location vocabulary is
        introduced here.
    match_type / reason: exactly the `IngredientMatch.match_type` /
        `.reason` produced by `ingredients_match()` for this pair --
        never recomputed or reinterpreted here.
    """

    recipe_ingredient_id: str
    ingredient_name_original: str
    inventory_item_id: int
    location: str
    match_type: str
    reason: Dict[str, Any]


@dataclass(frozen=True)
class MissingIngredient:
    """
    One recipe ingredient with no satisfying inventory item.

    ingredient_id: the recipe's canonical ingredient identity -- an
        identity field, same caveat as `MatchedIngredient.recipe_ingredient_id`.
    ingredient_name_original: the recipe's own raw, human-facing wording
        for this ingredient, read directly from
        `recipe_ingredient_lines_parsed.ingredient_name_original` --
        NOT run through `render_ingredient_name()` (no preparation
        phrases appended here), NOT derived from `ingredient_phrase`,
        and NOT produced by assembling the full recipe via
        `get_recipe()` (this module never calls that). Falls back to
        `ingredient_id` only if the raw column is itself blank, so the
        field is never empty.
    """

    ingredient_id: str
    ingredient_name_original: str


@dataclass(frozen=True)
class RecipeMatch:
    """
    One recipe's classification against the current inventory. Never
    includes recipe sections/instructions -- retrieving those, once a
    recipe is selected, remains `recipe_reader.get_recipe()`'s job.

    `alt_title`/`restaurant`/`source`/`attribution` (BE-06-CO1) are the
    same common recipe browse-card fields `RecipeSearchResult` and the
    favorites read path expose -- read straight through from the
    `RecipeCandidate` this match was classified from, never recomputed
    or re-queried here.
    """

    recipe_id: int
    recipe_name: str
    alt_title: Optional[str]
    restaurant: Optional[str]
    source: Optional[str]
    attribution: Optional[str]
    match_category: str  # "perfect" | "imperfect" | "pantry_only"
    matched_ingredients: Tuple[MatchedIngredient, ...]
    missing_ingredients: Tuple[MissingIngredient, ...]
    fridge_match_count: int
    pantry_match_count: int


_CATEGORY_ORDER = {"perfect": 0, "imperfect": 1, "pantry_only": 2}


# ---------------------------------------------------------------------------
# Inventory eligibility
# ---------------------------------------------------------------------------


def _is_eligible_inventory_item(item: Mapping[str, Any]) -> bool:
    """
    DELIBERATE DESIGN DECISION, made explicit per this work order's own
    instruction not to silently assume every non-null `ingredient_id` is
    matchable:

    An inventory item is eligible for recipe matching ONLY when its
    `resolution_status` is exactly `"resolved"` -- not merely because
    `ingredient_id` happens to be populated.

    Per BE-02D's established contract, `ingredient_id` can be non-null
    even when `resolution_status` is `"unresolved"` (e.g. "purple
    cabbage thing" -> `ingredient_id="cabbage"`,
    `resolution_status="unresolved"`, because "thing" is leftover
    unresolved material even though cabbage itself was identified). This
    work order explicitly warns against treating that as an automatic
    match. I chose the narrow reading: `"ambiguous"` / `"unresolved"` /
    `"invalid"` all represent the understanding pipeline being less than
    fully confident about SOME aspect of the observation, and recipe
    matching should only draw on inventory the pipeline is fully
    confident about -- not make its own judgment call about which
    partially-resolved observations are "confident enough" to use. If
    that reading is wrong for the product, this is the one function to
    change.
    """
    return item.get("resolution_status") == "resolved" and bool(item.get("ingredient_id"))


def _group_eligible_inventory_by_identity(
    inventory_items: Sequence[Mapping[str, Any]],
) -> Dict[str, List[Mapping[str, Any]]]:
    """
    Groups ELIGIBLE inventory items by canonical `ingredient_id`.
    Duplicate rows sharing an identity (explicitly permitted by the
    product) are kept together here so fridge/pantry presence can be
    determined across all of them, without ever exposing the duplicates
    themselves as separate matches later -- see `_pick_representative_item`.
    """
    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for item in inventory_items:
        if not _is_eligible_inventory_item(item):
            continue
        grouped.setdefault(item["ingredient_id"], []).append(item)
    return grouped


def _pick_representative_item(items: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    """Lowest `id`, purely for determinism -- see MatchedIngredient's
    docstring."""
    return min(items, key=lambda item: item["id"])


# ---------------------------------------------------------------------------
# Per-recipe-ingredient matching
# ---------------------------------------------------------------------------


def _find_matching_inventory_item(
    recipe_ingredient_id: str,
    grouped_inventory: Mapping[str, Sequence[Mapping[str, Any]]],
    knowledge: RuntimeKnowledgeLike,
) -> Tuple[Optional[Mapping[str, Any]], Optional[IngredientMatch]]:
    """
    Finds an eligible inventory item satisfying `recipe_ingredient_id`,
    via `ingredients_match()` for every distinct eligible inventory
    identity -- never raw string comparison of its own.

    Prefers a fridge item over a pantry item when more than one distinct
    inventory identity would satisfy this recipe ingredient. Under
    MATCH-01's current exact/alias-only rules this can only happen when
    the SAME canonical ingredient exists in inventory under both a
    fridge row and a pantry row (grouped together already, so this loop
    only sees it once) -- written defensively for when relationship-
    based matching (component/equivalent) arrives and one recipe
    ingredient might plausibly satisfy more than one distinct inventory
    identity at once.

    Returns `(representative_item, IngredientMatch)`, or `(None, None)`.
    """
    best_item: Optional[Mapping[str, Any]] = None
    best_match: Optional[IngredientMatch] = None

    for inventory_ingredient_id, items in grouped_inventory.items():
        match = ingredients_match(inventory_ingredient_id, recipe_ingredient_id, knowledge)
        if match is None:
            continue

        candidate_item = _pick_representative_item(items)
        if best_item is None:
            best_item, best_match = candidate_item, match
            if candidate_item.get("location") == "fridge":
                break  # fridge already found and preferred; stop scanning
        elif candidate_item.get("location") == "fridge" and best_item.get("location") != "fridge":
            best_item, best_match = candidate_item, match
            break

    return best_item, best_match


# ---------------------------------------------------------------------------
# Per-recipe classification
# ---------------------------------------------------------------------------


def _classify_recipe(
    candidate: RecipeCandidate,
    recipe_ingredient_rows: Sequence[Mapping[str, Any]],
    grouped_inventory: Mapping[str, Sequence[Mapping[str, Any]]],
    knowledge: RuntimeKnowledgeLike,
) -> Optional[RecipeMatch]:
    """
    Classifies one candidate recipe against the (already-grouped,
    already-eligibility-filtered) inventory.

    Returns `None` if the recipe doesn't qualify for any category --
    either no inventory overlap at all, or a fridge-less recipe with
    missing ingredients (explicitly excluded from `pantry_only` per the
    work order: "A recipe with no fridge match and missing ingredients
    does not qualify as a pantry-only result").
    """
    seen_recipe_ingredient_ids: Set[str] = set()
    matched: List[MatchedIngredient] = []
    missing: List[MissingIngredient] = []
    fridge_match_count = 0
    pantry_match_count = 0

    for row in recipe_ingredient_rows:
        recipe_ingredient_id = row.get("ingredient_id")
        if not recipe_ingredient_id:
            continue  # unresolved recipe ingredient line -- not a semantic identity to match against
        if recipe_ingredient_id in seen_recipe_ingredient_ids:
            continue  # duplicate recipe ingredient row for an identity already handled
        seen_recipe_ingredient_ids.add(recipe_ingredient_id)

        # Raw, un-rendered wording -- no preparation concatenation here
        # (that's render_ingredient_name()'s job, used only by BE-04's
        # full recipe assembly, not this module). Falls back to the
        # identity itself only if the raw column is blank.
        ingredient_name_original = row.get("ingredient_name_original") or recipe_ingredient_id

        inventory_item, ingredient_match = _find_matching_inventory_item(
            recipe_ingredient_id, grouped_inventory, knowledge
        )

        if inventory_item is None or ingredient_match is None:
            missing.append(
                MissingIngredient(
                    ingredient_id=recipe_ingredient_id,
                    ingredient_name_original=ingredient_name_original,
                )
            )
            continue

        matched.append(
            MatchedIngredient(
                recipe_ingredient_id=recipe_ingredient_id,
                ingredient_name_original=ingredient_name_original,
                inventory_item_id=inventory_item["id"],
                location=inventory_item["location"],
                match_type=ingredient_match.match_type,
                reason=ingredient_match.reason,
            )
        )
        if inventory_item.get("location") == "fridge":
            fridge_match_count += 1
        elif inventory_item.get("location") == "pantry":
            pantry_match_count += 1

    if fridge_match_count == 0 and pantry_match_count == 0:
        return None  # no inventory overlap at all -- excluded, per spec

    has_missing = bool(missing)

    if fridge_match_count > 0 and not has_missing:
        category = "perfect"
    elif fridge_match_count > 0 and has_missing:
        category = "imperfect"
    elif fridge_match_count == 0 and pantry_match_count > 0 and not has_missing:
        category = "pantry_only"
    else:
        # fridge_match_count == 0, pantry_match_count > 0, has_missing:
        # explicitly excluded, not "pantry_only" and not any other category.
        return None

    return RecipeMatch(
        recipe_id=candidate.recipe_id,
        recipe_name=candidate.recipe_name,
        alt_title=candidate.alt_title,
        restaurant=candidate.restaurant,
        source=candidate.source,
        attribution=candidate.attribution,
        match_category=category,
        matched_ingredients=tuple(matched),
        missing_ingredients=tuple(missing),
        fridge_match_count=fridge_match_count,
        pantry_match_count=pantry_match_count,
    )


def _sort_recipe_matches(matches: Sequence[RecipeMatch]) -> List[RecipeMatch]:
    """Perfect, then imperfect, then pantry_only; within each category,
    recipe_name ascending, recipe_id ascending as a stable tie-break --
    no relevance score, no ingredient-count heuristic, per the work
    order's explicit prohibition on inventing a ranking beyond this.
    """
    return sorted(
        matches,
        key=lambda m: (_CATEGORY_ORDER[m.match_category], m.recipe_name, m.recipe_id),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def find_matching_recipes(
    inventory_items: Sequence[Mapping[str, Any]],
    knowledge: RuntimeKnowledgeLike,
    db_path: Optional[str] = None,
) -> Tuple[RecipeMatch, ...]:
    """
    Given the caller's already-fetched inventory (the application-level
    item shape `inventory_editor.list_inventory_items()` returns) and
    runtime knowledge, returns every recipe with at least one inventory
    match, classified and deterministically ordered.

    `inventory_items` is supplied by the caller rather than fetched here
    -- this module never imports `inventory_editor` or
    `inventory_repository` -- keeping this module's only persistence
    dependency the existing `recipe_search.py` read functions
    (`find_recipes_containing_ingredient`, `get_recipe_ingredient_lines`),
    reused rather than duplicated.

    An empty or fully-ineligible inventory returns `()`, not an error.
    A recipe collection with no overlap against inventory returns `()`.
    """
    grouped_inventory = _group_eligible_inventory_by_identity(inventory_items)
    if not grouped_inventory:
        return ()

    # Candidate recipes: any recipe sharing at least one ingredient
    # identity with eligible inventory. Each inventory identity is
    # resolved to its canonical name(s) via find_ingredient_matches
    # FIRST -- not queried against recipes using the raw inventory
    # identity directly. This matters whenever an inventory identity
    # isn't already in its exact canonical form (e.g. stored as an
    # alias): recipe_ingredient_lines_parsed.ingredient_id is written in
    # canonical form, so querying with an unresolved alias string would
    # silently find nothing, even though ingredients_match() would
    # correctly recognize the two as the same ingredient once compared
    # pairwise. Resolving here first is what makes such a recipe become
    # a candidate at all. A dict collapses a recipe found via multiple
    # distinct inventory identities down to one entry.
    candidate_recipes: Dict[int, RecipeCandidate] = {}
    for ingredient_id in grouped_inventory:
        canonical_names = {
            match.ingredient_name for match in find_ingredient_matches(ingredient_id, knowledge)
        }
        if not canonical_names:
            # Doesn't resolve via the matcher at all (e.g. unexpected/
            # stale inventory data outside current knowledge). Fall back
            # to querying with the raw identity itself -- it might still
            # literally equal a recipe's persisted ingredient_id even if
            # knowledge doesn't currently recognize it, and this way
            # nothing is silently dropped from candidate consideration.
            canonical_names = {ingredient_id}
        for name in canonical_names:
            for candidate in find_recipes_containing_ingredient(name, db_path=db_path):
                candidate_recipes[candidate.recipe_id] = candidate

    if not candidate_recipes:
        return ()

    results: List[RecipeMatch] = []
    for recipe_id, candidate in candidate_recipes.items():
        recipe_ingredient_rows = get_recipe_ingredient_lines(recipe_id, db_path=db_path)
        match = _classify_recipe(
            candidate, recipe_ingredient_rows, grouped_inventory, knowledge
        )
        if match is not None:
            results.append(match)

    return tuple(_sort_recipe_matches(results))