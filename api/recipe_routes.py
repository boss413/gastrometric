"""
Recipe HTTP routes (BE-05: retrieval; BE-06: search; BE-07: inventory match).

Architecture this module must not violate:

    API route (this module)
        -> application.recipe_reader.get_recipe()              (BE-05)
        -> application.recipe_search.search_recipes_by_name()  (BE-06)
        -> application.recipe_search.search_recipes_by_ingredient()
             -> application.ingredient_matcher.find_ingredient_matches()
        -> application.recipe_matcher.find_matching_recipes()   (BE-07)
             -> application.ingredient_matcher.ingredients_match()
        -> HTTP response models (api.recipe_api_models)

Same pattern throughout: parse the request, call exactly one
application-layer function per route, shape its result into a response
model, translate exceptions into the API's existing error contract.
Nothing here touches SQLite, the lexer, parser, analyzer, recipe
presentation logic, or ingredient-matching logic directly -- all of that
already happened inside the application layer; this module only
dispatches and reshapes. In particular, `get_recipe_matches_route` does
not compare a single ingredient pair, classify a recipe, or decide
fridge-vs-pantry precedence -- all of that is `recipe_matcher.py`'s job.

ROUTE ORDERING NOTE: `search_recipes_route` (`/recipes/search`) and
`get_recipe_matches_route` (`/recipes/matches`) are both registered
BEFORE `get_recipe_route` (`/recipes/{recipe_id}`), deliberately.
FastAPI/Starlette matches routes in registration order, and `{recipe_id}`
is a plain string placeholder (not `{recipe_id:int}`) -- Starlette
matches it on the path shape alone, and only FastAPI's Pydantic-level
parameter binding then tries to convert the segment to `int`. If the
dynamic route were registered first, a request to `/api/recipes/search`
or `/api/recipes/matches` would match it first too (with "search"/
"matches" then failing int conversion -> the wrong 422), never reaching
either static route at all. Registering both static paths first avoids
that entirely.
"""

import dataclasses
from typing import Any, List, Literal, Optional

from fastapi import APIRouter, Depends, Query

from gastrometric.api.api_dependencies import get_db_path, get_knowledge
from gastrometric.api.api_errors import not_found_error
from gastrometric.api.recipe_api_models import (
    FavoriteRecipeItem,
    FavoriteRecipesResponse,
    MatchedIngredientResponse,
    MissingIngredientResponse,
    RecipeMatchListResponse,
    RecipeMatchResponse,
    RecipeResponse,
    RecipeSearchResponse,
    RecipeSearchResultItem,
)
from gastrometric.application.inventory_editor import list_inventory_items
from gastrometric.application.recipe_matcher import RecipeMatch, find_matching_recipes
from gastrometric.application.recipe_models import Recipe, RecipeNotFoundError
from gastrometric.application.recipe_reader import get_recipe
from gastrometric.application.recipe_search import (
    RecipeSearchResult,
    list_favorite_recipes,
    search_recipes_by_ingredient,
    search_recipes_by_name,
)

router = APIRouter(prefix="/api")


def _to_search_response(results: List[RecipeSearchResult]) -> RecipeSearchResponse:
    """Reshapes application-layer `RecipeSearchResult` tuples into the
    HTTP response model -- a field-for-field copy, same discipline as
    `_to_response` below: no recomputation, no reinterpretation. Matcher
    reason/type information (`match_type`, `match_reason`,
    `matched_term`) passes through unchanged so it survives into the
    response exactly as MATCH-01 -> recipe_search produced it.
    """
    return RecipeSearchResponse(
        results=[
            RecipeSearchResultItem(
                recipe_id=r.recipe_id,
                recipe_name=r.recipe_name,
                alt_title=r.alt_title,
                servings=r.servings,
                restaurant=r.restaurant,
                source=r.source,
                attribution=r.attribution,
                favorite=r.favorite,
                matched_ingredient=r.matched_ingredient,
                match_type=r.match_type,
                match_reason=r.match_reason,
                matched_term=r.matched_term,
            )
            for r in results
        ]
    )


@router.get("/recipes/search", response_model=RecipeSearchResponse)
def search_recipes_route(
    q: str = Query(..., min_length=1),
    search_type: Literal["name", "ingredient"] = Query(..., alias="type"),
    db_path: Optional[str] = Depends(get_db_path),
    knowledge: Any = Depends(get_knowledge),
) -> RecipeSearchResponse:
    """
    GET /api/recipes/search?q=<query>&type=name|ingredient

    Dispatches to exactly one application-layer search function based on
    `type` -- no search logic of any kind lives in this route body
    beyond that one dispatch. `q` and `type` are both required;
    FastAPI/Pydantic rejects a missing/empty `q` (`min_length=1`) or an
    invalid `type` (anything other than "name"/"ingredient") with the
    established 422/malformed_request response before this function's
    body ever runs -- this is the "existing API validation convention"
    the work order asks an empty query be handled by, rather than this
    route reinterpreting it as "return every recipe."

    `search_type` is bound to the Python parameter under an alias
    (`Query(..., alias="type")`) so the URL contract stays `?type=...`
    without the Python variable shadowing the `type` builtin.
    """
    if search_type == "name":
        results = search_recipes_by_name(q, db_path=db_path)
    else:
        results = search_recipes_by_ingredient(q, knowledge, db_path=db_path)

    return _to_search_response(list(results))


def _to_favorites_response(results: List[RecipeSearchResult]) -> FavoriteRecipesResponse:
    """
    Reshapes `list_favorite_recipes()`'s `RecipeSearchResult` tuples
    into the favorites-specific transport model -- field-for-field, same
    discipline as `_to_search_response`/`_to_response`. Deliberately
    does not pass through `matched_ingredient`/`match_type`/
    `match_reason`/`matched_term`: `FavoriteRecipeItem` doesn't declare
    those fields at all (BE-06-CO1), since favorites never involves the
    matcher and they'd always be `None` here anyway.
    """
    return FavoriteRecipesResponse(
        results=[
            FavoriteRecipeItem(
                recipe_id=r.recipe_id,
                recipe_name=r.recipe_name,
                alt_title=r.alt_title,
                restaurant=r.restaurant,
                source=r.source,
                attribution=r.attribution,
                favorite=r.favorite,
            )
            for r in results
        ]
    )


@router.get("/recipes/favorites", response_model=FavoriteRecipesResponse)
def get_favorite_recipes_route(
    db_path: Optional[str] = Depends(get_db_path),
) -> FavoriteRecipesResponse:
    """
    GET /api/recipes/favorites (BE-06-CO1)

    New read-only path over the existing `recipes.favorites` column --
    no favorite mutation, no user accounts, no favorite ordering
    persistence, no recommendation/popularity logic. Registered as a
    static route ahead of `/recipes/{recipe_id}`, same reasoning as
    `/recipes/search` and `/recipes/matches` above: `{recipe_id}` is an
    unconstrained path placeholder at the routing layer, so a static
    path registered after it would never be reached.

    Ordering (`recipe_id ASC`) is entirely `list_favorite_recipes()`'s
    responsibility; this route does not sort, filter, or otherwise
    touch the result list.

    No favorites at all returns `{"results": []}`, not an error.
    """
    results = list_favorite_recipes(db_path=db_path)
    return _to_favorites_response(list(results))


def _to_match_response(match: RecipeMatch) -> RecipeMatchResponse:
    """
    Reshapes one application-layer `RecipeMatch` into the HTTP response
    model -- field-for-field, same discipline as `_to_search_response`
    and `_to_response`: no category recomputation, no re-deriving
    matched/missing ingredients, no reinterpreting `match_type`/`reason`.
    All of that already happened inside `recipe_matcher.py`.
    """
    return RecipeMatchResponse(
        recipe_id=match.recipe_id,
        recipe_name=match.recipe_name,
        alt_title=match.alt_title,
        restaurant=match.restaurant,
        source=match.source,
        attribution=match.attribution,
        match_category=match.match_category,
        matched_ingredients=[
            MatchedIngredientResponse(
                recipe_ingredient_id=m.recipe_ingredient_id,
                ingredient_name_original=m.ingredient_name_original,
                inventory_item_id=m.inventory_item_id,
                location=m.location,
                match_type=m.match_type,
                reason=m.reason,
            )
            for m in match.matched_ingredients
        ],
        missing_ingredients=[
            MissingIngredientResponse(
                ingredient_id=m.ingredient_id,
                ingredient_name_original=m.ingredient_name_original,
            )
            for m in match.missing_ingredients
        ],
        fridge_match_count=match.fridge_match_count,
        pantry_match_count=match.pantry_match_count,
    )


@router.get("/recipes/matches", response_model=RecipeMatchListResponse)
def get_recipe_matches_route(
    db_path: Optional[str] = Depends(get_db_path),
    knowledge: Any = Depends(get_knowledge),
) -> RecipeMatchListResponse:
    """
    GET /api/recipes/matches

    Inventory-driven recipe matching (BE-07): no query parameter at all
    -- this isn't a search against user-typed text, it's "what can I
    make with what I have," so it's kept as its own endpoint rather than
    a third `type=` value on `/recipes/search` (which the work order
    explicitly warned against as an ambiguous overload of a genuinely
    different operation).

    This function does exactly three things: fetch current inventory via
    the existing `inventory_editor.list_inventory_items()` (no new
    inventory-reading code -- that capability already existed), fetch
    runtime knowledge via the same `get_knowledge` dependency
    `/recipes/search?type=ingredient` already uses, and call
    `find_matching_recipes()`. All classification, ordering, and
    fridge/pantry precedence logic lives entirely in `recipe_matcher.py`.

    An empty inventory, or an inventory with no matching recipes at all,
    both produce `{"results": []}` -- not an error.
    """
    inventory_items = list_inventory_items(db_path=db_path)
    matches = find_matching_recipes(inventory_items, knowledge, db_path=db_path)
    return RecipeMatchListResponse(results=[_to_match_response(m) for m in matches])


def _to_response(recipe: Recipe) -> RecipeResponse:
    """
    Converts the BE-04 `Recipe` dataclass into `RecipeResponse` by
    reshaping data only. `dataclasses.asdict` recursively turns the
    dataclass -- and its nested `RecipeSection`/`RecipeIngredient`/
    `RecipeInstruction` instances -- into plain dicts, which Pydantic
    then parses into the corresponding nested response models via
    `RecipeResponse`'s field type annotations. No field is recomputed,
    reformatted, or reinterpreted here: every value is exactly what
    `get_recipe()` returned, including already-rendered `name`/
    `quantity` strings and any Unicode content within them.
    """
    return RecipeResponse(**dataclasses.asdict(recipe))


@router.get("/recipes/{recipe_id}", response_model=RecipeResponse)
def get_recipe_route(
    recipe_id: int, db_path: Optional[str] = Depends(get_db_path)
) -> RecipeResponse:
    """
    GET /api/recipes/{recipe_id}.

    A non-integer `recipe_id` never reaches this function's body --
    FastAPI's own path-parameter validation rejects it first, producing
    the established 422/malformed_request response (work order Test 9).
    This function only has to handle "valid integer, but no such
    recipe" -- `RecipeNotFoundError` -> the established 404/not_found
    response, via the same `not_found_error()` helper the inventory
    routes already use, not a second error convention.
    """
    try:
        recipe = get_recipe(recipe_id, db_path=db_path)
    except RecipeNotFoundError as exc:
        raise not_found_error("recipe", exc.recipe_id) from exc
    return _to_response(recipe)