"""
Recipe HTTP routes (BE-05).

Architecture this module must not violate:

    API route (this module)
        -> application.recipe_reader.get_recipe()
        -> application recipe model (application.recipe_models.Recipe)
        -> HTTP response model (api.recipe_api_models.RecipeResponse)

Same pattern as `inventory_routes.py`: parse the request, call exactly
one application-layer function, shape its result into a response model,
translate its exception into the API's existing error contract. Nothing
here touches SQLite, the lexer, parser, analyzer, or recipe presentation
logic (`render_ingredient_name`/`render_quantity`) -- all of that already
ran inside `get_recipe()`; this module only reshapes what it returns.
"""

import dataclasses
from typing import Optional

from fastapi import APIRouter, Depends

from gastrometric.api.api_dependencies import get_db_path
from gastrometric.api.api_errors import not_found_error
from gastrometric.api.recipe_api_models import RecipeResponse
from gastrometric.application.recipe_models import Recipe, RecipeNotFoundError
from gastrometric.application.recipe_reader import get_recipe

router = APIRouter(prefix="/api")


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