"""
Typed HTTP response models for the recipe API.

These mirror `application.recipe_models`'s dataclasses field-for-field --
`RecipeResponse`/`RecipeSectionResponse`/`RecipeIngredientResponse`/
`RecipeInstructionResponse` exist so the route returns Pydantic models
(for FastAPI's response validation/serialization/OpenAPI schema) rather
than dataclass instances directly, not because the API needs a different
shape than BE-04 already produces. See `recipe_routes.py`'s conversion
function -- it is a field-for-field copy, not a reconstruction.

Deliberately excludes anything that's a persistence/internal detail
rather than part of the application-level recipe: no `recipe_section_id`,
`recipe_ingredient_line_id`, `ingredient_block_id`, raw ingredient-line
text, or any other database identifier/internal structure. The BE-04
`Recipe`/`RecipeSection`/`RecipeIngredient`/`RecipeInstruction`
dataclasses already omit all of that, so this file inherits that
omission simply by mirroring them field-for-field rather than by adding
its own filtering logic.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class RecipeInstructionResponse(BaseModel):
    """`number` is the 1-based, section-local presentation number BE-04
    assigns -- never a database id (work order section 14 / BE-04
    section 14)."""

    number: int
    text: str


class RecipeIngredientResponse(BaseModel):
    """
    Mirrors `application.recipe_models.RecipeIngredient` exactly.
    `name` and `quantity` are BE-04's already-rendered presentation
    strings -- this model does not reconstruct or reinterpret them.
    """

    ingredient_id: Optional[str] = None
    name: str
    quantity: Optional[str] = None
    grams: Optional[float] = None
    notes: Optional[str] = None
    optional: bool
    alt_group_id: Optional[str] = None
    alt_kind: Optional[str] = None


class RecipeSectionResponse(BaseModel):
    id: int
    name: Optional[str] = None
    ingredients: List[RecipeIngredientResponse]
    instructions: List[RecipeInstructionResponse]


class RecipeResponse(BaseModel):
    """
    Mirrors `application.recipe_models.Recipe` exactly. Optional
    metadata fields default to `None` and MUST serialize as JSON `null`
    when absent -- not be omitted from the response and not become an
    empty string (work order Test 2).
    """

    id: int
    recipe_name: str
    recipe_author: Optional[str] = None
    recipe_attribution: Optional[str] = None
    recipe_source: Optional[str] = None
    recipe_url: Optional[str] = None
    recipe_video: Optional[str] = None
    recipe_notes: Optional[str] = None
    recipe_yield: Optional[str] = None
    recipe_state: Optional[str] = None
    recipe_ingestion_method: Optional[str] = None
    sections: List[RecipeSectionResponse]


class RecipeSearchResultItem(BaseModel):
    """
    One lightweight recipe candidate from BE-06 search -- mirrors
    `application.recipe_search.RecipeSearchResult` field-for-field.
    Deliberately does NOT include `sections` or any recipe content:
    search results are candidates to choose from, not recipes.
    Retrieving the full representation for a selected result remains
    `GET /api/recipes/{recipe_id}`'s job (BE-05), backed by
    `recipe_reader.get_recipe()`.

    `matched_ingredient` through `matched_term` are populated only for
    ingredient-search results; always `None` for name-search results.
    """

    recipe_id: int
    recipe_name: str
    matched_ingredient: Optional[str] = None
    match_type: Optional[str] = None
    match_reason: Optional[Dict[str, Any]] = None
    matched_term: Optional[str] = None


class RecipeSearchResponse(BaseModel):
    results: List[RecipeSearchResultItem]