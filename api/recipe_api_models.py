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
    alt_title: Optional[str] = None
    servings: Optional[str] = None
    restaurant: Optional[str] = None
    favorite: bool = False
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

    `alt_title`/`servings`/`restaurant`/`favorite` are included here too
    (not just on the full `RecipeResponse`) since browsing/search
    results are where a `favorite` flag actually needs to be usable --
    filtering a list doesn't require opening each recipe individually.

    `source`/`attribution` (BE-06-CO1) are the same common recipe
    browse-card fields shared with favorites and inventory matches --
    read straight through from `RecipeSearchResult`, not derived from
    the search term or match info.

    `matched_ingredient` through `matched_term` are populated only for
    ingredient-search results; always `None` for name-search results.
    """

    recipe_id: int
    recipe_name: str
    alt_title: Optional[str] = None
    servings: Optional[str] = None
    restaurant: Optional[str] = None
    source: Optional[str] = None
    attribution: Optional[str] = None
    favorite: bool = False
    matched_ingredient: Optional[str] = None
    match_type: Optional[str] = None
    match_reason: Optional[Dict[str, Any]] = None
    matched_term: Optional[str] = None


class RecipeSearchResponse(BaseModel):
    results: List[RecipeSearchResultItem]


class FavoriteRecipeItem(BaseModel):
    """
    One favorited recipe -- BE-06-CO1's new `/api/recipes/favorites`
    read path. Deliberately its own model rather than reusing
    `RecipeSearchResultItem`: favorites involves no search and no
    matcher, so it carries none of `matched_ingredient`/`match_type`/
    `match_reason`/`matched_term` -- not even as always-null fields --
    to keep this contract free of fields that only ever mean something
    for a search result.
    """

    recipe_id: int
    recipe_name: str
    alt_title: Optional[str] = None
    restaurant: Optional[str] = None
    source: Optional[str] = None
    attribution: Optional[str] = None
    favorite: bool = True


class FavoriteRecipesResponse(BaseModel):
    results: List[FavoriteRecipeItem]


class MatchedIngredientResponse(BaseModel):
    """
    Mirrors `application.recipe_matcher.MatchedIngredient` field-for-field.
    `reason` is passed through exactly as MATCH-01 produced it -- never
    reinterpreted at this layer.

    `recipe_ingredient_id` and `ingredient_name_original` are
    deliberately separate, differently-named fields, not one field doing
    double duty: `recipe_ingredient_id` is an identity (never assume it's
    display-safe or that it can be concatenated/pluralized into
    something else); `ingredient_name_original` is the recipe's own raw
    wording, exactly as persisted -- no preparation phrases appended, no
    relation to `inventory_item_id`'s own identity string. A consumer
    must not assume `recipe_ingredient_id` and `ingredient_name_original`
    are interchangeable just because they can look similar (e.g.
    "tomato" vs "tomatoes").

    `location` (BE-06-CO1) is the actual fridge/pantry location of the
    specific inventory item referenced by `inventory_item_id` -- never
    inferred by the frontend from `fridge_match_count`/
    `pantry_match_count`, a second inventory request, or assumptions
    about inventory IDs. Always `"fridge"` or `"pantry"`.
    """

    recipe_ingredient_id: str
    ingredient_name_original: str
    inventory_item_id: int
    location: str
    match_type: str
    reason: Dict[str, Any]


class MissingIngredientResponse(BaseModel):
    """
    Mirrors `application.recipe_matcher.MissingIngredient` field-for-field.
    Same identity-vs-presentation distinction as `MatchedIngredientResponse`:
    `ingredient_id` is the canonical identity, `ingredient_name_original`
    is the recipe's raw wording -- never derived from `ingredient_phrase`,
    never preparation-augmented.
    """

    ingredient_id: str
    ingredient_name_original: str


class RecipeMatchResponse(BaseModel):
    """
    One recipe's inventory-match classification (BE-07). Mirrors
    `application.recipe_matcher.RecipeMatch` field-for-field.
    Deliberately does NOT include `sections`/recipe content -- same
    "candidates, not recipes" discipline as `RecipeSearchResultItem`;
    `GET /api/recipes/{recipe_id}` remains the way to retrieve a
    selected match's full representation.

    `alt_title`/`restaurant`/`source`/`attribution` (BE-06-CO1) are the
    same common recipe browse-card fields as favorites/search results.
    """

    recipe_id: int
    recipe_name: str
    alt_title: Optional[str] = None
    restaurant: Optional[str] = None
    source: Optional[str] = None
    attribution: Optional[str] = None
    match_category: str
    matched_ingredients: List[MatchedIngredientResponse]
    missing_ingredients: List[MissingIngredientResponse]
    fridge_match_count: int
    pantry_match_count: int


class RecipeMatchListResponse(BaseModel):
    results: List[RecipeMatchResponse]