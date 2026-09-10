"""
Application-level recipe representation (BE-04).

These dataclasses are the STABLE shape `recipe_reader.get_recipe()` will
return -- deliberately independent of the SQL schema underneath. They
describe what a recipe, section, ingredient, and instruction need to be
to be presented to a cook, not how they're stored.

NOTE: the read/assembly implementation (`get_recipe()` itself) is not in
this file yet. Building it requires knowing the actual column shape of
`recipes`, `recipe_sections`, `recipe_ingredient_lines_raw`,
`recipe_ingredient_lines_parsed`, and `recipe_instruction_blocks` --
none of which I have visibility into. In particular, BE-04 section 8
explicitly requires inspecting real analyzer output for representative
recipes (a natural portion + size, a package + package size, a
measurement + parenthetical per-item measurement, a range, multiple
expressions, embedded preparation) before the quantity-reconstruction
logic can be written correctly -- guessing at that would risk exactly
the kind of silent data loss the work order explicitly forbids ("do not
silently discard the information").

Field-naming choice: `Recipe`'s optional metadata fields are named
identically to the `recipes` table's columns as given in the work order
(`recipe_author`, `recipe_attribution`, etc.) rather than stripped-down
names (`author`, `attribution`, ...). That's a deliberate choice for
now, prioritizing unambiguous traceability back to source columns over
brevity, given how many similarly-shaped fields there are -- happy to
rename to whatever the project's existing model-naming convention
actually is once I can see it.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class RecipeInstruction:
    """
    One cook-facing instruction step within a section.

    `number` is a 1-based presentation number assigned by the reader,
    following persisted order (`recipe_instruction_blocks.id` ascending,
    per work order section 14). It is NOT the database id and must never
    be confused with one -- "the database ID is not the cook-facing
    instruction number."
    """

    number: int
    text: str


@dataclass(frozen=True)
class RecipeIngredient:
    """
    One ingredient line, reconstructed for human presentation.

    ingredient_id: the canonical semantic identity (e.g. "chuck roast"),
        or None if unresolved. Never used as the presented name -- see
        `name` below.

    name: the human-facing ingredient identity, built from
        `ingredient_name_original` (the author's own wording -- e.g.
        "chuck", not the canonical "chuck roast") plus any preparation
        phrases joined with ", " (work order section 5). If there is no
        preparation, `name` is `ingredient_name_original` unchanged --
        never a name with a trailing comma.

    quantity: the author-facing quantity expression, reconstructed from
        the analyzer's structured dimensions (natural portion, size,
        packaging, ranges, secondary per-item measurements) -- never a
        gram conversion, and never reduced to a single value if the
        analyzer produced multiple meaningful expressions (work order
        sections 6-9). THE EXACT SHAPE OF THIS FIELD IS NOT YET FINAL --
        see the module docstring. It may need to become a structured
        value (e.g. a list of expressions plus a rendered string) rather
        than a single string, depending on what real
        `recipe_ingredient_lines_parsed` data actually looks like for
        the multi-expression case in section 8.

    grams: optional, persisted-only. None if not already computed
        upstream -- this reader never calculates it (section 10).

    notes: parsed ingredient notes, kept separate from `name` and
        `quantity` -- never merged into either (section 11).

    optional: whether this ingredient is marked optional in the recipe.

    alt_group_id / alt_kind: passed through verbatim from the analyzer's
        alternative-ingredient metadata, with no interpretation applied
        (work order Test 18) -- e.g. grouping "butter OR margarine" as
        substitutable options. Kept as opaque strings since I don't know
        their actual value domain yet.
    """

    ingredient_id: Optional[str]
    name: str
    quantity: Optional[str]
    grams: Optional[float]
    notes: Optional[str]
    optional: bool
    alt_group_id: Optional[str]
    alt_kind: Optional[str]


@dataclass(frozen=True)
class RecipeSection:
    """
    One recipe section (e.g. "For the marinade", "For the sauce"), in
    persisted order (`recipe_sections.id` ascending -- section 13).

    `name` is optional: a recipe may have a single, unnamed section.
    `ingredients` and `instructions` may each be empty -- a section is
    not required to have both (section 17).
    """

    id: int
    name: Optional[str]
    ingredients: List[RecipeIngredient] = field(default_factory=list)
    instructions: List[RecipeInstruction] = field(default_factory=list)


@dataclass(frozen=True)
class Recipe:
    """
    The complete application-facing recipe representation returned by
    `recipe_reader.get_recipe()`.

    Only `id` and `recipe_name` are guaranteed. Every other field is
    optional and must remain `None` rather than being fabricated when
    the underlying `recipes` row doesn't have it populated (section 17).
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
    sections: List[RecipeSection] = field(default_factory=list)


class RecipeNotFoundError(Exception):
    """
    Raised by `recipe_reader.get_recipe()` when `recipe_id` does not
    correspond to a persisted recipe.

    NOTE: I don't yet know whether this repository already has an
    established application-layer not-found convention elsewhere (the
    inventory application boundary uses a different pattern -- returning
    `None` rather than raising -- for its own not-found cases). Work
    order section 18 says to use an existing convention if one exists,
    or create a dedicated exception otherwise; this is the latter,
    pending confirmation of which is actually wanted here.
    """

    def __init__(self, recipe_id: int):
        self.recipe_id = recipe_id
        super().__init__(f"Recipe {recipe_id!r} was not found.")