"""
Ingredient presentation rendering (BE-04).

Pure functions turning one `recipe_ingredient_lines_parsed` row into the
two human-facing strings `RecipeIngredient` needs: `name` (identity +
preparation) and `quantity` (natural portion / packaging / imperial
weight / imperial volume, concatenated).

Deliberately narrow in scope, per direction on this ticket: the harder
cases -- a size modifier that's ambiguous between natural portion and
packaging, an ingredient line with two independent quantity expressions
(e.g. "4 ... chicken breasts (5 to 6 ounces each)") -- are being
addressed upstream, in the analyzer, not solved here. This module's job
is the straightforward case: concatenate `ingredient_name_original` with
`preparation` phrases, and concatenate whichever of natural-portion /
packaging / imperial fields are actually populated into a sensible
`quantity` string, without silently dropping any of them.

Every function here takes a plain `Mapping[str, Any]` shaped like a
`recipe_ingredient_lines_parsed` row (column name -> value) rather than
a `sqlite3.Row` directly, so these are testable with plain dicts and
have no database dependency of their own.
"""

import json
from typing import Any, List, Mapping, Optional, Union

Number = Union[int, float]


def _is_blank(value: Any) -> bool:
    """True for None, empty string, and whitespace-only string -- the
    forms an absent numeric/text column value might take coming out of
    SQLite (a REAL column is either a real number or NULL/None; a TEXT
    column could be NULL, an empty string, or whitespace).
    """
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _format_number(value: Optional[Number]) -> str:
    """Formats a REAL column value for human presentation: whole numbers
    render without a trailing '.0' (3.0 -> "3"); fractional values
    render as-is (0.5 -> "0.5").
    """
    if _is_blank(value):
        return ""
    number = float(value)  # type: ignore[arg-type]
    if number == int(number):
        return str(int(number))
    return str(number)


def parse_preparation(preparation_column: Optional[str]) -> List[str]:
    """
    Parses the `preparation` column (a TEXT column holding a JSON array
    of phrases, e.g. `'["boneless", "cut into 1/2 \\u201d cubes"]'`)
    into a plain list of phrase strings, in the order the analyzer
    produced them.

    Returns `[]` for None/blank/malformed content rather than raising --
    an ingredient with no preparation is a normal, expected case (work
    order section 17), not an error.

    Unicode note: `json.loads` decodes `\\u201d`-style escapes into the
    actual Unicode character natively; no special handling is needed
    here for that (work order section 16) -- the risk is entirely in
    NOT running content through proper JSON parsing (e.g. naive string
    splitting), which this function avoids by construction.
    """
    if _is_blank(preparation_column):
        return []
    try:
        phrases = json.loads(preparation_column)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return []
    if not isinstance(phrases, list):
        return []
    return [str(phrase) for phrase in phrases]


def render_ingredient_name(ingredient_name_original: str, preparation_column: Optional[str]) -> str:
    """
    Builds the human-facing ingredient name: `ingredient_name_original`
    plus any preparation phrases, joined with ", " (work order section
    5). If there is no preparation, returns `ingredient_name_original`
    unchanged -- never with a trailing comma (work order Test 9).

    `ingredient_name_original` is used as-is, never replaced with
    `ingredient_id` -- the canonical id is the semantic identity, this
    is the author's own wording (work order section 5, Test 6).
    """
    phrases = parse_preparation(preparation_column)
    if not phrases:
        return ingredient_name_original
    return ", ".join([ingredient_name_original, *phrases])


def _render_natural_portion(row: Mapping[str, Any]) -> Optional[str]:
    """
    Renders the natural-portion dimension: a value or a min-max range,
    an optional counting-unit noun (`natural_portion`, e.g. "clove" for
    "1 clove garlic" -- distinct from cases like "2 carrots" where the
    ingredient name itself is already the counted noun and this column
    is empty), and an optional `size` modifier (e.g. "medium").

    Where `size` should attach when there's no natural-portion value at
    all (the "1 jar large artichoke hearts" case from the work order) is
    exactly the kind of ambiguity being handled upstream, not here: this
    function still surfaces `size` on its own in that situation rather
    than silently dropping it, but does not claim to resolve which
    dimension it "really" modifies.
    """
    value = row.get("natural_portion_value")
    min_value = row.get("natural_portion_min")
    max_value = row.get("natural_portion_max")
    unit_noun = row.get("natural_portion")
    size = row.get("size")

    if not _is_blank(min_value) and not _is_blank(max_value):
        amount = f"{_format_number(min_value)}-{_format_number(max_value)}"
    elif not _is_blank(value):
        amount = _format_number(value)
    else:
        amount = None

    pieces: List[str] = [str(piece) for piece in (amount, size, unit_noun) if not _is_blank(piece)]
    return " ".join(pieces) if pieces else None


def _render_packaging(row: Mapping[str, Any]) -> Optional[str]:
    """
    Renders the packaging dimension: count, packaging noun (`packaging`,
    e.g. "can", "package"), and packaging size (`packaging_size_value` +
    `packaging_size_unit`), when present.

    Renders consistently as "{count} {noun} ({size}-{unit})" whenever a
    packaging size is present -- this does not attempt to reproduce
    every stylistic variant a recipe author might use (e.g. "12-ounce
    package" vs. "cans (14-ounce)"); it prioritizes never dropping the
    package size over matching original phrasing exactly, per the work
    order's explicit requirement that "2 cans (14-ounce)" must never
    collapse to "2 cans".
    """
    count = row.get("packaging_count")
    noun = row.get("packaging")
    size_value = row.get("packaging_size_value")
    size_unit = row.get("packaging_size_unit")

    if _is_blank(count) and _is_blank(noun) and _is_blank(size_value):
        return None

    pieces: List[str] = []
    if not _is_blank(count):
        pieces.append(_format_number(count))
    if not _is_blank(noun):
        pieces.append(str(noun))
    if not _is_blank(size_value):
        size_piece = _format_number(size_value)
        if not _is_blank(size_unit):
            size_piece = f"{size_piece}-{size_unit}"
        pieces.append(f"({size_piece})")

    return " ".join(pieces) if pieces else None


def _render_imperial(value: Optional[Number], unit: Optional[str]) -> Optional[str]:
    """Renders a single imperial weight or volume dimension: "3 lb",
    "4 tablespoon". Returns None if there's no value at all -- a unit
    with no value would be meaningless and shouldn't happen, but if it
    does, this still surfaces the unit alone rather than silently
    dropping it.
    """
    if _is_blank(value):
        return str(unit) if not _is_blank(unit) else None
    formatted = _format_number(value)
    return f"{formatted} {unit}" if not _is_blank(unit) else formatted


def render_quantity(row: Mapping[str, Any]) -> Optional[str]:
    """
    Builds the author-facing quantity string for one parsed ingredient
    row by concatenating whichever of natural-portion / packaging /
    imperial-weight / imperial-volume dimensions are actually populated.
    Returns None only if none of them are populated at all (work order
    section 17: "ingredients with no quantity" is a valid, expected
    case).

    This never converts to grams (work order section 7) and never
    silently drops a populated dimension (section 9) -- if multiple
    dimensions are populated simultaneously in a way this function
    doesn't have a clean rule for (the "difficult cases" -- e.g. two
    independent quantity expressions on one line), they are still all
    concatenated in a fixed, deterministic order rather than one being
    chosen and the rest discarded. Producing a maximally natural-reading
    string for every such case is explicitly out of scope here; that
    refinement is expected to happen upstream in the analyzer.
    """
    raw_pieces = [
        _render_natural_portion(row),
        _render_packaging(row),
        _render_imperial(row.get("imperial_weight_value"), row.get("imperial_weight_unit")),
        _render_imperial(row.get("imperial_volume_value"), row.get("imperial_volume_unit")),
    ]
    pieces: List[str] = [piece for piece in raw_pieces if piece]
    return " ".join(pieces) if pieces else None