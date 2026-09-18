"""
Ingredient matcher (MATCH-01).

Narrow by design: recognizes exact canonical ingredient identity and
exact alias identity against the existing runtime knowledge loader, and
nothing else. No fuzzy matching, no relationship/component reasoning, no
SQLite access, no recipe or inventory awareness. Those are explicitly
future matcher capabilities (see the work order's non-goals) -- this
module's only job is turning one query string into zero or more
canonical ingredient candidates, each explaining why it matched.

--------------------------------------------------------------------
Runtime knowledge contract (as specified, not inferred)
--------------------------------------------------------------------
    knowledge.ingredients          frozenset[str] of canonical
                                    ingredient names -- membership only,
                                    no separate id space.
    knowledge.ingredient_aliases   immutable mapping: alias (str) ->
                                    canonical ingredient name (str).

There is no `ingredient_id` concept at this layer -- the canonical
ingredient NAME is the identity. This matcher returns that name as-is;
it has no awareness of `recipe_ingredient_lines_parsed.ingredient_id` or
any other persistence detail. Translating a canonical name into whatever
representation recipe search needs is that layer's job, not this one's
(see `application/recipe_search.py`).

This matcher was built against the loader contract as it was described
to me, not against the loader's actual source (which I have not seen).
If the real `gastrometric/knowledge/loader.py` implementation
contradicts anything below -- different attribute names, a different
container type, aliases mapping to something other than a plain
name -- that is a genuine contradiction to resolve, not something for
this module to silently paper over.
"""

from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Mapping, Optional, Protocol, Tuple


class RuntimeKnowledgeLike(Protocol):
    """
    Structural protocol capturing exactly the slice of the real runtime
    knowledge object this matcher depends on. Defined structurally
    (Protocol) rather than importing the real class, since only the
    module-level singleton's import path is confirmed
    (`from gastrometric.knowledge.loader import knowledge`) -- the
    class's actual name was not given to me. Swap this for a real
    nominal import once that's confirmed, if preferred.
    """

    ingredients: FrozenSet[str]
    ingredient_aliases: Mapping[str, str]


@dataclass(frozen=True)
class IngredientMatch:
    """
    One candidate canonical ingredient for a query, with structured
    information explaining why it matched.

    ingredient_name: the canonical ingredient name/expression, exactly
        as it appears in `knowledge.ingredients` -- never a database row
        id, never invented. For an alias match, this is the alias's
        target (the canonical name `knowledge.ingredient_aliases` maps
        to), not the query itself.
    match_type: "exact" | "alias" for MATCH-01. Left as a plain `str`
        rather than a closed enum so future match types (component,
        equivalence, substitution, ...) don't require a schema change
        here.
    reason: e.g. `{"type": "identity"}` or `{"type": "alias"}`. A plain
        dict for the same forward-compatibility reason as `match_type`
        -- future reason types may carry additional keys this PoC
        doesn't need yet.
    matched_term: the original query text that produced this match
        (not the normalized form), so a caller/UI can show what the
        user actually typed alongside what it resolved to.
    """

    ingredient_name: str
    match_type: str
    reason: Dict[str, Any]
    matched_term: str


def _normalize(text: str) -> str:
    """
    Whitespace-trimmed, lowercased.

    ASSUMPTION: `knowledge.ingredients` is specifically typed as a
    `frozenset[str]` -- the whole point of a frozenset here is O(1)
    membership testing (this is presumably called per lexer token in
    the hot path elsewhere in the system), which only holds together if
    its contents are already stored in one consistent normalized form
    ready for direct comparison, not something every caller
    re-normalizes per lookup. This function normalizes the QUERY to
    match that assumed convention; it does not touch or re-derive
    anything from `knowledge` itself. If the loader's canonical forms
    turn out not to be lowercase, this normalization needs to change to
    match whatever the loader's real convention is.
    """
    return text.strip().lower()


def find_ingredient_matches(
    query: str, knowledge: RuntimeKnowledgeLike
) -> Tuple[IngredientMatch, ...]:
    """
    Resolves `query` against runtime knowledge under MATCH-01's rules,
    in order:

      1. Exact canonical match: `normalized_query` is itself a member of
         `knowledge.ingredients` -> match_type="exact", reason
         {"type": "identity"}.
      2. Exact alias match: `normalized_query` is a key in
         `knowledge.ingredient_aliases` -> match_type="alias", reason
         {"type": "alias"}, ingredient_name is the alias's mapped
         canonical name.
      3. No match: returns `()`.

    No fuzzy matching, no relationship/component reasoning (see module
    docstring) -- if neither rule above fires, this returns an empty
    tuple rather than guessing.

    A blank/whitespace-only query returns `()` without touching
    `knowledge` at all.

    Deterministic: the same `query` against the same `knowledge` object
    always produces the same result, since this performs only pure
    membership/lookup operations against `knowledge`'s already-loaded,
    immutable structures -- never a database call, never anything
    time-dependent.
    """
    normalized_query = _normalize(query)
    if not normalized_query:
        return ()

    if normalized_query in knowledge.ingredients:
        return (
            IngredientMatch(
                ingredient_name=normalized_query,
                match_type="exact",
                reason={"type": "identity"},
                matched_term=query,
            ),
        )

    canonical_name = knowledge.ingredient_aliases.get(normalized_query)
    if canonical_name is not None:
        return (
            IngredientMatch(
                ingredient_name=canonical_name,
                match_type="alias",
                reason={"type": "alias"},
                matched_term=query,
            ),
        )

    return ()


def ingredients_match(
    ingredient_a: str,
    ingredient_b: str,
    knowledge: RuntimeKnowledgeLike,
) -> Optional[IngredientMatch]:
    """
    Pairwise comparison between two already-persisted ingredient
    identities (e.g. an inventory item's resolved `ingredient_id` and a
    recipe ingredient's persisted `ingredient_id`) -- for BE-07's
    inventory-to-recipe matching, which needs to ask "do these two
    already-resolved identities refer to the same ingredient?" rather
    than "what does this raw text resolve to?" (`find_ingredient_matches`'s
    actual purpose).

    Added here, in MATCH-01's own module, rather than in the caller (see
    the BE-07 work order: "extend the matcher with a small pairwise
    operation if that is cleaner" / "do not create a second semantic
    matching implementation" elsewhere) -- this reuses
    `find_ingredient_matches` for the non-trivial case rather than
    touching `knowledge.ingredients`/`knowledge.ingredient_aliases`
    directly a second time.

    Returns an `IngredientMatch` if both identities resolve to the same
    canonical ingredient name, or `None` if they don't -- either because
    one side is blank, neither resolves at all, or they resolve to
    different canonical names.

    match_type/reason on the returned match: "exact" (`{"type":
    "identity"}`) when the two identities are literally the same
    normalized string -- the expected common case, since both sides are
    already independently-resolved canonical-ish identities, not raw
    user text. "alias" (`{"type": "alias"}`) when they differ as
    strings but both resolve (via `find_ingredient_matches`) to the same
    canonical name -- e.g. one side stored as an alias form of the
    other's canonical form. `matched_term` is set to `ingredient_a` (the
    first argument), documented so callers know which side's original
    text it reflects.

    --------------------------------------------------------------
    IDENTITY-CONTRACT FINDING (investigated, not assumed)
    --------------------------------------------------------------
    A real question worth being explicit about: are persisted
    `ingredient_id` values (in both `inventory_items` and
    `recipe_ingredient_lines_parsed`) guaranteed to be in the same
    string space as `knowledge.ingredients`/`knowledge.ingredient_aliases`
    (canonical, space-separated names), or could they use a different
    convention (e.g. hyphenated slugs like "chicken-breast")?

    Evidence actually available to me: two confirmed real persisted
    `recipe_ingredient_lines_parsed.ingredient_id` values -- "chuck
    roast" and "chicken breast" -- are space-separated, not hyphenated,
    consistent with the documented runtime knowledge contract (a
    frozenset used for phrase-based lexer matching against natural
    recipe text, which requires space-separated multi-word forms to
    match at all). I have not seen the actual `ingredients` table
    schema, `build_ingredients.py`, or `knowledge/loader.py` source, so
    this is evidence, not proof.

    The EXACT-MATCH path above (the `normalized_a == normalized_b`
    check) does not depend on resolving this question either way: it
    only compares the two persisted values to EACH OTHER, never to
    `knowledge`. That's sound as long as both `inventory_items` and
    `recipe_ingredient_lines_parsed` populate `ingredient_id` via the
    same underlying analyzer output (`reference.ingredient.id`) -- an
    architectural fact established across BE-02C/BE-02D/BE-04's
    construction, not an accident of the two strings happening to
    resemble each other.

    The ALIAS path below DOES depend on it: if persisted `ingredient_id`
    values ever used a convention that never appears in
    `knowledge.ingredients`/`knowledge.ingredient_aliases` at all, two
    genuinely-related but differently-spelled stored values could not be
    bridged this way -- `find_ingredient_matches` would return empty for
    both, and this function would correctly report "no match" rather
    than silently produce a wrong one. That's a safe failure mode (see
    `test_alias_resolution_requires_matching_vocabulary_convention` in
    `test_recipe_matcher.py`), but a real functional limitation if the
    premise turns out to be false -- worth confirming against the actual
    `ingredients` table/loader source, which I cannot do from here.
    """
    normalized_a = _normalize(ingredient_a)
    normalized_b = _normalize(ingredient_b)

    if not normalized_a or not normalized_b:
        return None

    if normalized_a == normalized_b:
        return IngredientMatch(
            ingredient_name=normalized_b,
            match_type="exact",
            reason={"type": "identity"},
            matched_term=ingredient_a,
        )

    matches_a = find_ingredient_matches(ingredient_a, knowledge)
    matches_b = find_ingredient_matches(ingredient_b, knowledge)
    if not matches_a or not matches_b:
        return None

    if matches_a[0].ingredient_name != matches_b[0].ingredient_name:
        return None

    return IngredientMatch(
        ingredient_name=matches_b[0].ingredient_name,
        match_type="alias",
        reason={"type": "alias"},
        matched_term=ingredient_a,
    )