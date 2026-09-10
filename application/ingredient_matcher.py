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
from typing import Any, Dict, FrozenSet, Mapping, Protocol, Tuple


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