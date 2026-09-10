"""
RO-10: Deterministic Analyzer.

Consumes persisted parser output (`ingredient_parse_trees.parse_tree_json`,
i.e. a serialized `ParseResult`) plus `gastrometric.knowledge.loader`'s
`RuntimeKnowledge`, and produces a Canonical Semantic Result conforming to
`gastrometric/understanding/semantic_result_schema.json`.

Implements the RO-9 deterministic rule set (`analyzer_rules.md`) and the
RO-7 candidate-evaluation/evidence model (`candidate_evaluation_spec.md`).

===========================================================================
REPORTED MISMATCHES BETWEEN RO-9 AND THE ACTUAL PARSER OUTPUT
===========================================================================
Per the work order: "If the implementation encounters a mismatch between
the actual parser output and this work order, stop and report the
mismatch. Do not silently invent a transformation rule." The four items
below are genuine, empirically-verified mismatches (confirmed by running
`IngredientParser` against synthetic tokens, not just reading the code).
None of them are silently papered over -- each is handled by a narrow,
clearly-labeled, non-inventive fallback described at its call site, and
surfaced to callers as `unresolved` material (never fabricated values)
wherever it affects output. This module does not implement the fixes;
those belong to the parser/RO-9, not the Analyzer.

1. RANGE FRAGMENTATION (affects SS D "Range recognition").
   RO-9 SS D assumes the parser hands the Analyzer one MeasurementExpression
   per range, structurally distinguishing form=scalar vs form=range. It does
   not. Empirically, `_chunk_primitives`'s QuantityNode branch flushes the
   current MeasurementExpression as soon as a second QuantityNode is seen
   while the current one already contains a QuantityNode/RangeMarker -- this
   fires for a genuine range too, not just for "2 lbs 3 oz"-style back-to-
   back measurements. E.g. "3-5 medium peppers" parses to TWO adjacent
   MeasurementExpressions in `measurements`: [Quantity(3), RangeMarker] and
   [Quantity(5)] -- a dangling RangeMarker with no closing quantity, plus an
   orphaned second quantity, rather than one form=range node. The same
   fragmentation occurs inside a per-item parenthetical (verified for
   "(5-6 ounces each)"). Handling: `_resolve_dangling_ranges` detects a
   MeasurementExpression ending in an unresolved RangeMarker, does NOT
   invent lower/upper values, and instead emits one `unresolved` entry
   (reason `range_quantity_not_representable`) covering the real source
   text of both fragments. Range quantities are therefore never produced by
   this Analyzer today; this needs a parser fix or an RO-9 revision.

2/4. SIZE-CLASS MODIFIER ATTACHMENT (affects SS G's "large dice" /
   "1 medium garlic clove" examples). SS G's override rule assumes a size
   modifier can structurally attach to a PreparationExpression, and RO-9's
   own worked example for "1 medium garlic clove, minced" claims "medium"
   attaches to nothing (falling back to applies_to=reference). Empirically,
   `_chunk_primitives` routes every SizeNode/DescriptorNode/StateNode/
   TemperatureNode into an IngredientExpression container (or merges it into
   an existing one via the 7a fragmented-phrase rule) -- there is no code
   path by which one of these ever becomes a child of a PreparationExpression,
   and no code path by which one attaches to a "reference" or "quantity"
   node either. Verified: for "3-5 medium peppers", "medium" merges directly
   into the same IngredientExpression as "peppers". SS G's `preparation`/
   `reference` override targets for size/descriptor/state/temperature
   modifiers are therefore unreachable given the current grammar. Handling:
   this Analyzer implements only the reachable default
   (`applies_to="ingredient"`) for these four modifier classes; the override
   branches in SS G's table are dead code here, not silently reinterpreted.

3. [RESOLVED, CONFIRMED AGAINST REAL OUTPUT] CONNECTIVE SPAN LOSS
   (affected the schema's `relation.source_spans` -- "Lexical provenance
   for the connective ... e.g. the span(s) covering 'and'/'or'"). This WAS
   a real gap: `_classify_conjunction_groups` and the whole-reference
   merge step in `_build_references` used to build `AlternativeExpression`/
   `ConjunctionExpression` as `children=[left, right]` with the
   `AlternativeMarker`/`ConjunctionMarker` leaf discarded outright -- never
   added to `children`, never routed to `unresolved`, unrecoverable once
   serialized. `_relation_source_spans` originally returned a fixed
   sentinel rather than inventing a substitute (e.g. reusing the members'
   own source spans, which would misattribute text that produced the
   *members* as provenance for the *connective* itself).

   The parser was subsequently changed to add a `connective` field --
   a sibling of `children` on `Alternative`/`ConjunctionExpression`,
   holding the marker's own real span
   (e.g. `{"node_type": "AlternativeMarker", "span": {...,"text":"or"}}`).
   Confirmed directly against real production `parse_tree_json` (not
   assumed): both `"1 cup milk or cream"` and `"salt and pepper to
   taste"` carry this field with correct real text. `_relation_source_spans`
   now uses it when present, at every relation-construction call site
   (`_compound_tree`'s `visit()`, and the top-level whole-reference merge
   in `_evaluate_candidate`) -- the sentinel remains only as a defensive
   fallback for a tree that, for whatever reason, doesn't carry one.


Additionally, worth noting (not a blocking mismatch): the parser has no
`PreferenceExpression`/comma-plus-"preferably" node type at all, so RO-9
SS I.2's `relation_type: "preference"` construction path is simply never
reached by anything this parser can emit; no code is written for it here
beyond passing it through unchanged if some future parser version emits it
(`_relation_type_for` treats an unrecognized wrapper type conservatively).
===========================================================================

This module produces TWO outputs, per the current RO-10 revision: the
complete Canonical Semantic Result described above (unchanged), and a
projection of it into a primary downstream relational table,
`recipe_ingredient_lines_parsed`. The semantic-construction logic above
this line is entirely unaffected by that projection -- it performs no new
interpretation, only reshapes decisions already made above. See the
"PRIMARY DOWNSTREAM OUTPUT" section further down (before `main()`) for the
target DDL and a list of FLAGGED GAPS specific to that projection (unit
classification, preparation-list granularity, `ingredient_phrase` vs.
`ingredient_name_original`, package.size, optionality detection, and
alt-group id generation) -- distinct from the four mismatches above, which
are about the parser/Analyzer boundary, not the downstream projection.

What this module does NOT do (RO-10 boundary):
lex text; generate parser candidates; modify the parse tree; create
vocabulary/ingredients; modify relationship knowledge; query SQLite for
knowledge (all knowledge access goes through the injected `RuntimeKnowledge`);
invent missing relationships or entities; perform statistical inference or
confidence calibration; select among ambiguous interpretations; silently
discard candidates; convert measurements to grams or perform any other
unit conversion (see FLAGGED GAP #2); create columns for every parser
concept in the downstream table, or make the downstream table a dump of
the parser AST; touch SQLite at all, in either direction. That last point
changed with this revision: `analyze_all_lines` (the read-only DB
generator) and `persist_all_lines`/`main` (RO-8/RO-10's write path, added
once the persistence schema was confirmed) have moved OUT of this module
entirely, to `gastrometric.orchestration.recipe_ingredient_understanding`,
alongside the equivalent orchestration for the lex and parse stages. This
module is therefore now a pure function of its inputs end to end --
`analyze_line`/`analyze_parse_result` (dict/string in, dict out) plus the
row-projection helpers below them -- exactly the same separation
`gastrometric.understanding.ingredient_parser` already has from its own
orchestration counterpart, and importable/testable with no database at
all, same as before this revision, just now true of the WHOLE file rather
than only the semantic-construction half of it.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# Evidence-kind -> effect mapping (RO-9 SS L: fixed, categorical, non-numeric)
# ---------------------------------------------------------------------------

_EFFECT_BY_KIND: Dict[str, str] = {
    "unresolved_material": "detracting",
}


def _evidence(kind: str, record_id: Any, effect: Optional[str] = None) -> Dict[str, str]:
    if effect is None:
        effect = _EFFECT_BY_KIND.get(kind, "supporting")
    return {"kind": kind, "record_id": str(record_id), "effect": effect}


# ---------------------------------------------------------------------------
# Confidence (RO-9 SS M, REVISED). The original fixed four-value table
# (`resolved`->1.0, `ambiguous`/`unresolved`->0.5, `invalid`->0.0) was
# never actually consulted by `_derive_result` -- selection compared only
# the categorical `status` string, never `score`. That's what let two
# genuinely different, independently complete candidates for the same
# line (e.g. "2 ribs celery" modeled once via `component` and once via
# an embedded `NaturalPortionNode`) both land on status "resolved" with
# no way to prefer one: `_derive_result` saw ">1 strong candidate" and
# emitted nothing, discarding a line that actually had a clear winner.
#
# This replaces the fixed table with a deterministic, rule-based score
# computed from the KINDS of evidence an interpretation actually
# accumulated (see `_EVIDENCE_WEIGHT_BY_KIND` / `_evidence_score` below).
# This is still not "statistical inference or confidence calibration"
# (explicitly out of scope, see module docstring) -- every input is a
# categorical evidence-kind label already recorded above, and the
# mapping from kind to weight is a fixed table just like this one was,
# not anything learned or estimated. It is deliberately UNCAPPED: a
# `relationship_match` (a specific curated knowledge-base fact backing
# exactly this reading, e.g. "rib is a component_of celery") is
# categorically stronger than any amount of generic vocabulary/
# structural recognition, so its weight is set far above the sum any
# realistic combination of the other weights could reach for one
# reference (an ingredient line is a handful of words) -- this
# guarantees, not just usually produces, a relationship-backed
# interpretation outscoring an otherwise-identical one that lacks it.
# ---------------------------------------------------------------------------

_EVIDENCE_WEIGHT_BY_KIND: Dict[str, float] = {
    "relationship_match": 100.0,
    "exact_ingredient_match": 1.0,
    "alias_match": 1.0,
    "structural_match": 0.5,
    "vocabulary_match": 0.25,
    "unresolved_material": 1.0,  # magnitude only; sign comes from `effect` below.
}
_DEFAULT_EVIDENCE_WEIGHT = 0.1


def _evidence_score(evidence: List[Dict[str, str]]) -> float:
    """Deterministic evidence-weighted score for one interpretation.
    Sums `_EVIDENCE_WEIGHT_BY_KIND[kind]` for every evidence entry,
    flipping the sign for entries marked `effect="detracting"`
    (currently only `unresolved_material`, per `_EFFECT_BY_KIND` above).
    An evidence kind not in the table (should not happen given the fixed
    kind set this module emits, but defensive rather than a KeyError)
    falls back to a small default weight rather than crashing."""
    total = 0.0
    for entry in evidence:
        weight = _EVIDENCE_WEIGHT_BY_KIND.get(entry["kind"], _DEFAULT_EVIDENCE_WEIGHT)
        if entry.get("effect") == "detracting":
            total -= weight
        else:
            total += weight
    return total


# ---------------------------------------------------------------------------
# Small numeric parsing helper.
#
# Nothing upstream of the Analyzer ever converts a QuantityNode's lexical
# text into a number -- the parser is "PURE SYNTACTIC" (its own docstring)
# and never infers numeric/nutritional meaning, and `RuntimeKnowledge`
# exposes only `unicode_fractions` (a lex-time normalization table), not a
# numeric-parsing routine. Producing `quantity.value`/`lower`/`upper` (the
# schema requires these as JSON numbers) is therefore necessarily part of
# "evaluate quantities", which RO-10 explicitly assigns to this module.
# This is the minimal, literal parse of already-classified quantity text --
# not culinary inference, not calibration.
# ---------------------------------------------------------------------------

_FRACTION_RE = re.compile(r"^(\d+)\s*/\s*(\d+)$")
_MIXED_NUMBER_RE = re.compile(r"^(\d+)\s+(\d+)\s*/\s*(\d+)$")


def _parse_number(raw: Optional[str], knowledge: Any) -> Optional[float]:
    """Best-effort literal parse of a quantity span's text into a float.

    Returns None (never a guessed/rounded value) if the text cannot be
    parsed -- callers must treat that as unresolved material, never as 0
    or any other invented placeholder.
    """
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    unicode_fractions = getattr(knowledge, "unicode_fractions", {}) or {}
    if text in unicode_fractions:
        return float(unicode_fractions[text])
    match = _MIXED_NUMBER_RE.match(text)
    if match:
        whole, num, den = match.groups()
        den_i = int(den)
        if den_i == 0:
            return None
        return float(whole) + float(num) / den_i
    match = _FRACTION_RE.match(text)
    if match:
        num, den = match.groups()
        den_i = int(den)
        if den_i == 0:
            return None
        return float(num) / den_i
    try:
        return float(text)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Generic tree-walking helpers over the `ASTNode.to_dict()` shape.
#
# A leaf (SpanNode subclass) dict has a "span" key. A container dict has a
# "children" list. `IngredientReference`/`Candidate`/`ParseResult` dicts have
# their own named fields instead of "children" and are handled explicitly
# where used, not through these generic helpers.
# ---------------------------------------------------------------------------


def _node_type(node: Optional[dict]) -> Optional[str]:
    return node.get("node_type") if node else None


def _is_leaf(node: Optional[dict]) -> bool:
    return bool(node) and "span" in node


def _as_node_list(value: Union[dict, List[dict], None]) -> List[dict]:
    """Normalizes a reference field that is EITHER a single node dict (the
    older parser shape some fields used, and any not-yet-migrated
    `preparation` value from a pre-refactor `ingredient_parse_trees` row)
    OR a list of node dicts (the current `preparation`/`measurements`
    shape) into a plain list, without crashing on either. `None`/missing
    becomes an empty list. Used wherever a field needs to support both
    shapes during the transition -- see `_build_preparation_modifiers` and
    `_reference_source_spans`.
    """
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]
    return list(value)


def _children(node: Optional[dict]) -> List[dict]:
    return list(node.get("children", [])) if node else []


def _iter_leaves(node: Optional[dict]) -> Iterable[dict]:
    """Yield every leaf (SpanNode) dict under `node`, document order."""
    if node is None:
        return
    if _is_leaf(node):
        yield node
    else:
        for child in _children(node):
            yield from _iter_leaves(child)


def _span_text(leaf: dict) -> str:
    return leaf["span"]["text"]


def _span_norm(leaf: dict) -> str:
    return (leaf["span"].get("normalized_value") or leaf["span"]["text"]).strip()


def _source_spans_of(node: Optional[dict]) -> List[str]:
    """Provenance list for one semantic object: the raw source text of
    every leaf lexical span under `node`, in document order."""
    return [_span_text(leaf) for leaf in _iter_leaves(node)]


def _normalized_phrase(node: Optional[dict]) -> str:
    return " ".join(_span_norm(leaf) for leaf in _iter_leaves(node)).strip()


def _fallback_spans(spans: List[str], placeholder: str) -> List[str]:
    """schema requires source_spans minItems 1 in several places; this
    guards against ever emitting an empty array without inventing text
    that looks like it came from the source line."""
    return spans if spans else [placeholder]


# ---------------------------------------------------------------------------
# Ingredient resolution -- RO-9 SS A
# ---------------------------------------------------------------------------

_INGREDIENT_MODIFIER_CLASS_BY_NODE_TYPE: Dict[str, str] = {
    "SizeNode": "size",
    "DescriptorNode": "descriptor",
    "StateNode": "state",
    "TemperatureNode": "temperature",
}


def _resolve_ingredient_term(term: str, knowledge: Any) -> Tuple[Optional[str], Optional[str]]:
    """Implements SS A.1-2 given the loader's actual data shape.

    NOTE: `knowledge.ingredients` (backing `is_ingredient`/membership
    checks) contains BOTH canonical ingredient names AND ingredient alias
    surface forms -- `RuntimeKnowledge._load_ingredient_aliases` tags every
    alias with the same "ingredient" vocabulary class as the canonical
    name. Membership in `knowledge.ingredients` therefore cannot, by
    itself, distinguish "this is the canonical form" from "this is an
    alias" the way SS A.1's phrasing ("If it is a member of
    knowledge.ingredients -> exact") suggests in isolation. This resolves
    it correctly by always consulting `resolve_ingredient_alias` first:
    unchanged output + known ingredient => exact; changed output => alias;
    unchanged output + NOT a known ingredient => unresolved. This produces
    exactly SS A's fixed precedence (exact then alias, mutually exclusive)
    against the loader's real semantics.
    """
    if not term:
        return None, None
    resolved = knowledge.resolve_ingredient_alias(term)
    if resolved == term:
        if term in knowledge.ingredients:
            return term, "exact_ingredient_match"
        return None, None
    return resolved, "alias_match"


def _resolve_ingredient(
    ingredient_expr: Optional[dict],
    knowledge: Any,
    evidence: List[dict],
    unresolved_out: List[dict],
) -> Tuple[Optional[str], List[dict], Optional[str]]:
    """Resolves one (non-compound) IngredientExpression per SS A, and
    extracts its size/descriptor/state/temperature children as `modifier`
    objects per SS G (see mismatch #2/4 above for why `applies_to` is
    always "ingredient" here).

    Returns (ingredient_id_or_None, modifiers, original_text_or_None).

    `original_text` is the VERBATIM, as-written text of specifically the
    `IngredientNode` leaf(ves) that were joined into `core_term` and
    looked up via `resolve_ingredient_alias`/`knowledge.ingredients` --
    i.e. exactly what triggered this resolution, before any alias
    normalization is applied. E.g. a line written "2 scallions" that
    resolves via a curated alias to canonical id "green onion" gets
    `original_text="scallions"` -- alias resolution changes
    `ingredient_id`, never this. This is what downstream
    `ingredient_name_original` should display instead of the canonical
    id's own name (see `_project_reference_to_row`). None whenever there
    is no `IngredientNode` leaf at all to have triggered anything -- the
    same cases (`ingredient_expr is None`, or one with only modifier
    children and no noun) that already return `ingredient_id=None` today.
    """
    if ingredient_expr is None:
        # A reference the parser never attached ANY ingredient expression
        # to (e.g. "ribs" fully absorbed into `component`, leaving
        # `ingredient` entirely unset) has no canonical identity at all --
        # per the schema's own entity/vocabulary boundary rule, a
        # component is a vocabulary term, never a substitute for the
        # ingredient entity. This must not be silently treated as
        # "resolved" just because nothing else about it failed; a
        # reference describing a quantity of literally nothing identified
        # is itself the defect. Confirmed as a real bug via production
        # data: a component-only candidate for "2 ribs" was being scored
        # "resolved" (no unresolved entries) purely because component
        # resolution doesn't touch `unresolved_out`, which meant it
        # counted as a second "genuinely good" reading alongside the
        # correct bare-ingredient candidate and forced the whole line into
        # "ambiguous" (see _derive_result's updated docstring for the
        # other half of this fix).
        unresolved_out.append(
            {
                "text": "<no ingredient>",
                "reason": "no_ingredient_identified",
                "source_spans": ["<no ingredient>"],
            }
        )
        evidence.append(_evidence("unresolved_material", "no_ingredient_identified"))
        return None, [], None

    core_leaves: List[dict] = []
    modifiers: List[dict] = []

    for child in _children(ingredient_expr):
        child_type = _node_type(child)
        if child_type == "IngredientNode":
            core_leaves.append(child)
        elif child_type in _INGREDIENT_MODIFIER_CLASS_BY_NODE_TYPE:
            term = _span_norm(child)
            modifiers.append(
                {
                    "modifier_class": _INGREDIENT_MODIFIER_CLASS_BY_NODE_TYPE[child_type],
                    "term": term,
                    "applies_to": "ingredient",
                    "source_spans": [_span_text(child)],
                }
            )
            evidence.append(_evidence("vocabulary_match", term))
        else:
            # Not one of the classes chunk_primitives ever merges into an
            # IngredientExpression -- preserve rather than discard (SS J).
            spans = [_span_text(child)] if _is_leaf(child) else _source_spans_of(child)
            unresolved_out.append(
                {
                    "text": " ".join(spans),
                    "reason": "unrecognized_span",
                    "source_spans": _fallback_spans(spans, "<unrecognized ingredient-position span>"),
                }
            )
            evidence.append(_evidence("unresolved_material", " ".join(spans) or "unknown"))

    if not core_leaves:
        return None, modifiers, None

    core_term = " ".join(_span_norm(leaf) for leaf in core_leaves).strip()
    core_spans = [_span_text(leaf) for leaf in core_leaves]
    original_text = " ".join(core_spans).strip() or None

    ingredient_id, kind = _resolve_ingredient_term(core_term, knowledge)
    if ingredient_id is not None:
        # _resolve_ingredient_term guarantees kind is not None whenever
        # ingredient_id is not None (both come from the same branch).
        assert kind is not None
        evidence.append(_evidence(kind, ingredient_id))
    else:
        unresolved_out.append(
            {
                "text": core_term,
                "reason": "unknown_ingredient",
                "source_spans": _fallback_spans(core_spans, core_term),
            }
        )
        evidence.append(_evidence("unresolved_material", core_term))

    return ingredient_id, modifiers, original_text


# ---------------------------------------------------------------------------
# Preparation -- one modifier, RO-9 SS G ("preparation" row: applies_to=ingredient)
# ---------------------------------------------------------------------------


def _preparation_clause_text(
    clause_node: dict,
    evidence: List[dict],
    unresolved_out: List[dict],
) -> Tuple[str, List[str]]:
    """Extracts (term_text, source_spans) for ONE plain
    PreparationExpression-shaped clause node, walking its direct children:
      - a leaf `PreparationNode` contributes its text to the term;
      - a leaf `UnknownNode` also contributes its text (preserve, don't
        discard) but is ALSO recorded as unresolved;
      - a leaf `QuantityNode` contributes its literal written text (e.g.
        "1/2"), not `normalized_value`'s decimal form ("0.5") -- a
        preparation clause is read as natural-language instruction text
        (by a person, or downstream nutrition search), and the decimal
        form is only useful when the quantity is being computed with,
        which it deliberately is not once it's embedded in prep text
        rather than a `quantity` object (see
        `_merge_trailing_preparation_measurement`);
      - any other leaf contributes its normalized form as before;
      - a non-leaf child is the parser's embedded-measurement nesting
        (Pass 2c, e.g. "1/2-inch" inside a "cut ... cubes" clause) -- its
        own leaves are folded in via `_iter_leaves`, type-agnostically,
        without being flagged as unrecognized.
    Shared by `_build_preparation_modifiers` for both a plain clause and
    each side of an Alternative/ConjunctionExpression-wrapped clause pair
    (see that function), and by `_merge_trailing_preparation_measurement`
    for its reconstructed clause.
    """
    term_parts: List[str] = []
    source_spans: List[str] = []
    for child in _children(clause_node):
        if _is_leaf(child):
            text = _span_text(child)
            term_parts.append(text if _node_type(child) == "QuantityNode" else _span_norm(child))
            source_spans.append(text)
            if _node_type(child) == "UnknownNode":
                unresolved_out.append(
                    {"text": text, "reason": "unrecognized_span", "source_spans": [text]}
                )
                evidence.append(_evidence("unresolved_material", text))
        else:
            nested_leaves = list(_iter_leaves(child))
            if nested_leaves:
                term_parts.append(" ".join(_span_norm(leaf) for leaf in nested_leaves))
                source_spans.extend(_span_text(leaf) for leaf in nested_leaves)
    return " ".join(part for part in term_parts if part).strip(), source_spans


def _leaf_offsets(node: Optional[dict]) -> Tuple[int, int]:
    """(min start_offset, max end_offset) across every leaf under `node`.
    (0, 0) for a node with no leaves at all (should not happen for any
    real clause/measurement, but defensive rather than crashing)."""
    leaves = list(_iter_leaves(node))
    if not leaves:
        return (0, 0)
    return (
        min(leaf["span"]["start_offset"] for leaf in leaves),
        max(leaf["span"]["end_offset"] for leaf in leaves),
    )


def _merge_trailing_preparation_measurement(
    prep_clauses: List[dict],
    measurements: List[dict],
    notes: List[dict],
) -> Tuple[List[dict], List[dict], List[dict]]:
    """Detects and reconstructs the "cut into 1/2-inch cubes" shape:
    a preparation clause (e.g. "cut") whose true completion is a LATER,
    structurally separate measurement -- one the parser currently emits
    as an ordinary extra `measurements` entry (e.g. "1/2 cubes"), with
    the connective word bridging them ("into", "to", "through", ...)
    emitted separately as a `notes` GrammarNode leaf.

    FLAGGED GAP #3 (further down in this file) originally assumed the
    parser nests this kind of embedded measurement INSIDE the
    preparation clause itself and explicitly flagged that assumption as
    unverified against real output. It's confirmed wrong: real parser
    output for "3 lb boneless chuck, cut into 1/2 cubes" gives
    `preparation: [<boneless>, <cut>]` (two SEPARATE one-word clauses),
    `notes: [<into>]`, and a second, unrelated-looking bare
    `MeasurementExpression` for "1/2 cubes" sitting in `measurements`.
    Left alone, `_assign_quantities` discards that second measurement as
    `additional_measurement_unsupported`, "cut" is emitted as a
    meaningless one-word preparation modifier, and "into" is emitted as
    a stray note -- exactly the fragmentation this reassembles.

    Detection is purely structural/positional, using only span offsets
    and node types already in the tree -- no hardcoded vocabulary for
    "cubes"/"into"/etc, so this also covers e.g. "sliced lengthwise to
    ribbons":
      - `prep_clauses` is non-empty;
      - `measurements` has more than one entry, and the LAST one is a
        bare `MeasurementExpression` (NOT inside a
        `ParentheticalExpression` -- those are per-item quantities, an
        unrelated, already-correct concept) whose own quantity/unit
        shape has no dangling material and whose unit is a
        `NaturalPortionNode` (a countable shape/portion word -- the same
        node type "1 clove garlic" already uses; a `MeasurementNode`
        like "ounce" is a real, independent measurement and is
        deliberately NOT matched here, see id-16-shaped lines);
      - that measurement's own start offset is AFTER the last
        preparation clause's own end offset -- i.e. it is positioned in
        the part of the line that follows the preparation word, not
        overlapping the ingredient/quantity region earlier in the line.

    Any `notes` leaf sitting strictly between the two (a bridging
    connective) is folded into the merged clause instead of being
    dropped or double-counted as a separate note. On a match, returns
    (preparation list with the last clause replaced by the merged one,
    measurements list with the last entry removed, notes list with any
    consumed bridging note(s) removed). Returns the three inputs
    unchanged when the shape doesn't match -- never invents a merge
    that isn't structurally justified by real offsets already in the
    tree.
    """
    if not prep_clauses or len(measurements) < 2:
        return prep_clauses, measurements, notes

    last_prep_clause = prep_clauses[-1]
    if _node_type(last_prep_clause) != "PreparationExpression":
        return prep_clauses, measurements, notes

    last_measurement = measurements[-1]
    if _node_type(last_measurement) != "MeasurementExpression":
        return prep_clauses, measurements, notes

    _, unit_leaf, dangling = _measurement_expr_shape(last_measurement)
    if dangling or unit_leaf is None or _node_type(unit_leaf) != "NaturalPortionNode":
        return prep_clauses, measurements, notes

    _, prep_end = _leaf_offsets(last_prep_clause)
    meas_start, _ = _leaf_offsets(last_measurement)
    if meas_start < prep_end:
        return prep_clauses, measurements, notes

    bridging_notes = [
        note
        for note in notes
        if _is_leaf(note) and prep_end <= note["span"]["start_offset"] < meas_start
    ]
    bridging_ids = {id(note) for note in bridging_notes}
    remaining_notes = [note for note in notes if id(note) not in bridging_ids]

    ordered_pieces = sorted(
        [last_prep_clause] + bridging_notes + [last_measurement],
        key=lambda piece: _leaf_offsets(piece)[0],
    )
    merged_children: List[dict] = []
    for piece in ordered_pieces:
        merged_children.extend([piece] if _is_leaf(piece) else _children(piece))

    merged_clause = {"node_type": "PreparationExpression", "children": merged_children}
    remaining_prep = prep_clauses[:-1] + [merged_clause]
    remaining_measurements = measurements[:-1]
    return remaining_prep, remaining_measurements, remaining_notes


def _build_preparation_modifiers(
    prep_clauses: Union[dict, List[dict], None],
    evidence: List[dict],
    unresolved_out: List[dict],
) -> List[dict]:
    """SS G ('preparation' row: applies_to=ingredient), updated for the
    parser change reported [date of this fix]: `reference.preparation` is
    now `List[ASTNode]` -- one PreparationExpression-shaped node per
    clause, in source order, mirroring the pre-existing
    `measurements: List[ASTNode]` pattern. Clause boundaries (pre- vs.
    post-nominal) are determined entirely by the parser
    (`_attach_preparation_clause`) and are NOT re-derived here -- this
    function trusts the list's grouping and order as given.

    Returns one modifier object per clause (was: at most one modifier
    total). This finally resolves the FLAGGED GAP #3 note further down in
    this file (preparation lists were previously always 0-1 elements,
    never the multi-clause shape RO-10 originally asked for) -- see that
    comment block for the history.

    Per-clause term text is built by `_preparation_clause_text`. A clause
    entry that is itself an `AlternativeExpression`/`ConjunctionExpression`
    wrapping two PreparationExpression operands (e.g. "minced or
    pressed") is now normally intercepted BEFORE this function ever runs
    -- `_process_parser_reference` detects it
    (`_find_compound_preparation_clause`) and dispatches to
    `_build_preparation_alternative_references`, which decomposes the
    whole reference into one row per alternative (explicit confirmation:
    "minced or pressed" -> two rows, same ingredient, first optional=0,
    second optional=1 -- the same convention as an ingredient alternative
    like "butter or olive oil"). SS I.6 had explicitly left compound
    preparation decomposition open; that's now resolved at the
    reference-decomposition level, not by combining text here.

    The combining behavior below (build one text like "minced or
    pressed" instead of decomposing) is kept ONLY as a fallback for an
    edge case the decomposition dispatch doesn't reach: a reference whose
    `ingredient` is ALSO compound (SS I's own decomposition triggers
    first in `_process_parser_reference`'s dispatch order), so this
    function still receives the original, undecomposed preparation
    clause list. That combination (compound ingredient AND compound
    preparation on the same reference) is not itself decomposed for
    preparation -- reconstructing "minced or pressed" as literal,
    faithful text here is a safe fallback (not an invented culinary
    interpretation -- no choice is being made, just transcribed), but a
    curator wanting BOTH dimensions decomposed together would need that
    built separately if it comes up in practice.

    ROBUSTNESS NOTE: any `ingredient_parse_trees` row persisted by the
    PRE-refactor parser (not yet re-parsed) still holds the OLD shape --
    `preparation` as a single `PreparationExpression`-shaped dict, not a
    list. `persist_all_lines`/`analyze_all_lines` read every persisted row
    regardless of which parser version produced it, so this WILL occur on
    any database that hasn't been fully re-parsed after the upgrade, not
    just in stale test fixtures. Detected here (a dict where a list was
    expected, via `_as_node_list`) and handled by treating it as a single
    one-clause list -- the exact same result this function's predecessor
    produced for that shape -- rather than crashing or silently misreading
    it.
    """
    prep_clauses = _as_node_list(prep_clauses)
    if not prep_clauses:
        return []

    modifiers: List[dict] = []
    for clause_node in prep_clauses:
        clause_type = _node_type(clause_node)

        if clause_type in ("AlternativeExpression", "ConjunctionExpression"):
            children = _children(clause_node)
            if len(children) == 2:
                left_term, left_spans = _preparation_clause_text(children[0], evidence, unresolved_out)
                right_term, right_spans = _preparation_clause_text(children[1], evidence, unresolved_out)
                connective = clause_node.get("connective")
                if connective is not None:
                    connective_text = _span_norm(connective)
                    connective_spans = [_span_text(connective)]
                else:
                    # Defensive fallback only -- the node TYPE itself is
                    # unambiguous about which connective it represents,
                    # this is not a guess about uncertain content.
                    connective_text = "or" if clause_type == "AlternativeExpression" else "and"
                    connective_spans = []
                term = f"{left_term} {connective_text} {right_term}".strip()
                spans = left_spans + connective_spans + right_spans
                if term:
                    evidence.append(_evidence("vocabulary_match", term))
                    modifiers.append(
                        {
                            "modifier_class": "preparation",
                            "term": term,
                            "applies_to": "ingredient",
                            "source_spans": _fallback_spans(spans, term),
                        }
                    )
                continue
            # Not the expected 2-operand shape -- fall through to the
            # generic single-clause handling below rather than guessing
            # further (children walked directly; won't crash, may just
            # produce an odd/partial term for this genuinely unanticipated
            # shape).

        term, source_spans = _preparation_clause_text(clause_node, evidence, unresolved_out)
        if not term:
            continue
        evidence.append(_evidence("vocabulary_match", term))
        modifiers.append(
            {
                "modifier_class": "preparation",
                "term": term,
                "applies_to": "ingredient",
                "source_spans": _fallback_spans(source_spans, term),
            }
        )
    return modifiers


# ---------------------------------------------------------------------------
# Component / natural-portion -- RO-9 SS B, SS C
# ---------------------------------------------------------------------------


def _relationship_lookup_terms(term: str) -> List[str]:
    """Candidate `subject_id` strings to try, in preference order, against
    `RuntimeKnowledge.find_relationships`.

    CONFIRMED (not assumed): `find_relationships` currently does plain
    exact-string matching on `subject_id` -- there is no pluralizer or
    other normalization inside `knowledge/` yet (that is an explicitly
    separate, future ticket). Meanwhile a vocabulary-class span's own
    `span.normalized_value` (as set by the PARSER, which this module
    does not control) is not itself singularized -- e.g. a
    `ComponentNode`/`NaturalPortionNode` for "ribs" carries
    `normalized_value: "ribs"`, while a curated relationship for the
    same vocabulary word is authored as `subject_id: "rib"` (confirmed
    via `loader_diagnostic` output). An exact-match lookup on the
    unmodified term therefore silently misses a real, curated
    relationship for the single most common case: a plain trailing "s".
    Both `_resolve_component` and `_add_natural_portion_evidence` route
    through this helper so the fix applies uniformly to both predicates.

    This is deliberately NOT a pluralizer: it tries the term exactly as
    given first, and only as a fallback strips a single trailing "s".
    No other inflection is attempted, nothing is guessed beyond that one
    narrow, extremely common English shape, and this whole function goes
    away once `knowledge/` grows a real pluralizer and callers can go
    back to a single `find_relationships` call.
    """
    candidates = [term]
    if term.endswith("s") and len(term) > 1:
        singular = term[:-1]
        if singular != term:
            candidates.append(singular)
    return candidates


def _find_relationships_any(
    knowledge: Any,
    subject_id: str,
    predicate: str,
    object_type: str,
    object_id: str,
) -> tuple:
    """`knowledge.find_relationships`, tried across
    `_relationship_lookup_terms(subject_id)` in order, returning the
    first non-empty result (or `()` if none match). See that function's
    docstring for why more than one term is tried at all."""
    for candidate_term in _relationship_lookup_terms(subject_id):
        relationships = knowledge.find_relationships(
            subject_type="vocabulary",
            subject_id=candidate_term,
            predicate=predicate,
            object_type=object_type,
            object_id=object_id,
        )
        if relationships:
            return relationships
    return ()


def _resolve_component(
    component_expr: Optional[dict],
    ingredient_id: Optional[str],
    knowledge: Any,
    evidence: List[dict],
    unresolved_out: List[dict],
) -> Optional[str]:
    """Structural presence of a ComponentExpression already means this
    candidate committed to the component reading (a between-candidate
    lexical choice, SS A.5/SS C); this function only decides which
    evidence backs that reading (SS B.4 vs SS B.5)."""
    if component_expr is None:
        return None

    comp_leaves = [c for c in _children(component_expr) if _node_type(c) == "ComponentNode"]
    for child in _children(component_expr):
        if _node_type(child) != "ComponentNode":
            spans = [_span_text(child)] if _is_leaf(child) else _source_spans_of(child)
            unresolved_out.append(
                {
                    "text": " ".join(spans),
                    "reason": "unrecognized_span",
                    "source_spans": _fallback_spans(spans, "<unrecognized component-position span>"),
                }
            )
            evidence.append(_evidence("unresolved_material", " ".join(spans) or "unknown"))

    if not comp_leaves:
        return None

    term = " ".join(_span_norm(leaf) for leaf in comp_leaves).strip()
    evidence.append(_evidence("structural_match", "component"))

    relationships = ()
    if ingredient_id:
        relationships = _find_relationships_any(
            knowledge, term, "component_of", "ingredient", ingredient_id
        )
    if relationships:
        # SS B.6: one evidence entry per matching row, not deduplicated.
        for rel in relationships:
            evidence.append(_evidence("relationship_match", rel.relationship_id))
    else:
        evidence.append(_evidence("vocabulary_match", term))

    return term


def _add_natural_portion_evidence(
    unit_term: str,
    ingredient_id: Optional[str],
    knowledge: Any,
    evidence: List[dict],
) -> None:
    """SS C evidence step for the case where a NaturalPortionNode is
    embedded directly in a MeasurementExpression (component left unset)."""
    if not ingredient_id or not unit_term or unit_term == ingredient_id:
        return
    relationships = _find_relationships_any(
        knowledge, unit_term, "natural_portion_of", "ingredient", ingredient_id
    )
    if relationships:
        for rel in relationships:
            evidence.append(_evidence("relationship_match", rel.relationship_id))
    else:
        evidence.append(_evidence("vocabulary_match", unit_term))


# ---------------------------------------------------------------------------
# Quantity construction -- RO-9 SS D, SS H
# ---------------------------------------------------------------------------


def _measurement_expr_shape(
    node: dict,
) -> Tuple[Optional[dict], Optional[dict], bool]:
    """Classifies a MeasurementExpression's direct children.

    Returns (quantity_leaf, unit_leaf, dangling_range) where dangling_range
    is True iff a RangeMarker is present with no closing quantity in THIS
    node (see mismatch #1 -- this is how a fragmented range head looks).
    """
    quantity_leaf = None
    unit_leaf = None
    dangling_range = False
    for child in _children(node):
        child_type = _node_type(child)
        if child_type == "QuantityNode":
            quantity_leaf = child
        elif child_type in ("MeasurementNode", "NaturalPortionNode"):
            unit_leaf = child
        elif child_type == "RangeMarker":
            dangling_range = True
    return quantity_leaf, unit_leaf, dangling_range


def _is_bare_quantity_only(node: dict) -> bool:
    quantity_leaf, unit_leaf, dangling = _measurement_expr_shape(node)
    return quantity_leaf is not None and unit_leaf is None and not dangling


def _build_scalar_quantity(
    node: dict,
    knowledge: Any,
    component_term: Optional[str] = None,
    ingredient_resolved: Optional[str] = None,
    ingredient_raw: Optional[str] = None,
    allow_bare_ingredient_fallback: bool = False,
) -> Tuple[Optional[dict], Optional[str]]:
    """Builds one schema `quantity` object (always form=scalar -- see
    mismatch #1, ranges are never constructed by this Analyzer today) from
    a clean (non-dangling) MeasurementExpression. Returns (quantity, None)
    on success or (None, failure_reason) on failure -- never a guessed
    value.

    Implements SS H's structural-input table:
      - unit_leaf present (Measurement/NaturalPortion) -> that unit, directly.
      - else component_term given -> natural_portion, unit_term=component (SS B/H).
      - else, if allowed, bare ingredient noun -> natural_portion,
        unit_term = the resolved (or raw, if unresolved) ingredient form
        (the "2 carrots" case, SS H's resolution of the parser's deferred
        question).
      - else -> failure ("missing_unit"): never fabricate a unit.
    """
    quantity_leaf, unit_leaf, dangling = _measurement_expr_shape(node)
    if dangling:
        return None, "dangling_range"
    if quantity_leaf is None:
        return None, "no_quantity_value"
    value = _parse_number(_span_norm(quantity_leaf), knowledge)
    if value is None:
        return None, "unparseable_quantity_value"

    source_spans = _fallback_spans(_source_spans_of(node), _span_text(quantity_leaf))

    if unit_leaf is not None:
        unit_term = _span_norm(unit_leaf)
        unit_type = "measurement" if _node_type(unit_leaf) == "MeasurementNode" else "natural_portion"
    elif component_term:
        unit_type, unit_term = "natural_portion", component_term
    elif allow_bare_ingredient_fallback and (ingredient_resolved or ingredient_raw):
        unit_type, unit_term = "natural_portion", (ingredient_resolved or ingredient_raw)
    else:
        return None, "missing_unit"

    return (
        {
            "form": "scalar",
            "value": value,
            "unit_type": unit_type,
            "unit_term": unit_term,
            "source_spans": source_spans,
        },
        None,
    )


def _range_expr_shape(node: dict) -> Tuple[Optional[dict], Optional[dict], Optional[dict]]:
    """Classifies a RangeExpression's direct children into
    (lower_leaf, upper_leaf, unit_leaf). Confirmed against real parser
    output: a bare range like "3-5" is `[QuantityNode, RangeMarker,
    QuantityNode]` -- the first QuantityNode is the lower bound, the
    second is the upper bound, and RangeMarker (or UnitConnectorMarker,
    should one ever appear here) is a pure connective carrying no data.
    An optional trailing MeasurementNode/NaturalPortionNode is the unit
    shared by both bounds (a ranged measurement, e.g. "3-5 ounces"),
    mirroring `_measurement_expr_shape`'s scalar handling -- not yet
    confirmed against real output for that specific sub-case, but this is
    the same structural pattern, not a new guess.
    """
    lower_leaf = None
    upper_leaf = None
    unit_leaf = None
    for child in _children(node):
        child_type = _node_type(child)
        if child_type == "QuantityNode":
            if lower_leaf is None:
                lower_leaf = child
            elif upper_leaf is None:
                upper_leaf = child
        elif child_type in ("MeasurementNode", "NaturalPortionNode"):
            unit_leaf = child
        # RangeMarker / UnitConnectorMarker: pure connectives, no data.
    return lower_leaf, upper_leaf, unit_leaf


def _build_range_quantity(
    node: dict,
    knowledge: Any,
    component_term: Optional[str] = None,
    ingredient_resolved: Optional[str] = None,
    ingredient_raw: Optional[str] = None,
    allow_bare_ingredient_fallback: bool = False,
) -> Tuple[Optional[dict], Optional[str]]:
    """Builds one schema `quantity` object with form="range" from a clean
    RangeExpression node. Mirrors `_build_scalar_quantity`'s SS H
    unit-resolution table exactly, just for two bounds instead of one --
    see that function's docstring for the branch-by-branch rationale,
    unchanged here.
    """
    lower_leaf, upper_leaf, unit_leaf = _range_expr_shape(node)
    if lower_leaf is None or upper_leaf is None:
        return None, "incomplete_range"
    lower = _parse_number(_span_norm(lower_leaf), knowledge)
    upper = _parse_number(_span_norm(upper_leaf), knowledge)
    if lower is None or upper is None:
        return None, "unparseable_quantity_value"

    source_spans = _fallback_spans(_source_spans_of(node), _span_text(lower_leaf))

    if unit_leaf is not None:
        unit_term = _span_norm(unit_leaf)
        unit_type = "measurement" if _node_type(unit_leaf) == "MeasurementNode" else "natural_portion"
    elif component_term:
        unit_type, unit_term = "natural_portion", component_term
    elif allow_bare_ingredient_fallback and (ingredient_resolved or ingredient_raw):
        unit_type, unit_term = "natural_portion", (ingredient_resolved or ingredient_raw)
    else:
        return None, "missing_unit"

    return (
        {
            "form": "range",
            "lower": lower,
            "upper": upper,
            "unit_type": unit_type,
            "unit_term": unit_term,
            "source_spans": source_spans,
        },
        None,
    )


def _build_quantity_from_slot(
    slot: dict,
    knowledge: Any,
    component_term: Optional[str] = None,
    ingredient_resolved: Optional[str] = None,
    ingredient_raw: Optional[str] = None,
    allow_bare_ingredient_fallback: bool = False,
) -> Tuple[Optional[dict], Optional[str]]:
    """Dispatches to scalar or range quantity construction based on the
    slot's tagged `kind` (see `_flatten_measurement_slots`). Used by both
    `_assign_quantities` and `_build_package`'s size-search so a package
    size can also be range-shaped (e.g. "2 (14-16 oz) cans"), not just a
    reference's primary/per-item quantity."""
    if slot.get("kind") == "range":
        return _build_range_quantity(
            slot["expr"], knowledge, component_term, ingredient_resolved, ingredient_raw,
            allow_bare_ingredient_fallback,
        )
    return _build_scalar_quantity(
        slot["expr"], knowledge, component_term, ingredient_resolved, ingredient_raw,
        allow_bare_ingredient_fallback,
    )


def _flatten_measurement_slots(measurements: List[dict]) -> List[dict]:
    """Normalizes `reference.measurements` (a mix of MeasurementExpression /
    RangeExpression / ParentheticalExpression / Alternative-or-
    ConjunctionExpression-of-measurements, in source order) into a flat,
    ordered list of slot dicts:
        {"container": <ParentheticalExpression dict or None>,
         "expr": <MeasurementExpression or RangeExpression dict or None>,
         "kind": "scalar" | "range" (only meaningful when "expr" is set),
         "dangling": bool,
         "unrecognized": bool (optional),
         "raw": <original node, for unrecognized slots>,
         "per_item_marker": <list[str] of verbatim source text, or None>}
    `per_item_marker` is set only for a container (parenthetical) slot
    that has a genuine "each"-type sibling (e.g. "(5 to 6 ounces each)"),
    distinguishing it from a parenthetical that's purely an alternate-unit
    expression of the SAME total quantity (e.g. "(1.15 kg)") -- see
    `_project_reference_to_row`'s per_item_quantity handling.
    """
    slots: List[dict] = []
    for node in measurements:
        node_type = _node_type(node)
        if node_type == "MeasurementExpression":
            _, _, dangling = _measurement_expr_shape(node)
            slots.append({"container": None, "expr": node, "dangling": dangling, "kind": "scalar"})
        elif node_type == "RangeExpression":
            # Confirmed via real parser output: a clean range like "3-5"
            # is now its own dedicated node type (mismatch #1's fix), not
            # a fragmented pair of MeasurementExpressions to reassemble.
            slots.append({"container": None, "expr": node, "dangling": False, "kind": "range"})
        elif node_type == "ParentheticalExpression":
            inner = [
                c for c in _children(node)
                if _node_type(c) in ("MeasurementExpression", "RangeExpression")
            ]
            # A sibling inside the parenthetical that ISN'T the
            # measurement/range itself -- e.g. the `NotesExpression`
            # wrapping "each" in "(5 to 6 ounces each)" -- is a genuine
            # per-item marker, not incidental material to discard (it was
            # previously dropped silently here: filtered out of `inner`
            # and never touched again). Preserved on the slot (SS J:
            # preserve, don't discard) so `_project_reference_to_row` can
            # tell "(1.15 kg)" (an alternate-unit expression of the SAME
            # total quantity) apart from "(5 to 6 ounces each)" (a
            # genuinely PER-ITEM value) -- confirmed via real parser
            # output that both shapes parse identically as "container +
            # scalar/range measurement" except for exactly this sibling.
            inner_ids = {id(c) for c in inner}
            marker_spans: List[str] = []
            for sibling in _children(node):
                if id(sibling) not in inner_ids:
                    marker_spans.extend(_source_spans_of(sibling))
            per_item_marker = marker_spans or None

            if len(inner) == 1:
                inner_node = inner[0]
                if _node_type(inner_node) == "RangeExpression":
                    slots.append({
                        "container": node, "expr": inner_node, "dangling": False,
                        "kind": "range", "per_item_marker": per_item_marker,
                    })
                else:
                    _, _, dangling = _measurement_expr_shape(inner_node)
                    slots.append({
                        "container": node, "expr": inner_node, "dangling": dangling,
                        "kind": "scalar", "per_item_marker": per_item_marker,
                    })
            elif len(inner) >= 2:
                # Fragmented range inside a parenthetical (mismatch #1) --
                # e.g. "(5-6 ounces each)". Not modeled by SS D at all.
                # NOTE: now that RangeExpression exists as its own node
                # type, this branch (>=2 MeasurementExpression siblings
                # inside one Parenthetical) may be dead in practice --
                # left in place defensively rather than removed, since it
                # hasn't been proven unreachable against real output.
                slots.append({"container": node, "expr": None, "dangling": True, "multi": inner})
            else:
                slots.append({"container": node, "expr": None, "dangling": False, "unrecognized": True, "raw": node})
        else:
            # AlternativeExpression/ConjunctionExpression of measurements
            # ("2 cups or 500 ml") -- no worked example or construction
            # rule anywhere in RO-9/RO-7; preserved, not guessed.
            slots.append({"container": None, "expr": None, "dangling": False, "unrecognized": True, "raw": node})
    return slots


def _resolve_dangling_ranges(slots: List[dict], unresolved_out: List[dict]) -> List[dict]:
    """Consumes fragmented-range slots (mismatch #1) and unrecognized slots,
    emitting one honest `unresolved` entry per fragment/group (never
    guessing lower/upper), and returns the remaining clean slots."""
    cleaned: List[dict] = []
    i = 0
    n = len(slots)
    while i < n:
        slot = slots[i]
        if slot.get("unrecognized"):
            raw = slot.get("raw")
            spans = _source_spans_of(raw) if raw else []
            unresolved_out.append(
                {
                    "text": " ".join(spans) if spans else "<unrecognized measurement structure>",
                    "reason": "unrecognized_measurement_structure",
                    "source_spans": _fallback_spans(spans, "<unrecognized measurement structure>"),
                }
            )
            i += 1
            continue
        if slot.get("dangling"):
            spans: List[str] = []
            if slot.get("expr") is not None:
                spans += _source_spans_of(slot["expr"])
            elif slot.get("multi"):
                for inner in slot["multi"]:
                    spans += _source_spans_of(inner)
            elif slot.get("container") is not None:
                spans += _source_spans_of(slot["container"])
            paired = False
            if (
                i + 1 < n
                and not slots[i + 1].get("dangling")
                and not slots[i + 1].get("unrecognized")
                and slots[i + 1].get("container") is None
                and slots[i + 1].get("expr") is not None
            ):
                spans += _source_spans_of(slots[i + 1]["expr"])
                paired = True
            unresolved_out.append(
                {
                    "text": " ".join(spans),
                    "reason": "range_quantity_not_representable",
                    "source_spans": _fallback_spans(spans, "<range quantity>"),
                }
            )
            i += 2 if paired else 1
            continue
        cleaned.append(slot)
        i += 1
    return cleaned


def _assign_quantities(
    slots: List[dict],
    component_term: Optional[str],
    ingredient_resolved: Optional[str],
    ingredient_raw: Optional[str],
    knowledge: Any,
    evidence: List[dict],
    unresolved_out: List[dict],
) -> Tuple[Optional[dict], Optional[dict], Optional[str]]:
    """Implements SS D/SS H's primary-quantity and per-item-quantity
    construction, plus SS K.3 (a per-item-shaped parenthetical with no
    primary quantity is a structural contradiction -> invalid).

    Returns (quantity, per_item_quantity, invalid_reason_or_None).
    """
    clean_slots = _resolve_dangling_ranges(slots, unresolved_out)
    if not clean_slots:
        return None, None, None

    primary_slot = clean_slots[0]
    if primary_slot.get("container") is not None:
        # SS K.3: a per-item-shaped parenthetical with nothing to scope
        # against is a structural contradiction, not merely unresolved.
        return None, None, "per_item_quantity_without_primary"

    quantity, reason = _build_quantity_from_slot(
        primary_slot,
        knowledge,
        component_term=component_term,
        ingredient_resolved=ingredient_resolved,
        ingredient_raw=ingredient_raw,
        allow_bare_ingredient_fallback=True,
    )
    if quantity is None:
        spans = _source_spans_of(primary_slot["expr"])
        unresolved_out.append(
            {
                "text": " ".join(spans),
                "reason": reason or "unparseable_quantity",
                "source_spans": _fallback_spans(spans, "<quantity>"),
            }
        )
        evidence.append(_evidence("unresolved_material", " ".join(spans) or "quantity"))
    else:
        evidence.append(_evidence("structural_match", "quantity"))

    per_item_quantity = None
    if len(clean_slots) >= 2:
        second_slot = clean_slots[1]
        if second_slot.get("container") is not None:
            per_item_quantity, reason2 = _build_quantity_from_slot(second_slot, knowledge)
            if per_item_quantity is not None:
                # See `_flatten_measurement_slots`'s docstring: carried
                # through so `_project_reference_to_row` can tell a
                # genuine per-item value ("(5 to 6 ounces each)") apart
                # from an alternate-unit expression of the SAME total
                # ("(1.15 kg)"). Internal Canonical Semantic Result field,
                # not a downstream-table column.
                per_item_quantity["per_item_marker"] = second_slot.get("per_item_marker")
                evidence.append(_evidence("structural_match", "per_item_quantity"))
            else:
                spans = _source_spans_of(second_slot["expr"])
                unresolved_out.append(
                    {
                        "text": " ".join(spans),
                        "reason": reason2 or "unparseable_quantity",
                        "source_spans": _fallback_spans(spans, "<per-item quantity>"),
                    }
                )
        else:
            # A second bare (non-parenthetical) measurement -- e.g. "2 lbs
            # 3 oz" -- has no construction rule anywhere in RO-9/RO-7.
            spans = _source_spans_of(second_slot["expr"]) if second_slot.get("expr") else []
            unresolved_out.append(
                {
                    "text": " ".join(spans),
                    "reason": "additional_measurement_unsupported",
                    "source_spans": _fallback_spans(spans, "<additional measurement>"),
                }
            )
        for extra_slot in clean_slots[2:]:
            spans = _source_spans_of(extra_slot.get("expr")) if extra_slot.get("expr") else []
            if not spans and extra_slot.get("container") is not None:
                spans = _source_spans_of(extra_slot["container"])
            unresolved_out.append(
                {
                    "text": " ".join(spans),
                    "reason": "additional_measurement_unsupported",
                    "source_spans": _fallback_spans(spans, "<additional measurement>"),
                }
            )

    return quantity, per_item_quantity, None


# ---------------------------------------------------------------------------
# Package -- RO-9 SS E
# ---------------------------------------------------------------------------


def _slot_has_unit(slot: dict) -> bool:
    """True iff a measurement slot (scalar or range, see
    `_flatten_measurement_slots`) carries an explicit unit
    (Measurement/NaturalPortion), as opposed to being bare. Used by
    `_build_package`'s size-search, which needs to recognize a unit
    regardless of whether the underlying expression is a
    MeasurementExpression or a RangeExpression."""
    expr = slot.get("expr")
    if expr is None:
        return False
    if slot.get("kind") == "range":
        _, _, unit_leaf = _range_expr_shape(expr)
    else:
        _, unit_leaf, _ = _measurement_expr_shape(expr)
    return unit_leaf is not None


def _build_package(
    package_node: dict,
    measurement_slots: List[dict],
    knowledge: Any,
    evidence: List[dict],
    unresolved_out: List[dict],
) -> Tuple[Optional[dict], List[dict]]:
    """Constructs `package` {count, package_term, size?} per SS E.

    `package.count` ("its associated bare number") and `package.size` are
    recovered from `measurement_slots` (the same list ordinary quantities
    draw from) rather than from any ordinal link the parser preserves
    between PackageExpression and its siblings -- `IngredientReference`
    only has a singular `package` field and a separate `measurements` list,
    with no index connecting them (parser output contract rule 6 promises
    adjacency in "original source order", but that ordering is not
    reconstructable across two different IngredientReference fields once
    serialized). The deterministic rule used here: the first bare
    (unit-less) quantity slot is the package count; the first remaining
    slot that carries an actual unit is the package size. This matches
    every worked example in RO-9 (a package is never seeded with more than
    one bare quantity or more than one sized measurement in this codebase's
    examples), and is documented here as the concrete resolution RO-10 was
    asked to supply for SS E's structural rule.

    Returns (package_or_None, remaining_slots).
    """
    packaging_leaf = None
    for child in _children(package_node):
        if _node_type(child) == "PackagingNode":
            packaging_leaf = child
            break
    package_term = _span_norm(packaging_leaf) if packaging_leaf else None

    remaining = list(measurement_slots)

    count_value = None
    count_source_spans: List[str] = []
    for index, slot in enumerate(remaining):
        if (
            slot.get("container") is None
            and slot.get("expr") is not None
            and not slot.get("dangling")
            and not slot.get("unrecognized")
            and slot.get("kind") != "range"
            and _is_bare_quantity_only(slot["expr"])
        ):
            quantity_leaf, _, _ = _measurement_expr_shape(slot["expr"])
            # _is_bare_quantity_only already guarantees a QuantityNode is
            # present (that's the definition of "bare quantity only").
            assert quantity_leaf is not None
            count_value = _parse_number(_span_norm(quantity_leaf), knowledge)
            count_source_spans = _source_spans_of(slot["expr"])
            del remaining[index]
            break

    size_obj = None
    for index, slot in enumerate(remaining):
        if slot.get("expr") is not None and not slot.get("dangling") and not slot.get("unrecognized"):
            has_unit = _slot_has_unit(slot)
            if has_unit:
                built, _reason = _build_quantity_from_slot(slot, knowledge)
                if built is not None:
                    size_obj = built
                    del remaining[index]
                break

    if package_term is None or count_value is None:
        # Cannot construct a schema-valid package (count is required) --
        # preserve the attempt rather than fabricate a count/term.
        spans = _source_spans_of(package_node)
        unresolved_out.append(
            {
                "text": " ".join(spans),
                "reason": "package_count_or_term_not_determined",
                "source_spans": _fallback_spans(spans, "<package>"),
            }
        )
        evidence.append(_evidence("unresolved_material", " ".join(spans) or "package"))
        return None, remaining

    package_obj: Dict[str, Any] = {
        "count": count_value,
        "package_term": package_term,
        "source_spans": _fallback_spans(count_source_spans + _source_spans_of(package_node), package_term),
    }
    if size_obj is not None:
        package_obj["size"] = size_obj

    evidence.append(_evidence("structural_match", "package"))
    evidence.append(_evidence("vocabulary_match", package_term))
    return package_obj, remaining


# ---------------------------------------------------------------------------
# Notes -- RO-9 SS J (proposed RO-6 amendment; see semantic_result_schema.json)
# ---------------------------------------------------------------------------


def _build_notes(notes_children: List[dict], evidence: List[dict]) -> List[dict]:
    """`IngredientReference.notes` is already a flat list of the *children*
    of one or more NotesExpression nodes (see `IngredientParser._attach`:
    `ref.notes.extend(e.children)`), i.e. one entry per recognized
    Grammar/annotation span -- not per NotesExpression container. Each
    becomes one canonical note object."""
    notes: List[dict] = []
    for child in notes_children:
        if _is_leaf(child):
            text = _span_text(child)
            notes.append({"text": text, "source_spans": [text]})
            evidence.append(_evidence("vocabulary_match", _span_norm(child)))
        else:
            spans = _source_spans_of(child)
            notes.append({"text": " ".join(spans), "source_spans": _fallback_spans(spans, "<note>")})
    return notes


# ---------------------------------------------------------------------------
# Reference-level `unresolved` passthrough for parser-emitted UnknownSequence
# nodes (SS J: carried through 1:1, reason "unrecognized_span").
# ---------------------------------------------------------------------------


def _carry_through_unresolved(
    parser_unresolved: List[dict],
    unresolved_out: List[dict],
    evidence: List[dict],
) -> None:
    for node in parser_unresolved:
        spans = _source_spans_of(node)
        unresolved_out.append(
            {
                "text": " ".join(spans) if spans else "<unrecognized>",
                "reason": "unrecognized_span",
                "source_spans": _fallback_spans(spans, "<unrecognized>"),
            }
        )
        evidence.append(_evidence("unresolved_material", " ".join(spans) or "unknown"))


# ---------------------------------------------------------------------------
# Reference source_spans (whole-reference provenance, used as a fallback
# when constructing relation.source_spans -- see mismatch #3)
# ---------------------------------------------------------------------------


def _reference_source_spans(parser_ref: dict) -> List[str]:
    """Aggregate provenance for one reference: every leaf span across
    `measurements`/`package`/`ingredient`/`component`/`preparation`/
    `notes`/`unresolved`, deduplicated by offset, returned in ORIGINAL
    SOURCE-TEXT ORDER (sorted by each leaf's own `start_offset`) -- NOT
    grouped by which parser_ref field it came from.

    The sort matters, confirmed by two concrete failing examples: field-
    order grouping silently produces the WRONG word order whenever the
    parser attaches material from different fields in an order that
    doesn't match how the line was actually written --
      - "2 large boneless chicken breasts": `preparation` ("boneless")
        sits BETWEEN the `ingredient` expression's own `SizeNode`
        ("large") and `IngredientNode` ("chicken breasts") in the
        original text, but field-order grouping walks all of `ingredient`
        before any of `preparation`, producing "large chicken breasts
        boneless" instead of "large boneless chicken breasts".
      - "3 lb boneless chuck, cut into 1/2 cubes": a second `measurements`
        entry is positioned, in the actual text, AFTER the ingredient and
        preparation material, but field-order grouping walks ALL of
        `measurements` first, producing "... 1/2 cubes chuck boneless cut
        into" instead of "... boneless chuck cut into 1/2 cubes".
    Sorting the final flat list by offset reconstructs correct order
    regardless of which structural field the parser attached each leaf
    to, and does so directly from `parser_ref`'s own original fields --
    it needs no awareness of `_merge_trailing_preparation_measurement`'s
    local restructuring of those same fields for `preparation`/`quantity`
    purposes elsewhere in `_build_single_reference`; offset order is a
    property of the original text, not of how the Analyzer chooses to
    regroup clauses afterward.
    """
    seen = set()
    entries: List[Tuple[int, str]] = []

    def add_from(node: Optional[dict]) -> None:
        for leaf in _iter_leaves(node):
            key = (leaf["span"]["start_offset"], leaf["span"]["end_offset"])
            if key not in seen:
                seen.add(key)
                entries.append((leaf["span"]["start_offset"], _span_text(leaf)))

    for measurement in parser_ref.get("measurements", []) or []:
        add_from(measurement)
    add_from(parser_ref.get("package"))
    add_from(parser_ref.get("ingredient"))
    add_from(parser_ref.get("component"))
    for prep_clause in _as_node_list(parser_ref.get("preparation")):
        add_from(prep_clause)
    for note in parser_ref.get("notes", []) or []:
        add_from(note)
    for unresolved in parser_ref.get("unresolved", []) or []:
        add_from(unresolved)

    entries.sort(key=lambda entry: entry[0])
    spans = [text for _, text in entries]
    return _fallback_spans(spans, "<empty reference>")


# ---------------------------------------------------------------------------
# Single (non-compound-ingredient) reference construction
# ---------------------------------------------------------------------------


def _build_single_reference(
    ref_id: str,
    parser_ref: dict,
    knowledge: Any,
    evidence: List[dict],
) -> Tuple[dict, Optional[str]]:
    """Builds one canonical Reference from a parser IngredientReference
    whose `ingredient` field (if any) is a plain IngredientExpression, not
    a compound Alternative/ConjunctionExpression (SS I handles that case
    separately via `_build_decomposed_references`).

    Returns (reference, invalid_reason_or_None).
    """
    unresolved: List[dict] = []

    ingredient_id, own_modifiers, ingredient_original_text = _resolve_ingredient(
        parser_ref.get("ingredient"), knowledge, evidence, unresolved
    )
    modifiers: List[dict] = list(own_modifiers)

    # RO-9 SS G / FLAGGED GAP #3 revision: fold a trailing "cut into 1/2
    # cubes"-shaped preparation clause + measurement + connective note
    # back into one clause before building modifiers/quantities/notes
    # from them individually -- see `_merge_trailing_preparation_measurement`.
    # No-op (returns its inputs unchanged) for every line that doesn't
    # structurally match that shape.
    prep_clauses, raw_measurements, raw_notes = _merge_trailing_preparation_measurement(
        _as_node_list(parser_ref.get("preparation")),
        parser_ref.get("measurements", []) or [],
        parser_ref.get("notes", []) or [],
    )

    prep_modifiers = _build_preparation_modifiers(prep_clauses, evidence, unresolved)
    modifiers.extend(prep_modifiers)

    component_term = _resolve_component(
        parser_ref.get("component"), ingredient_id, knowledge, evidence, unresolved
    )

    measurement_slots = _flatten_measurement_slots(raw_measurements)

    package_obj = None
    if parser_ref.get("package") is not None:
        package_obj, measurement_slots = _build_package(
            parser_ref["package"], measurement_slots, knowledge, evidence, unresolved
        )

    ingredient_raw = _normalized_phrase(parser_ref.get("ingredient")) or None
    quantity, per_item_quantity, invalid_reason = _assign_quantities(
        measurement_slots, component_term, ingredient_id, ingredient_raw, knowledge, evidence, unresolved
    )

    if (
        quantity is not None
        and quantity.get("unit_type") == "natural_portion"
        and component_term is None
        and quantity.get("unit_term") not in (None, ingredient_raw)
    ):
        _add_natural_portion_evidence(quantity["unit_term"], ingredient_id, knowledge, evidence)

    notes = _build_notes(raw_notes, evidence)
    _carry_through_unresolved(parser_ref.get("unresolved", []) or [], unresolved, evidence)

    reference: Dict[str, Any] = {
        "id": ref_id,
        "ingredient": (
            {"id": ingredient_id, "original_text": ingredient_original_text}
            if ingredient_id is not None
            else None
        ),
        "source_spans": _reference_source_spans(parser_ref),
    }
    if component_term is not None:
        reference["component"] = {"term": component_term}
    if quantity is not None:
        reference["quantity"] = quantity
    if per_item_quantity is not None:
        reference["per_item_quantity"] = per_item_quantity
    if package_obj is not None:
        reference["package"] = package_obj
    if modifiers:
        reference["modifiers"] = modifiers
    if unresolved:
        reference["unresolved"] = unresolved
    if notes:
        reference["notes"] = notes

    return reference, invalid_reason


# ---------------------------------------------------------------------------
# Compound-ingredient decomposition -- RO-9 SS I
# ---------------------------------------------------------------------------


def _compound_tree(node: dict) -> tuple:
    """Recursively describes an Alternative/ConjunctionExpression over
    IngredientExpression operands as a nested tuple tree:
    ("leaf", IngredientExpression) or
    (node_type, left_subtree, right_subtree, connective_node_or_None).
    `connective_node` is the parser's own AlternativeMarker/
    ConjunctionMarker (see `_relation_source_spans`), read from the node's
    `connective` field -- a sibling of `children`, NOT one of the two
    operands.
    """
    if _node_type(node) == "IngredientExpression":
        return ("leaf", node)
    children = _children(node)
    return (_node_type(node), _compound_tree(children[0]), _compound_tree(children[1]), node.get("connective"))


def _is_compound_ingredient_tree(node: Optional[dict]) -> bool:
    if node is None:
        return False
    node_type = _node_type(node)
    if node_type == "IngredientExpression":
        return False
    if node_type not in ("AlternativeExpression", "ConjunctionExpression"):
        return False
    children = _children(node)
    if len(children) != 2:
        return False
    return all(
        _node_type(child) == "IngredientExpression" or _is_compound_ingredient_tree(child)
        for child in children
    )


def _relation_type_for(node_type: str) -> str:
    return "conjunction" if node_type == "ConjunctionExpression" else "alternative"


def _is_compound_preparation_clause(clause_node: dict) -> bool:
    """True iff a single entry in `reference.preparation`'s clause list is
    itself an Alternative/ConjunctionExpression wrapping exactly two
    PreparationExpression operands -- e.g. "minced or pressed" -- rather
    than a plain single clause. Mirrors `_is_compound_ingredient_tree`'s
    shape check, for preparation instead of ingredient."""
    if _node_type(clause_node) not in ("AlternativeExpression", "ConjunctionExpression"):
        return False
    children = _children(clause_node)
    return len(children) == 2 and all(_node_type(child) == "PreparationExpression" for child in children)


def _find_compound_preparation_clause(prep_clauses: List[dict]) -> Optional[Tuple[int, dict]]:
    """Returns (index, clause_node) for the first compound preparation
    clause in the list, or None. Only the first is decomposed -- a
    reference with more than one independent preparation alternative
    (e.g. two separate "X or Y" clauses) is an unanticipated shape, not
    modeled; the first one found drives decomposition and any additional
    ones are simply inherited unchanged as shared clauses on every
    resulting reference, same as an ordinary plain clause would be."""
    for index, clause in enumerate(prep_clauses):
        if _is_compound_preparation_clause(clause):
            return index, clause
    return None


def _build_preparation_alternative_references(
    parser_ref: dict,
    prep_clauses: List[dict],
    compound_prep: Tuple[int, dict],
    next_ref_id: Callable[[], str],
    knowledge: Any,
    evidence: List[dict],
    invalid_reasons: List[str],
) -> Tuple[List[dict], List[dict]]:
    """Decomposes a reference whose preparation includes a compound
    (Alternative/ConjunctionExpression) clause -- e.g. "1 medium garlic
    clove, minced or pressed" -- into one reference PER preparation
    alternative, sharing everything else (ingredient, quantity, package,
    component, notes, and any OTHER preparation clauses on this same
    reference) unmodified, connected by a relation carrying the real
    connective span (see `_relation_source_spans`).

    SS I.6 explicitly left compound preparation/component decomposition
    open ("Whether compound preparation/component... should decompose the
    same way is explicitly left open"). This is that open item's
    resolution, per explicit confirmation: "minced or pressed" -> two
    downstream rows, same ingredient and quantity, differing only in
    preparation -- the first (leftmost, "minced") optional=0, the second
    ("pressed") optional=1 -- the SAME optionality convention already used
    for a structurally-optional INGREDIENT alternative (e.g. "butter or
    olive oil"), just triggered by a preparation-level choice instead of
    an ingredient-level one. `optional`/`alt_group_id`/`alt_kind` are
    still derived entirely at the projection layer from the relation this
    function returns (`_project_selected_references`'s existing
    membership logic), not computed here -- this function's only job is
    to produce the right SET of canonical references plus the relation
    connecting them; it does not decide optionality itself.

    Implementation: reuses `_build_single_reference` once per operand,
    substituting ONLY that operand's clause into a shallow-copied
    `parser_ref["preparation"]` in place of the compound wrapper (every
    other field -- ingredient, measurements, package, component, notes,
    any other preparation clauses -- passed through unchanged). This is
    simpler and lower-risk than threading shared-field copying through a
    second bespoke construction path, at the cost of re-running
    ingredient/quantity resolution once per operand -- deterministic, so
    this never produces a different result across operands for those
    shared fields, just repeats the same work.
    """
    index, clause_node = compound_prep
    operands = _children(clause_node)
    connective = clause_node.get("connective")
    clause_node_type = _node_type(clause_node)
    # _find_compound_preparation_clause only returns a clause that already
    # passed _is_compound_preparation_clause, which requires node_type to
    # be "AlternativeExpression" or "ConjunctionExpression" -- never None
    # at this point.
    assert clause_node_type is not None
    relation_type = _relation_type_for(clause_node_type)

    references: List[dict] = []
    for operand in operands:
        ref_id = next_ref_id()
        variant_prep_clauses = prep_clauses[:index] + [operand] + prep_clauses[index + 1 :]
        variant_parser_ref = dict(parser_ref)
        variant_parser_ref["preparation"] = variant_prep_clauses
        reference, invalid_reason = _build_single_reference(ref_id, variant_parser_ref, knowledge, evidence)
        if invalid_reason:
            invalid_reasons.append(invalid_reason)
        references.append(reference)

    member_ids = [reference["id"] for reference in references]
    relation = {
        "relation_type": relation_type,
        "members": member_ids,
        "source_spans": _relation_source_spans(connective),
    }
    evidence.append(_evidence("structural_match", "relation"))
    return references, [relation]


_CONNECTIVE_SPAN_UNAVAILABLE = (
    "<connective span not preserved by parser -- see analyzer.py module "
    "docstring, mismatch #3: AlternativeMarker/ConjunctionMarker leaves are "
    "discarded by _classify_conjunction_groups and by the whole-reference "
    "merge in IngredientParser._build_references, so the literal 'and'/'or' "
    "token has no surviving span in the persisted parse tree>"
)


def _relation_source_spans(connective: Optional[dict] = None) -> List[str]:
    """See mismatch #3 in the module docstring: the parser USED TO discard
    the 'and'/'or' token when building an Alternative/ConjunctionExpression.

    CONFIRMED FIXED by a subsequent parser update, verified against real
    production output: `Alternative`/`ConjunctionExpression` now carries a
    `connective` field (a sibling of `children`, e.g.
    `{"node_type": "AlternativeMarker", "span": {...,"text":"or"...}}`)
    holding the marker's own real span. When available, that real text is
    used here directly -- no more fabrication needed for this case. The
    sentinel below remains ONLY as a defensive fallback for a tree that,
    for whatever reason (an older cached parse predating this fix, or some
    future connective-less shape), doesn't carry one -- so this never
    crashes and never fabricates provenance either way.
    """
    if connective is not None:
        spans = _source_spans_of(connective)
        if spans:
            return spans
    return [_CONNECTIVE_SPAN_UNAVAILABLE]


def _build_decomposed_references(
    parser_ref: dict,
    compound_node: dict,
    next_ref_id: Callable[[], str],
    knowledge: Any,
    evidence: List[dict],
    ambiguous_flag: List[bool],
) -> Tuple[List[dict], List[dict]]:
    """SS I: decomposes a compound `ingredient` field into one Reference
    per operand ingredient plus a relation per binary join.

    - preparation/modifiers/notes are inherited unmodified, one copy each
      (SS I.3).
    - a real (non-empty) quantity/package on the compound reference cannot
      be distributed without guessing scope; both/all decomposed references
      get an identical copy and the whole interpretation is flagged
      ambiguous instead (SS I.4/I.5), via `ambiguous_flag[0] = True`.
    - component is not addressed by SS I at all (SS I.6 explicitly leaves
      compound component/preparation open); it is treated here by the same
      quantity-like reasoning as quantity/package (a component names a part
      of a *specific* ingredient, so its scope across a compound is just as
      indeterminate) -- flagged as this Analyzer's own documented extension
      of SS I.4's principle, not an SS I rule.
    - N-ary compounds (SS I.7): nested binary wrapping produces one
      relation per binary node; a relation whose operand is itself a
      compound subtree (not a single leaf ingredient) lists every leaf
      reference id under that subtree in `members`, since the schema has
      no id for an unmaterialized sub-group. Flagged here as this
      Analyzer's own documented resolution of an item SS I.7 left open.
    """
    tree = _compound_tree(compound_node)

    prep_unresolved: List[dict] = []
    shared_prep_modifiers = _build_preparation_modifiers(parser_ref.get("preparation"), evidence, prep_unresolved)
    shared_notes = _build_notes(parser_ref.get("notes", []) or [], evidence)

    has_quantity_material = bool(parser_ref.get("measurements"))
    has_package_material = parser_ref.get("package") is not None
    has_component_material = parser_ref.get("component") is not None

    shared_quantity = None
    shared_per_item = None
    shared_package = None
    shared_component = None
    shared_scope_unresolved: List[dict] = []

    if has_quantity_material or has_package_material or has_component_material:
        ambiguous_flag[0] = True
        measurement_slots = _flatten_measurement_slots(parser_ref.get("measurements", []) or [])
        if has_package_material:
            shared_package, measurement_slots = _build_package(
                parser_ref["package"], measurement_slots, knowledge, evidence, shared_scope_unresolved
            )
        if has_component_material:
            shared_component = _resolve_component(
                parser_ref.get("component"), None, knowledge, evidence, shared_scope_unresolved
            )
        shared_quantity, shared_per_item, _reason = _assign_quantities(
            measurement_slots, shared_component, None, None, knowledge, evidence, shared_scope_unresolved
        )

    references: List[dict] = []
    relations: List[dict] = []

    def visit(subtree: tuple) -> List[str]:
        if subtree[0] == "leaf":
            ingredient_expr = subtree[1]
            ref_id = next_ref_id()
            own_unresolved: List[dict] = []
            ingredient_id, own_modifiers, ingredient_original_text = _resolve_ingredient(
                ingredient_expr, knowledge, evidence, own_unresolved
            )

            modifiers = list(own_modifiers)
            for shared_modifier in shared_prep_modifiers:
                modifiers.append(dict(shared_modifier))

            reference: Dict[str, Any] = {
                "id": ref_id,
                "ingredient": (
                    {"id": ingredient_id, "original_text": ingredient_original_text}
                    if ingredient_id is not None
                    else None
                ),
                "source_spans": _fallback_spans(_source_spans_of(ingredient_expr), "<empty>"),
            }
            if shared_component is not None:
                reference["component"] = {"term": shared_component}
            if modifiers:
                reference["modifiers"] = modifiers
            if shared_notes:
                reference["notes"] = [dict(note) for note in shared_notes]
            if shared_quantity is not None:
                reference["quantity"] = dict(shared_quantity)
            if shared_per_item is not None:
                reference["per_item_quantity"] = dict(shared_per_item)
            if shared_package is not None:
                reference["package"] = dict(shared_package)

            unresolved = list(own_unresolved)
            if not references:
                # First reference built in this decomposition absorbs the
                # shared (preparation/scope) unresolved material once,
                # rather than duplicating it identically on every operand.
                unresolved = prep_unresolved + shared_scope_unresolved + unresolved
            if unresolved:
                reference["unresolved"] = unresolved

            references.append(reference)
            return [ref_id]

        node_type, left, right, connective = subtree
        left_ids = visit(left)
        right_ids = visit(right)
        member_ids = left_ids + right_ids
        relations.append(
            {
                "relation_type": _relation_type_for(node_type),
                "members": member_ids,
                "source_spans": _relation_source_spans(connective),
            }
        )
        evidence.append(_evidence("structural_match", "relation"))
        return member_ids

    visit(tree)
    return references, relations


# ---------------------------------------------------------------------------
# Per-parser-reference dispatch: compound vs. plain
# ---------------------------------------------------------------------------


def _process_parser_reference(
    parser_ref: dict,
    next_ref_id: Callable[[], str],
    knowledge: Any,
    evidence: List[dict],
    ambiguous_flag: List[bool],
    invalid_reasons: List[str],
) -> Tuple[List[dict], List[dict]]:
    ingredient_field = parser_ref.get("ingredient")
    if ingredient_field is not None and _is_compound_ingredient_tree(ingredient_field):
        return _build_decomposed_references(
            parser_ref, ingredient_field, next_ref_id, knowledge, evidence, ambiguous_flag
        )

    prep_clauses = _as_node_list(parser_ref.get("preparation"))
    compound_prep = _find_compound_preparation_clause(prep_clauses)
    if compound_prep is not None:
        return _build_preparation_alternative_references(
            parser_ref, prep_clauses, compound_prep, next_ref_id, knowledge, evidence, invalid_reasons
        )

    ref_id = next_ref_id()
    reference, invalid_reason = _build_single_reference(ref_id, parser_ref, knowledge, evidence)
    if invalid_reason:
        invalid_reasons.append(invalid_reason)
    return [reference], []


# ---------------------------------------------------------------------------
# Candidate evaluation -- RO-7 SS1-SS13, RO-9 SS K/L/M
# ---------------------------------------------------------------------------


def _evaluate_candidate(candidate: dict, index: int, knowledge: Any) -> dict:
    interpretation_id = f"interp_{index + 1}"
    tree = candidate.get("tree") or {}
    line_children = _children(tree)

    counter = [0]

    def next_ref_id() -> str:
        counter[0] += 1
        return f"r{counter[0]}"

    references: List[dict] = []
    relations: List[dict] = []
    evidence: List[dict] = []
    invalid_reasons: List[str] = []
    ambiguous_flag = [False]

    for child in line_children:
        child_type = _node_type(child)
        if child_type == "IngredientReference":
            new_refs, new_rels = _process_parser_reference(
                child, next_ref_id, knowledge, evidence, ambiguous_flag, invalid_reasons
            )
            references.extend(new_refs)
            relations.extend(new_rels)
        elif child_type in ("AlternativeExpression", "ConjunctionExpression"):
            sub = _children(child)
            if len(sub) == 2 and all(_node_type(c) == "IngredientReference" for c in sub):
                # A whole-reference alternative/conjunction (e.g. "1 egg or
                # 2 tbsp flax") -- each side is already a complete parser
                # reference; no ingredient-field decomposition is needed,
                # though a side could independently contain one.
                left_refs, left_rels = _process_parser_reference(
                    sub[0], next_ref_id, knowledge, evidence, ambiguous_flag, invalid_reasons
                )
                right_refs, right_rels = _process_parser_reference(
                    sub[1], next_ref_id, knowledge, evidence, ambiguous_flag, invalid_reasons
                )
                references.extend(left_refs)
                relations.extend(left_rels)
                references.extend(right_refs)
                relations.extend(right_rels)
                member_ids = [r["id"] for r in left_refs] + [r["id"] for r in right_refs]
                # child_type was already confirmed to be
                # "AlternativeExpression"/"ConjunctionExpression" by the
                # `elif child_type in (...)` above -- never None here,
                # though an `in` check over a tuple isn't narrowing a
                # type checker relies on.
                assert child_type is not None
                relations.append(
                    {
                        "relation_type": _relation_type_for(child_type),
                        "members": member_ids,
                        "source_spans": _relation_source_spans(child.get("connective")),
                    }
                )
                evidence.append(_evidence("structural_match", "relation"))
            else:
                invalid_reasons.append("unrecognized_top_level_structure")
        else:
            invalid_reasons.append("unrecognized_top_level_structure")

    if not references:
        references = [
            {
                "id": "r0",
                "ingredient": None,
                "source_spans": ["<empty candidate>"],
                "unresolved": [
                    {
                        "text": "<empty candidate>",
                        "reason": "no_references_produced",
                        "source_spans": ["<empty candidate>"],
                    }
                ],
            }
        ]
        invalid_reasons.append("no_references_produced")

    # SS K.1: relation members must reference ids actually present here.
    known_ids = {ref["id"] for ref in references}
    for relation in relations:
        endpoints = list(relation.get("members") or [])
        if "base" in relation:
            endpoints.append(relation["base"])
        if "preferred" in relation:
            endpoints.append(relation["preferred"])
        if any(endpoint not in known_ids for endpoint in endpoints):
            invalid_reasons.append("dangling_relation_member")

    has_unresolved = any(ref.get("unresolved") for ref in references)

    if invalid_reasons:
        status = "invalid"
    elif ambiguous_flag[0]:
        status = "ambiguous"
    elif has_unresolved:
        status = "unresolved"
    else:
        status = "resolved"

    interpretation: Dict[str, Any] = {
        "id": interpretation_id,
        "status": status,
        "score": _evidence_score(evidence),
        "references": references,
    }
    if relations:
        interpretation["relations"] = relations
    if evidence:
        interpretation["evidence"] = evidence
    return interpretation


# ---------------------------------------------------------------------------
# Top-level result derivation -- RO-7 SS13 (extended for per-interpretation
# "ambiguous", RO-9 SS I.4/SS M's flagged correction)
# ---------------------------------------------------------------------------


def _derive_result(interpretations: List[dict]) -> Tuple[str, Optional[str]]:
    """RO-7 SS13 derives top-level status from the count of viable
    (resolved/unresolved) interpretations. RO-9 introduces a genuine
    per-interpretation "ambiguous" state (SS I.4) that RO-7 didn't
    anticipate; this treats it as viable-but-unselectable, consistent with
    RO-9's own note (SS M) that this exact interaction is a flagged, not
    silently assumed, extension.

    POLICY FIX, confirmed against real production data: the original rule
    here was "more than one non-invalid interpretation -> ambiguous",
    full stop, with no regard for what STATUS those interpretations
    individually held. This was too coarse. Concretely: for "2 ribs
    celery", the parser (correctly, per SS A.5/SS C's between-candidate
    lexical ambiguity model) emits two candidates -- one reading "ribs
    celery" as one nonsensical compound bare-ingredient phrase (which
    fails ingredient resolution -> status "unresolved"), and one reading
    it as component="rib" + ingredient="celery" (which resolves cleanly
    -> status "resolved"). Under the old rule, BOTH counted as "viable"
    purely because neither was "invalid", forcing the line to
    "ambiguous" and producing NO downstream row -- even though only ONE
    of the two candidates was actually a complete, valid reading. This
    pattern recurred across "2 cloves garlic", "2 ribs", "2 ribs
    celery", and "2 ribs, celery" in the same test batch, silently
    dropping all of them from the primary output.

    "unresolved" is not a competing GOOD reading in the same sense
    "resolved" or "ambiguous" (SS I.4) are -- by RO-6's own status
    definitions, it specifically means required semantic material could
    NOT be resolved. A structurally-incomplete candidate existing
    alongside a complete one is not genuine interpretive uncertainty
    about what the line means; it's noise. This is a deterministic,
    structural distinction (whether resolution succeeded or not), not a
    confidence/likelihood judgment -- it does not violate "do not
    automatically populate a winning interpretation merely because one
    candidate has the highest confidence", since no score/confidence
    value is consulted anywhere below; only the fixed status category is.

    Revised rule: only "resolved" and "ambiguous" candidates count as
    "strong" (complete, non-deficient) for the purposes of this decision.
      - >1 strong candidates -> compare `score` (see `_evidence_score`).
        A genuine curated-relationship match (RO-9 SS M REVISION, see
        that section) always outweighs generic vocabulary/structural
        recognition, so this is NOT "pick whichever has the highest
        confidence" in the sense the original SS13 language warns
        against -- it is a deterministic tiebreak over evidence KINDS
        this module already recorded, the same evidence a human curator
        reading the interpretation's own `evidence` list would use.
          - a single top scorer -> it wins outright, exactly like the
            len(strong)==1 case below.
          - still tied even after evidence weighting (no evidence
            distinguishes them) -> genuine, unresolvable ambiguity.
            UNLIKE the old rule, this does NOT mean "no output": one
            candidate (first among the tied, for determinism) is still
            selected so a downstream row is produced, but the top-level
            `status` stays "ambiguous" so curation can triage it. This
            is the one case where an "ambiguous" result carries a
            non-None `selected_interpretation` -- see the updated note
            in `_project_selected_references`.
      - exactly 1 strong candidate -> it wins outright, REGARDLESS of how
        many "unresolved" siblings exist alongside it (those are
        discarded as noise, not "selected among").
      - 0 strong candidates, exactly 1 "unresolved" -> that one wins
        (unchanged from before: a lone imperfect reading still selects).
      - 0 strong candidates, >1 "unresolved" -> none of them are
        trustworthy enough to prefer over the others -> "ambiguous"
        (matches the old rule's outcome for this specific sub-case; none
        of them have any evidence-based claim to preference either, so
        no tiebreak is attempted here -- this bucket predates and is
        orthogonal to the score-based tiebreak above).
    """
    non_invalid = [interp for interp in interpretations if interp["status"] != "invalid"]
    if not non_invalid:
        return "invalid", None

    strong = [interp for interp in non_invalid if interp["status"] in ("resolved", "ambiguous")]

    if len(strong) > 1:
        best_score = max(interp["score"] for interp in strong)
        top = [interp for interp in strong if interp["score"] == best_score]
        winner = top[0]
        if len(top) == 1:
            selected = winner["id"] if winner["status"] == "resolved" else None
            return winner["status"], selected
        # Genuinely tied even after evidence weighting -- still flagged
        # "ambiguous" for curation, but a candidate is selected so the
        # line still produces a downstream row rather than none at all.
        return "ambiguous", winner["id"]
    if len(strong) == 1:
        winner = strong[0]
        selected = winner["id"] if winner["status"] == "resolved" else None
        return winner["status"], selected

    # No resolved/ambiguous candidate at all -- everything left is
    # "unresolved".
    if len(non_invalid) == 1:
        return "unresolved", non_invalid[0]["id"]
    return "ambiguous", None


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def analyze_parse_result(parse_result: dict, knowledge: Any) -> dict:
    """Consumes one `ParseResult.to_dict()` (already-loaded dict) and
    `RuntimeKnowledge`, and returns a Canonical Semantic Result dict
    conforming to `semantic_result_schema.json`.
    """
    candidates = parse_result.get("candidates", []) or []

    if not candidates:
        # No structurally valid parser candidate at all.
        interpretation = {
            "id": "interp_1",
            "status": "invalid",
            "score": 0.0,
            "references": [
                {
                    "id": "r0",
                    "ingredient": None,
                    "source_spans": ["<no parser candidates>"],
                    "unresolved": [
                        {
                            "text": "<no parser candidates>",
                            "reason": "no_parser_candidates",
                            "source_spans": ["<no parser candidates>"],
                        }
                    ],
                }
            ],
        }
        return {"status": "invalid", "interpretations": [interpretation], "selected_interpretation": None}

    interpretations = [_evaluate_candidate(candidate, index, knowledge) for index, candidate in enumerate(candidates)]
    status, selected = _derive_result(interpretations)
    return {"status": status, "interpretations": interpretations, "selected_interpretation": selected}


def analyze_line(parse_tree_json: str, knowledge: Optional[Any] = None) -> dict:
    """Convenience wrapper: consumes the raw `parse_tree_json` string as
    persisted in `ingredient_parse_trees.parse_tree_json`."""
    if knowledge is None:
        from gastrometric.knowledge.loader import knowledge as default_knowledge

        knowledge = default_knowledge
    parse_result = json.loads(parse_tree_json)
    return analyze_parse_result(parse_result, knowledge)




# ===========================================================================
# PRIMARY DOWNSTREAM OUTPUT: recipe_ingredient_lines_parsed
# ===========================================================================
#
# Everything below this point implements RO-10's second work order: a
# projection from the already-complete Canonical Semantic Result (built
# entirely above, unchanged) into flat rows for a NEW downstream table,
# `recipe_ingredient_lines_parsed`. This performs no new semantic
# interpretation -- it only reshapes decisions the Analyzer already made.
#
# Target DDL (not applied here -- init_db.py is not an artifact available
# to this module; this is the authoritative reference the projection code
# below assumes). SS1/SS2 of the work order ask for the recipe/section/
# block lineage FKs to match the PROJECT'S EXISTING convention rather than
# be invented fresh; the existing convention, confirmed by inspecting
# `recipe_ingredient_lines_raw`'s own DDL and the code that populates it
# (`parse_ingredient_blocks.py`), denormalizes THREE lineage FKs directly
# onto every line row: `ingredient_block_id`, `recipe_id`, and
# `recipe_section_id` (NOT `section_id` -- that column name in the work
# order's draft DDL does not match anything in the existing schema). This
# reproduces that exact convention rather than the work order's literal
# draft column names, per the instruction to default to the real schema
# on conflict.
#
#   CREATE TABLE IF NOT EXISTS recipe_ingredient_lines_parsed (
#       id INTEGER PRIMARY KEY AUTOINCREMENT,
#
#       recipe_id                 INTEGER NOT NULL,
#       recipe_section_id         INTEGER NOT NULL,
#       ingredient_block_id       INTEGER NOT NULL,
#       recipe_ingredient_line_id INTEGER NOT NULL,
#
#       ingredient_id TEXT,
#       ingredient_phrase TEXT,
#       ingredient_name_original TEXT,
#
#       grams REAL,
#       ml REAL,
#
#       imperial_weight_value REAL,
#       imperial_weight_unit TEXT,
#
#       imperial_volume_value REAL,
#       imperial_volume_unit TEXT,
#
#       natural_portion_value REAL,
#       natural_portion_min REAL,
#       natural_portion_max REAL,
#       natural_portion TEXT,
#
#       packaging_count REAL,
#       packaging_size_value REAL,
#       packaging_size_unit TEXT,
#       packaging TEXT,
#
#       preparation TEXT,   -- JSON array, genuinely multi-element (clause
#                            -- boundaries are now parser-provided -- see
#                            -- FLAGGED GAP #3's "resolved, pending
#                            -- verification" note below)
#       size TEXT,          -- size descriptor (e.g. "large", "medium"),
#                            -- currently only ever populated from a
#                            -- SizeNode attached to the ingredient itself
#                            -- ("2 large eggs") -- see the comment above
#                            -- its collection in `_project_reference_to_row`
#                            -- for what's NOT yet handled (a SizeNode
#                            -- describing the package instead)
#       notes TEXT,
#
#       optional INTEGER NOT NULL DEFAULT 0,
#       alt_group_id TEXT,
#       alt_kind TEXT,
#
#       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#
#       FOREIGN KEY(recipe_ingredient_line_id) REFERENCES recipe_ingredient_lines_raw(id),
#       FOREIGN KEY(ingredient_block_id)       REFERENCES recipe_ingredient_blocks(id),
#       FOREIGN KEY(recipe_id)                 REFERENCES recipes(id),
#       FOREIGN KEY(recipe_section_id)         REFERENCES recipe_sections(id)
#   );
#
# `natural_portion_min`/`natural_portion_max`/`packaging_size_value`/
# `packaging_size_unit` are now populated by `_project_reference_to_row`,
# verified against real `RangeExpression` parser output (e.g. "3-5 medium
# peppers"), not assumed. REMAINING GAP: a RANGED measurement quantity
# (e.g. a hypothetical "3-5 ounces", unit_type="measurement" rather than
# "natural_portion") and a RANGED package size (e.g. "2 (14-16 oz) cans")
# still have nowhere to go -- this schema only added min/max columns for
# `natural_portion`, not for `imperial_weight`/`imperial_volume`/`grams`/
# `ml`/`packaging_size`. These are counted into the execution report's
# CRITICAL ISSUES (via `unmapped_units`/`unmapped_package_size`) rather
# than silently dropped or collapsed into a single fabricated value.
#
# No column here links back to `analysis_records` -- one isn't needed.
# Both this table and `analysis_records` already share
# `recipe_ingredient_line_id` as a natural join key back to the full
# diagnostic artifact for any row a curator wants to drill into; adding a
# second FK would be exactly the kind of column-for-every-concept the
# work order prohibits (SS "What RO-10 should NOT do").
#
# ---------------------------------------------------------------------------
# FLAGGED GAPS -- read before trusting this projection's output
# ---------------------------------------------------------------------------
#
# 1. UNIT CLASSIFICATION HAS NO KNOWLEDGE-SYSTEM SOURCE.
#    Routing a `quantity` with unit_type="measurement" into
#    grams/ml/imperial_weight_*/imperial_volume_* requires knowing whether
#    a unit term (e.g. "cup", "ounce", "gram") is metric or imperial and
#    weight or volume. `RuntimeKnowledge`, per everything inspected in
#    this codebase, exposes ingredient/alias/relationship/phrase-matching
#    data but no unit-family/-system classification API. The work order
#    itself states the opposite principle for natural portions ("that
#    knowledge should come from the existing knowledge system, not from a
#    hardcoded Analyzer list") -- the same reasoning applies here, but
#    there is currently nothing in RuntimeKnowledge to draw on instead.
#    `_route_measurement_quantity` below uses a small, explicit, clearly
#    labeled hardcoded table as a stopgap. This is NOT a silent
#    workaround: every quantity whose unit isn't in that table is left
#    entirely unrouted (no column populated) and counted into the
#    execution report's CRITICAL ISSUES section, rather than guessed.
#    Recommend adding a real unit-family/-system lookup to
#    `RuntimeKnowledge` and deleting this table once one exists.
#
# 2. NO UNIT CONVERSION IS PERFORMED, INCLUDING WHERE THE DOWNSTREAM
#    SCHEMA MIGHT SEEM TO WANT IT. `grams`/`ml` are populated ONLY when
#    the source already used a metric unit (with kg->g / L->mL SI-prefix
#    scaling ONLY -- a decimal shift within the metric system, not a
#    system conversion). "4 oz" is never converted into grams, and
#    "2 cups" is never converted into ml, even though both are
#    mathematically possible with a fixed conversion factor. This follows
#    directly from RO-9's explicit, repeated rule that "the Analyzer is
#    not the measurement normalization layer" and "do not convert
#    measurements to grams merely because the downstream system likes
#    grams." This work order's own SS4 text ("resolved metric quantities
#    where conversion is possible") is ambiguous about whether it wants
#    that boundary crossed; this implementation keeps the existing,
#    explicit RO-9 boundary rather than silently reversing it, and flags
#    the ambiguity for confirmation. The practical consequence: for any
#    recipe written in cups/tbsp/tsp/oz/lb (the majority of
#    English-language recipes), `grams`/`ml` will be NULL and the
#    imperial_* columns will carry the value instead -- this is expected
#    given the above, not a bug.
#
# 3. [RESOLVED, PENDING EMPIRICAL VERIFICATION] PREPARATION LIST
#    GRANULARITY. This WAS a hard blocker: the parser used to hand the
#    Analyzer one flat, unsegmented `PreparationExpression` per reference,
#    with no clause-boundary signal and no raw source text available to
#    reconstruct one (confirmed empirically at the time, not guessed --
#    see the conversation history around this exact example). The parser
#    was subsequently changed specifically to fix this:
#    `reference.preparation` is now `List[ASTNode]`, one
#    `PreparationExpression`-shaped node per clause, with clause
#    boundaries determined positionally (pre- vs. post-nominal, relative
#    to when `ref.ingredient` was recognized) and embedded measurements
#    (e.g. "1/2-inch" inside "cut ... cubes") nested as a child of the
#    clause they belong to, rather than ejected to the reference-level
#    `measurements` list.
#
#    `_build_preparation_modifiers` (below) was updated to consume this
#    new shape: one modifier per clause, in source order, walking each
#    clause's own direct children and recursing into any nested non-leaf
#    child (the embedded measurement) via the existing type-agnostic
#    `_iter_leaves` helper rather than assuming any new node's specific
#    field names.
#
#    IMPORTANT CAVEAT: this fix was written against a written report of
#    the parser change, NOT against the actual updated
#    `ingredient_parser.py` output -- unlike every other fix in this file,
#    which was verified by running real parser output through this code
#    (see this module's git/conversation history for the range-
#    fragmentation and connective-span fixes, both confirmed empirically
#    before being trusted). The two specific worked examples given
#    (`"3 lb boneless chuck, cut into 1/2-inch cubes"` and
#    `"peeled, quartered lengthwise, cut crosswise into 1/4-inch slices"`)
#    have NOT yet been run through this code against the real parser.
#    Treat this as implemented-but-unverified until that happens; do not
#    assume it is correct merely because the reasoning above is sound.
#
# 4. [RESOLVED] `ingredient_phrase` AND `ingredient_name_original` NO
#    LONGER COMPUTE IDENTICALLY. Per explicit clarification:
#    `ingredient_name_original` must preserve the ingredient noun
#    PHRASE AS WRITTEN even when it resolves (via a curated alias) to a
#    differently-named canonical ingredient -- e.g. a line written
#    "scallions" that resolves to canonical id "green onion" must still
#    display "scallions", not "green onion" and not the fuller
#    `ingredient_phrase` text. `_resolve_ingredient` now returns that
#    verbatim text (the specific `IngredientNode` leaf(ves) joined into
#    `core_term` before alias resolution, never the resolved id's own
#    name) as `reference["ingredient"]["original_text"]`, and
#    `_project_reference_to_row` uses it directly for
#    `ingredient_name_original`, falling back to the broader
#    `ingredient_phrase` value only for a reference with no ingredient
#    noun to point to (component-only or fully unresolved references,
#    where `reference["ingredient"]` is None) -- matching this column's
#    prior behavior for exactly that edge case rather than introducing a
#    new NULL where a value previously existed. `ingredient_phrase`
#    itself is unchanged: still the reference's full aggregate source
#    text minus notes, via `_derive_ingredient_phrase`.
#
# 5. `package.size` HAS NO DOWNSTREAM COLUMN -- AND IS ALSO RARELY EVEN
#    CONSTRUCTED IN THE FIRST PLACE. E.g. "2 cans (14-ounce) tomatoes" ->
#    `package.count`/`package_term` project cleanly into
#    `packaging_count`/`packaging`, but the "14-ounce" (the CONTENTS of
#    one can) has no target column in the DDL above, and is deliberately
#    NOT stuffed into `notes` (notes has a specific, narrower meaning
#    already -- "meta-instructions/context for the cook", not precise
#    quantity data). It is preserved only in `canonical_result_json`
#    (fully queryable there, per RO-8) and counted in the execution
#    report's CRITICAL ISSUES section so the gap is visible rather than
#    silently dropped.
#
#    CONFIRMED BY TESTING, not just reasoned about: the most common
#    real-world spelling of a can/package size -- a hyphenated
#    parenthetical like "(14-ounce)" -- triggers mismatch #1 (range
#    fragmentation) BEFORE `_build_package` ever gets a clean measurement
#    to consume as `size`. "14-ounce" lexes as Quantity(14) + RangeMarker
#    + Measurement(ounce), the exact dangling-range shape mismatch #1
#    describes; `_build_scalar_quantity` therefore returns None for it,
#    `_build_package`'s size-search leaves it unconsumed, and it falls
#    through to `_resolve_dangling_ranges` as an ordinary
#    `range_quantity_not_representable` unresolved entry instead of ever
#    becoming `package.size` at all. So for this common phrasing,
#    `package["size"]` is never even constructed upstream -- the
#    `unmapped_package_size` counter below only catches the rarer case
#    where `package.size` WAS constructed (e.g. a non-hyphenated "14
#    ounce"). The execution report's count of this issue is therefore a
#    floor, not the true frequency; the range-fragmentation unresolved
#    count already captures most real occurrences under a different
#    reason string.
#
# 6. "EXPLICITLY OPTIONAL" PHRASING IS NOT DETECTED. Item 8's first case
#    ("olive oil, if desired", "butter (optional)") has NO structural
#    signal anywhere in the parser/Analyzer data model -- a
#    `NotesExpression` carries recognized annotation TEXT only, with no
#    optionality sub-classification. Detecting it here would require a
#    hardcoded keyword match against note text ("optional", "if
#    desired", ...), which is exactly the kind of ad hoc,
#    knowledge-system-should-own-this classification the work order
#    itself warns against elsewhere. `optional` below is therefore set
#    ONLY from item 8's second, structural case (non-first member of an
#    "alternative" relation) -- never from note-text matching.
#
# 7. `alt_group_id` HAS NO ESTABLISHED PROJECT CONVENTION TO REPRODUCE.
#    SS9 says "the exact identifier-generation mechanism should follow
#    the project's existing conventions", but nothing inspected in this
#    codebase establishes one (no prior alt-group/relation-id scheme
#    exists anywhere in the schemas or code seen so far). This uses
#    `f"{selected_interpretation_id}:{relation_index}"` -- unique within a
#    line, deterministic/stable across re-runs of the same parse -- as a
#    reasonable default, not a discovered convention. Replace it if an
#    actual project convention turns out to exist.
# ---------------------------------------------------------------------------

_METRIC_WEIGHT_UNITS: Dict[str, float] = {
    "gram": 1.0, "grams": 1.0, "g": 1.0,
    "kilogram": 1000.0, "kilograms": 1000.0, "kg": 1000.0,
    "milligram": 0.001, "milligrams": 0.001, "mg": 0.001,
}
_METRIC_VOLUME_UNITS: Dict[str, float] = {
    "milliliter": 1.0, "milliliters": 1.0, "millilitre": 1.0, "millilitres": 1.0, "ml": 1.0,
    "liter": 1000.0, "liters": 1000.0, "litre": 1000.0, "litres": 1000.0, "l": 1000.0,
}
_IMPERIAL_WEIGHT_UNITS = {"ounce", "ounces", "oz", "pound", "pounds", "lb", "lbs"}
_IMPERIAL_VOLUME_UNITS = {
    "cup", "cups", "tablespoon", "tablespoons", "tbsp", "tbs",
    "teaspoon", "teaspoons", "tsp",
    "fluid ounce", "fluid ounces", "fluid_ounce", "fl oz", "fl_oz",
    "pint", "pints", "quart", "quarts", "gallon", "gallons",
}


def _route_measurement_quantity(quantity: dict) -> Dict[str, Any]:
    """Routes a resolved `quantity` (unit_type == "measurement") into the
    downstream table's grams/ml/imperial_weight_*/imperial_volume_*
    columns. Returns a dict with only the keys that apply; the caller
    merges it into the row. See FLAGGED GAPS #1/#2 above: no cross-system
    (imperial<->metric) or cross-dimension (weight<->volume) conversion is
    performed here, ever. An unclassified unit is left entirely unrouted
    -- the caller records this as a critical issue rather than guessing.
    """
    unit_term = (quantity.get("unit_term") or "").strip().lower()
    value = quantity.get("value")
    if value is None or not unit_term:
        return {}
    if unit_term in _METRIC_WEIGHT_UNITS:
        return {"grams": value * _METRIC_WEIGHT_UNITS[unit_term]}
    if unit_term in _METRIC_VOLUME_UNITS:
        return {"ml": value * _METRIC_VOLUME_UNITS[unit_term]}
    if unit_term in _IMPERIAL_WEIGHT_UNITS:
        return {"imperial_weight_value": value, "imperial_weight_unit": unit_term}
    if unit_term in _IMPERIAL_VOLUME_UNITS:
        return {"imperial_volume_value": value, "imperial_volume_unit": unit_term}
    return {}


def _format_quantity_diagnostic(quantity: Optional[dict]) -> Optional[str]:
    """Human-readable, diagnostic-only rendering of a quantity for
    inclusion in the `notes` column (used for `per_item_quantity`, per the
    work order's explicit "retain as notes" instruction for per-item
    information). This is string formatting for display only -- it is
    never parsed back out or fed into any typed numeric column, so it
    does not cross the "no measurement normalization" boundary above."""
    if not quantity:
        return None
    unit = quantity.get("unit_term", "") or ""
    if quantity.get("form") == "range":
        # Not currently constructible (mismatch #1), handled defensively
        # rather than assumed unreachable.
        return f"{quantity.get('lower')}-{quantity.get('upper')} {unit}".strip()
    return f"{quantity.get('value')} {unit}".strip()


def _derive_ingredient_phrase(reference: dict) -> Optional[str]:
    """`ingredient_phrase` is NOT `ingredient_id` and is NOT an
    id-derived display string -- per explicit correction: it is "the
    analyzer's ingredient input set of spans that led to the ingredient
    it selected, like the alias, prep words, and substance words that
    were in the attached parse sub-section." Concretely: the reference's
    own verbatim source text (aliases as actually written, preparation
    words, substance/unresolved words -- everything that was available as
    input), NOT a narrowed "identity-only" phrase, and NOT the canonical
    resolved id.

    A prior version of this function used only size/descriptor/state/
    temperature modifiers plus the canonical id turned into display text
    -- that was wrong on two counts, confirmed by concrete failing
    examples: (1) for a reference with no ingredient noun at all (e.g.
    "1 clove", where the parser attaches nothing to `ingredient`), it
    returned None instead of the expected "1 clove" -- the quantity/unit
    material IS legitimate input even when it never yields an ingredient
    id; (2) it dropped unresolved/substance material (e.g. "xyz powder")
    that should be preserved as input evidence even though resolution
    failed on it.

    Implementation: `reference["source_spans"]` (the reference's full
    aggregate provenance -- measurements + package + ingredient +
    component + preparation + notes + unresolved, per
    `_reference_source_spans`) MINUS whatever spans came specifically
    from `reference["notes"]`. Notes are the one category explicitly
    excluded ("everything that isn't clearly a note", per the same
    clarification) -- e.g. "to taste" must not appear here. Subtraction
    is done by matching each note's own `source_spans` text against the
    aggregate list and removing at most that many occurrences (order-
    preserving), using only data already present in the canonical
    schema -- no parser_ref access needed, and no risk of over-removing
    a coincidentally-identical word that appears elsewhere for an
    unrelated reason, beyond the same count actually attributed to notes.

    `ingredient_name_original`, unlike this function's return value, is
    NOT computed here -- see `_project_reference_to_row`, which uses
    `reference["ingredient"]["original_text"]` (the verbatim ingredient-
    noun text that specifically triggered `ingredient_id` resolution, set
    by `_resolve_ingredient`) instead, falling back to this function's
    broader phrase only when there's no ingredient noun to point to at
    all (see FLAGGED GAP #4, resolved below).
    """
    all_spans = reference.get("source_spans") or []
    if not all_spans:
        return None

    note_span_counts: "Counter[str]" = Counter()
    for note in reference.get("notes", []) or []:
        for span in note.get("source_spans", []) or []:
            note_span_counts[span] += 1

    kept: List[str] = []
    for span in all_spans:
        if note_span_counts.get(span, 0) > 0:
            note_span_counts[span] -= 1
            continue
        kept.append(span)

    return " ".join(kept).strip() or None


def _project_reference_to_row(
    reference: dict,
    *,
    optional: int,
    alt_group_id: Optional[str],
    alt_kind: Optional[str],
    unmapped_units: List[str],
    unmapped_package_size: List[str],
) -> Dict[str, Any]:
    """Projects one already-fully-resolved canonical `reference` into one
    `recipe_ingredient_lines_parsed` row. Performs no new semantic
    interpretation -- every value here was already decided by the
    Analyzer's construction logic above; this only reshapes it into the
    flat downstream column set. See the FLAGGED GAPS block above for every
    place this projection cannot faithfully preserve something the
    canonical result carries.
    """
    ingredient = reference.get("ingredient")
    ingredient_id = ingredient["id"] if ingredient else None

    phrase_text = _derive_ingredient_phrase(reference)
    # See FLAGGED GAP #4 (resolved): the original as-written ingredient
    # noun phrase, preserved through alias resolution -- "scallions" even
    # when `ingredient_id` resolved to canonical "green onion" -- not the
    # broader `ingredient_phrase` text and never the resolved id's own
    # name. Falls back to `phrase_text` only when there's no ingredient
    # noun at all to point to (component-only/fully-unresolved
    # references), matching this column's prior behavior for that case.
    original_name = ingredient.get("original_text") if ingredient else None
    name_original = original_name if original_name is not None else phrase_text

    row: Dict[str, Any] = {
        "ingredient_id": ingredient_id,
        "ingredient_phrase": phrase_text,
        "ingredient_name_original": name_original,
        "grams": None,
        "ml": None,
        "imperial_weight_value": None,
        "imperial_weight_unit": None,
        "imperial_volume_value": None,
        "imperial_volume_unit": None,
        "natural_portion_value": None,
        "natural_portion_min": None,
        "natural_portion_max": None,
        "natural_portion": None,
        "packaging_count": None,
        "packaging_size_value": None,
        "packaging_size_unit": None,
        "packaging": None,
        "preparation": None,
        "size": None,
        "notes": None,
        "optional": optional,
        "alt_group_id": alt_group_id,
        "alt_kind": alt_kind,
    }

    quantity = reference.get("quantity")
    if quantity:
        if quantity.get("form") == "range":
            if quantity.get("unit_type") == "natural_portion":
                row["natural_portion_min"] = quantity.get("lower")
                row["natural_portion_max"] = quantity.get("upper")
                row["natural_portion"] = quantity.get("unit_term")
            elif quantity.get("unit_type") == "measurement":
                # No imperial_weight_min/max, grams_min/max, etc. exist in
                # the current schema for a RANGED measurement -- only
                # natural_portion has min/max columns. Flagged, not
                # silently dropped or collapsed into a single fabricated
                # value.
                unmapped_units.append(
                    f"{quantity.get('lower')}-{quantity.get('upper')} "
                    f"{quantity.get('unit_term')} (range measurement, no downstream column)"
                )
        elif quantity.get("unit_type") == "natural_portion":
            row["natural_portion_value"] = quantity.get("value")
            row["natural_portion"] = quantity.get("unit_term")
        elif quantity.get("unit_type") == "measurement":
            routed = _route_measurement_quantity(quantity)
            if not routed:
                unmapped_units.append(quantity.get("unit_term") or "<empty>")
            row.update(routed)

    package = reference.get("package")
    if package:
        row["packaging_count"] = package.get("count")
        row["packaging"] = package.get("package_term")
        size = package.get("size")
        if size:
            if size.get("form") == "scalar":
                row["packaging_size_value"] = size.get("value")
                row["packaging_size_unit"] = size.get("unit_term")
            else:
                # Ranged package size (e.g. "2 (14-16 oz) cans") -- no
                # packaging_size_min/max columns exist in the current
                # schema either.
                unmapped_package_size.append(
                    f"{size.get('lower')}-{size.get('upper')} "
                    f"{size.get('unit_term')} (range package size, no downstream column)"
                )

    preparation_terms = [
        modifier["term"]
        for modifier in reference.get("modifiers", [])
        if modifier.get("modifier_class") == "preparation"
    ]
    if preparation_terms:
        # One entry per clause, in source order -- see the (formerly
        # FLAGGED GAP #3, now resolved-pending-verification) note above
        # _build_preparation_modifiers. This list-collection logic itself
        # needed no change: it was already written to handle N clauses,
        # it simply never received more than one before the parser fix.
        row["preparation"] = json.dumps(preparation_terms)

    # `size` (RO-... new downstream column): SizeNode modifiers currently
    # only ever attach to the ingredient's own IngredientExpression (see
    # `_INGREDIENT_MODIFIER_CLASS_BY_NODE_TYPE`/`_resolve_ingredient`) --
    # "2 large eggs", "1 medium garlic clove" -- so that's the one place
    # this can be populated from with real evidence right now. NOTE: a
    # `SizeNode` sitting inside a `PackageExpression` instead (a
    # hypothetical "2 large cans tomatoes", size describing the can, not
    # the ingredient) would currently be silently dropped by
    # `_build_package`, which only ever looks for a `PackagingNode` among
    # its children -- confirmed by reading that function, not by seeing
    # this shape in real data. Left unhandled rather than guessed at
    # until a real example turns up (per your own "we'll see what we
    # get" -- this collects whichever shape the corpus actually
    # produces, it doesn't invent handling for shapes it hasn't seen).
    size_terms = [
        modifier["term"]
        for modifier in reference.get("modifiers", [])
        if modifier.get("modifier_class") == "size"
    ]
    if size_terms:
        row["size"] = json.dumps(size_terms) if len(size_terms) > 1 else size_terms[0]

    note_fragments = [note["text"] for note in reference.get("notes", [])]
    per_item = reference.get("per_item_quantity")
    per_item_marker = per_item.get("per_item_marker") if per_item else None
    if (
        per_item
        and quantity
        and quantity.get("unit_type") == "measurement"
        and per_item.get("unit_type") == "natural_portion"
    ):
        # CONFIRMED FIX from a real test case: "4 tablespoons butter
        # (1/2 stick)". The primary reading here has no discrete countable
        # item (a plain measurement, "tablespoons") -- so the parenthetical
        # isn't scoping "each of N items" the way "6 chicken breasts
        # (5-6 oz each)" does; it's an ALTERNATE unit expression of the
        # SAME total amount ("4 tbsp" and "1/2 stick" both describe the
        # same quantity of butter). Route it into natural_portion_* rather
        # than `notes`. Genuine per-item scoping -- primary quantity IS
        # itself a natural-portion count, e.g. "6 chicken breasts" -- is
        # deliberately left as diagnostic-only `notes` text, UNCHANGED,
        # per the original explicit "do not build a separate per-item
        # calculation dimension" instruction; only this different,
        # narrower shape (primary=measurement, per-item=natural_portion)
        # is rerouted.
        if per_item.get("form") == "range":
            row["natural_portion_min"] = per_item.get("lower")
            row["natural_portion_max"] = per_item.get("upper")
        else:
            row["natural_portion_value"] = per_item.get("value")
        row["natural_portion"] = per_item.get("unit_term")
    elif (
        per_item
        and quantity
        and quantity.get("unit_type") == "measurement"
        and per_item.get("unit_type") == "measurement"
        and not per_item_marker
    ):
        # GENERALIZATION of the case just above, same reasoning: no
        # per-item marker ("each"/etc -- see `_flatten_measurement_slots`)
        # means this parenthetical isn't scoping a per-unit value at all,
        # it's an ALTERNATE-UNIT expression of the SAME total quantity --
        # "2 1/2 pounds (1.15 kg)", "1 tablespoon (15 ml)". Route it
        # exactly like a primary quantity would be: whichever of the two
        # is metric lands in grams/ml, whichever is imperial lands in
        # imperial_*, regardless of which one the parser attached as
        # primary vs. parenthetical. This is the fix for metric
        # conversions being silently lost whenever they appear SECOND
        # rather than first -- they must always be extracted.
        routed = _route_measurement_quantity(per_item)
        if not routed:
            unmapped_units.append(per_item.get("unit_term") or "<empty>")
        row.update(routed)
    elif per_item:
        # Genuine per-item scoping (a marker like "each" is present, e.g.
        # "4 boneless skinless chicken breasts (5 to 6 ounces each)"), or
        # a shape with no established construction rule -- left as
        # diagnostic-only `notes` text, unchanged from before, using the
        # marker's own captured text rather than a hardcoded "each" when
        # one was actually found.
        per_item_text = _format_quantity_diagnostic(per_item)
        if per_item_text:
            marker_text = " ".join(per_item_marker) if per_item_marker else "each"
            note_fragments.append(f"{per_item_text} {marker_text}")

    if note_fragments:
        row["notes"] = "; ".join(note_fragments)

    return row


def _project_selected_references(
    result: dict,
) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    """Projects the SELECTED interpretation's references into downstream
    rows. Returns (rows, unmapped_units, unmapped_package_size).

    Item 12, REVISED (RO-9 SS M revision): a line whose top-level status
    is "ambiguous" no longer *always* has `selected_interpretation is
    None`. There are now three distinct "ambiguous" cases:
      1. A single viable interpretation whose own status is "ambiguous"
         per SS I.4's compound-scope case -> `selected_interpretation`
         is still None (unchanged).
      2. Multiple viable interpretations where none is genuinely tied on
         evidence-weighted `score` -> `_derive_result` now resolves this
         outright to a clear winner, so top-level status is "resolved"
         or "unresolved", not "ambiguous" at all (this used to always be
         "ambiguous"; see `_derive_result`'s docstring).
      3. Multiple viable interpretations that remain genuinely tied even
         after evidence weighting -> top-level status stays "ambiguous",
         but `selected_interpretation` IS populated (one of the tied
         candidates, chosen for determinism) so this line still produces
         a downstream row -- flagged ambiguous for curation triage
         rather than silently dropped.
    This function's own logic doesn't need to distinguish these cases --
    it still just checks whether `selected_interpretation` is truthy,
    which is exactly the right gate for all three. Status "invalid"
    (zero viable interpretations) likewise never has a selected
    interpretation, so it is handled by the same check.
    """
    selected_id = result.get("selected_interpretation")
    if not selected_id:
        return [], [], []

    interpretation = next(
        (i for i in result["interpretations"] if i["id"] == selected_id), None
    )
    if interpretation is None:
        return [], [], []

    references = interpretation.get("references", [])
    relations = interpretation.get("relations", [])

    # reference id -> (alt_group_id, alt_kind, is_first_member)
    membership: Dict[str, Tuple[str, str, bool]] = {}
    for relation_index, relation in enumerate(relations):
        members = relation.get("members") or []
        if not members:
            continue
        group_id = f"{selected_id}:{relation_index}"
        kind = relation.get("relation_type")
        for position, ref_id in enumerate(members):
            if ref_id in membership:
                # A reference can belong to more than one relation in a
                # nested N-ary compound (SS I.7's flatten-to-leaves
                # resolution puts every leaf in the outer relation's
                # members too). First relation encountered wins for this
                # flat row; the full nested structure remains available
                # in canonical_result_json.
                continue
            membership[ref_id] = (group_id, kind, position == 0)

    rows: List[Dict[str, Any]] = []
    unmapped_units: List[str] = []
    unmapped_package_size: List[str] = []
    for reference in references:
        alt_group_id: Optional[str] = None
        alt_kind: Optional[str] = None
        optional = 0
        info = membership.get(reference["id"])
        if info is not None:
            alt_group_id, alt_kind, is_first = info
            if alt_kind == "alternative" and not is_first:
                optional = 1
        # FLAGGED GAP #6 -- "explicitly optional" phrasing is not
        # detected; optional is set ONLY from the structural alternative
        # case above.
        row = _project_reference_to_row(
            reference,
            optional=optional,
            alt_group_id=alt_group_id,
            alt_kind=alt_kind,
            unmapped_units=unmapped_units,
            unmapped_package_size=unmapped_package_size,
        )
        rows.append(row)

    return rows, unmapped_units, unmapped_package_size