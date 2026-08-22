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

3. CONNECTIVE SPAN LOSS (affects the schema's
   `relation.source_spans` -- "Lexical provenance for the connective ...
   e.g. the span(s) covering 'and'/'or'"). Empirically,
   `_classify_conjunction_groups` and the whole-reference merge step in
   `_build_references` both build `AlternativeExpression`/
   `ConjunctionExpression` as `children=[left, right]` -- the
   `AlternativeMarker`/`ConjunctionMarker` leaf itself is discarded, never
   added to `children`, and never routed to `unresolved` either. Its
   source span is gone by the time the tree is serialized; there is no
   place left in the persisted tree to recover it. Handling: the literal
   connective text is unrecoverable and this Analyzer does NOT invent a
   substitute for it -- in particular it does not reuse the members' own
   source spans as stand-in provenance, since that would misattribute
   lexical material that produced the *members* as if it were provenance
   for the *connective* itself. `_relation_source_spans` instead returns a
   fixed, unmistakable sentinel string so the gap is surfaced rather than
   disguised. This is an open parser-level gap requiring a parser fix
   (retain the marker's span somewhere in the tree) or an RO-9/schema
   revision -- not something this Analyzer can correctly resolve on its
   own.

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
the parser AST. It DOES now persist both outputs (`persist_all_lines`) --
this was RO-8/RO-10's explicit responsibility once the persistence schema
was confirmed; it is no longer a functional read-only boundary the way
`analyze_all_lines`/`analyze_parse_result` remain.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

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
# Confidence (RO-9 SS M: fixed three-... four-value placeholder table)
# ---------------------------------------------------------------------------

_SCORE_BY_STATUS: Dict[str, float] = {
    "resolved": 1.0,
    "ambiguous": 0.5,
    "unresolved": 0.5,
    "invalid": 0.0,
}


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
) -> Tuple[Optional[str], List[dict]]:
    """Resolves one (non-compound) IngredientExpression per SS A, and
    extracts its size/descriptor/state/temperature children as `modifier`
    objects per SS G (see mismatch #2/4 above for why `applies_to` is
    always "ingredient" here).

    Returns (ingredient_id_or_None, modifiers).
    """
    if ingredient_expr is None:
        return None, []

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
        return None, modifiers

    core_term = " ".join(_span_norm(leaf) for leaf in core_leaves).strip()
    core_spans = [_span_text(leaf) for leaf in core_leaves]

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

    return ingredient_id, modifiers


# ---------------------------------------------------------------------------
# Preparation -- one modifier, RO-9 SS G ("preparation" row: applies_to=ingredient)
# ---------------------------------------------------------------------------


def _build_preparation_modifier(
    prep_node: Optional[dict],
    evidence: List[dict],
    unresolved_out: List[dict],
) -> Optional[dict]:
    if prep_node is None:
        return None

    prep_leaves: List[dict] = []
    for child in _children(prep_node):
        if _node_type(child) == "PreparationNode":
            prep_leaves.append(child)
        else:
            # UnknownNode can land inside a PreparationExpression (parser
            # keeps unrecognized vocabulary in its preparation context
            # rather than ejecting it) -- preserved, not discarded.
            spans = [_span_text(child)] if _is_leaf(child) else _source_spans_of(child)
            unresolved_out.append(
                {
                    "text": " ".join(spans),
                    "reason": "unrecognized_span",
                    "source_spans": _fallback_spans(spans, "<unrecognized preparation-position span>"),
                }
            )
            evidence.append(_evidence("unresolved_material", " ".join(spans) or "unknown"))

    if not prep_leaves:
        return None

    term = " ".join(_span_norm(leaf) for leaf in prep_leaves).strip()
    spans = [_span_text(leaf) for leaf in prep_leaves]
    evidence.append(_evidence("vocabulary_match", term))
    return {
        "modifier_class": "preparation",
        "term": term,
        "applies_to": "ingredient",
        "source_spans": spans,
    }


# ---------------------------------------------------------------------------
# Component / natural-portion -- RO-9 SS B, SS C
# ---------------------------------------------------------------------------


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
        relationships = knowledge.find_relationships(
            subject_type="vocabulary",
            subject_id=term,
            predicate="component_of",
            object_type="ingredient",
            object_id=ingredient_id,
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
    relationships = knowledge.find_relationships(
        subject_type="vocabulary",
        subject_id=unit_term,
        predicate="natural_portion_of",
        object_type="ingredient",
        object_id=ingredient_id,
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


def _flatten_measurement_slots(measurements: List[dict]) -> List[dict]:
    """Normalizes `reference.measurements` (a mix of MeasurementExpression /
    ParentheticalExpression / Alternative-or-ConjunctionExpression-of-
    measurements, in source order) into a flat, ordered list of slot dicts:
        {"container": <ParentheticalExpression dict or None>,
         "expr": <MeasurementExpression dict or None>,
         "dangling": bool,
         "unrecognized": bool (optional),
         "raw": <original node, for unrecognized slots>}
    """
    slots: List[dict] = []
    for node in measurements:
        node_type = _node_type(node)
        if node_type == "MeasurementExpression":
            _, _, dangling = _measurement_expr_shape(node)
            slots.append({"container": None, "expr": node, "dangling": dangling})
        elif node_type == "ParentheticalExpression":
            inner = [c for c in _children(node) if _node_type(c) == "MeasurementExpression"]
            if len(inner) == 1:
                _, _, dangling = _measurement_expr_shape(inner[0])
                slots.append({"container": node, "expr": inner[0], "dangling": dangling})
            elif len(inner) >= 2:
                # Fragmented range inside a parenthetical (mismatch #1) --
                # e.g. "(5-6 ounces each)". Not modeled by SS D at all.
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

    quantity, reason = _build_scalar_quantity(
        primary_slot["expr"],
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
            per_item_quantity, reason2 = _build_scalar_quantity(second_slot["expr"], knowledge)
            if per_item_quantity is not None:
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
            _, unit_leaf, _ = _measurement_expr_shape(slot["expr"])
            if unit_leaf is not None:
                built, _reason = _build_scalar_quantity(slot["expr"], knowledge)
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
    spans: List[str] = []
    seen = set()

    def add_from(node: Optional[dict]) -> None:
        for leaf in _iter_leaves(node):
            key = (leaf["span"]["start_offset"], leaf["span"]["end_offset"])
            if key not in seen:
                seen.add(key)
                spans.append(_span_text(leaf))

    for measurement in parser_ref.get("measurements", []) or []:
        add_from(measurement)
    add_from(parser_ref.get("package"))
    add_from(parser_ref.get("ingredient"))
    add_from(parser_ref.get("component"))
    add_from(parser_ref.get("preparation"))
    for note in parser_ref.get("notes", []) or []:
        add_from(note)
    for unresolved in parser_ref.get("unresolved", []) or []:
        add_from(unresolved)

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

    ingredient_id, own_modifiers = _resolve_ingredient(
        parser_ref.get("ingredient"), knowledge, evidence, unresolved
    )
    modifiers: List[dict] = list(own_modifiers)

    prep_modifier = _build_preparation_modifier(parser_ref.get("preparation"), evidence, unresolved)
    if prep_modifier:
        modifiers.append(prep_modifier)

    component_term = _resolve_component(
        parser_ref.get("component"), ingredient_id, knowledge, evidence, unresolved
    )

    measurement_slots = _flatten_measurement_slots(parser_ref.get("measurements", []) or [])

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

    notes = _build_notes(parser_ref.get("notes", []) or [], evidence)
    _carry_through_unresolved(parser_ref.get("unresolved", []) or [], unresolved, evidence)

    reference: Dict[str, Any] = {
        "id": ref_id,
        "ingredient": {"id": ingredient_id} if ingredient_id is not None else None,
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
    ("leaf", IngredientExpression) or (node_type, left_subtree, right_subtree).
    """
    if _node_type(node) == "IngredientExpression":
        return ("leaf", node)
    children = _children(node)
    return (_node_type(node), _compound_tree(children[0]), _compound_tree(children[1]))


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


_CONNECTIVE_SPAN_UNAVAILABLE = (
    "<connective span not preserved by parser -- see analyzer.py module "
    "docstring, mismatch #3: AlternativeMarker/ConjunctionMarker leaves are "
    "discarded by _classify_conjunction_groups and by the whole-reference "
    "merge in IngredientParser._build_references, so the literal 'and'/'or' "
    "token has no surviving span in the persisted parse tree>"
)


def _relation_source_spans() -> List[str]:
    """See mismatch #3 in the module docstring: the parser discards the
    'and'/'or' token itself when building an Alternative/ConjunctionExpression,
    so the literal connective span is unrecoverable from the persisted tree.

    This deliberately does NOT substitute the members' own source spans as
    stand-in provenance -- doing so would misattribute lexical material
    that produced the *members* as if it were provenance for the
    *connective*, which is exactly the invented transformation the work
    order prohibits. Instead the gap is surfaced explicitly via a fixed,
    unmistakable sentinel; this needs a parser fix (retain the marker span
    somewhere in the tree) or an RO-9 schema revision, not an Analyzer-side
    guess."""
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
    shared_prep_modifier = _build_preparation_modifier(parser_ref.get("preparation"), evidence, prep_unresolved)
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
            ingredient_id, own_modifiers = _resolve_ingredient(ingredient_expr, knowledge, evidence, own_unresolved)

            modifiers = list(own_modifiers)
            if shared_prep_modifier:
                modifiers.append(dict(shared_prep_modifier))

            reference: Dict[str, Any] = {
                "id": ref_id,
                "ingredient": {"id": ingredient_id} if ingredient_id is not None else None,
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

        node_type, left, right = subtree
        left_ids = visit(left)
        right_ids = visit(right)
        member_ids = left_ids + right_ids
        relations.append(
            {
                "relation_type": _relation_type_for(node_type),
                "members": member_ids,
                "source_spans": _relation_source_spans(),
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
                relations.append(
                    {
                        "relation_type": _relation_type_for(child_type),
                        "members": member_ids,
                        "source_spans": _relation_source_spans(),
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
        "score": _SCORE_BY_STATUS[status],
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
    """
    viable = [interp for interp in interpretations if interp["status"] != "invalid"]
    viable_count = len(viable)

    if viable_count == 0:
        status = "invalid"
    elif viable_count == 1:
        status = viable[0]["status"]
    else:
        status = "ambiguous"

    selected = None
    if viable_count == 1 and status in ("resolved", "unresolved"):
        selected = viable[0]["id"]
    return status, selected


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


# ---------------------------------------------------------------------------
# DB reader + orchestration
#
# `ingredient_parse_trees` had no existing reader prior to this module. This
# reads it and evaluates each row; per the RO-10 work order boundary, it
# does NOT write anything -- RO-8 (not yet implemented) owns the analysis
# persistence artifact, and if RO-8 assigns write responsibility to the
# Analyzer, a thin caller can iterate this generator and persist each
# result without that write path living inside evaluation logic here.
# ---------------------------------------------------------------------------


def _read_parse_trees(
    conn: sqlite3.Connection, line_ids: Optional[Iterable[int]] = None
) -> Iterable[sqlite3.Row]:
    cursor = conn.cursor()
    if line_ids is not None:
        line_ids = list(line_ids)
        placeholders = ",".join("?" for _ in line_ids)
        cursor.execute(
            f"""
            SELECT id, recipe_ingredient_line_id, parse_tree_json
            FROM ingredient_parse_trees
            WHERE recipe_ingredient_line_id IN ({placeholders})
            ORDER BY recipe_ingredient_line_id, id
            """,
            line_ids,
        )
    else:
        cursor.execute(
            """
            SELECT id, recipe_ingredient_line_id, parse_tree_json
            FROM ingredient_parse_trees
            ORDER BY recipe_ingredient_line_id, id
            """
        )
    return cursor.fetchall()


def analyze_all_lines(
    db_path: Optional[Any] = None, line_ids: Optional[Iterable[int]] = None
) -> Iterable[Tuple[int, int, dict]]:
    """Reads persisted parse trees from `ingredient_parse_trees` and yields
    `(recipe_ingredient_line_id, parse_tree_id, canonical_semantic_result)`
    triples. `parse_tree_id` is the id of the exact `ingredient_parse_trees`
    row the result was produced from, so a persistence layer can populate
    `analysis_records.parse_tree_id` without a second lookup. Read-only:
    see module docstring above.

    `line_ids=None` (default) reads every persisted line -- the current
    whole-database debugging convenience (RO-10 SS15). Passing an explicit
    iterable of `recipe_ingredient_line_id` values restricts the read to
    just those lines, without changing anything else about this function's
    behavior -- this is the "clean invocation boundary" SS15 asks for, so
    a future targeted-pipeline caller can reuse this unchanged.
    """
    from gastrometric.config.paths import DB_PATH
    from gastrometric.knowledge.loader import knowledge as runtime_knowledge

    path = str(db_path or DB_PATH)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        for row in _read_parse_trees(conn, line_ids):
            parse_result = json.loads(row["parse_tree_json"])
            result = analyze_parse_result(parse_result, runtime_knowledge)
            yield row["recipe_ingredient_line_id"], row["id"], result
    finally:
        conn.close()


_STATUS_ORDER = ["resolved", "ambiguous", "unresolved", "invalid"]

# Every `unresolved[].reason` string this Analyzer can currently emit,
# verified by direct inspection of the source (grep '"reason":'), not
# guessed or restated from an external description. Kept here purely as
# documentation -- the report below tallies whatever actually appears via
# a Counter, so this list is a reading aid, not a hardcoded bucket set,
# and a reason absent from this list would still show up correctly if the
# analyzer ever grew one.
#
#   unrecognized_span                    -- _resolve_ingredient,
#                                            _build_preparation_modifier,
#                                            _resolve_component,
#                                            _carry_through_unresolved
#   unknown_ingredient                   -- _resolve_ingredient
#   unrecognized_measurement_structure   -- _resolve_dangling_ranges
#   range_quantity_not_representable     -- _resolve_dangling_ranges (mismatch #1)
#   dangling_range                       -- _build_scalar_quantity (mismatch #1)
#   no_quantity_value                    -- _build_scalar_quantity
#   unparseable_quantity_value           -- _build_scalar_quantity
#   missing_unit                         -- _build_scalar_quantity
#   unparseable_quantity                 -- _assign_quantities' fallback;
#                                            currently unreachable, since
#                                            _build_scalar_quantity always
#                                            returns one of the four
#                                            specific reasons above rather
#                                            than None -- kept as a
#                                            defensive catch-all, not dead
#                                            code to be relied on staying
#                                            empty if that contract changes.
#   additional_measurement_unsupported   -- _assign_quantities
#   package_count_or_term_not_determined -- _build_package
#   no_references_produced               -- _evaluate_candidate (empty candidate)
#   no_parser_candidates                 -- analyze_parse_result (empty ParseResult)


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
#       recipe_ingredient_line_id INTEGER NOT NULL,
#       ingredient_block_id       INTEGER NOT NULL,
#       recipe_id                 INTEGER NOT NULL,
#       recipe_section_id         INTEGER NOT NULL,
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
#       natural_portion TEXT,
#
#       packaging_count REAL,
#       packaging TEXT,
#
#       preparation TEXT,   -- JSON array (see FLAGGED GAP #3 below)
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
# 3. PREPARATION LIST GRANULARITY IS NOT VERIFIABLE FROM CURRENT INPUTS.
#    The work order's own example wants
#    `["peeled", "quartered lengthwise", "cut crosswise into
#    0.25-inch-thick slices"]` -- three DISCRETE technique phrases.
#    `_build_preparation_modifier` (upstream, unchanged) currently merges
#    every `PreparationNode` leaf in one `PreparationExpression` into a
#    SINGLE joined modifier term (per the parser's 7a fragmented-phrase
#    merge rule, the same mechanism that merges "boneless, skinless
#    chicken breasts" into one ingredient phrase). Whether "cut crosswise
#    into 0.25-inch-thick slices" arrives as ONE lexical span (a
#    multi-word vocabulary phrase match) or as several separate
#    PreparationNode leaves that get merged word-by-word is a property of
#    the seed vocabulary's phrase-matching granularity, which this module
#    has no visibility into and cannot verify. Splitting on comma
#    positions in the original source text is not implementable either --
#    the Analyzer's input contract (a serialized `ParseResult`) never
#    includes the original raw line text, only tokenized/lexed spans, so
#    there is no string to scan for comma boundaries even if that were
#    otherwise a safe heuristic (it is not, in general: "cut crosswise
#    into 0.25-inch-thick slices" contains no comma at all). Given this,
#    `preparation` below is a JSON list wrapper around whatever single
#    joined term the Analyzer already produced -- currently always 0 or 1
#    elements, never the 3-element list the work order's example shows.
#    Do not assume multi-technique lists work until this is verified
#    against real seed-vocabulary output.
#
# 4. `ingredient_phrase` AND `ingredient_name_original` CURRENTLY COMPUTE
#    IDENTICALLY. The work order describes them with materially
#    overlapping language ("source phrase/evidence used to resolve
#    ingredient_id... may include size/descriptor/component/..." vs.
#    "verbatim source-language representation... original casing,
#    wording, hyphenation, spelling") but gives no computation that would
#    make them differ given what a `reference` actually carries. Both are
#    implemented here as the raw (unnormalized) source text of the
#    reference's own `source_spans` -- the only "verbatim identity-bearing
#    phrase" text this data model actually has. If these are meant to
#    differ (e.g. `ingredient_phrase` should also fold in `component`
#    text), that needs to be specified; nothing here invents a difference
#    to make the two columns look distinct.
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

    # ingredient_phrase / ingredient_name_original: see FLAGGED GAP #4 --
    # both derived from the same underlying source-span text, since the
    # Analyzer's data model has no separate "verbatim identity phrase"
    # distinct from the reference's own source_spans (which already
    # exclude preparation/notes/unresolved material, since those are
    # separate fields -- see _reference_source_spans).
    spans = reference.get("source_spans") or []
    phrase_text = " ".join(spans) if spans else None

    row: Dict[str, Any] = {
        "ingredient_id": ingredient_id,
        "ingredient_phrase": phrase_text,
        "ingredient_name_original": phrase_text,
        "grams": None,
        "ml": None,
        "imperial_weight_value": None,
        "imperial_weight_unit": None,
        "imperial_volume_value": None,
        "imperial_volume_unit": None,
        "natural_portion_value": None,
        "natural_portion": None,
        "packaging_count": None,
        "packaging": None,
        "preparation": None,
        "notes": None,
        "optional": optional,
        "alt_group_id": alt_group_id,
        "alt_kind": alt_kind,
    }

    quantity = reference.get("quantity")
    if quantity:
        if quantity.get("unit_type") == "natural_portion":
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
        if package.get("size"):
            # FLAGGED GAP #5 -- no downstream column for package.size.
            size = package["size"]
            unmapped_package_size.append(f"{size.get('value')} {size.get('unit_term')}")

    preparation_terms = [
        modifier["term"]
        for modifier in reference.get("modifiers", [])
        if modifier.get("modifier_class") == "preparation"
    ]
    if preparation_terms:
        # FLAGGED GAP #3 -- currently always 0 or 1 entries; a list
        # wrapper around whatever the Analyzer already produced, not a
        # guess at discrete technique boundaries.
        row["preparation"] = json.dumps(preparation_terms)

    note_fragments = [note["text"] for note in reference.get("notes", [])]
    per_item_text = _format_quantity_diagnostic(reference.get("per_item_quantity"))
    if per_item_text:
        # Per-item quantities are deliberately NOT a separate calculation
        # dimension in this downstream schema (work order's explicit
        # instruction) -- retained as diagnostic text only.
        note_fragments.append(f"{per_item_text} each")
    if note_fragments:
        row["notes"] = "; ".join(note_fragments)

    return row


def _project_selected_references(
    result: dict,
) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    """Projects the SELECTED interpretation's references into downstream
    rows. Returns (rows, unmapped_units, unmapped_package_size).

    Item 12: a line whose top-level status is "ambiguous" -- either
    multiple viable interpretations, or a single viable interpretation
    whose own status is "ambiguous" per SS I.4's compound-scope case --
    has `selected_interpretation is None` by construction (`_derive_result`
    only ever populates it for resolved/unresolved). This relies on
    exactly that signal rather than re-deriving ambiguity: no
    `selected_interpretation` means no rows, full stop -- never a "best
    guess" projection of one candidate's references. Status "invalid"
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


# ---------------------------------------------------------------------------
# Persistence -- both RO-10 outputs, one transaction per run
# ---------------------------------------------------------------------------


def _fetch_line_lineage(conn: sqlite3.Connection) -> Dict[int, Tuple[int, int, int]]:
    """Bulk-fetches (ingredient_block_id, recipe_id, recipe_section_id) for
    every `recipe_ingredient_lines_raw` row, keyed by its id -- one query
    for the whole run rather than one per line. Reproduces the lineage
    dimensions already denormalized directly onto that table (see the
    target-DDL comment above), rather than inventing a new lineage model
    (work order SS1)."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, ingredient_block_id, recipe_id, recipe_section_id
        FROM recipe_ingredient_lines_raw
        """
    )
    return {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}


def _persist_analysis_record(
    conn: sqlite3.Connection,
    recipe_ingredient_line_id: int,
    parse_tree_id: int,
    result: dict,
) -> int:
    """Writes the diagnostic artifact (RO-8): one canonical semantic result
    plus its evaluation/evidence projections. `result` is inserted into
    `canonical_result_json` unmodified -- the complete, schema-conforming
    return value of `analyze_parse_result()`, never narrowed. The rows
    below are query projections derived from it, not a second source of
    truth. Caller owns the transaction. Returns the new
    `analysis_records.id`.

    Assumes init_db.py's DDL for `analysis_candidate_evaluations` has:
      - `interpretation_id TEXT NOT NULL` (NOT `candidate_id` -- there is
        no candidate identity independent of `interpretation.id`).
      - `evaluation_state`'s CHECK allowing exactly
        {'resolved','ambiguous','unresolved','invalid'}, identical to
        `interpretation.status` -- inserted verbatim below, no
        translation.
    Both confirmed in prior review; this will raise a sqlite3
    IntegrityError/OperationalError if that DDL hasn't actually been
    applied yet.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO analysis_records
            (recipe_ingredient_line_id, parse_tree_id, status,
             selected_interpretation_id, canonical_result_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            recipe_ingredient_line_id,
            parse_tree_id,
            result["status"],
            result.get("selected_interpretation"),
            json.dumps(result),
        ),
    )
    analysis_record_id = cursor.lastrowid
    # cursor.lastrowid is typed Optional[int] (None only when the last
    # statement wasn't an INSERT, or no row was inserted); immediately
    # after the INSERT above on an AUTOINCREMENT table, it is always a
    # real int.
    assert analysis_record_id is not None
    for interpretation in result["interpretations"]:
        cursor.execute(
            """
            INSERT INTO analysis_candidate_evaluations
                (analysis_record_id, interpretation_id, evaluation_state)
            VALUES (?, ?, ?)
            """,
            (analysis_record_id, interpretation["id"], interpretation["status"]),
        )
        evaluation_id = cursor.lastrowid
        for evidence in interpretation.get("evidence", []):
            cursor.execute(
                """
                INSERT INTO analysis_evidence
                    (analysis_candidate_evaluation_id, kind, record_id, effect)
                VALUES (?, ?, ?, ?)
                """,
                (evaluation_id, evidence["kind"], evidence["record_id"], evidence["effect"]),
            )
    return analysis_record_id


def _persist_parsed_rows(
    conn: sqlite3.Connection,
    recipe_ingredient_line_id: int,
    ingredient_block_id: int,
    recipe_id: int,
    recipe_section_id: int,
    rows: List[Dict[str, Any]],
) -> int:
    """Writes the primary downstream output (`recipe_ingredient_lines_parsed`)
    for one line. Lineage FKs mirror `recipe_ingredient_lines_raw`'s own
    convention exactly. Caller owns the transaction. Returns the number of
    rows written."""
    cursor = conn.cursor()
    for row in rows:
        cursor.execute(
            """
            INSERT INTO recipe_ingredient_lines_parsed (
                recipe_ingredient_line_id, ingredient_block_id, recipe_id, recipe_section_id,
                ingredient_id, ingredient_phrase, ingredient_name_original,
                grams, ml,
                imperial_weight_value, imperial_weight_unit,
                imperial_volume_value, imperial_volume_unit,
                natural_portion_value, natural_portion,
                packaging_count, packaging,
                preparation, notes,
                optional, alt_group_id, alt_kind
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recipe_ingredient_line_id, ingredient_block_id, recipe_id, recipe_section_id,
                row["ingredient_id"], row["ingredient_phrase"], row["ingredient_name_original"],
                row["grams"], row["ml"],
                row["imperial_weight_value"], row["imperial_weight_unit"],
                row["imperial_volume_value"], row["imperial_volume_unit"],
                row["natural_portion_value"], row["natural_portion"],
                row["packaging_count"], row["packaging"],
                row["preparation"], row["notes"],
                row["optional"], row["alt_group_id"], row["alt_kind"],
            ),
        )
    return len(rows)


def _classify_ambiguous_reason(result: dict) -> str:
    """Distinguishes SS I.4's compound quantity/package-scope ambiguity (a
    single viable interpretation whose own status is "ambiguous") from
    genuine multi-candidate ambiguity (more than one viable
    interpretation), using only `analyze_parse_result()`'s public output
    -- the same viable-count logic `_derive_result` already uses, not a
    new heuristic."""
    viable = [i for i in result["interpretations"] if i["status"] != "invalid"]
    if len(viable) == 1 and viable[0]["status"] == "ambiguous":
        return "compound_quantity_or_package_scope"
    return "other"


class ReportStats:
    """Aggregate counters for the execution report (item 17). Built
    incrementally from `analyze_all_lines()`'s output -- no per-line state
    is retained, so this never prints a line-by-line dump regardless of
    database size."""

    def __init__(self) -> None:
        self.total_lines = 0
        self.status_counts: "Counter[str]" = Counter()
        self.ambiguous_reason_counts: "Counter[str]" = Counter()
        self.unresolved_reason_counts: "Counter[str]" = Counter()
        self.lines_zero_row = 0
        self.lines_one_row = 0
        self.lines_multi_row = 0
        self.unmapped_units: "Counter[str]" = Counter()
        self.unmapped_package_size_count = 0
        self.missing_lineage_line_ids: List[int] = []

    def record_result(self, result: dict) -> None:
        self.total_lines += 1
        self.status_counts[result["status"]] += 1
        if result["status"] == "ambiguous":
            self.ambiguous_reason_counts[_classify_ambiguous_reason(result)] += 1
        # Tally every unresolved[].reason occurrence, across every
        # reference, across every interpretation -- not one reason per
        # line. A line can fail for more than one reason at once, and a
        # line that decomposes into multiple references (SS I) can
        # independently accumulate several unresolved reasons across
        # those references. These counts are NOT expected to sum to
        # status_counts["unresolved"].
        for interpretation in result["interpretations"]:
            for reference in interpretation.get("references", []):
                for entry in reference.get("unresolved", []):
                    self.unresolved_reason_counts[entry["reason"]] += 1

    def record_projection(
        self, rows: List[dict], unmapped_units: List[str], unmapped_package_size: List[str]
    ) -> None:
        if len(rows) == 0:
            self.lines_zero_row += 1
        elif len(rows) == 1:
            self.lines_one_row += 1
        else:
            self.lines_multi_row += 1
        for unit in unmapped_units:
            self.unmapped_units[unit] += 1
        self.unmapped_package_size_count += len(unmapped_package_size)

    def record_missing_lineage(self, recipe_ingredient_line_id: int) -> None:
        self.missing_lineage_line_ids.append(recipe_ingredient_line_id)


def _print_report(stats: "ReportStats") -> None:
    rule = "\u2500" * 64
    total = stats.total_lines

    print("Analyzer Execution Report")
    print(rule)
    print(f"{'Lines analyzed:':<40}{total:>10,}")
    print(f"{'Lines producing zero downstream rows:':<40}{stats.lines_zero_row:>10,}")
    print(f"{'Lines producing one downstream row:':<40}{stats.lines_one_row:>10,}")
    print(f"{'Lines producing multiple downstream rows:':<40}{stats.lines_multi_row:>10,}")

    print()
    for status in _STATUS_ORDER:
        count = stats.status_counts.get(status, 0)
        pct = (count / total * 100) if total else 0.0
        print(f"{status.capitalize() + ':':<40}{count:>10,} ({pct:5.1f}%)")

    print()
    print("Unresolved reasons (occurrences, not lines -- a line can carry")
    print("more than one; see ReportStats.record_result):")
    if stats.unresolved_reason_counts:
        for reason, count in sorted(
            stats.unresolved_reason_counts.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            print(f"  {reason:<42}{count:>8,}")
    else:
        print("  none")

    print()
    print("Ambiguous reasons (one per ambiguous line):")
    if stats.ambiguous_reason_counts:
        for reason, count in sorted(
            stats.ambiguous_reason_counts.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            print(f"  {reason:<42}{count:>8,}")
    else:
        print("  none")

    critical_issues: List[str] = []
    if stats.unmapped_units:
        total_unmapped = sum(stats.unmapped_units.values())
        units_list = ", ".join(sorted(stats.unmapped_units))
        critical_issues.append(
            f"{total_unmapped} measurement-quantity occurrence(s) used a unit "
            f"this projection cannot classify as metric/imperial weight/"
            f"volume: {units_list}. No grams/ml/imperial_* value was "
            f"populated for these -- see _route_measurement_quantity's "
            f"docstring (FLAGGED GAP #1)."
        )
    if stats.unmapped_package_size_count:
        critical_issues.append(
            f"{stats.unmapped_package_size_count} package.size occurrence(s) "
            f"have no column in the current downstream schema -- preserved "
            f"only in canonical_result_json (FLAGGED GAP #5)."
        )
    if stats.missing_lineage_line_ids:
        sample = stats.missing_lineage_line_ids[:10]
        critical_issues.append(
            f"{len(stats.missing_lineage_line_ids)} line(s) produced downstream "
            f"rows but had no matching recipe_ingredient_lines_raw lineage row "
            f"-- those rows were NOT written. Sample "
            f"recipe_ingredient_line_id values: {sample}."
        )

    if critical_issues:
        print()
        print("CRITICAL ISSUES:")
        for issue in critical_issues:
            print(f"  - {issue}")

    print(rule)


def persist_all_lines(
    db_path: Optional[Any] = None, line_ids: Optional[Iterable[int]] = None
) -> ReportStats:
    """Runs `analyze_all_lines()` and writes BOTH RO-10 outputs for every
    line: the primary downstream rows (`recipe_ingredient_lines_parsed`)
    and the diagnostic artifact (`analysis_records`/
    `analysis_candidate_evaluations`/`analysis_evidence`).

    `line_ids=None` (default) analyzes the whole database -- a debugging
    convenience (item 15), not a permanent design assumption. Pass an
    explicit iterable of `recipe_ingredient_line_id` values for a future
    targeted-pipeline invocation; nothing else here needs to change.

    Single transaction for the whole run (matching
    `process_recipe_lines()`'s existing commit-once pattern); rolls back
    entirely on any error so no line is left with diagnostic rows but no
    primary rows, or vice versa. Returns a `ReportStats` (see `main()` for
    the printed report built from it).
    """
    from gastrometric.config.paths import DB_PATH

    path = str(db_path or DB_PATH)
    write_conn = sqlite3.connect(path)
    stats = ReportStats()
    try:
        lineage = _fetch_line_lineage(write_conn)
        for recipe_ingredient_line_id, parse_tree_id, result in analyze_all_lines(path, line_ids):
            stats.record_result(result)

            _persist_analysis_record(write_conn, recipe_ingredient_line_id, parse_tree_id, result)

            rows, unmapped_units, unmapped_package_size = _project_selected_references(result)
            stats.record_projection(rows, unmapped_units, unmapped_package_size)

            if rows:
                line_lineage = lineage.get(recipe_ingredient_line_id)
                if line_lineage is None:
                    stats.record_missing_lineage(recipe_ingredient_line_id)
                else:
                    ingredient_block_id, recipe_id, recipe_section_id = line_lineage
                    _persist_parsed_rows(
                        write_conn,
                        recipe_ingredient_line_id,
                        ingredient_block_id,
                        recipe_id,
                        recipe_section_id,
                        rows,
                    )
        write_conn.commit()
    except Exception:
        write_conn.rollback()
        raise
    finally:
        write_conn.close()
    return stats


def main() -> None:
    stats = persist_all_lines()
    _print_report(stats)


if __name__ == "__main__":
    main()