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

What this module does NOT do (RO-10 boundary, unchanged):
lex text; generate parser candidates; modify the parse tree; create
vocabulary/ingredients; modify relationship knowledge; query SQLite for
knowledge (all knowledge access goes through the injected `RuntimeKnowledge`);
invent missing relationships or entities; perform statistical inference or
confidence calibration; select among ambiguous interpretations; silently
discard candidates; convert measurements to grams; persist analysis records
(that is RO-8's responsibility -- see `analyze_all_lines`'s docstring).
"""

from __future__ import annotations

import json
import re
import sqlite3
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


def _read_parse_trees(conn: sqlite3.Connection) -> Iterable[sqlite3.Row]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, recipe_ingredient_line_id, parse_tree_json
        FROM ingredient_parse_trees
        ORDER BY recipe_ingredient_line_id, id
        """
    )
    return cursor.fetchall()


def analyze_all_lines(db_path: Optional[Any] = None) -> Iterable[Tuple[int, int, dict]]:
    """Reads every persisted parse tree from `ingredient_parse_trees` and
    yields `(recipe_ingredient_line_id, parse_tree_id, canonical_semantic_result)`
    triples. parse_tree_id is the id of the exact `ingredient_parse_trees` row
    the result was produced from, so a persistence layer can populate
    analysis_records.parse_tree_id without a second lookup.
    Read-only: see module docstring above.
    """
    from gastrometric.config.paths import DB_PATH
    from gastrometric.knowledge.loader import knowledge as runtime_knowledge
    path = str(db_path or DB_PATH)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        for row in _read_parse_trees(conn):
            parse_result = json.loads(row["parse_tree_json"])
            result = analyze_parse_result(parse_result, runtime_knowledge)
            yield row["recipe_ingredient_line_id"], row["id"], result
    finally:
        conn.close()
        
from collections import Counter

KNOWN_UNRESOLVED_REASONS = (
    "ingredient_not_resolved",
    "quantity_not_resolved",
    "measurement_not_resolved",
    "relationship_unresolved",
    "unresolved_material",
    "range_quantity_not_representable",
)


def _has_multiple_references(result: dict) -> bool:
    """True if any interpretation in this result has more than one
    reference (e.g. a conjunction like "onions and garlic" split into two
    references within one interpretation). Assumption: scoped across all
    interpretations, not just the selected one -- adjust if that's wrong.
    """
    return any(len(interp.get("references", [])) > 1 for interp in result["interpretations"])


def _tally_unresolved_reasons(result: dict, reason_counts: Counter) -> None:
    """Counts unresolved[].reason across every reference in every
    interpretation of an unresolved-status result. Reasons outside
    KNOWN_UNRESOLVED_REASONS fall into 'other' rather than being dropped
    or silently mis-tallied.
    """
    for interpretation in result["interpretations"]:
        for reference in interpretation.get("references", []):
            for item in reference.get("unresolved", []):
                reason = item.get("reason", "")
                reason_counts[reason if reason in KNOWN_UNRESOLVED_REASONS else "other"] += 1


def _print_summary(
    total: int,
    multi_ref_lines: int,
    status_counts: Counter,
    reason_counts: Counter,
) -> None:
    def pct(n: int) -> float:
        return (n / total * 100) if total else 0.0

    sep = "─" * 38
    print("Analyzer Summary")
    print(sep)
    print(f"{'Lines analyzed:':<34}{total:>6,}")
    print(f"{'Lines split into multiple refs:':<34}{multi_ref_lines:>6,}")
    for label, key in (
        ("Resolved:", "resolved"),
        ("Ambiguous:", "ambiguous"),
        ("Unresolved:", "unresolved"),
        ("Invalid:", "invalid"),
    ):
        n = status_counts.get(key, 0)
        print(f"{label:<34}{n:>6,} ({pct(n):>4.1f}%)")
    print("Unresolved reasons:")
    for reason in (*KNOWN_UNRESOLVED_REASONS, "other"):
        print(f"  {reason:<32}{reason_counts.get(reason, 0):>5,}")
    print(sep)


def main() -> None:
    total = 0
    multi_ref_lines = 0
    status_counts: Counter = Counter()
    reason_counts: Counter = Counter()

    for _, _, result in analyze_all_lines():
        total += 1
        status_counts[result["status"]] += 1
        if _has_multiple_references(result):
            multi_ref_lines += 1
        if result["status"] == "unresolved":
            _tally_unresolved_reasons(result, reason_counts)

    _print_summary(total, multi_ref_lines, status_counts, reason_counts)


if __name__ == "__main__":
    main()