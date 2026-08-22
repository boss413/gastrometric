import sqlite3
import json
import itertools
from dataclasses import dataclass, field, asdict
from typing import List, Any, Type, Dict, Optional, cast

from gastrometric.config.paths import DB_PATH

# ---------------------------------------------------------------------------
# AST NODE DEFINITIONS
# ---------------------------------------------------------------------------
#
# This module implements a PURE SYNTACTIC parser. It converts lexical spans
# (already classified by the lexer's vocabulary) into a parse tree of
# culinary grammar constituents. It does not resolve ingredient identity,
# does not pick a "preferred" measurement, and does not infer nutritional
# meaning. All of that is analyzer work and happens downstream of this file.
#
# See the "PARSER OUTPUT CONTRACT" docstring below `IngredientReference` for
# the authoritative description of what each field means and does not mean.

@dataclass
class LexicalToken:
    """Represents a span from the `lexical_spans` table.

    `knowledge_id` / `source_vocabulary` are carried through purely for
    traceability back to the lexer's vocabulary entry -- the parser never
    branches on them. They are optional because older callers may not
    populate them.
    """
    span_id: int
    span_order: int
    start_offset: int
    end_offset: int
    text: str
    normalized_value: str
    span_type: str
    knowledge_id: Optional[int] = None
    source_vocabulary: Optional[str] = None

@dataclass
class ASTNode:
    """Base class for all Abstract Syntax Tree nodes."""
    def to_dict(self) -> dict:
        def _convert(obj: Any) -> Any:
            if isinstance(obj, ASTNode):
                d = {"node_type": obj.__class__.__name__}
                for f in obj.__dataclass_fields__:
                    d[f] = _convert(getattr(obj, f))
                return d
            elif isinstance(obj, list):
                return [_convert(i) for i in obj]
            elif isinstance(obj, LexicalToken):
                return asdict(obj)
            else:
                return obj
        return _convert(self)

@dataclass
class ContainerNode(ASTNode):
    """Base class for any node that contains children."""
    children: List[ASTNode] = field(default_factory=list)

# --- Leaf Nodes (Lexical Spans) ---
# Every leaf wraps exactly one LexicalToken, so its source span is always
# traceable back to `lexical_spans.span_id`.

@dataclass
class SpanNode(ASTNode):
    """Wraps one lexical span. This node's concrete class (e.g.
    NaturalPortionNode vs IngredientNode) is the role that particular
    candidate classification plays -- when a lexical position has more
    than one candidate `span_type`, that ambiguity is NOT recorded as
    metadata here (see LEXICAL AMBIGUITY CONTRACT below): it produces a
    separate candidate parse tree per role, each with an ordinary,
    single-role SpanNode at this position."""
    span: LexicalToken

@dataclass
class QuantityNode(SpanNode):
    pass

@dataclass
class MeasurementNode(SpanNode):
    pass

@dataclass
class IngredientNode(SpanNode):
    pass

@dataclass
class SizeNode(SpanNode):
    pass

@dataclass
class NaturalPortionNode(SpanNode):
    pass

@dataclass
class PackagingNode(SpanNode):
    pass

@dataclass
class PreparationNode(SpanNode):
    pass

@dataclass
class ComponentNode(SpanNode):
    pass

@dataclass
class DescriptorNode(SpanNode):
    pass

@dataclass
class StateNode(SpanNode):
    pass

@dataclass
class TemperatureNode(SpanNode):
    pass

@dataclass
class GrammarNode(SpanNode):
    pass

@dataclass
class UnknownNode(SpanNode):
    pass

# --- Grammar Operators (Pass 1) ---
#
# The lexer only tells us a span is "Grammar" or punctuation. The parser is
# responsible for recognizing which *syntactic role* that span plays (an
# alternative operator, a conjunction operator, a delimiter, ...). These
# marker nodes are the parser's operator vocabulary; they are consumed
# during constituent formation and never survive into the final tree.
# Parentheses are also grammar operators, but they are resolved structurally
# by `_group_parens` (Pass 3) rather than materialized as marker nodes.

@dataclass
class AlternativeMarker(SpanNode):
    """Functions as the 'or' operator."""
    pass

@dataclass
class ConjunctionMarker(SpanNode):
    """Functions as the 'and' / 'plus' operator."""
    pass

@dataclass
class RangeMarker(SpanNode):
    """Functions as a range operator ('to', '-') between quantities."""
    pass

@dataclass
class CommaMarker(SpanNode):
    """Functions as a list/attachment delimiter."""
    pass

@dataclass
class ParentheticalMarker(ContainerNode):
    """Intermediate marker for a parenthesized span, before Pass 3 resolves
    it into a ParentheticalExpression."""
    pass

# --- Culinary Expressions (Pure Syntactic AST) ---

@dataclass
class MeasurementExpression(ContainerNode):
    """A single discrete quantity/measurement constituent (Quantity,
    Measurement, and/or NaturalPortion spans). Distinct measurements are
    never merged into one MeasurementExpression -- see Section 6/14 of the
    parser contract below."""
    pass

@dataclass
class PackageExpression(ContainerNode):
    """Packaging constructs (e.g., cans, jars)."""
    pass

@dataclass
class IngredientExpression(ContainerNode):
    """The core ingredient noun phrase along with its direct modifiers
    (size, descriptor, state, temperature)."""
    pass

@dataclass
class ComponentExpression(ContainerNode):
    """Ingredient parts (e.g., seeds, stems, ribs)."""
    pass

@dataclass
class PreparationExpression(ContainerNode):
    """Preparation directives and actions."""
    pass

@dataclass
class NotesExpression(ContainerNode):
    """Wraps a lexically-recognized Grammar span that the vocabulary itself
    identifies as a note/annotation phrase (e.g. 'to taste'). This is the
    ONLY way a node becomes a note -- see the contract note on `notes`
    below."""
    pass

@dataclass
class UnknownSequence(ContainerNode):
    """Preserves unclassified lexical spans, without inventing meaning for
    them, while keeping their position in the sequence. This is the final
    recovery mechanism (Pass 10), not a normal parsing destination."""
    pass

@dataclass
class ParentheticalExpression(ContainerNode):
    """A syntactic group bounded by parentheses in the source text. Its
    children are whatever expressions were formed from the tokens inside
    the parens. The parser does not decide here whether the group is a
    package size, an alternate measurement, or anything else -- it only
    preserves the fact that this content was grouped by parentheses."""
    pass

@dataclass
class AlternativeExpression(ContainerNode):
    """An 'or' relationship between two syntactically-compatible operands
    (same constituent type on both sides). The type of the operands, not
    the operator, determines what the group means -- e.g. two
    IngredientExpressions joined by 'or' is an ingredient alternative.

    `connective` preserves the lexical span of the operator itself (the
    `AlternativeMarker` for the "or"/"nor" that produced this group) --
    `children` holds only the two operands, exactly as before this field
    was added, so nothing that already reads `children[0]`/`children[1]`
    is affected. `connective` is the actual provenance for this relation;
    it must not be reconstructed from the operands' spans downstream."""
    connective: Optional[ASTNode] = None

@dataclass
class ConjunctionExpression(ContainerNode):
    """An 'and' / 'plus' relationship between two syntactically-compatible
    operands. Same classification rule and `connective` provenance
    convention as AlternativeExpression."""
    connective: Optional[ASTNode] = None

@dataclass
class ListExpression(ContainerNode):
    """Generic structural container reserved for constructs that are
    grammatically list-like but do not fit a more specific expression
    type. Not used as a semantic fallback."""
    pass

@dataclass
class IngredientReference(ASTNode):
    """
    A syntactic ingredient-reference production, assembled only after its
    constituent expressions have already been formed (Pass 7). See the
    "PARSER OUTPUT CONTRACT" below for the meaning of every field.
    """
    measurements: List[ASTNode] = field(default_factory=list)
    package: Optional[ASTNode] = None
    ingredient: Optional[ASTNode] = None
    component: Optional[ASTNode] = None
    preparation: Optional[ASTNode] = None
    notes: List[ASTNode] = field(default_factory=list)
    unresolved: List[ASTNode] = field(default_factory=list)

@dataclass
class IngredientLine(ContainerNode):
    """One complete structural interpretation of an ingredient line's
    lexical spans -- a list of `IngredientReference` children. This is a
    single candidate; see `ParseResult` for how multiple candidates are
    represented when the line's lexical spans are ambiguous."""
    pass

@dataclass
class Candidate(ASTNode):
    """One parse branch: a complete structural interpretation (`tree`,
    an `IngredientLine`) plus a flattened rollup (`unresolved`) of
    everything within it the parser could not resolve into a recognized
    constituent.

    `unresolved` here is a convenience view across every
    `IngredientReference` in `tree` -- each reference also keeps its own
    `unresolved` list (which reference a fragment belongs to is still
    meaningful when a line has more than one), but surfacing the flattened
    set at the candidate level lets a consumer gauge how much of this
    particular branch went unresolved without walking the tree.
    """
    tree: ASTNode
    unresolved: List[ASTNode] = field(default_factory=list)

@dataclass
class ParseResult(ASTNode):
    """The parser's output for one ingredient line: a set of complete,
    independently-valid structural interpretations ("candidates"), one per
    combination of lexical role choices across the line's ambiguous
    positions (see LEXICAL AMBIGUITY CONTRACT below). Each candidate is a
    `Candidate` wrapping a full `IngredientLine`.

    Ambiguity lives BETWEEN candidates, not as metadata on a chosen node
    inside one tree -- a NaturalPortion-vs-Ingredient reading of "cloves"
    is two different candidates in `candidates`, not one tree with a note
    attached to whichever role happened to be picked. The analyzer
    selects/eliminates among candidates using knowledge the parser
    deliberately does not have (ingredient/component relationships,
    measurement semantics, surrounding structure, etc.).
    """
    candidates: List[ASTNode] = field(default_factory=list)

# ---------------------------------------------------------------------------
# PARSER OUTPUT CONTRACT  (see work order Section 23)
# ---------------------------------------------------------------------------
#
# 1. MeasurementExpression: a single Quantity/Measurement/NaturalPortion
#    constituent, e.g. "4 tablespoons", "1/2 stick", "14 ounces", "2".
#
# 2. Multiple measurements: `IngredientReference.measurements` is a LIST.
#    There is no "primary quantity" vs. "secondary measurement" split --
#    every MeasurementExpression (or ParentheticalExpression wrapping one)
#    found for a reference is appended to this list in source order. The
#    analyzer decides which, if any, is nutritionally authoritative.
#
# 3. Parenthetical expressions: content inside `( ... )` is resolved before
#    ordinary attachment (Pass 3) into a ParentheticalExpression, which
#    preserves the fact that its children were grouped by parentheses. If
#    that content is itself a measurement, the ParentheticalExpression (not
#    an unwrapped MeasurementExpression) is what gets appended to
#    `measurements` -- so the analyzer can still see it was parenthetical.
#
# 4. Conjunctions: an AlternativeExpression ('or') or ConjunctionExpression
#    ('and'/'plus') wraps exactly two syntactically-compatible operands
#    (same constituent type on both sides), preserved in source order.
#
# 5. Conjunction group classification: the TYPE of the operands determines
#    where the group attaches on IngredientReference -- two
#    IngredientExpressions -> `ingredient`; two PreparationExpressions ->
#    `preparation`; two ComponentExpressions -> `component`; two
#    MeasurementExpressions (or parenthetical measurements) -> appended to
#    `measurements`. This is a lookup on constituent type, not a table of
#    per-example special cases.
#
# 6. Package relationships: `PackageExpression` (e.g. "cans") and any
#    associated MeasurementExpression / ParentheticalExpression remain
#    siblings on the IngredientReference in their original order. The
#    parser does not merge them into a single node or decide that one is
#    the "package size" -- it only preserves adjacency and grouping.
#
# 7. Preparation expressions attach to `IngredientReference.preparation`
#    after the core ingredient has been recognized (preparation is
#    post-nominal), whether they are a single PreparationExpression or an
#    Alternative/ConjunctionExpression of PreparationExpressions.
#
# 7a. Fragmented phrases: `ingredient`, `component`, `preparation`, and
#     `package` are singular fields, but the chunker can still produce more
#     than one same-typed top-level expression for a single reference --
#     typically a noun/adjective phrase interrupted by an intervening
#     token, e.g. a comma between pre-nominal modifiers ("boneless,
#     skinless chicken breasts") or a differently-classified word splitting
#     what is really one phrase ("large boneless chicken breasts"). A
#     second same-typed occurrence is merged into the existing value
#     (`_attach_singular`) rather than discarded to `unresolved` -- it is
#     virtually always a continuation of the same phrase, not a competing
#     second value. This only merges expressions of the identical plain
#     type; if the field already holds an Alternative/ConjunctionExpression
#     (a genuine 'or'/'and' group), a further plain expression still falls
#     back to `unresolved` rather than being guessed into the group.
#
# 8. Unknown spans become `UnknownSequence` nodes and are appended to
#    `IngredientReference.unresolved`. Being unknown does not remove the
#    span from the tree, and it does not get promoted into a recognized
#    ingredient/preparation/etc. just because nothing else claimed it.
#
# 9. `notes`: ONLY populated from `NotesExpression` nodes, which are only
#    created when the lexer/vocabulary has already classified a span as a
#    known Grammar/annotation phrase (e.g. "to taste"). `notes` is never
#    used as a catch-all for constituents the parser is unsure about --
#    that is what `unresolved` is for.
#
# 10. Deferred to the analyzer: which measurement is nutritionally
#     authoritative, package-to-quantity conversion, ingredient identity
#     resolution, and any culinary-knowledge-based disambiguation (e.g.
#     whether "2 carrots" means two natural-portion units of carrot).
#
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# LEXICAL AMBIGUITY CONTRACT
# ---------------------------------------------------------------------------
#
# This parser separates three distinct concerns that are easy to conflate:
#
#   1. LEXICAL CANDIDATES (the lexer's job, consumed as-is by the parser)
#      For one source span, e.g. "ribs", the lexer may propose several
#      candidate classifications with NO preference between them:
#          Ingredient(ribs), Component(ribs), NaturalPortion(ribs)
#      These arrive as multiple `lexical_spans` rows sharing the exact same
#      (start_offset, end_offset, text). `_group_positions` groups them into
#      one lexical position's candidate set; no row is ever dropped merely
#      because another row exists for the same position.
#
#   2. PARSE BRANCHES (this parser's job)
#      The parser combines lexical candidates into every STRUCTURALLY valid
#      complete interpretation -- e.g. for "2 ribs celery":
#          Candidate A: Quantity(2) + NaturalPortion(ribs) [one
#                       MeasurementExpression] + Ingredient(celery)
#          Candidate B: Quantity(2) + IngredientExpression(ribs, celery)
#                       [one compound ingredient phrase]
#          Candidate C: Quantity(2) + Component(ribs) + Ingredient(celery)
#      The parser's job is to determine what structures are grammatically
#      possible. It does NOT decide which one is true -- there is no
#      "Ingredient wins" or "NaturalPortion wins" rule anywhere in this
#      file, and no candidate is scored, ranked, or dropped for being
#      semantically less likely.
#
#   3. ANALYZER SCORING (deliberately NOT this parser's job)
#      Once `ParseResult.candidates` reaches the analyzer, it can ask the
#      knowledge system questions the parser has no access to -- e.g.
#      "ribs IS_A pork ribs", "ribs COMPONENT_OF celery?", "ribs
#      NATURAL_PORTION_OF celery?" -- and use that evidence to prefer one
#      candidate over another. That resolution step belongs entirely to the
#      analyzer.
#
# The key invariant: the parser may eliminate structurally IMPOSSIBLE
# interpretations (e.g. a candidate role that can't grammatically combine
# with its neighbors at all), but it must never select among structurally
# valid, semantically plausible interpretations. That selection is the
# analyzer's job, not the parser's.
#
#     LexicalSpan  = source text + candidate lexical classifications
#     Parser       = determines what structures the candidates can
#                    participate in, as a SET of complete interpretations
#     ParseResult  = every structurally valid interpretation, each a
#                    Candidate(tree=<complete IngredientLine>, unresolved=...)
#
# Ambiguity therefore lives BETWEEN candidates, never as metadata on a
# chosen node inside one tree. A NaturalPortion-vs-Ingredient reading of
# "cloves" is two different candidates in `ParseResult.candidates`, not one
# tree with a note attached to whichever role happened to be picked -- a
# role chosen for one node, with the alternative recorded as metadata on
# it, is already a preference, which is exactly the semantic judgment the
# parser must not make. (An earlier iteration of this parser did exactly
# that via an `alternate_roles` field; it was removed for this reason, not
# renamed.)
#
# Concretely, how ambiguity becomes branches:
#
# 1. `_group_positions` groups same-extent `lexical_spans` rows into ONE
#    lexical position with N candidate classifications (layer 1, above).
#
# 2. `_expand_candidate_sequences` takes the cross product of candidate
#    choices across every ambiguous position in the line, producing one
#    concrete (single-classification-per-position) token sequence per
#    combination. A word with 2 candidate roles doubles the number of
#    sequences; two independently-ambiguous words in one line multiply.
#    This is a structural expansion, not a semantic one -- it does not ask
#    which role is more likely, only which roles exist.
#
# 3. Each concrete sequence is parsed by the ORDINARY single-interpretation
#    pipeline (`_group_parens` -> `_process_level` -> `_build_references`),
#    completely unaware that it's one of several candidates. This is what
#    keeps role interpretation from being decided ahead of structure: the
#    same general grammar rules (e.g. "adjacent Ingredient-compatible spans
#    form one IngredientExpression", "a bare NaturalPortion directly after
#    a bare Quantity completes it") apply uniformly, and simply produce a
#    different resulting tree depending on which candidate the sequence
#    picked at each ambiguous position. This is layer 2 (parse branches).
#
# 4. The resulting `IngredientLine`s are deduplicated (candidates that
#    happen to produce byte-identical trees collapse to one), each wrapped
#    in a `Candidate` alongside a flattened `unresolved` rollup, and
#    collected into `ParseResult.candidates`. Nothing here ranks or
#    eliminates candidates on culinary grounds -- that's layer 3, entirely
#    deferred to the analyzer.
#
# 5. This never depends on which specific word is involved. Expansion
#    operates purely on however many candidate rows a position has, so no
#    special-casing is needed for particular words (clove, ribs, skin,
#    chili, ...) -- any ambiguously-classified word is handled by the same
#    general mechanism, and an ordinary single-classification word (the
#    common case) produces exactly one sequence, unchanged from before this
#    mechanism existed.
#
# ---------------------------------------------------------------------------

SPAN_TYPE_MAP: Dict[str, Type[SpanNode]] = {
    "QUANTITY": QuantityNode,
    "MEASUREMENT": MeasurementNode,
    "INGREDIENT": IngredientNode,
    "SIZE": SizeNode,
    "NATURALPORTION": NaturalPortionNode,
    "PACKAGING": PackagingNode,
    "PREPARATION": PreparationNode,
    "COMPONENT": ComponentNode,
    "DESCRIPTOR": DescriptorNode,
    "STATE": StateNode,
    "TEMPERATURE": TemperatureNode,
    "GRAMMAR": GrammarNode,
    "UNKNOWN": UnknownNode,
}

# Constituent types whose Alternative/ConjunctionExpression groups attach to
# a singular IngredientReference field. MeasurementExpression is handled
# separately since `measurements` is a list, not a singular slot.
_GROUP_OPERAND_FIELD: Dict[Type[ASTNode], str] = {
    IngredientExpression: "ingredient",
    PreparationExpression: "preparation",
    ComponentExpression: "component",
}


class IngredientParser:
    """
    Pure syntactic parser mapping lexical spans into hierarchical culinary
    grammar constituents. See "Parsing Order" (work order Section 18):

        1. Identify grammar/parser operators
        2. Form atomic constituents
        3. Resolve parenthetical/delimited groups
        4. Identify conjunction boundaries
        5. Classify conjunctions from their operand constituent types
        6. Resolve local measurement/package relationships (sibling order)
        7. Form the core ingredient-reference structure
        8. Resolve preparation/component structures
        9. Attach explicitly recognized notes/grammar
        10. Preserve anything still unresolved as UnknownSequence

    This is implemented as a small set of recursive helper passes (to
    naturally handle nested parenthetical content) rather than as ten
    literal, separate database-style passes, per Section 18's closing note.
    """

    def parse(self, spans: List[LexicalToken]) -> ParseResult:
        position_groups = self._group_positions(spans)
        sequences = self._expand_candidate_sequences(position_groups)

        candidates: List[ASTNode] = []
        seen: set = set()
        for seq in sequences:
            nested = self._group_parens(seq)              # Pass 3 (structural)
            exprs = self._process_level(nested)            # Passes 1,2,4,5
            refs = self._build_references(exprs)            # Passes 6-10
            line = IngredientLine(children=refs)

            # Different candidate role choices usually produce visibly
            # different trees, but a redundant lexer row (the same
            # classification listed twice for one position) would produce
            # two identical trees -- collapse those rather than showing
            # the analyzer a duplicate hypothesis.
            key = json.dumps(line.to_dict(), sort_keys=True)
            if key not in seen:
                seen.add(key)
                flattened_unresolved: List[ASTNode] = []
                for ref in line.children:
                    if isinstance(ref, IngredientReference):
                        flattened_unresolved.extend(ref.unresolved)
                candidates.append(Candidate(tree=line, unresolved=flattened_unresolved))

        return ParseResult(candidates=candidates)

    # -- Span hygiene -------------------------------------------------

    def _group_positions(self, spans: List[LexicalToken]) -> List[List[LexicalToken]]:
        """Groups `lexical_spans` rows into lexical positions.

        The lexer may emit more than one row for the exact same source
        extent -- multiple candidate `span_type` classifications for one
        word. Those rows are always kept together as a single position's
        candidate set; they are NEVER reduced to one here. What happens
        with multiple candidates is decided in `_expand_candidate_sequences`
        (branch into separate candidate trees), not by discarding rows at
        this stage.

        Spans with genuinely different, overlapping extents (a distinct,
        coarser kind of ambiguity -- alternative segmentations of the
        text, not alternative classifications of the same word) fall back
        to keeping the outermost extent, as before this change.
        """
        by_extent: Dict[Any, List[LexicalToken]] = {}
        order: List[Any] = []
        for s in spans:
            key = (s.start_offset, s.end_offset)
            if key not in by_extent:
                by_extent[key] = []
                order.append(key)
            by_extent[key].append(s)

        for key in by_extent:
            by_extent[key].sort(key=lambda s: s.span_order)

        extents = sorted(by_extent.keys(), key=lambda k: (k[0], -k[1]))

        kept: List[List[LexicalToken]] = []
        max_end = -1
        for start, end in extents:
            if start >= max_end:
                kept.append(by_extent[(start, end)])
                max_end = end
        return kept

    # Defensive cap on how many candidate token sequences one line can
    # expand into. This is a safety valve against pathological input (many
    # independently-ambiguous positions multiplying together), not a
    # designed limit -- ordinary ingredient lines have at most a handful of
    # ambiguous words, so this should not be reached in practice.
    MAX_CANDIDATE_SEQUENCES = 64

    def _expand_candidate_sequences(
        self, position_groups: List[List[LexicalToken]]
    ) -> List[List[LexicalToken]]:
        """Cross-multiplies candidate classifications across every lexical
        position in the line into concrete (one-classification-per-position)
        token sequences -- one per combination of role choices. A position
        with a single candidate contributes only one choice, so an
        ordinary, fully-unambiguous line always expands to exactly one
        sequence, identical to running the pre-ambiguity single-candidate
        pipeline directly.

        This is a purely structural expansion (which roles EXIST), not a
        semantic one (which role is more likely) -- see LEXICAL AMBIGUITY
        CONTRACT.
        """
        sequences: List[List[LexicalToken]] = []
        for i, combo in enumerate(itertools.product(*position_groups)):
            if i >= self.MAX_CANDIDATE_SEQUENCES:
                break
            sequences.append(list(combo))
        return sequences

    # -- Pass 3 (structural half): parenthesis grouping ---------------

    def _group_parens(self, spans: List[LexicalToken]) -> List[Any]:
        """Turns one concrete (already-disambiguated) span sequence into a
        nested list-of-lists wherever parentheses occur, so that everything
        inside `( ... )` can be parsed as its own constituent sequence
        before being resolved into a ParentheticalExpression."""
        stack: List[List[Any]] = []
        current_level: List[Any] = []

        for span in spans:
            is_open = span.text == '(' or span.span_type == 'PAREN_OPEN'
            is_close = span.text == ')' or span.span_type == 'PAREN_CLOSE'

            if is_open:
                stack.append(current_level)
                current_level = []
            elif is_close and stack:
                finished_level = current_level
                current_level = stack.pop()
                current_level.append(finished_level)
            else:
                current_level.append(span)

        # Unbalanced open-paren: keep the content rather than dropping it.
        while stack:
            finished_level = current_level
            current_level = stack.pop()
            current_level.append(finished_level)

        return current_level


    # -- Pass 1: grammar operator recognition --------------------------

    def _to_leaf(self, span: LexicalToken) -> ASTNode:
        """Maps one lexical span to a leaf AST node, distinguishing the
        syntactic ROLE of Grammar/punctuation spans (operator) from their
        lexical CLASS (which the lexer already gave us)."""
        stype = span.span_type.upper()
        text = span.text.lower()

        if stype in ("GRAMMAR", "CONJUNCTION"):
            if text == "or":
                return AlternativeMarker(span=span)
            if text in ("and", "+", "plus"):
                return ConjunctionMarker(span=span)
            if text in ("to", "-"):
                return RangeMarker(span=span)
            # A recognized Grammar span that isn't an operator (e.g.
            # "to taste") is a genuine grammar/note constituent, not an
            # unknown one.
            return GrammarNode(span=span)

        if stype in ("PUNCTUATION", "SYMBOL"):
            if text == ",":
                return CommaMarker(span=span)
            if text == "-":
                return RangeMarker(span=span)
            return UnknownNode(span=span)

        node_class = SPAN_TYPE_MAP.get(stype, UnknownNode)
        return node_class(span=span)

    # -- Passes 2, 3(content), 4/5: constituents + groups per level ---

    def _process_level(self, tokens: List[Any]) -> List[ASTNode]:
        """Processes one bracketing level: forms atomic constituents, then
        (recursively, for nested levels) resolves parenthetical groups,
        then classifies conjunction/alternative groups.

        `tokens` is a mix of concrete `LexicalToken`s (already
        disambiguated by `_expand_candidate_sequences` -- by this point
        each lexical position has exactly one classification) and nested
        paren groups (`list`, produced by `_group_parens`).
        """
        leaves: List[ASTNode] = []
        for t in tokens:
            if isinstance(t, list):
                inner_exprs = self._process_level(t)
                leaves.append(ParentheticalMarker(children=inner_exprs))
            else:
                leaves.append(self._to_leaf(t))

        exprs = self._chunk_primitives(leaves)                        # Pass 2
        exprs = self._merge_discontinuous_quantity_unit(exprs)       # Pass 2b
        exprs = self._resolve_parenthetical_groups(exprs)            # Pass 3
        exprs = self._classify_conjunction_groups(exprs)             # Passes 4-5
        exprs = [e for e in exprs if not isinstance(e, CommaMarker)]
        return exprs

    def _chunk_primitives(self, nodes: List[ASTNode]) -> List[ASTNode]:
        """Pass 2: form the smallest obvious syntactic constituents by
        merging adjacent, grammatically-compatible leaves. This never
        merges across a marker (alternative/conjunction/comma/paren
        boundary), and it never merges two independent measurements into
        one MeasurementExpression.

        By the time this runs, ambiguity has already been resolved into
        separate candidate sequences upstream (`_expand_candidate_sequences`
        in `parse`) -- every node here has exactly one classification, so
        this is the same single-interpretation grammar regardless of how
        many candidates the line as a whole produces.
        """
        exprs: List[ASTNode] = []
        current: Any = None

        def flush():
            nonlocal current
            if current is not None and current.children:
                exprs.append(current)
            current = None

        for node in nodes:
            if isinstance(node, (AlternativeMarker, ConjunctionMarker, CommaMarker, ParentheticalMarker)):
                flush()
                exprs.append(node)
                continue

            if isinstance(node, QuantityNode):
                # A new Quantity starting while we're already mid-way
                # through a measurement that has its own quantity means
                # this is a *second* measurement, e.g. "2 lbs 3 oz" --
                # don't let it merge into the first one.
                if isinstance(current, MeasurementExpression) and any(
                    isinstance(c, (QuantityNode, RangeMarker)) for c in current.children
                ):
                    flush()
                if not isinstance(current, MeasurementExpression):
                    flush()
                    current = MeasurementExpression()
                current.children.append(node)

            elif isinstance(node, RangeMarker):
                # Rule B: a hyphen (or "to") gluing a bare quantity to its
                # unit -- "14-ounce", "28-oz", "750-ml" -- is part of one
                # atomic quantity+unit constituent, not a boundary between
                # two measurements. It only marks a genuine separator (e.g.
                # a real range like "2 cups - 3 cups") once the current
                # measurement already has a completed unit attached. This
                # is a general grammar rule, not specific to any one unit.
                if isinstance(current, MeasurementExpression) and any(
                    isinstance(c, (MeasurementNode, NaturalPortionNode)) for c in current.children
                ):
                    flush()
                if not isinstance(current, MeasurementExpression):
                    flush()
                    current = MeasurementExpression()
                current.children.append(node)

            elif isinstance(node, (MeasurementNode, NaturalPortionNode)):
                if not isinstance(current, MeasurementExpression):
                    flush()
                    current = MeasurementExpression()
                current.children.append(node)

            elif isinstance(node, PackagingNode):
                flush()
                exprs.append(PackageExpression(children=cast(List[ASTNode], [node])))

            elif isinstance(node, (IngredientNode, SizeNode, DescriptorNode, StateNode, TemperatureNode)):
                if not isinstance(current, IngredientExpression):
                    flush()
                    current = IngredientExpression()
                current.children.append(node)

            elif isinstance(node, ComponentNode):
                if not isinstance(current, ComponentExpression):
                    flush()
                    current = ComponentExpression()
                current.children.append(node)

            elif isinstance(node, PreparationNode):
                if not isinstance(current, PreparationExpression):
                    flush()
                    current = PreparationExpression()
                current.children.append(node)

            elif isinstance(node, GrammarNode):
                # Only a lexically-recognized Grammar span becomes a note.
                flush()
                exprs.append(NotesExpression(children=cast(List[ASTNode], [node])))

            elif isinstance(node, UnknownNode):
                if isinstance(current, PreparationExpression):
                    # Keeps unknown vocabulary inside its preparation
                    # context instead of ejecting it (Section 10).
                    current.children.append(node)
                elif isinstance(current, UnknownSequence):
                    current.children.append(node)
                else:
                    flush()
                    current = UnknownSequence(children=cast(List[ASTNode], [node]))

        flush()
        return exprs

    def _merge_discontinuous_quantity_unit(self, exprs: List[ASTNode]) -> List[ASTNode]:
        """Pass 2b (grammar rule):

            MeasurementExpression := Quantity MeasurementUnit?
            MeasurementUnit        := Measurement | NaturalPortion

        `_chunk_primitives` already merges a Quantity directly adjacent to
        its unit. But a unit word can be separated from its quantity by an
        intervening ingredient description purely due to English word
        order -- "1 medium garlic clove" is "1 clove [of] medium garlic",
        not "1" plus an unrelated "clove". The lexical vocabulary class of
        the unit (Measurement vs. NaturalPortion) is irrelevant here; what
        matters syntactically is that it completes the quantity.

        This pass recognizes exactly that discontinuous pattern -- a bare
        quantity, then a single IngredientExpression, then a bare unit --
        and merges the quantity and unit into one MeasurementExpression,
        leaving the ingredient description in place. It does not merge
        across anything else (markers, parentheses, multiple intervening
        expressions), so it only fires on this specific, general shape
        rather than guessing across arbitrary distances.
        """

        def is_bare_quantity(e: ASTNode) -> bool:
            return (
                isinstance(e, MeasurementExpression) and bool(e.children)
                and all(isinstance(c, (QuantityNode, RangeMarker)) for c in e.children)
            )

        def is_bare_unit(e: ASTNode) -> bool:
            return (
                isinstance(e, MeasurementExpression) and bool(e.children)
                and all(isinstance(c, (MeasurementNode, NaturalPortionNode)) for c in e.children)
            )

        result: List[ASTNode] = []
        i = 0
        n = len(exprs)
        while i < n:
            if (
                i + 2 < n
                and is_bare_quantity(exprs[i])
                and isinstance(exprs[i + 1], IngredientExpression)
                and is_bare_unit(exprs[i + 2])
            ):
                quantity_expr = cast(MeasurementExpression, exprs[i])
                unit_expr = cast(MeasurementExpression, exprs[i + 2])
                merged = MeasurementExpression(
                    children=cast(List[ASTNode], list(quantity_expr.children) + list(unit_expr.children))
                )
                result.append(merged)
                result.append(exprs[i + 1])
                i += 3
            else:
                result.append(exprs[i])
                i += 1
        return result

    def _resolve_parenthetical_groups(self, exprs: List[ASTNode]) -> List[ASTNode]:
        """Pass 3: turn each ParentheticalMarker into a ParentheticalExpression
        that simply wraps whatever constituents were already formed from its
        contents. This does NOT try to guess whether the group is a
        measurement, a package size, or anything else -- that classification
        (if any) happens later, when the reference is assembled, and even
        then only by preserving the wrapping rather than discarding it."""
        resolved: List[ASTNode] = []
        for e in exprs:
            if isinstance(e, ParentheticalMarker):
                resolved.append(ParentheticalExpression(children=e.children))
            else:
                resolved.append(e)
        return resolved

    def _classify_conjunction_groups(self, exprs: List[ASTNode]) -> List[ASTNode]:
        """Passes 4-5: find AlternativeMarker/ConjunctionMarker operators
        and, if the constituents on both sides are of the same syntactic
        type, merge them into an Alternative/ConjunctionExpression. The
        classification of *what kind* of group this is comes later, from
        the operand type -- this pass only establishes the group."""

        def process_markers(nodes: List[ASTNode]) -> List[ASTNode]:
            # Alternatives ('or') bind first.
            i = 1
            while i < len(nodes) - 1:
                if isinstance(nodes[i], AlternativeMarker):
                    marker, left, right = nodes[i], nodes[i - 1], nodes[i + 1]
                    if type(left) == type(right) and isinstance(
                        left, (IngredientExpression, PreparationExpression,
                               MeasurementExpression, ComponentExpression, DescriptorNode)
                    ):
                        merged: ASTNode = AlternativeExpression(
                            children=cast(List[ASTNode], [left, right]), connective=marker
                        )
                        nodes[i - 1:i + 2] = [merged]
                        i -= 1
                i += 1

            # Conjunctions ('and', 'plus', '+').
            i = 1
            while i < len(nodes) - 1:
                if isinstance(nodes[i], ConjunctionMarker):
                    marker, left, right = nodes[i], nodes[i - 1], nodes[i + 1]
                    if type(left) == type(right) and isinstance(
                        left, (IngredientExpression, PreparationExpression,
                               MeasurementExpression, ComponentExpression, DescriptorNode)
                    ):
                        merged = ConjunctionExpression(
                            children=cast(List[ASTNode], [left, right]), connective=marker
                        )
                        nodes[i - 1:i + 2] = [merged]
                        i -= 1
                i += 1
            return nodes

        # Also classify inside any already-formed containers (e.g. a
        # ParentheticalExpression whose contents include an alternative).
        for e in exprs:
            if isinstance(e, ContainerNode) and e.children:
                e.children = process_markers(e.children)

        return process_markers(exprs)

    # -- Passes 6-10: reference assembly -------------------------------

    def _contains_measurement(self, node: ASTNode) -> bool:
        """True if `node` is, or wraps, a MeasurementExpression -- used to
        decide whether a ParentheticalExpression belongs in `measurements`
        without unwrapping (and thus losing) the parenthetical context."""
        if isinstance(node, MeasurementExpression):
            return True
        if isinstance(node, (AlternativeExpression, ConjunctionExpression, ParentheticalExpression)):
            return any(self._contains_measurement(c) for c in node.children)
        return False

    def _group_operand_field(self, group: ASTNode) -> Optional[str]:
        """For an Alternative/ConjunctionExpression, look up which singular
        IngredientReference field its operand type corresponds to. Returns
        None if the operand type has no singular field (e.g. measurements,
        which is handled separately as a list)."""
        if not isinstance(group, ContainerNode) or not group.children:
            return None
        sample = group.children[0]
        for operand_type, field_name in _GROUP_OPERAND_FIELD.items():
            if isinstance(sample, operand_type):
                return field_name
        return None

    def _attach_singular(
        self, ref: IngredientReference, field_name: str, e: ContainerNode, expr_type: Type[ContainerNode]
    ) -> None:
        """Attaches `e` to a singular IngredientReference field (package,
        ingredient, component, preparation).

        `IngredientReference` has exactly ONE slot for each of these, but
        the chunker can still produce more than one same-typed top-level
        expression for a single reference -- most commonly a noun/adjective
        phrase interrupted by an intervening token, e.g. a comma between
        pre-nominal modifiers ("boneless, skinless chicken breasts") or a
        differently-classified word splitting what is really one phrase
        ("large boneless chicken breasts", where "boneless" briefly opens
        its own PreparationExpression between two IngredientExpression
        fragments). A second occurrence of the SAME expression type is
        virtually always a continuation of that one phrase, not a second,
        competing value -- so it is merged rather than discarded into
        `unresolved`.

        If the field is already occupied by something of a DIFFERENT shape
        (e.g. an Alternative/ConjunctionExpression already assembled from a
        genuine 'or'/'and'), merging into it would misrepresent the
        grammar, so that case still falls back to `unresolved` rather than
        guessing how to combine them.
        """
        current_value = getattr(ref, field_name)
        if current_value is None:
            setattr(ref, field_name, e)
        elif isinstance(current_value, expr_type):
            current_value.children.extend(e.children)
        else:
            ref.unresolved.append(e)

    def _attach(self, ref: IngredientReference, e: ASTNode) -> None:
        """Attaches one top-level expression to the reference being built,
        by dispatching on the expression's own grammatical type. This is
        the general attachment rule referenced in Sections 13/19: it never
        inspects raw tokens and never uses a 'first slot wins, everything
        else is a note' fallback chain."""

        if isinstance(e, MeasurementExpression):
            ref.measurements.append(e)

        elif isinstance(e, ParentheticalExpression):
            if self._contains_measurement(e):
                ref.measurements.append(e)
            else:
                # Syntactically real, but the grammar doesn't tell us what
                # this parenthetical content means -- preserve it rather
                # than guessing or discarding it.
                ref.unresolved.append(e)

        elif isinstance(e, PackageExpression):
            self._attach_singular(ref, "package", e, PackageExpression)

        elif isinstance(e, IngredientExpression):
            self._attach_singular(ref, "ingredient", e, IngredientExpression)

        elif isinstance(e, ComponentExpression):
            self._attach_singular(ref, "component", e, ComponentExpression)

        elif isinstance(e, PreparationExpression):
            self._attach_singular(ref, "preparation", e, PreparationExpression)

        elif isinstance(e, NotesExpression):
            ref.notes.extend(e.children)

        elif isinstance(e, UnknownSequence):
            ref.unresolved.append(e)

        elif isinstance(e, (AlternativeExpression, ConjunctionExpression)):
            if self._contains_measurement(e):
                ref.measurements.append(e)
                return
            field_name = self._group_operand_field(e)
            if field_name is None:
                ref.unresolved.append(e)
                return
            if getattr(ref, field_name) is None:
                setattr(ref, field_name, e)
            else:
                ref.unresolved.append(e)

        else:
            # Final recovery: a constituent the grammar above doesn't
            # produce a specific attachment rule for. Preserved, not
            # discarded, and NOT written into `notes`.
            ref.unresolved.append(e)


    def _reference_is_empty(self, ref: IngredientReference) -> bool:
        return not (
            ref.measurements or ref.package or ref.ingredient or ref.component
            or ref.preparation or ref.notes or ref.unresolved
        )

    def _build_references(self, exprs: List[ASTNode]) -> List[ASTNode]:
        """Passes 6-10: assembles the already-formed top-level expressions
        into IngredientReference productions. A literal, unmatched
        Alternative/ConjunctionMarker here means the two sides were not
        syntactically compatible constituents (Pass 4/5 declined to merge
        them) -- in that case they separate whole references rather than
        merging within one."""
        refs: List[ASTNode] = []
        current_ref = IngredientReference()

        def commit_ref():
            nonlocal current_ref
            if not self._reference_is_empty(current_ref):
                refs.append(current_ref)
            current_ref = IngredientReference()

        for e in exprs:
            if isinstance(e, (AlternativeMarker, ConjunctionMarker)):
                commit_ref()
                refs.append(e)
                continue
            self._attach(current_ref, e)

        commit_ref()

        # Resolve top-level alternatives/conjunctions between whole
        # references (e.g. "1 egg or 2 tbsp flax", two full references
        # joined by a marker that Pass 4/5 could not resolve locally).
        i = 1
        while i < len(refs) - 1:
            if isinstance(refs[i], AlternativeMarker):
                marker, left, right = refs[i], refs[i - 1], refs[i + 1]
                if isinstance(left, IngredientReference) and isinstance(right, IngredientReference):
                    merged: ASTNode = AlternativeExpression(
                        children=cast(List[ASTNode], [left, right]), connective=marker
                    )
                    refs[i - 1:i + 2] = [merged]
                    i -= 1
            i += 1

        i = 1
        while i < len(refs) - 1:
            if isinstance(refs[i], ConjunctionMarker):
                marker, left, right = refs[i], refs[i - 1], refs[i + 1]
                if isinstance(left, IngredientReference) and isinstance(right, IngredientReference):
                    merged = ConjunctionExpression(
                        children=cast(List[ASTNode], [left, right]), connective=marker
                    )
                    refs[i - 1:i + 2] = [merged]
                    i -= 1
            i += 1

        return [r for r in refs if not isinstance(r, (AlternativeMarker, ConjunctionMarker))]


# ---------------------------------------------------------------------------
# DATABASE ORCHESTRATOR
# ---------------------------------------------------------------------------
#
# Required schema (created by init_db.py -- NOT by this module):
#
#   recipe_ingredient_lines_raw(
#       id, ingredient_block_id, recipe_id, recipe_section_id,
#       recipe_name, section_name, line_index, raw_text
#   )
#   lexical_spans(
#       span_id, recipe_ingredient_line_id, span_order, start_offset,
#       end_offset, text, normalized_value, span_type, knowledge_id,
#       source_vocabulary
#   )
#   ingredient_parse_trees(
#       id INTEGER PRIMARY KEY, recipe_ingredient_line_id INTEGER,
#       parse_tree_json TEXT
#   )
#
# `parse_tree_json` holds a serialized `ParseResult` (node_type
# "ParseResult", with a `candidates` list of one or more `Candidate`
# objects, each `{tree: <complete IngredientLine>, unresolved: [...]}`)
# -- not a single tree. An unambiguous line still produces exactly one
# candidate, so existing consumers that only need "the" tree can read
# `candidates[0].tree`, but the column now always carries the full
# candidate set rather than a pre-chosen interpretation.
#
# `recipe_ingredient_line_id` on both `lexical_spans` and
# `ingredient_parse_trees` is expected to reference
# `recipe_ingredient_lines_raw.id`. This module only reads `lexical_spans`
# and writes `ingredient_parse_trees`; it does not create, alter, or assume
# write access to any table.

def process_recipe_lines(db_path: Any = DB_PATH):
    db_path_str = str(db_path)
    conn = sqlite3.connect(db_path_str)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing_tables = {row['name'] for row in cursor.fetchall()}
    required_tables = {'lexical_spans', 'recipe_ingredient_lines_raw', 'ingredient_parse_trees'}

    missing_tables = required_tables - existing_tables
    if missing_tables:
        conn.close()
        raise RuntimeError(
            f"Expected tables {missing_tables} are missing in the database at {db_path_str}. "
            "Please run init_db.py first to create the necessary tables."
        )

    cursor.execute('''
        SELECT DISTINCT recipe_ingredient_line_id
        FROM lexical_spans
        ORDER BY recipe_ingredient_line_id
    ''')
    line_ids = [row['recipe_ingredient_line_id'] for row in cursor.fetchall()]

    parser = IngredientParser()

    for line_id in line_ids:
        cursor.execute('''
            SELECT * FROM lexical_spans
            WHERE recipe_ingredient_line_id = ?
            ORDER BY span_order ASC
        ''', (line_id,))

        spans = []
        for row in cursor.fetchall():
            row_keys = row.keys()
            token = LexicalToken(
                span_id=row['span_id'],
                span_order=row['span_order'],
                start_offset=row['start_offset'],
                end_offset=row['end_offset'],
                text=row['text'],
                normalized_value=row['normalized_value'],
                span_type=row['span_type'],
                knowledge_id=row['knowledge_id'] if 'knowledge_id' in row_keys else None,
                source_vocabulary=row['source_vocabulary'] if 'source_vocabulary' in row_keys else None,
            )
            spans.append(token)

        parse_result = parser.parse(spans)
        tree_json = json.dumps(parse_result.to_dict())

        cursor.execute('''
            INSERT INTO ingredient_parse_trees
            (recipe_ingredient_line_id, parse_tree_json)
            VALUES (?, ?)
        ''', (line_id, tree_json))

    conn.commit()
    conn.close()
    print(f"Successfully built ASTs for {len(line_ids)} ingredient lines.")

if __name__ == "__main__":
    process_recipe_lines(DB_PATH)