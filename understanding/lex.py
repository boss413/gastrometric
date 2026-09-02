"""
gastrometric.understanding.lex
===============================

Lexical analysis stage for Gastrometric.

Converts a raw ingredient clause (or any culinary text) into a deterministic,
ordered sequence of ``LexicalSpan`` objects. This module performs
RECOGNITION ONLY:

    * no grammatical interpretation
    * no ingredient grouping
    * no normalization of meaning (beyond numeric surface -> decimal value)
    * no confidence scoring
    * no observation creation
    * no identity resolution

This is a pure, reusable understanding component. It has no knowledge of
recipes, the database, or the pipeline. It accepts a string and returns
spans. Reading input text from a recipe/observation store, and writing
spans back out, are the job of pipeline adapters that live outside this
module.

Runtime vocabulary contract
----------------------------
Per the work order, all runtime vocabulary must come exclusively from
``gastrometric.knowledge.loader``, via its module-level ``knowledge``
singleton:

    from gastrometric.knowledge.loader import knowledge

    knowledge.phrase_index_for(vocabulary_class: str)
        -> Tuple[Mapping[str, Tuple[PhraseMatch, ...]], int]

``phrase_index_for`` returns an already-precomputed (lowercase phrase ->
matches, max_phrase_length_in_words) pair -- this module builds nothing
itself; it only looks up the per-class views each stage needs and reads
``knowledge_id`` / ``normalized_value`` off each ``PhraseMatch``.

    knowledge.vocabulary_classes: frozenset[str]
        Every vocabulary class known at process startup (precomputed from
        the DB's ``vocabulary_class`` column). Stages 3-7 use five fixed,
        named classes; Stage 8 sweeps whatever else is in this set --
        e.g. packaging, size, descriptor, modifier, state, seasoning,
        shape, ingredient_form, natural_portion, temperature -- with no
        hardcoded list on the lex.py side, so new seed-data vocabulary
        classes are picked up automatically.

Design note: resolving an apparent tension in the work order
--------------------------------------------------------------
The work order's Stage 1 (punctuation) lists ``-`` (hyphen), ``/`` and
implicitly ``.`` among preserved punctuation, worked via the example
``15-oz`` -> ``Quantity(15) Symbol("-") Measurement("oz")``. Its Stage 2
(numeric) simultaneously lists ``1/2``, ``1 1/2`` and ``1-1/4`` as
*supported numeric surface forms*, which requires treating the hyphen in
"1-1/4" and the slash in "1/2" as part of a single numeric token rather
than as standalone symbols.

These two instructions only agree if the ambiguous punctuation characters
(``-``, ``/``, ``.``) are treated as symbols UNLESS they participate in a
genuine numeric literal (mixed number, simple fraction, or decimal). This
module resolves the tension exactly that way: Stage 1 checks whether an
ambiguous punctuation character sits inside a numeric literal before
claiming it, and defers to Stage 2 when it does. This reproduces both
worked examples exactly:

    "15-oz"   -> Quantity(15), Symbol("-"), Measurement("oz")
    "1-1/4"   -> Quantity(1.25)   [text preserved as "1-1/4"]
    "1/2"     -> Quantity(0.5)    [text preserved as "1/2"]

This is a deliberate implementation decision, not an assumption about
vocabulary -- flagging it here since it isn't spelled out explicitly in
the work order.
"""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from gastrometric.knowledge.loader import knowledge


# ---------------------------------------------------------------------------
# Public data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LexicalSpan:
    """A single recognized span of the original text.

    No span references another span. Spans carry no grammatical meaning --
    that is the job of later stages (observation_builder.py,
    observation_analyzer.py).

    Multi-classification note (per work order "Refactor lex.py for
    Multi-Class Vocabulary"): a vocabulary term may now legitimately
    belong to more than one vocabulary class (e.g. "clove" is both an
    ``ingredient`` and a ``natural_portion``). ``span_types`` therefore
    carries ALL classifications the knowledge loader associated with this
    span's text, not just one -- the lexer performs no disambiguation and
    picks no "primary" classification; that is a downstream parser/
    analyzer concern. A uniquely classified term is simply a one-element
    tuple, e.g. ``("ingredient",)``.

    Ordering within ``span_types`` (and the parallel ``source_vocabulary``
    tuple) is first-seen order across the fixed stage sequence in
    ``lex()`` -- ingredient, measurement, preparation, brand, grammar,
    then any additional loader-exposed classes in sorted order -- with
    duplicates removed. This is deterministic without imposing an
    arbitrary/alphabetical resort on otherwise-unordered classifications;
    see ``_merge_vocabulary_spans`` for exactly where this happens.

    For non-vocabulary spans (Symbol, Quantity, Unknown) ``span_types``
    is always a single-element tuple naming that stage's fixed type.

    ``normalized_value`` / ``knowledge_id`` shape note: for those same
    non-vocabulary spans they remain plain scalars exactly as before
    (e.g. Quantity's ``normalized_value`` is a float like ``1.5``). For
    vocabulary-matched spans (Stages 3-8, identifiable by
    ``source_vocabulary is not None``), they are instead tuples
    positionally aligned with ``span_types`` -- index i of
    ``normalized_value``, ``knowledge_id`` and ``source_vocabulary`` all
    describe ``span_types[i]`` and nothing else. This is what keeps
    classifications' normalization independent of one another: for
    "ribs" with ``span_types=("Ingredient", "Component",
    "NaturalPortion")``, ``normalized_value`` might be
    ``("pork ribs", "rib", "rib")`` -- Ingredient keeps its own
    ingredient-alias-resolved canonical, while Component/NaturalPortion
    each keep their own generic-vocabulary canonical term, never
    borrowing Ingredient's (or each other's). A single-classified
    vocabulary span is still a 1-tuple, e.g. ``("carrot",)`` -- no
    implicit "unwrap when there's only one" special case. See
    ``_merge_vocabulary_spans`` for where this alignment is built and
    preserved.
    """

    span_types: Tuple[str, ...]
    text: str
    start_offset: int
    end_offset: int
    span_order: int
    normalized_value: Optional[Any] = None
    knowledge_id: Optional[Any] = None
    source_vocabulary: Optional[Tuple[str, ...]] = None


__all__ = ["LexicalSpan", "lex", "reconstruct"]


# ---------------------------------------------------------------------------
# Stage 1: Symbols (formerly "punctuation")
# ---------------------------------------------------------------------------

# Renamed from Punctuation to Symbol per review: these characters are
# lexical participants in culinary grammar (fraction separators, unit
# delimiters, package-weight parentheses), not mere English punctuation.
SYMBOL_CHARS = set(',.()/-"\'%&*+:;~')

# Characters that are ambiguous between "standalone symbol" and "part of a
# numeric literal". Resolved by checking numeric-literal membership first.
NUMERIC_AMBIGUOUS_CHARS = {"-", "/", "."}


# ---------------------------------------------------------------------------
# Stage 2: Numeric expressions
# ---------------------------------------------------------------------------

_UNICODE_FRACTION_CHARS = "½¼¾⅓⅔⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞"

FRACTION_VALUES: Dict[str, float] = {
    "¼": 0.25,
    "½": 0.5,
    "¾": 0.75,
    "⅓": 1 / 3,
    "⅔": 2 / 3,
    "⅕": 0.2,
    "⅖": 0.4,
    "⅗": 0.6,
    "⅘": 0.8,
    "⅙": 1 / 6,
    "⅚": 5 / 6,
    "⅛": 0.125,
    "⅜": 0.375,
    "⅝": 0.625,
    "⅞": 0.875,
}

NUMERIC_START_CHARS = set("0123456789" + _UNICODE_FRACTION_CHARS)

# Used to decide whether an ambiguous Stage-1 symbol is really part of a
# numeric literal (see module docstring "Design note").
_MIXED_HYPHEN_RE = re.compile(r"\d+-\d+/\d+")
_MIXED_SPACE_RE = re.compile(r"\d+\s+\d+/\d+")
_SIMPLE_FRACTION_RE = re.compile(r"\d+/\d+")
_DECIMAL_RE = re.compile(r"\d+\.\d+")

# Priority-ordered (most specific/longest form first) numeric matcher used
# by Stage 2 itself. Python regex alternation picks the first alternative
# that matches at a given anchored position, so ordering here is what
# gives mixed numbers priority over bare integers, etc.
NUMERIC_PATTERN = re.compile(
    r"(?P<mixed_hyphen>\d+-\d+/\d+)"
    r"|(?P<mixed_space>\d+\s+\d+/\d+)"
    rf"|(?P<digit_unicode_frac>\d+[{_UNICODE_FRACTION_CHARS}])"
    r"|(?P<simple_fraction>\d+/\d+)"
    r"|(?P<decimal>\d+\.\d+)"
    r"|(?P<integer>\d+)"
    rf"|(?P<unicode_frac>[{_UNICODE_FRACTION_CHARS}])"
)


def _numeric_reserved_positions(text: str) -> set:
    """Character offsets that belong to a numeric literal.

    Used only to decide whether Stage 1 should defer an ambiguous symbol
    character (-, /, .) to Stage 2.
    """
    reserved: set = set()
    for pattern in (_MIXED_HYPHEN_RE, _MIXED_SPACE_RE, _SIMPLE_FRACTION_RE, _DECIMAL_RE):
        for m in pattern.finditer(text):
            reserved.update(range(m.start(), m.end()))
    return reserved


def _fraction_value(fraction_text: str) -> float:
    num, den = fraction_text.split("/")
    return float(num) / float(den)


def _numeric_value(match: "re.Match[str]") -> Optional[float]:
    gd = match.groupdict()
    if gd.get("mixed_hyphen"):
        whole, frac = gd["mixed_hyphen"].split("-", 1)
        return float(whole) + _fraction_value(frac)
    if gd.get("mixed_space"):
        whole, frac = re.split(r"\s+", gd["mixed_space"].strip(), maxsplit=1)
        return float(whole) + _fraction_value(frac)
    if gd.get("digit_unicode_frac"):
        s = gd["digit_unicode_frac"]
        return float(s[:-1]) + FRACTION_VALUES[s[-1]]
    if gd.get("simple_fraction"):
        return _fraction_value(gd["simple_fraction"])
    if gd.get("decimal"):
        return float(gd["decimal"])
    if gd.get("integer"):
        return float(gd["integer"])
    if gd.get("unicode_frac"):
        return FRACTION_VALUES[gd["unicode_frac"]]
    return None


def _lex_numeric(text: str, claimed: bytearray) -> List[LexicalSpan]:
    spans: List[LexicalSpan] = []
    i = 0
    n = len(text)
    while i < n:
        if claimed[i] or text[i] not in NUMERIC_START_CHARS:
            i += 1
            continue
        m = NUMERIC_PATTERN.match(text, i)
        if not m:
            i += 1
            continue
        start, end = m.start(), m.end()
        value = _numeric_value(m)
        spans.append(
            LexicalSpan(
                span_types=("Quantity",),
                text=text[start:end],
                start_offset=start,
                end_offset=end,
                span_order=-1,
                normalized_value=value,
            )
        )
        for k in range(start, end):
            claimed[k] = 1
        i = end
    return spans


# ---------------------------------------------------------------------------
# Stages 3-8: vocabulary-driven recognition (ingredient, measurement,
# preparation, brand, grammar, and any additional runtime vocabularies)
# ---------------------------------------------------------------------------

_WORD_RE = re.compile(r"[A-Za-z][A-Za-z']*")


def _find_word_tokens(text: str) -> List[Tuple[str, int, int]]:
    """Maximal alphabetic runs, with offsets, independent of claim state."""
    return [(m.group(0), m.start(), m.end()) for m in _WORD_RE.finditer(text)]


def _phrase_match_metadata(match: Any) -> Tuple[Any, Any, Any]:
    """Pull (knowledge_id, normalized_value, source_vocabulary) off a
    PhraseMatch.

    Real shape, confirmed against actual loader output:

        PhraseMatch(tokens=('fl', 'oz'), canonical='fl oz', vocabulary_class='measurement')

    There is no ``knowledge_id`` field in this data model at all --
    ``canonical`` is the closest thing to an identity a PhraseMatch
    carries, and is used here as ``normalized_value``. If a true
    database-level id is needed downstream, that's a join against
    ``canonical`` (or a proper ingredient/vocabulary id table) performed
    by a later stage, not something this module can synthesize.
    """
    canonical = getattr(match, "canonical", None)
    vocabulary_class = getattr(match, "vocabulary_class", None)
    return None, canonical, vocabulary_class


# Connector character a vocabulary phrase's FIRST token may itself
# contain with no surrounding whitespace (e.g. "bone-in" is indexed as
# its own literal first-token key, distinct from "bone" -> tokens=
# ("bone", "in")). Confirmed against real phrase_index_for() output.
# When two adjacent word-regex tokens are joined by exactly one such
# character, the merged string is tried as an ADDITIONAL bucket key
# alongside the plain (unmerged) first token -- both segmentations are
# legitimate and may both match (this mirrors the existing "grape
# tomatoes" / "grape" / "tomatoes" overlap philosophy: the lexer doesn't
# choose, later stages do).
_MERGEABLE_FIRST_TOKEN_CHARS = {"-"}


def _first_token_variants(
    text: str, words: Sequence[Tuple[str, int, int]], start_idx: int
) -> List[Tuple[str, int]]:
    """Candidate (first_token_string, words_consumed) pairs to use as
    phrase_index bucket keys starting at words[start_idx].
    """
    variants = [(words[start_idx][0].lower(), 1)]
    if start_idx + 1 < len(words):
        gap = text[words[start_idx][2] : words[start_idx + 1][1]]
        if gap in _MERGEABLE_FIRST_TOKEN_CHARS:
            merged = words[start_idx][0] + gap + words[start_idx + 1][0]
            variants.append((merged.lower(), 2))
    return variants


def _extend_candidates(
    text: str,
    claimed: bytearray,
    words: Sequence[Tuple[str, int, int]],
    start_idx: int,
    first_token: str,
    consumed: int,
    max_extra_words: int,
) -> Iterable[Tuple[Tuple[str, ...], int]]:
    """Yield (candidate_tokens, end_idx) pairs starting from the given
    first-token variant, extending with zero or more subsequent plain
    words joined only by whitespace. ``candidate_tokens`` is compared
    directly against ``PhraseMatch.tokens`` -- this module never guesses
    phrase length, it only proposes candidates and lets the loader's own
    token tuples decide what matches.
    """
    base_end_idx = start_idx + consumed - 1
    if base_end_idx >= len(words):
        return
    covered_words = range(start_idx, base_end_idx + 1)
    if any(claimed[k] for idx in covered_words for k in range(words[idx][1], words[idx][2])):
        return

    candidate: Tuple[str, ...] = (first_token,)
    end_idx = base_end_idx
    yield candidate, end_idx

    n = len(words)
    for _ in range(max_extra_words):
        next_idx = end_idx + 1
        if next_idx >= n:
            break
        gap = text[words[end_idx][2] : words[next_idx][1]]
        if not gap or not gap.isspace():
            break
        w_start, w_end = words[next_idx][1], words[next_idx][2]
        if any(claimed[k] for k in range(w_start, w_end)):
            break
        candidate = candidate + (words[next_idx][0].lower(),)
        end_idx = next_idx
        yield candidate, end_idx


def _match_vocabulary(
    text: str,
    claimed: bytearray,
    words: Sequence[Tuple[str, int, int]],
    phrase_index: Mapping[str, Tuple[Any, ...]],
    max_words: int,
    span_type: str,
) -> List[LexicalSpan]:
    """Emit spans for ALL overlapping vocabulary matches (not only the
    longest) for one vocabulary category.

    Matching algorithm (bucket-by-first-token + exact tokens-tuple
    verification, per the real loader contract): look the candidate's
    first token up in ``phrase_index`` to get a bucket of PhraseMatch
    candidates, then only accept a match when the full candidate token
    sequence equals ``match.tokens`` exactly. The lexer proposes
    candidate token sequences (including a hyphen-merged first-token
    variant); it never assumes the loader has already filtered anything,
    and it never builds a joined-substring dict key.

    Claiming is deferred until the whole category has been scanned, so
    overlapping matches within this stage remain independently visible.
    """
    spans: List[LexicalSpan] = []
    claim_ranges: List[Tuple[int, int]] = []
    n = len(words)

    for start_idx in range(n):
        w_start = words[start_idx][1]
        if claimed[w_start]:
            continue

        for first_token, consumed in _first_token_variants(text, words, start_idx):
            bucket = phrase_index.get(first_token)
            if not bucket:
                continue

            max_extra = max(0, max_words - 1)
            for candidate, end_idx in _extend_candidates(
                text, claimed, words, start_idx, first_token, consumed, max_extra
            ):
                matches = [m for m in bucket if getattr(m, "tokens", None) == candidate]
                if not matches:
                    continue
                phrase_start = words[start_idx][1]
                phrase_end = words[end_idx][2]
                phrase_text = text[phrase_start:phrase_end]
                for match in matches:
                    knowledge_id, normalized_value, source_vocabulary = _phrase_match_metadata(match)
                    spans.append(
                        LexicalSpan(
                            span_types=(span_type,),
                            text=phrase_text,
                            start_offset=phrase_start,
                            end_offset=phrase_end,
                            span_order=-1,
                            # Wrapped as 1-tuples, positionally aligned
                            # with span_types, even though there's only
                            # one classification here -- this is what
                            # _merge_vocabulary_spans relies on to keep
                            # each classification's own normalized value
                            # from bleeding into a different
                            # classification's when spans covering the
                            # same text range get merged (see that
                            # function's docstring, and LexicalSpan's).
                            normalized_value=(normalized_value,),
                            knowledge_id=(knowledge_id,),
                            source_vocabulary=(source_vocabulary or span_type.lower(),),
                        )
                    )
                claim_ranges.append((phrase_start, phrase_end))

    for start, end in claim_ranges:
        for k in range(start, end):
            claimed[k] = 1
    return spans


def _merge_vocabulary_spans(spans: List[LexicalSpan]) -> List[LexicalSpan]:
    """Collapse vocabulary-matched spans covering the EXACT same text
    range (identical start/end offsets) into a single ``LexicalSpan``
    carrying every classification found for that range.

    Why this exists: ``lex()`` now matches each Stage 3-8 vocabulary
    category (ingredient, measurement, preparation, brand, grammar, plus
    any additional loader-exposed classes) against its own snapshot of
    claimed positions, rather than letting the first category to claim a
    word lock every later category out of it. That is what makes
    multi-class terms possible at all (e.g. "clove" matching both
    ``ingredient`` and ``natural_portion``) -- see the call site in
    ``lex()``. Left unmerged, this would surface as multiple spans with
    identical offsets and different ``span_types``, one per matching
    category. Per the work order, that's exactly the outcome to avoid:
    one textual occurrence must produce exactly one ``LexicalSpan``,
    carrying the full set of classifications, with none discarded.

    Grouping key is the exact ``(start_offset, end_offset)`` pair --
    i.e. the identical textual match. Overlapping-but-different-length
    matches (e.g. "grape" vs "grape tomatoes" at different offsets, or a
    shorter/longer phrase within the same category) are a pre-existing,
    intentional ambiguity this work order explicitly leaves alone, and
    are NOT merged here; they remain separate spans exactly as before.

    Ordering rule for the merged ``span_types`` tuple: first-seen order
    over ``spans`` as passed in (which reflects the fixed,
    already-deterministic stage sequence in ``lex()``), with duplicates
    removed. Classifications are never alphabetically resorted -- the
    work order asks for that only if a stable order is otherwise
    required, and stage sequence already provides one.

    Per-classification normalization (the ingredient-alias boundary fix):
    each contributing span already carries its own ``normalized_value``/
    ``knowledge_id``/``source_vocabulary`` as 1-tuples describing ONLY
    its own single classification (see ``_match_vocabulary``). Merging
    must keep that alignment -- classification i's normalized value here
    must come from classification i's own PhraseMatch, never from a
    different classification's. Concretely: if "ribs" resolves as an
    Ingredient alias to "pork ribs" AND separately as a generic
    Component/NaturalPortion vocabulary term canonicalized as "rib",
    picking a single shared normalized_value for the merged span (e.g.
    "first non-None wins") would incorrectly stamp the ingredient
    alias's resolution onto the generic vocabulary classifications too.
    Instead we build one (classification -> its own normalized_value /
    knowledge_id / source_vocabulary) mapping, first-seen per
    classification, and only then flatten it back into tuples aligned
    with the deduplicated ``span_types`` order. A classification that
    appears more than once in the group (e.g. duplicate canonicals
    within one category) keeps its first-seen values, matching how a
    single-classified span already behaved.
    """
    groups: "Dict[Tuple[int, int], List[LexicalSpan]]" = {}
    order: List[Tuple[int, int]] = []
    for span in spans:
        key = (span.start_offset, span.end_offset)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(span)

    merged: List[LexicalSpan] = []
    for key in order:
        group = groups[key]
        if len(group) == 1:
            merged.append(group[0])
            continue

        types_in_order: List[str] = []
        normalized_by_type: Dict[str, Optional[Any]] = {}
        knowledge_by_type: Dict[str, Optional[Any]] = {}
        source_by_type: Dict[str, Optional[str]] = {}

        for span in group:
            span_normalized = span.normalized_value if isinstance(span.normalized_value, tuple) else ()
            span_knowledge = span.knowledge_id if isinstance(span.knowledge_id, tuple) else ()
            span_sources = span.source_vocabulary or ()
            for i, classification in enumerate(span.span_types):
                if classification in normalized_by_type:
                    continue  # already recorded from an earlier (first-seen) span
                types_in_order.append(classification)
                normalized_by_type[classification] = (
                    span_normalized[i] if i < len(span_normalized) else None
                )
                knowledge_by_type[classification] = (
                    span_knowledge[i] if i < len(span_knowledge) else None
                )
                source_by_type[classification] = span_sources[i] if i < len(span_sources) else None

        merged.append(
            dataclasses.replace(
                group[0],
                span_types=tuple(types_in_order),
                normalized_value=tuple(normalized_by_type[t] for t in types_in_order),
                knowledge_id=tuple(knowledge_by_type[t] for t in types_in_order),
                source_vocabulary=tuple(source_by_type[t] for t in types_in_order),
            )
        )
    return merged


def _additional_vocab_span_type(vocab_name: str) -> str:
    """Derive a span_type label for a Stage-8 loader-exposed vocabulary
    category that wasn't one of the named stages (ingredient/measurement/
    preparation/brand/grammar). E.g. "allergen" -> "Allergen".
    """
    words = re.split(r"[\s_\-]+", vocab_name.strip())
    return "".join(w.capitalize() for w in words if w) or "Vocabulary"


# ---------------------------------------------------------------------------
# Stage 9: Unknown
# ---------------------------------------------------------------------------


def _lex_unknown(text: str, claimed: bytearray) -> List[LexicalSpan]:
    spans: List[LexicalSpan] = []
    i = 0
    n = len(text)
    while i < n:
        if claimed[i] or text[i].isspace():
            i += 1
            continue
        start = i
        while i < n and not claimed[i] and not text[i].isspace():
            i += 1
        end = i
        spans.append(
            LexicalSpan(
                span_types=("Unknown",),
                text=text[start:end],
                start_offset=start,
                end_offset=end,
                span_order=-1,
            )
        )
        for k in range(start, end):
            claimed[k] = 1
    return spans


# ---------------------------------------------------------------------------
# Loader access (Stage 0: runtime vocabulary acquisition)
# ---------------------------------------------------------------------------


_NAMED_CLASSES = {"ingredient", "measurement", "preparation", "brand", "grammar"}


def _load_all_vocabularies() -> Dict[str, Tuple[Mapping[str, Tuple[Any, ...]], int]]:
    """The ONLY place this module talks to gastrometric.knowledge.loader.

    Returns a dict of stage-name -> (phrase_index, max_words) for the five
    named vocabularies, keyed for stages 3-7 respectively. Nothing here is
    built by this module -- `knowledge` already precomputed every phrase
    index at process startup. This just picks out the five per-class
    views the stages need.
    """
    return {
        "ingredient": knowledge.phrase_index_for("ingredient"),
        "measurement": knowledge.phrase_index_for("measurement"),
        "preparation": knowledge.phrase_index_for("preparation"),
        "brand": knowledge.phrase_index_for("brand"),
        "grammar": knowledge.phrase_index_for("grammar"),
    }


def _load_additional_vocabularies() -> Dict[str, Tuple[Mapping[str, Tuple[Any, ...]], int]]:
    """Stage 8: every vocabulary class the loader knows about beyond the
    five named ones (packaging, size, descriptor, modifier, state,
    seasoning, shape, ingredient_form, natural_portion, temperature, and
    whatever else shows up in the seed data later). No hardcoded list on
    the lex.py side -- driven entirely by `knowledge.vocabulary_classes`.
    """
    return {
        vocabulary_class: knowledge.phrase_index_for(vocabulary_class)
        for vocabulary_class in knowledge.vocabulary_classes - _NAMED_CLASSES
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def lex(text: str) -> List[LexicalSpan]:
    """Lex a raw ingredient clause (or any culinary text) into an ordered,
    deterministic sequence of LexicalSpan objects.

    Pure function: no I/O, no database access, no mutation of shared
    state. All runtime vocabulary is pulled from
    gastrometric.knowledge.loader at call time.

    Every character of ``text`` is accounted for by some span, except
    whitespace that is not part of a numeric literal (offsets alone are
    sufficient to reconstruct the original text -- see ``reconstruct``).
    """
    if not isinstance(text, str):
        raise TypeError(f"lex() requires a str, got {type(text).__name__}")

    claimed = bytearray(len(text))
    spans: List[LexicalSpan] = []
    words = _find_word_tokens(text)

    # Stage 1: Symbols (deferring numeric-ambiguous characters and any
    # character our own tokenizer already treats as word-internal, e.g.
    # the apostrophe in "bob's" -- WORD_RE's char class includes it, so
    # Stage 1 must not fragment it out from under Stage 3-8's matching).
    reserved = _numeric_reserved_positions(text)
    word_reserved: set = set()
    for _word_text, w_start, w_end in words:
        word_reserved.update(range(w_start, w_end))
    for i, ch in enumerate(text):
        if claimed[i]:
            continue
        if ch in SYMBOL_CHARS:
            if ch in NUMERIC_AMBIGUOUS_CHARS and i in reserved:
                continue
            if i in word_reserved:
                continue
            spans.append(
                LexicalSpan(
                    span_types=("Symbol",),
                    text=ch,
                    start_offset=i,
                    end_offset=i + 1,
                    span_order=-1,
                )
            )
            claimed[i] = 1

    # Stage 2: Numeric expressions
    spans.extend(_lex_numeric(text, claimed))

    # Stages 3-8: named + additional runtime vocabularies.
    #
    # Each category is matched against its OWN copy of ``claimed``,
    # snapshotted here right after Stage 1/2 -- not the shared, mutating
    # ``claimed`` array. Previously every category claimed its matched
    # ranges on that shared array before the next category ran, which
    # meant the first category to match a word silently locked every
    # later category out of it -- exactly the "arbitrarily selecting one
    # class" behavior the work order requires the lexer to stop doing. A
    # term such as "clove" can now match in both the ``ingredient`` and
    # ``natural_portion`` categories independently; ``_merge_vocabulary_
    # spans`` below then collapses same-range matches from different
    # categories into one LexicalSpan carrying every classification,
    # rather than emitting one span per category. Only after that merge
    # are the combined ranges claimed on the real ``claimed`` array, so
    # Stage 9 (Unknown) still sees the union of everything matched here,
    # same as before.
    pre_vocab_claimed = bytes(claimed)
    vocab_spans: List[LexicalSpan] = []

    vocabularies = _load_all_vocabularies()

    ing_index, ing_max = vocabularies["ingredient"]
    vocab_spans.extend(
        _match_vocabulary(text, bytearray(pre_vocab_claimed), words, ing_index, ing_max, "Ingredient")
    )

    meas_index, meas_max = vocabularies["measurement"]
    vocab_spans.extend(
        _match_vocabulary(text, bytearray(pre_vocab_claimed), words, meas_index, meas_max, "Measurement")
    )

    prep_index, prep_max = vocabularies["preparation"]
    vocab_spans.extend(
        _match_vocabulary(text, bytearray(pre_vocab_claimed), words, prep_index, prep_max, "Preparation")
    )

    brand_index, brand_max = vocabularies["brand"]
    vocab_spans.extend(
        _match_vocabulary(text, bytearray(pre_vocab_claimed), words, brand_index, brand_max, "Brand")
    )

    grammar_index, grammar_max = vocabularies["grammar"]
    vocab_spans.extend(
        _match_vocabulary(text, bytearray(pre_vocab_claimed), words, grammar_index, grammar_max, "Grammar")
    )

    # Stage 8: any remaining runtime vocabularies exposed by the loader.
    # Iterated in sorted-by-name order for determinism -- the loader
    # exposes this set as a frozenset (no defined iteration order), and
    # merge order now matters because it feeds the first-seen ordering
    # of _merge_vocabulary_spans's span_types tuple. This affects only
    # classification tie-breaking, not matching behavior.
    additional_vocabularies = _load_additional_vocabularies()
    for vocab_name in sorted(additional_vocabularies):
        idx, mx = additional_vocabularies[vocab_name]
        span_type = _additional_vocab_span_type(vocab_name)
        vocab_spans.extend(_match_vocabulary(text, bytearray(pre_vocab_claimed), words, idx, mx, span_type))

    merged_vocab_spans = _merge_vocabulary_spans(vocab_spans)
    spans.extend(merged_vocab_spans)
    for span in merged_vocab_spans:
        for k in range(span.start_offset, span.end_offset):
            claimed[k] = 1

    # Stage 9: Unknown
    spans.extend(_lex_unknown(text, claimed))

    # Final ordering: left-to-right by start offset; when multiple spans
    # share a start offset (overlapping vocabulary matches), the longest
    # is presented first, consistent with "longest match" precedence.
    spans.sort(key=lambda s: (s.start_offset, -(s.end_offset - s.start_offset)))
    ordered = [dataclasses.replace(s, span_order=i) for i, s in enumerate(spans)]
    return ordered


def reconstruct(spans: Sequence[LexicalSpan], original_text: str) -> str:
    """Rebuild the original text from spans, filling any gaps between
    spans with the corresponding slice of ``original_text`` (this is
    exactly the omitted whitespace permitted by the work order). Useful
    for tests / verification that no character was silently dropped.
    """
    pieces: List[str] = []
    cursor = 0
    for span in sorted(spans, key=lambda s: s.start_offset):
        if span.start_offset > cursor:
            pieces.append(original_text[cursor : span.start_offset])
        if span.start_offset >= cursor:
            pieces.append(span.text)
            cursor = max(cursor, span.end_offset)
    if cursor < len(original_text):
        pieces.append(original_text[cursor:])
    return "".join(pieces)