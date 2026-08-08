"""
parse_ingredient_lines.py

SPAN-BASED EVIDENCE EXTRACTOR.

This module answers exactly one question about a raw ingredient line:

    What recognizable grammatical structures and vocabulary occur in
    this line, and where?

It does NOT decide what the ingredient "is". It does not assemble a
name, attach modifiers to an ingredient, or resolve ambiguity. Every
recognized piece of the line — a quantity, a unit, a known ingredient
phrase, a preparation word, a stray "and" — is emitted as a Span and
kept. Nothing is deleted to "leave the name behind"; there is no name
built here at all.

Pipeline position:

    RAW RECIPE -> INGEST -> PARSE -> NORMALIZE -> IDENTITY RESOLUTION -> ...
                             ^^^^^
                        this file lives here

Downstream stages (not implemented here — see the stub modules
referenced at the bottom of this docstring) consume `recognized_spans`:

    normalize_ingredient_observations.py
        Assembles vocabulary around ingredient candidates, decides
        which modifiers/preparations attach to which ingredient,
        decides whether "ham and bean soup mix" is one ingredient or
        two, expands truncated/corrupted tokens (lexical inference,
        not grammar), and produces normalized ingredient observations.

    identity_resolution.py
        Resolves normalized observations to canonical ingredient
        entities, aliases, and ingredient relationships; handles
        ambiguity a normalized observation still carries.

    nutrition_resolution.py
        USDA entity selection, gram conversion, density, package
        weights, yield adjustments for a resolved ingredient.

    analyzer.py
        Reads parser spans directly (not normalized output) and asks
        the diagnostic questions a human curator needs answered: did
        the parser find zero/multiple ingredients, leave unknown
        spans, leave modifiers unattached, fail to associate a
        quantity or package expression, etc. Produces the curator
        interface.

SPAN GRAMMAR CLASSES this parser draws on and is allowed to consult for
recognition purposes ONLY (dictionary lookups, not semantic reasoning):

    parser vocabulary       Pure grammar/tokenization: hedge phrases,
                             physical unit-routing facts, cut-pattern
                             sentence templates. Permanent, lives in this
                             file (see PARSE-TIME GRAMMAR below).

    culinary vocabulary     CulinaryVocabulary (gastrometric/knowledge/
                             loader.py): preparation, state, temperature,
                             modifier, seasoning, descriptor,
                             ingredient_form, size, natural_portion,
                             packaging, measurement, brand terms.

    ingredient identities/
    aliases                 VocabularyProvider.ingredient_identities():
                             every known ingredient name/alias, single-
                             or multi-word, used for longest-match span
                             recognition ONLY — the parser does not
                             attach an ingredient_id-bearing identity
                             claim beyond "this text matches a known
                             name/alias string" (see IngredientSpan
                             below for the honest limit of what that
                             means with the current provider interface).
"""

import sqlite3
import json
import re
import sys
import time
import uuid
import logging
from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict

from gastrometric.config.paths import DB_PATH
from gastrometric.config.vocabulary_provider import (
    StaticVocabularyProvider,
    DatabaseVocabularyProvider,
)
from gastrometric.knowledge.loader import CulinaryVocabulary

_logger = logging.getLogger(__name__)


# ============================================================
# PARSE-TIME GRAMMAR (permanent, owned by the parsing stage)
# Grammar/tokenization only — sentence structure, hedge phrases, and
# closed physical-unit facts. No culinary word list lives here; those
# come from CulinaryVocabulary/VocabularyProvider at runtime.
# ============================================================

NOISE_PHRASES = [
    "plus more", "as needed", "to taste", "if desired",
    "as desired", "optional", "if needed", "add more"
]

GRAM_UNITS = frozenset({
    'g', 'kg', 'gram', 'grams', 'kilogram', 'kilograms',
})
ML_UNITS = frozenset({
    'ml', 'mls', 'milliliter', 'milliliters', 'millilitre', 'millilitres',
    'liter', 'liters', 'litre', 'litres', 'l',
})
IMPERIAL_WEIGHT_UNITS = frozenset({
    'oz', 'ounce', 'ounces', 'lb', 'pound', 'pounds',
})
IMPERIAL_VOLUME_UNITS = frozenset({
    'cup', 'cups',
    'tbsp', 'tablespoon', 'tablespoons',
    'tsp', 'teaspoon', 'teaspoons',
    'pint', 'pints',
    'quart', 'quarts', 'qt',
    'gallon', 'gallons',
    'fl oz', 'fluid ounce', 'fluid ounces',
})

CUT_TEMPLATE_PATTERNS = [
    r'(?:cut|sliced|chopped|torn)\s+(?:crosswise\s+|lengthwise\s+)?'
    r'(?:in|into)\s+(?:\d[\d./]*\s*(?:x|by)\s*)?\d[\d./]*(?:-inch|")[a-z-]*\s+'
    r'(?:{shapes})',
    r'(?:cut|sliced|chopped|torn)\s+(?:crosswise\s+|lengthwise\s+)?'
    r'(?:in|into)\s+\d[\d./]*\s+'
    r'(?:{shapes})',
    r'(?:cut|sliced|chopped|torn)\s+into\s+'
    r'(?:bite[- ]sized\s+|large\s+|small\s+)?(?:{shapes})',
]

GRAMMAR_ONLY_PREP_PATTERNS = [
    r'cut in half',
    r'cut in \d+ pieces',
    r'cut in \d+',
    r'cut crosswise into [^,;]+',
    r'sliced crosswise into [^,;]+',
    r'crosswise into [^,;]+',
    r'cut lengthwise into [^,;]+',
    r'sliced lengthwise into [^,;]+',
    r'lengthwise into [^,;]+',
]


# ============================================================
# SPAN DATA MODEL
# The parser's entire output shape. See recipe_ingredient_spans in the
# schema notes at the bottom of this file for how this maps to storage.
# ============================================================

SPAN_TYPES = (
    "Ingredient", "IngredientAlias", "Modifier", "Preparation", "State",
    "Temperature", "Brand", "Measurement", "PackageExpression",
    "WeightExpression", "VolumeExpression", "NaturalPortionExpression",
    "GrammarMarker", "Noise", "Unknown",
)


@dataclass
class Span:
    span_type: str
    raw_text: str
    normalized_text: str
    start_offset: Optional[int] = None
    end_offset: Optional[int] = None
    vocabulary_id: Optional[str] = None
    ingredient_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    parser_order: int = 0

    def to_dict(self):
        return {
            "start_offset":   self.start_offset,
            "end_offset":     self.end_offset,
            "raw_text":       self.raw_text,
            "normalized_text": self.normalized_text,
            "span_type":      self.span_type,
            "vocabulary_id":  self.vocabulary_id,
            "ingredient_id":  self.ingredient_id,
            "metadata_json":  json.dumps(self.metadata) if self.metadata else None,
            "parser_order":   self.parser_order,
        }


def _locate(source, needle, cursor=0):
    """Best-effort forward search for `needle` in `source`, starting no
    earlier than `cursor`. Case-insensitive; word-boundary-anchored when
    `needle` is alphabetic (so "cup" doesn't match inside "cupboard").
    Returns (start, end) or None.

    THIS IS DELIBERATELY BEST-EFFORT, NOT EXACT OFFSET MATH. Several
    extraction passes work off a normalized copy of the text
    (word-numbers -> digits, unicode fractions -> decimals, "T"/"t" ->
    tbsp/tsp, "1/2\"" -> "0.5-inch", ...), so a matched value doesn't
    always appear verbatim in the original source. This is the same
    trade-off the codebase already made for evidence recovery (see the
    prior _recover_original_span, which this generalizes) rather than a
    new one introduced by this refactor: a full formal offset-alignment
    between raw and normalized text would require rewriting every
    numeral-handling regex in normalize_text to operate offset-preserving
    over the raw text directly, which is a much larger, higher-risk
    change than the one requested here. When a match can't be found,
    callers record the span with start_offset/end_offset = None rather
    than fabricate a position — see _SpanEmitter.emit.
    """
    if not needle:
        return None
    anchor = r'\b' if re.search(r'[a-zA-Z]', needle[0]) and re.search(r'[a-zA-Z]', needle[-1]) else ''
    try:
        pat = re.compile(anchor + re.escape(needle) + anchor, re.IGNORECASE)
    except re.error:
        return None
    m = pat.search(source, cursor)
    if m:
        return m.start(), m.end()
    # A match earlier than `cursor` is still real evidence, just not in
    # the expected left-to-right order (can happen when an earlier
    # extraction step ran out of source order, or normalization
    # reordered/collapsed tokens).
    m = pat.search(source)
    if m:
        return m.start(), m.end()
    return None


class _SpanEmitter:
    """Accumulates recognized_spans for one line/slot's evidence scope.

    `source` is the case-preserved ORIGINAL text for this scope (before
    normalize_text ran) — every span's offsets are resolved against it
    via best-effort forward search (_locate), advancing a cursor so
    repeated identical tokens resolve in left-to-right order rather than
    all collapsing onto the first occurrence.
    """

    def __init__(self, source, cursor=0):
        self.source = source or ""
        self.cursor = cursor
        self.spans: List[Span] = []
        self._order = 0

    def emit(self, span_type, normalized_text, metadata=None, raw_hint=None,
             vocabulary_id=None, ingredient_id=None):
        assert span_type in SPAN_TYPES, f"unknown span_type {span_type!r}"
        needle = raw_hint if raw_hint is not None else normalized_text
        loc = _locate(self.source, needle, self.cursor)
        if loc:
            start, end = loc
            raw_text = self.source[start:end]
            self.cursor = end
        else:
            start = end = None
            raw_text = needle
        span = Span(
            span_type=span_type,
            raw_text=raw_text,
            normalized_text=normalized_text,
            start_offset=start,
            end_offset=end,
            vocabulary_id=vocabulary_id,
            ingredient_id=ingredient_id,
            metadata=metadata or {},
            parser_order=self._order,
        )
        self._order += 1
        self.spans.append(span)
        return span

    def sorted_spans(self):
        """Spans ordered for human/debug consumption: by source position
        when known, pipeline order otherwise. parser_order on each Span
        still reflects true pipeline sequence regardless of this
        ordering."""
        return sorted(
            self.spans,
            key=lambda s: (s.start_offset if s.start_offset is not None else 10**9, s.parser_order)
        )


# ============================================================
# VOCABULARY INJECTION SEAM
#
# Two independent runtime sources, compiled once into the regex/set
# shapes the algorithm actually needs:
#
#   VocabularyProvider    Ingredient identity ONLY — every known
#                          ingredient name/alias, single- or multi-word.
#                          Backed by `ingredients`/`ingredient_aliases`.
#                          Exposed as ingredient_identities() — renamed
#                          from the old protected_phrases(), which
#                          filtered to multi-word terms only (it existed
#                          purely to shield phrases from being split by
#                          later regex passes). That filtering no longer
#                          makes sense: under the span model EVERY known
#                          ingredient name, single-word included, needs
#                          to be recognized and emitted as an
#                          IngredientSpan (e.g. "tuna"), not just
#                          protected from splitting. See
#                          vocabulary_provider.py.
#
#   CulinaryVocabulary     Everything else, read through its public
#                          per-category accessor methods, bridged via
#                          _fetch_vocab_class (see below — the loader has
#                          no single generic by_class(name) method).
#
# `_ACTIVE` is built LAZILY: CulinaryVocabulary() does real I/O and can
# raise if the schema is missing, so importing this module must never
# have that side effect. The real pipeline entry point (`_run`) calls
# `set_vocabulary(...)` explicitly.
# ============================================================

_VOCAB_CLASSES = [
    "measurement", "packaging", "natural_portion", "preparation",
    "ingredient_form", "size", "descriptor", "shape", "state",
    "temperature", "modifier", "seasoning", "brand",
]

# Classes folded together into ONE evidence-scanning pass, tagged by
# named regex group so a single left-to-right finditer still produces
# properly-typed spans (Preparation/State/Temperature/Modifier/Brand)
# instead of losing which vocabulary class matched. "modifier" absorbs
# descriptor/ingredient_form/seasoning/size — none of those has its own
# entry in the span_type enum (see SPAN_TYPES); they are all, at the
# grammar level, the same kind of thing: a word that modifies the
# ingredient rather than naming, measuring, or preparing it. Which
# specific culinary distinction a given modifier represents is exactly
# the kind of interpretation this parser is no longer responsible for.
_EVIDENCE_GROUP_CLASSES = {
    "preparation": ("preparation",),
    "state":       ("state",),
    "temperature": ("temperature",),
    "modifier":    ("descriptor", "ingredient_form", "seasoning", "size", "modifier"),
    "brand":       ("brand",),
}
_EVIDENCE_GROUP_SPAN_TYPE = {
    "preparation": "Preparation",
    "state":       "State",
    "temperature": "Temperature",
    "modifier":    "Modifier",
    "brand":       "Brand",
}

_VOCAB_CLASS_ACCESSORS = {
    "measurement": ("measurements",),
    "packaging": ("packaging",),
    "natural_portion": ("natural_portions",),
    "preparation": ("preparations",),
    "ingredient_form": ("ingredient_forms",),
    "size": ("sizes",),
    "descriptor": ("descriptors",),
    "shape": ("shapes",),
    "state": ("state",),
    "temperature": ("temperature", "temperatuure"),
    "modifier": ("modifier",),
    "seasoning": ("seasoning",),
    "brand": ("brand",),
}


def _fetch_vocab_class(culinary, cls):
    """Fetch one CulinaryVocabulary class's term set through its public
    API, trying each candidate accessor in _VOCAB_CLASS_ACCESSORS.
    Classes with no working accessor fall back to the loader's private
    _class_members(cls) rather than hard-failing, logging a warning each
    time — reaching past a module's public contract should never be
    silent."""
    for name in _VOCAB_CLASS_ACCESSORS.get(cls, ()):
        accessor = getattr(culinary, name, None)
        if accessor is not None:
            return set(accessor())
    if hasattr(culinary, "_class_members"):
        _logger.warning(
            "CulinaryVocabulary has no public accessor for class '%s' — "
            "falling back to the private _class_members() API. Add a "
            "public accessor to gastrometric/knowledge/loader.py "
            "(matching its sibling one-liners) to remove this warning.",
            cls
        )
        return set(culinary._class_members(cls))
    raise AttributeError(
        f"CulinaryVocabulary exposes no way to fetch vocabulary class "
        f"'{cls}' — no public accessor in {_VOCAB_CLASS_ACCESSORS.get(cls, ())} "
        f"and no _class_members() fallback available."
    )


def _sorted_fragments(terms, flexible_separator=False):
    """Escape and order a set of vocabulary terms for use inside a regex
    alternation: longest first (by token count, then character length),
    so multi-word terms outrank single-word substrings."""
    sep = r'[\s-]+' if flexible_separator else r'\s+'
    fragments = []
    for term in terms:
        tokens = term.split()
        escaped = sep.join(re.escape(t) for t in tokens)
        fragments.append((len(tokens), len(term), escaped))
    fragments.sort(key=lambda f: (f[0], f[1]), reverse=True)
    return fragments


def _word_pattern(terms):
    """Word-boundary-safe, case-insensitive alternation from a set of
    vocabulary terms."""
    if not terms:
        return re.compile(r'(?!)')
    fragments = _sorted_fragments(terms)
    return re.compile(
        r'\b(?:' + '|'.join(f[2] for f in fragments) + r')\b',
        re.IGNORECASE
    )


class CompiledVocabulary:
    def __init__(self, provider, culinary):
        # -- ingredient identity (VocabularyProvider only) --
        self.ingredient_identities = list(provider.ingredient_identities())

        # -- every CulinaryVocabulary class this parser draws on --
        self.by_class = {
            cls: _fetch_vocab_class(culinary, cls) for cls in _VOCAB_CLASSES
        }

        # -- units: measurement + packaging + natural portions --
        self.unit_vocab = (
            self.by_class["measurement"]
            | self.by_class["packaging"]
            | self.by_class["natural_portion"]
        )
        self.unit_pattern = _word_pattern(self.unit_vocab)

        # -- cut-pattern grammar, {shapes} filled in from by_class["shape"] --
        shapes = sorted(self.by_class["shape"], key=len, reverse=True)
        shape_alternation = (
            '|'.join(re.escape(s) for s in shapes) if shapes else r'(?!)'
        )
        self.cut_patterns = [
            template.format(shapes=shape_alternation)
            for template in CUT_TEMPLATE_PATTERNS
        ]

        # -- size / clove-bare-descriptor patterns (grammar helpers,
        # not evidence spans themselves) --
        self.size_pattern = _word_pattern(self.by_class["size"])
        self.clove_bare_descriptor_pattern = _word_pattern(
            self.by_class["size"] | self.by_class["descriptor"]
        )

        # -- Stage 4 evidence pattern: ONE combined, named-group
        # alternation across preparation/state/temperature/modifier(+its
        # folded-in classes)/brand, so a single left-to-right finditer
        # pass still yields a properly-typed span per match. Cut-pattern
        # grammar is included as its own unnamed-but-still-grouped
        # alternative so "cut into 1/2-inch dice" matches as one
        # Preparation span rather than fragmenting into "cut" + "dice".
        group_alternatives = []
        for group_name, source_classes in _EVIDENCE_GROUP_CLASSES.items():
            terms = set()
            for cls in source_classes:
                terms |= self.by_class[cls]
            fragments = _sorted_fragments(terms)
            if group_name == "preparation":
                # Cut-pattern grammar (dimensioned templates first) and
                # GRAMMAR_ONLY_PREP_PATTERNS ride inside the same
                # "preparation" named group, ahead of the flat
                # vocabulary fragments — same priority convention the
                # old prep_state_pattern relied on.
                alt_parts = (
                    self.cut_patterns
                    + list(GRAMMAR_ONLY_PREP_PATTERNS)
                    + [f[2] for f in fragments]
                )
            else:
                alt_parts = [f[2] for f in fragments]
            if not alt_parts:
                continue
            group_alternatives.append(
                r'(?P<%s>\b(?:%s)\b)' % (group_name, '|'.join(alt_parts))
            )
        self.evidence_pattern = (
            re.compile('|'.join(group_alternatives), re.IGNORECASE)
            if group_alternatives else None
        )

        # -- "or <prep-method/modifier alternative>" grammar (Stage 1
        # helper, and used by the leaked-clause Noise pattern below) --
        or_prep_terms = self.by_class["preparation"] | self.by_class["modifier"]
        or_prep_fragments = _sorted_fragments(or_prep_terms, flexible_separator=True)
        or_prep_alt = (
            '|'.join(f[2] for f in or_prep_fragments) if or_prep_fragments else r'(?!)'
        )
        self.or_prep_pattern = re.compile(
            r'\bor\s+(?:' + or_prep_alt + r')', re.IGNORECASE
        )
        self.or_prep_cleanup_pattern = re.compile(
            r'\s+or\s+(?:' + or_prep_alt + r')[\w\s-]*$', re.IGNORECASE
        )


_ACTIVE: "CompiledVocabulary | None" = None


def set_vocabulary(provider, culinary):
    """Rebuild the active compiled vocabulary from `provider` (ingredient
    identity) and `culinary` (everything else). Call once before
    processing a batch of lines — not per-line, since it recompiles
    regex. Not thread-safe: this module assumes one vocabulary is active
    for the duration of a parse run."""
    global _ACTIVE
    _ACTIVE = CompiledVocabulary(provider, culinary)
    _log_vocabulary_diagnostics(_ACTIVE)


def _active() -> "CompiledVocabulary":
    """Returns the active CompiledVocabulary, lazily constructing a
    default one on first use if set_vocabulary() was never called. The
    assert below is purely for static type checkers: _ACTIVE's inferred
    type is Optional (its initial value is None), so without it every
    `_active().some_attr` call site reads as a None-attribute access even
    though set_vocabulary() above always assigns a real instance before
    this function can return one."""
    global _ACTIVE
    if _ACTIVE is None:
        set_vocabulary(StaticVocabularyProvider(), CulinaryVocabulary())
    assert _ACTIVE is not None
    return _ACTIVE


def _log_vocabulary_diagnostics(compiled):
    """Startup summary confirming the runtime vocabulary actually
    loaded. Two aggregate counts, not one line per class — see the
    parse_ingredient_lines conversation history for why per-class empty
    warnings were dropped (a class being empty isn't necessarily a bug)."""
    total_terms = sum(len(terms) for terms in compiled.by_class.values())
    print(f"Vocabulary has {total_terms} terms")
    print(f"Ingredients has {len(compiled.ingredient_identities)} foods")


# ============================================================
# TEXT NORMALIZATION
# Converts abbreviations, Unicode fractions, and word-numbers to
# canonical forms used by extraction below. Unchanged in effect from
# before this refactor — see _locate's docstring for why span offsets
# are recovered against the pre-normalization source rather than
# computed as exact offset math through this function.
# ============================================================

def normalize_text(text):
    if not text:
        return text

    text = re.sub(r'\bTbsp\b|\bTBSP\b', 'tbsp', text)
    text = re.sub(r'\bTSP\b', 'tsp', text)
    text = re.sub(r'(?<![a-zA-Z])T(?![a-zA-Z])', 'tbsp', text)
    text = re.sub(r'(?<![a-zA-Z])t(?![a-zA-Z])', 'tsp', text)

    text = text.lower()
    text = re.sub(r'^[\u2022\u2013•\-–]\s*', '', text)
    text = text.replace('&', ' and ')

    text, phrase_map = _protect_phrases(text, _active().ingredient_identities)

    text = text.replace("weight", "__weight__")
    text = text.replace("eighth", "__eighth__")

    def _and_frac(m):
        return str(float(m.group(1)) + float(m.group(2)) / float(m.group(3)))
    text = re.sub(r'(\d+)\s+and\s+(\d+)\s*/\s*(\d+)', _and_frac, text)
    text = re.sub(r'(\d+)\s+and\s+a\s+half',
                lambda m: str(float(m.group(1)) + 0.5), text)

    fractions = [
        ("2 1/2", "2.5"), ("1-1/2", "1.5"), ("1 1/2", "1.5"),
        ("1-½",   "1.5"), ("1½",    "1.5"), ("1 ½",   "1.5"),
        ("½",     "0.5"), ("1/2",   "0.5"),
        ("⅓",    "0.333"), ("1/3",  "0.333"),
        ("⅔",    "0.667"), ("2/3",  "0.667"),
        ("¼",    "0.25"),  ("1/4",  "0.25"),
        ("¾",    "0.75"),  ("3/4",  "0.75"),
        ("⅛",   "0.125"), ("1/8",  "0.125"),
        ("⅜",   "0.375"), ("3/8",  "0.375"),
        ("⅝",   "0.625"), ("5/8",  "0.625"),
        ("⅞",   "0.875"), ("7/8",  "0.875"),
    ]
    for k, v in fractions:
        text = text.replace(k, v)

    text = re.sub(r'(\d(?:\.\d+)?)\s*["\u201d\u2033]', r'\1-inch', text)

    word_numbers = {
        "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
        "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
        "half": "0.5",
    }
    for word, num in word_numbers.items():
        text = re.sub(r'\b' + word + r'\b', num, text)

    text = text.replace("__weight__", "weight")
    text = text.replace("__eighth__", "eighth")
    text = _restore_phrases(text, phrase_map)

    text = text.replace("tsp.", "tsp").replace("tbsp.", "tbsp").replace("oz.", "oz")
    text = text.replace("lbs.", "lb").replace("lbs", "lb")
    text = re.sub(r'(?<![a-zA-Z])c(?![a-zA-Z])', 'cup', text)
    text = re.sub(r'\bfrom\s+\d+\s+', 'from ', text)

    return text.strip()


def _protect_phrases(text, phrases):
    mapping = {}
    for i, phrase in enumerate(sorted(phrases, key=len, reverse=True)):
        pat = re.compile(re.escape(phrase), re.IGNORECASE)
        m = pat.search(text)
        if m:
            token = "__PROTECTED_%d__" % i
            mapping[token] = m.group(0)
            text = pat.sub(token, text)
    return text, mapping


def _restore_phrases(text, mapping):
    for token, phrase in mapping.items():
        text = text.replace(token, phrase)
    return text


_LEADING_SYMBOLS = re.compile(r'^[\-\u2013\u2022\+\*•\s]+')


def _remove_leading_symbols(text):
    return _LEADING_SYMBOLS.sub('', text).strip()


# ============================================================
# STAGE 1 — LINE-LEVEL GRAMMAR: optional / or / preferably / plus
# Unchanged from before this refactor except in one respect: these no
# longer just silently consume the "or"/"preferably"/"+" tokens as a
# side effect of splitting. _grammar_marker_spans below independently
# re-locates those same tokens in the raw line and emits them as
# GrammarMarker spans, so the fact that a split happened — and on what
# word — is itself recorded evidence, not just an implicit consequence
# of which slot a row ended up in.
# ============================================================

def _is_optional(text):
    stripped = text.strip().lstrip('•-–*').strip()
    return bool(
        re.match(r'^optional\s*:', stripped, re.IGNORECASE)
        or re.search(r'\(optional\)', stripped, re.IGNORECASE)
        or re.search(r'\boptional\b', stripped, re.IGNORECASE)
    )


_OR_QTY_UNIT = re.compile(
    r'^(.+?)\s+or\s+(\d[\d./]*(?:\s*[-–]\s*\d[\d./]*)?)\s+(\w+)\s+(.+)$',
    re.IGNORECASE
)
_OR_RELATIVE = re.compile(
    r'^(.+?)\s+or\s+(double|triple|half)\s+the\s+amount\s+of\s+(.+)$',
    re.IGNORECASE
)
_OR_ALT_BLOCK = re.compile(
    r'^(?:as\s+needed|as\s+desired|if\s+desired|desired|needed|required|'
    r'to\s+taste|more|additional|so\s+desired|'
    r'\d+(?:\.\d+)?%\s+by\s+weight)\b',
    re.IGNORECASE
)
_OR_TOKEN = re.compile(r'\bor\b', re.IGNORECASE)


def _split_on_or(text):
    """Returns (primary_text, alt_text, alt_kind). See module history:
    "measure"/"scale"/"ingredient" record WHICH GRAMMATICAL PATTERN
    matched, not a semantic claim about the two sides."""
    stripped = text.strip()
    if _active().or_prep_pattern.search(stripped):
        return stripped, None, None

    m = _OR_QTY_UNIT.match(stripped)
    if m and m.group(3).strip().lower() in _active().unit_vocab:
        alt = "%s %s %s" % (m.group(2).strip(), m.group(3).strip(), m.group(4).strip())
        return m.group(1).strip(), alt, "measure"

    m = _OR_RELATIVE.match(stripped)
    if m:
        alt = "%s the amount of %s" % (m.group(2).strip(), m.group(3).strip())
        return m.group(1).strip(), alt, "scale"

    paren_masked = re.sub(r'\([^)]*\)', lambda mm: 'X' * len(mm.group(0)), stripped)
    or_matches = list(_OR_TOKEN.finditer(paren_masked))
    if len(or_matches) == 1:
        idx = or_matches[0]
        primary = stripped[:idx.start()].strip().rstrip(',').strip()
        alt = stripped[idx.end():].strip()
        if (primary and alt
                and re.search(r'[a-zA-Z]', primary)
                and re.search(r'[a-zA-Z]', alt)
                and not _OR_ALT_BLOCK.match(alt)):
            return primary, alt, "ingredient"

    return stripped, None, None


_PREFERABLY_QTY_UNIT = re.compile(
    r'^(.+?)\s*,?\s+preferably\s+(\d[\d./]*(?:\s*[-–]\s*\d[\d./]*)?)\s+(\w+)\s+(.+)$',
    re.IGNORECASE
)
_PREFERABLY_TOKEN = re.compile(r'\bpreferably\b', re.IGNORECASE)


def _split_on_preferably(text):
    """Returns (primary_text, alt_text, alt_kind). Mirrors _split_on_or
    but "preferably" is a stated PREFERENCE, not an either/or — see
    _split_alt for which side that makes structurally optional."""
    stripped = text.strip()

    m = _PREFERABLY_QTY_UNIT.match(stripped)
    if m and m.group(3).strip().lower() in _active().unit_vocab:
        alt = "%s %s %s" % (m.group(2).strip(), m.group(3).strip(), m.group(4).strip())
        return m.group(1).strip().rstrip(',').strip(), alt, "measure"

    paren_masked = re.sub(r'\([^)]*\)', lambda mm: 'X' * len(mm.group(0)), stripped)
    pref_matches = list(_PREFERABLY_TOKEN.finditer(paren_masked))
    if len(pref_matches) == 1:
        idx = pref_matches[0]
        primary = stripped[:idx.start()].strip().rstrip(',').strip()
        alt = stripped[idx.end():].strip()
        if (primary and alt
                and re.search(r'[a-zA-Z]', primary)
                and re.search(r'[a-zA-Z]', alt)
                and not _OR_ALT_BLOCK.match(alt)):
            return primary, alt, "preferred"

    return stripped, None, None


def _split_alt(text):
    """Tries OR-splitting, then PREFERABLY-splitting. Returns
    (primary_text, alt_text, alt_kind, optional_slot): optional_slot is
    1 ("or" — the alt side is the swappable/optional one), 0
    ("preferably" — the primary side is the fallback/optional one), or
    None (no split)."""
    primary_text, alt_text, alt_kind = _split_on_or(text)
    if alt_text is not None:
        alt_text = re.sub(r'^\s*preferably\s+', '', alt_text, flags=re.IGNORECASE)
        return primary_text, alt_text, alt_kind, 1

    primary_text, alt_text, alt_kind = _split_on_preferably(text)
    if alt_text is not None:
        return primary_text, alt_text, alt_kind, 0

    return primary_text, None, None, None


_PLUS_SPLIT = re.compile(
    r'(?<!\w)\+(?!\w)|'
    r'\bplus\b(?!\s+(?:more|additional)\b)',
    re.IGNORECASE
)
_WORD_NUMBERS_RE = r'(?:one|two|three|four|five|six|seven|eight|nine|ten|half|a)\b'


def _split_on_plus(text):
    paren_masked = re.sub(r'\([^)]*\)', lambda m: 'X' * len(m.group(0)), text)
    matches = list(_PLUS_SPLIT.finditer(paren_masked))
    if not matches:
        return [text]
    parts = []
    cursor = 0
    for m in matches:
        parts.append(text[cursor:m.start()])
        cursor = m.end()
    parts.append(text[cursor:])
    parts = [p.strip() for p in parts if p.strip()]
    # Only split when every non-first segment starts with a quantity —
    # "plus more for dusting" isn't a real second ingredient.
    for p in parts[1:]:
        if not re.match(r'^\d|^' + _WORD_NUMBERS_RE, p, re.IGNORECASE):
            return [text]
    return parts if len(parts) > 1 else [text]


def _grammar_marker_spans(raw_text, emitter):
    """Independently re-locates or/preferably/+/plus/optional tokens in
    the raw line and emits them as GrammarMarker spans. These describe
    the WHOLE line's grammar, not a specific post-split slot, so this is
    called once per raw line (in parse_ingredient_line) before any
    slot-level splitting, using its own emitter over the full raw_text."""
    for m in _OR_TOKEN.finditer(raw_text):
        emitter.emit("GrammarMarker", "or", metadata={"marker": "or"},
                     raw_hint=m.group(0))
    for m in _PREFERABLY_TOKEN.finditer(raw_text):
        emitter.emit("GrammarMarker", "preferably", metadata={"marker": "preferably"},
                     raw_hint=m.group(0))
    for m in _PLUS_SPLIT.finditer(raw_text):
        emitter.emit("GrammarMarker", "plus", metadata={"marker": "plus"},
                     raw_hint=m.group(0))
    if re.search(r'\boptional\b', raw_text, re.IGNORECASE):
        emitter.emit("GrammarMarker", "optional", metadata={"marker": "optional"},
                     raw_hint="optional")


# ============================================================
# JUICE FORM
# ============================================================

_JUICE_PAT = re.compile(r'^(juice(?:\s+(?:from|of))?\s+)', re.IGNORECASE)


def _extract_juice_form(text, emitter):
    m = _JUICE_PAT.match(text)
    if m:
        emitter.emit("Preparation", "juice", metadata={"form": "juice_of"},
                     raw_hint=m.group(1).strip())
        return text[m.end():].strip()
    return text


# ============================================================
# PERCENT-BY-WEIGHT SUBSTITUTION NOTE  ("... or 20% by weight of X")
# ============================================================

_PCT_WEIGHT_PAT = re.compile(
    r',?\s*or\s+\d+(?:\.\d+)?%\s+by\s+weight\s+of\s+[^,)]+', re.IGNORECASE
)


def _extract_pct_weight(text, emitter):
    m = _PCT_WEIGHT_PAT.search(text)
    if m:
        note = m.group(0).strip().lstrip(',').strip()
        emitter.emit("Noise", note, metadata={"category": "pct_weight_note"})
        text = (text[:m.start()] + text[m.end():]).strip()
    return text


# ============================================================
# EXPLICIT MEASURE EXTRACTION — grams / ml / percent
# ============================================================

_APPROX = r'(?:approximately|approx\.?|about)?\s*'

_MASS_PAT = re.compile(
    r'(?P<open>\()?[\s,/|]*' + _APPROX +
    r'(\d+(?:\.\d+)?)\s*(?:grams?|g(?![a-zA-Z]))\s*(?(open)\)|)(?:\s*,)?',
    re.IGNORECASE
)
_ML_PAT = re.compile(
    r'(?P<open>\()?\s*,?\s*' + _APPROX +
    r'(\d+(?:\.\d+)?)\s*(?:ml|milliliters?|millilitres?|mls?)\s*(?(open)\)|)(?:\s*,)?',
    re.IGNORECASE
)
_LITER_PAT = re.compile(
    r',?\s*' + _APPROX + r'(\d+(?:\.\d+)?)\s*(?:liters?|litres?)\b',
    re.IGNORECASE
)
_PCT_PAT = re.compile(
    r',?\s*(\d+(?:\.\d+)?)\s*%(?:\s+(?:total|by\s+weight(?:\s+of\s+[^,)]+)?))?',
    re.IGNORECASE
)
_APPROX_SECONDARY = re.compile(
    r',?\s*(?:approximately|approx\.?|about)\s+\d+(?:\.\d+)?\s+\w+[^,)]*',
    re.IGNORECASE
)
_PLUS_ADDITIONAL = re.compile(r',?\s*plus\s+additional(?:\s+for\s+\w+)?', re.IGNORECASE)


def _strip_approx_secondary_outside_parens(text):
    parens_found = []

    def _mask(m):
        parens_found.append(m.group(0))
        return "__PARENTOK_%d__" % (len(parens_found) - 1)

    masked = re.sub(r'\([^)]*\)', _mask, text)
    masked = _APPROX_SECONDARY.sub('', masked).strip().rstrip(',').strip()
    for i, p in enumerate(parens_found):
        masked = masked.replace("__PARENTOK_%d__" % i, p)
    return masked


def _extract_explicit_measures(text, emitter):
    grams = ml = pct = None
    m = _MASS_PAT.search(text)
    if m:
        grams = m.group(2)
        emitter.emit("WeightExpression", grams, metadata={"grams": grams},
                     raw_hint=m.group(0).strip())
        text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    m = _ML_PAT.search(text)
    if m:
        ml = m.group(2)
        emitter.emit("VolumeExpression", ml, metadata={"ml": ml},
                     raw_hint=m.group(0).strip())
        text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    else:
        m = _LITER_PAT.search(text)
        if m:
            ml = str(float(m.group(1)) * 1000)
            emitter.emit("VolumeExpression", ml, metadata={"ml": ml, "raw_unit": "liter"},
                         raw_hint=m.group(0).strip())
            text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    m = _PCT_PAT.search(text)
    if m:
        pct = m.group(1)
        emitter.emit("Measurement", pct, metadata={"percent": pct, "role": "scaling"},
                     raw_hint=m.group(0).strip())
        text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    text = _strip_approx_secondary_outside_parens(text)
    m = _PLUS_ADDITIONAL.search(text)
    if m:
        note = m.group(0).strip().lstrip(',').strip()
        emitter.emit("Noise", note, metadata={"category": "plus_additional"})
        text = (text[:m.start()] + text[m.end():]).strip().rstrip(',').strip()
    return text, grams, ml, pct


# ============================================================
# PARENTHETICAL EXTRACTION
# ============================================================

_DROP_PAREN = [
    re.compile(r'see\s+note',   re.IGNORECASE),
    re.compile(r'page\s+\d+',  re.IGNORECASE),
    re.compile(r'note\s*\d*',  re.IGNORECASE),
    re.compile(r'^\s*\*+\s*$'),
    re.compile(r'^\s*optional\s*$', re.IGNORECASE),
]

_PAREN_MEASURE = re.compile(
    r'(?:about|approximately|approx\.?)?\s*'
    r'(\d+(?:\.\d+)?(?:\s*[-–]\s*\d+(?:\.\d+)?)?)\s*'
    r'(cup|cups|oz|ounce|ounces|tbsp|tablespoon|tablespoons|'
    r'tsp|teaspoon|teaspoons|pint|quart|gallon|lb|pound|pounds|stick|sticks)',
    re.IGNORECASE
)
_PAREN_PRIORITY = {
    'oz':1,'ounce':1,'ounces':1,'lb':1,'pound':1,'pounds':1,
    'pint':1,'quart':1,'gallon':1,
    'cup':2,'cups':2,
    'tbsp':3,'tablespoon':3,'tablespoons':3,
    'tsp':4,'teaspoon':4,'teaspoons':4,
    'stick':5,'sticks':5,
}


def _paren_measure(paren_text):
    candidates = []
    for m in _PAREN_MEASURE.finditer(paren_text):
        raw_qty = re.split(r'[-–]', m.group(1))[0].strip()
        unit = m.group(2).lower()
        candidates.append((_PAREN_PRIORITY.get(unit, 99), raw_qty, unit))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1], candidates[0][2]


def _extract_parentheticals(text, emitter):
    matches = re.findall(r'\((.*?)\)', text)
    text = re.sub(r'\(.*?\)', '', text).strip()
    kept = []
    for m in matches:
        if any(p.search(m) for p in _DROP_PAREN) or not re.search(r'[a-zA-Z0-9]', m):
            continue
        kept.append(m)
        pq, pu = _paren_measure(m)
        if pq and pu:
            # The measure inside is captured separately by the caller
            # (via _paren_measure again, to decide priority against
            # grams/can-size) — here we just record that a parenthetical
            # aside existed; the caller emits the Weight/VolumeExpression
            # span for it if it's actually used as the primary measure,
            # and a Noise span otherwise, once that decision is made.
            pass
    return text, kept


# ============================================================
# CAN / JAR SIZE EXTRACTION   "1 28-oz can" -> PackageExpression
# ============================================================

_CAN_PAT = re.compile(
    r'(?:(\d+(?:\.\d+)?)\s+)?'
    r'(\d+(?:\.\d+)?)\s*[-\s]?(?:ounce|oz)\.?\s+'
    r'(cans?|jars?|bottles?|packages?|bags?|boxes?)',
    re.IGNORECASE
)


def _extract_can_size(text, emitter):
    m = _CAN_PAT.search(text)
    if m:
        count     = m.group(1) or "1"
        size_note = "%s oz" % m.group(2)
        container = m.group(3).lower()
        emitter.emit(
            "PackageExpression", "%s %s (%s)" % (count, container, size_note),
            metadata={"quantity": count, "package": container, "package_size": size_note},
            raw_hint=m.group(0).strip()
        )
        text = (text[:m.start()] + text[m.end():]).strip().lstrip(',').strip()
        return text, count, container, size_note
    return text, None, None, None


# ============================================================
# SIZE DESCRIPTOR PROTECTION
# "1/4-inch-thick" must not be visible to quantity/unit extraction, but
# must survive to be matched (as part of a larger Preparation span, e.g.
# "cut into 1/4-inch-thick slices") by the Stage 4 cut-pattern grammar.
# Not a span itself — an internal shield, restored before Stage 4 runs.
# ============================================================

_SIZE_DESCRIPTOR = re.compile(r'\d[\d./]*-inch[a-z-]*', re.IGNORECASE)


def _protect_size_descriptors(text):
    mapping = {}

    def _tok(m):
        token = "__SIZETOK_%d__" % len(mapping)
        mapping[token] = m.group(0)
        return token

    text = _SIZE_DESCRIPTOR.sub(_tok, text)
    return text, mapping


# ============================================================
# QUANTITY EXTRACTION
# ============================================================

_QTY_PAT = re.compile(
    r'^(\d+(?:\.\d+)?)'
    r'(?:\s+(\d+(?:\.\d+)?)(?!\s*[-–]?\s*(?:oz|g|ml|lb|cup|tsp|tbsp)))?'
    r'(?:\s*(to|-)\s*(\d+(?:\.\d+)?))?'
)


def _extract_quantity(text, emitter):
    """Returns (text, quantity_str, quantity_span). quantity_span is
    provisional — Measurement by default — and gets RETYPED in place
    (mutating span_type/metadata) once the caller knows which unit
    family, if any, claims it (see _extract_line_spans)."""
    m = _QTY_PAT.match(text)
    if m:
        full = m.group(0)
        if m.group(4):
            qty = float(m.group(4))
        elif m.group(2):
            qty = float(m.group(1)) + float(m.group(2))
        else:
            qty = float(m.group(1))
        qty_str = str(int(qty)) if qty == int(qty) else str(qty)
        span = emitter.emit("Measurement", qty_str, metadata={"quantity": qty_str},
                             raw_hint=full.strip())
        text = text[len(full):].strip()
        return text, qty_str, span
    return text, None, None


# ============================================================
# UNIT EXTRACTION
# ============================================================

_GARLIC_CONTEXT = re.compile(r'\bcloves?\s+(?:of\s+)?(garlic|shallot)\b', re.IGNORECASE)
_CLOVES_OF = re.compile(r'\bcloves?\s+of\b', re.IGNORECASE)


def _extract_unit(text):
    """Unchanged matching logic from before this refactor — still
    returns (text, unit); the caller (_extract_line_spans) is
    responsible for emitting the resulting Measurement-family span,
    since it needs to know the quantity too."""
    unit_pat = _active().unit_pattern
    if re.search(r'\bcloves?\b', text, re.IGNORECASE):
        if _GARLIC_CONTEXT.search(text):
            if _CLOVES_OF.search(text):
                text = _CLOVES_OF.sub('', text, count=1).strip()
                return text, "clove"
        else:
            without = re.sub(r'\bcloves?\b', '', text, flags=re.IGNORECASE).strip()
            without_units = unit_pat.sub('', without).strip()
            if not without_units:
                m = unit_pat.search(without)
                if m:
                    real_unit = m.group(0)
                    new_text = re.sub(
                        r'\b' + re.escape(real_unit) + r'\b', '', text,
                        count=1, flags=re.IGNORECASE
                    ).strip()
                    return new_text, real_unit
                return text, None
            if not _active().clove_bare_descriptor_pattern.sub('', without_units).strip():
                return text, None
    m = unit_pat.search(text)
    if m:
        unit = m.group(0)
        text = re.sub(r'\b' + re.escape(unit) + r'\b', '', text, count=1, flags=re.IGNORECASE).strip()
        return text, unit
    return text, None


# ============================================================
# UNIT ROUTING
# Closed physical-system facts (grams/ml conversion), not culinary
# knowledge — see the grammar-constants section at the top of this file.
# ============================================================

_KG_TO_G = 1000.0
_L_TO_ML  = 1000.0


def _route_unit(quantity, unit, grams, ml):
    """Unchanged routing logic from before this refactor. Returns a flat
    dict of DB-column-shaped values; _extract_line_spans below turns
    this into the appropriate Measurement-family span's metadata rather
    than flat top-level result fields."""
    qty = float(quantity) if quantity is not None else None
    key = unit.lower().strip() if unit else None

    imp_wt_val = imp_wt_unit = None
    imp_vol_val = imp_vol_unit = None

    if key in GRAM_UNITS:
        if grams is None and qty is not None:
            factor = _KG_TO_G if key in ('kg', 'kilogram', 'kilograms') else 1.0
            grams = str(qty * factor)
        qty = key = None

    elif key in ML_UNITS:
        if ml is None and qty is not None:
            factor = _L_TO_ML if key in ('liter', 'litre', 'liters', 'litres', 'l') else 1.0
            ml = str(qty * factor)
        qty = key = None

    elif key in IMPERIAL_WEIGHT_UNITS:
        imp_wt_val  = str(qty) if qty is not None else None
        imp_wt_unit = key
        qty = key = None

    elif key in IMPERIAL_VOLUME_UNITS:
        imp_vol_val  = str(qty) if qty is not None else None
        imp_vol_unit = key
        qty = key = None

    def _fmt(v):
        if v is None:
            return None
        f = float(v)
        return str(int(f)) if f == int(f) else str(f)

    return {
        "quantity":              _fmt(qty),
        "unit":                  key,
        "imperial_weight_value": imp_wt_val,
        "imperial_weight_unit":  imp_wt_unit,
        "imperial_volume_value": imp_vol_val,
        "imperial_volume_unit":  imp_vol_unit,
        "grams":                 grams,
        "ml":                    ml,
    }


# ============================================================
# STAGE 2 — INGREDIENT IDENTITY / ALIAS PROTECTION
# Moved EARLY: immediately after Stage-1 line splitting and
# normalize_text, before ANY quantity/measure/vocabulary extraction.
# This is the fix for the failure mode the old late-protection order
# had: a later regex pass (unit/prep extraction) could consume a piece
# of a multi-word ingredient name before the name was ever recognized as
# one. Longest match wins, single- or multi-word alike (see
# ingredient_identities() in vocabulary_provider.py).
#
# Each match is masked out of the working text with a stable placeholder
# (not restored) — nothing downstream in this pipeline needs the literal
# ingredient text back in the working stream, since the IngredientSpan
# already recorded it with its own resolved offsets. Masking (rather
# than deleting) prevents Stage 3/4 from re-matching inside it while
# keeping the working text's remaining structure intact for the rest of
# the pipeline's regexes (many of which anchor on surrounding words).
# ============================================================

def _extract_ingredient_spans(text, emitter):
    identities = _active().ingredient_identities
    mapping = {}
    for i, phrase in enumerate(sorted(identities, key=len, reverse=True)):
        pat = re.compile(r'\b' + re.escape(phrase) + r'\b', re.IGNORECASE)
        m = pat.search(text)
        if m:
            emitter.emit("Ingredient", phrase, raw_hint=m.group(0))
            token = "__INGREDIENT_%d__" % i
            mapping[token] = m.group(0)
            text = pat.sub(token, text, count=1)
    return text


# ============================================================
# NOISE PHRASE EXTRACTION
# Hedge phrases ("plus more", "to taste", "as needed", "if desired")
# carry real cooking guidance, so they're recorded as Noise evidence
# rather than deleted. "optional" is the one exception — it's already
# represented via the GrammarMarker emitted by _grammar_marker_spans and
# the line record's own `optional` flag, so recording it a third time
# here would just be redundant.
# ============================================================

def _extract_noise(text, emitter):
    for phrase in NOISE_PHRASES:
        if phrase.lower() == "optional":
            continue
        pat = re.compile(r'\b' + re.escape(phrase) + r'\b', re.IGNORECASE)
        m = pat.search(text)
        if m:
            emitter.emit("Noise", phrase, metadata={"category": "hedge_phrase"},
                         raw_hint=m.group(0))
            text = pat.sub('', text, count=1)
    return text.strip()


# ============================================================
# STAGE 4 — CULINARY VOCABULARY EVIDENCE
# Preparation / State / Temperature / Modifier / Brand, scanned in one
# left-to-right pass via CompiledVocabulary.evidence_pattern's named
# groups (see CompiledVocabulary.__init__) so match order reflects
# source order and each match still comes out properly typed.
# ============================================================

def _extract_evidence(text, emitter):
    pat = _active().evidence_pattern
    if pat is None:
        return text
    for m in pat.finditer(text):
        # evidence_pattern (CompiledVocabulary.__init__) is built ENTIRELY
        # from named-group alternatives — every top-level branch is one of
        # _EVIDENCE_GROUP_CLASSES's group names, so a match here always
        # has a lastgroup. re.Match.lastgroup is still typed
        # Optional[str] though (it's None for a pattern with no named
        # groups, or when the matched alternative itself has none).
        # Bound to a local before narrowing: lastgroup is a PROPERTY, not
        # a plain attribute, so a type checker won't necessarily treat a
        # second `m.lastgroup` read after `if m.lastgroup is None: ...`
        # as still-narrowed — binding it once and narrowing the local
        # avoids relying on that.
        lastgroup = m.lastgroup
        if lastgroup is None:
            continue
        span_type = _EVIDENCE_GROUP_SPAN_TYPE[lastgroup]
        emitter.emit(span_type, m.group(0).strip(), raw_hint=m.group(0).strip())
    text = pat.sub('', text).strip()
    return text


# ============================================================
# GRAMMATICAL NOISE SUB-PATTERNS
# Deterministic sentence structures the old _clean_name recognized and
# stripped as "cleanup" — these are genuinely grammar (a fixed sentence
# shape, not an interpretation of what the words mean), so they stay in
# the parser, but as recorded Noise evidence instead of discarded text.
# What does NOT stay: assembling whatever text survives all of this into
# an ingredient name. That's gone — see _extract_line_spans.
# ============================================================

_SOURCE_CLAUSE = re.compile(
    r'\bfrom\s+\d*\s*(?:large|small|medium|fresh|whole)?\s*\w+\s*$', re.IGNORECASE
)
_PURPOSE_CLAUSE = re.compile(r'\s+for\s+\S.*$', re.IGNORECASE)


def _extract_grammatical_notes(text, emitter):
    m = _SOURCE_CLAUSE.search(text)
    if m:
        emitter.emit("Noise", m.group(0).strip(), metadata={"category": "source_clause"},
                     raw_hint=m.group(0).strip())
        text = text[:m.start()].strip()
    text = re.sub(r'\bfrom\s+', '', text, flags=re.IGNORECASE).strip()

    leak = _active().or_prep_cleanup_pattern.search(text)
    if leak:
        emitter.emit("Noise", leak.group(0).strip(),
                     metadata={"category": "leaked_alt_clause"},
                     raw_hint=leak.group(0).strip())
        text = _active().or_prep_cleanup_pattern.sub('', text).strip()

    m = _PURPOSE_CLAUSE.search(text)
    if m:
        emitter.emit("Noise", m.group(0).strip(), metadata={"category": "purpose_clause"},
                     raw_hint=m.group(0).strip())
        text = text[:m.start()].strip()

    return text


_INGREDIENT_PLACEHOLDER = re.compile(r'__INGREDIENT_\d+__')


def _punctuation_cleanup(text):
    """PUNCTUATION-ONLY cleanup of whatever text is left over after every
    recognized span has been pulled out — the one piece of the old
    _clean_name that is genuinely still a parsing concern per the work
    order ("only punctuation cleanup belongs in parsing; semantic
    cleanup belongs later"). Does not decide what the remaining text
    MEANS, does not build a name — just strips stray commas, dangling
    hyphens, isolated digits/letters, so the leftover Modifier/Unknown
    spans below aren't full of punctuation noise. Also strips the
    ingredient-protection placeholder tokens (__INGREDIENT_N__) left
    behind by Stage 2 — those regions are already recorded as
    IngredientSpans; the placeholder itself is bookkeeping, not
    unrecognized text, and must not leak into an Unknown span."""
    text = _INGREDIENT_PLACEHOLDER.sub(' ', text)
    text = re.sub(r'[,;()]', ' ', text)
    text = " ".join(text.split())
    text = re.sub(r'^(?:of|and|or|\*|:|with)\b\s*|^[-–]\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+(and|or|\*|with|in)$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'(?<!\w)\d+/\d+(?!\w)', '', text).strip()
    text = re.sub(r'(?<!\w)\d+(?:\.\d+)?(?!\w)', '', text).strip()
    text = re.sub(r'(?<=[a-z])-(?=\s|$)', '', text).strip()
    text = re.sub(r'(?:^|\s)-(?=[a-z])', ' ', text).strip()
    text = re.sub(r'(?<!\w)\.(?!\w)', '', text).strip()
    text = re.sub(r'(?<!\w)[a-z](?!\w)', '', text, flags=re.IGNORECASE).strip()
    return " ".join(text.split()).strip('*').strip()


_AND_TOKEN = re.compile(r'\band\b', re.IGNORECASE)


def _leftover_spans(text, emitter):
    """Whatever survives every recognized-span extraction becomes the
    parser's honest admission of what it couldn't classify: each
    whitespace-delimited run becomes an Unknown span, EXCEPT a bare
    "and" token, which is recorded as a GrammarMarker instead (see
    module docstring: the parser recognizes "and" as grammar; deciding
    whether the words on either side are one ingredient or two is
    Normalization's job — _split_multi_ingredients in
    normalize_ingredient_observations.py)."""
    text = _punctuation_cleanup(text)
    if not text:
        return
    for chunk in text.split():
        if _AND_TOKEN.fullmatch(chunk):
            emitter.emit("GrammarMarker", "and", metadata={"marker": "and"}, raw_hint=chunk)
        else:
            emitter.emit("Unknown", chunk, raw_hint=chunk)


# ============================================================
# CORE LINE EXTRACTOR
# Runs one normalized sub-line (a "slot" from _split_alt / _split_on_plus)
# through every recognition pass and returns its recognized_spans.
# Unlike the old _parse_one_line, this returns NO name, NO quantity/unit
# top-level fields — everything is a Span. `text` is still progressively
# shrunk/masked internally exactly as before (that's just control flow —
# it prevents a later pass from re-matching something an earlier pass
# already claimed), but nothing is discarded: every step that used to
# throw its matched text away now records it via `emitter` first.
# ============================================================

def _extract_line_spans(text, source_line, base_cursor=0):
    """`source_line` is the FULL original raw block line (what actually
    gets stored as the parent row's raw_text) — span offsets are resolved
    against it directly, starting the search no earlier than
    `base_cursor` (this slot's approximate starting position within
    source_line, located by the caller — see parse_ingredient_line), so
    offsets come out ABSOLUTE within source_line rather than relative to
    an isolated, unstored slot substring."""
    emitter = _SpanEmitter(source_line, cursor=base_cursor)

    text = normalize_text(text)
    text = _remove_leading_symbols(text)

    text = _extract_juice_form(text, emitter)

    # STAGE 2 — moved early, before any quantity/measure/vocabulary
    # extraction (see the section header above _extract_ingredient_spans).
    text = _extract_ingredient_spans(text, emitter)

    text = _extract_pct_weight(text, emitter)
    text, grams, ml, pct = _extract_explicit_measures(text, emitter)
    text, parens = _extract_parentheticals(text, emitter)

    paren_qty = paren_unit = None
    paren_source_text = None
    for pc in parens:
        pq, pu = _paren_measure(pc)
        if pq and pu:
            paren_qty, paren_unit = pq, pu
            paren_source_text = pc
            break

    text, can_qty, can_unit, can_size_note = _extract_can_size(text, emitter)
    text, size_map = _protect_size_descriptors(text)

    if grams is not None:
        text = re.sub(
            r'(?:^|(?<=\s))(\d+(?:\.\d+)?)\s*'
            r'(cup|cups|tbsp|tsp|tablespoon|tablespoons|teaspoon|teaspoons|'
            r'oz|ounce|ounces|lb|pound|pounds|pint|pints|quart|quarts|'
            r'ml|liter|litre|g|kg)(?!\w)',
            '', text, flags=re.IGNORECASE
        ).strip().strip(',').strip()

    text, quantity, quantity_span = _extract_quantity(text, emitter)

    # Priority: explicit can/jar size > parenthetical imperial measure
    # (only when paired with an explicit gram weight) > gram weight alone.
    # Unchanged from before this refactor. This ONLY decides what the
    # bare leading-quantity span gets retyped to below — the can-size,
    # gram, ml, and parenthetical-measure spans were ALREADY emitted
    # above at their own textual locations regardless of which one
    # "wins" here. Nothing is hidden; this just avoids a second,
    # redundant Weight/VolumeExpression duplicating one already emitted.
    unit = None
    if can_qty is not None:
        quantity, unit = can_qty, can_unit
    elif grams is not None:
        if paren_qty and paren_unit:
            quantity, unit = paren_qty, paren_unit
        elif quantity is None:
            quantity, unit = grams, "g"

    if unit is None:
        text, unit = _extract_unit(text)

    text = _restore_phrases(text, size_map)

    text = _extract_noise(text, emitter)
    text = _extract_evidence(text, emitter)
    text = _extract_grammatical_notes(text, emitter)

    # Parenthetical measure, if one was found, as its own span — separate
    # from whether it "won" the priority above (see loop below); parens
    # that did NOT parse as a measure become Noise.
    if paren_qty and paren_unit:
        prouted = _route_unit(paren_qty, paren_unit, None, None)
        ptype, pnorm, pmeta = _routed_to_span(prouted)
        pmeta = dict(pmeta, source="parenthetical")
        emitter.emit(ptype, pnorm, metadata=pmeta, raw_hint=paren_source_text)
    for pc in parens:
        if pc == paren_source_text:
            continue
        emitter.emit("Noise", pc, metadata={"category": "parenthetical"}, raw_hint=pc)

    # Retype (or freshly emit) the resolved bare quantity+unit as the
    # appropriate Measurement-family span — but ONLY when it represents
    # genuinely new evidence. can-size, parenthetical-measure, and the
    # grams-fallback branches above each already have their OWN span
    # (emitted at their own textual location, earlier); re-routing the
    # SAME (quantity, unit) pair they produced through _route_unit here
    # would emit a second, redundant span for the identical evidence.
    skip_retype = (
        can_qty is not None
        or (paren_qty and paren_unit and quantity == paren_qty and unit == paren_unit)
        or (grams is not None and quantity == grams and unit == "g" and quantity_span is None)
    )
    if not skip_retype:
        routed = _route_unit(quantity, unit, None, None)
        if routed["quantity"] or routed["unit"] or routed["imperial_weight_value"] or routed["imperial_volume_value"]:
            rtype, rnorm, rmeta = _routed_to_span(routed)
            if quantity_span is not None:
                quantity_span.span_type = rtype
                quantity_span.normalized_text = rnorm
                quantity_span.metadata = rmeta
            else:
                emitter.emit(rtype, rnorm, metadata=rmeta,
                             raw_hint=(quantity or "") + ((" " + unit) if unit else ""))
    if can_size_note:
        # Already folded into the PackageExpression's own metadata at
        # emission time in _extract_can_size — nothing further to do.
        pass

    _leftover_spans(text, emitter)

    return emitter.sorted_spans()


def _routed_to_span(routed):
    """Maps a _route_unit() result dict to (span_type, normalized_text,
    metadata) for the ONE span that represents "the" resolved
    quantity+unit combination for this line."""
    if routed["grams"] is not None and not routed["quantity"] and not routed["unit"]:
        return "WeightExpression", routed["grams"], {"grams": routed["grams"]}
    if routed["ml"] is not None and not routed["quantity"] and not routed["unit"]:
        return "VolumeExpression", routed["ml"], {"ml": routed["ml"]}
    if routed["imperial_weight_value"] is not None:
        return ("WeightExpression", routed["imperial_weight_value"],
                {"imperial_weight_value": routed["imperial_weight_value"],
                 "imperial_weight_unit": routed["imperial_weight_unit"]})
    if routed["imperial_volume_value"] is not None:
        return ("VolumeExpression", routed["imperial_volume_value"],
                {"imperial_volume_value": routed["imperial_volume_value"],
                 "imperial_volume_unit": routed["imperial_volume_unit"]})
    if routed["quantity"] is not None and routed["unit"] is not None:
        ul = routed["unit"]
        meta = {"quantity": routed["quantity"], "unit": ul}
        if ul in _active().by_class["natural_portion"]:
            return "NaturalPortionExpression", "%s %s" % (routed["quantity"], ul), meta
        if ul in _active().by_class["packaging"]:
            return "PackageExpression", "%s %s" % (routed["quantity"], ul), meta
        return "Measurement", "%s %s" % (routed["quantity"], ul), meta
    if routed["quantity"] is not None:
        return "Measurement", routed["quantity"], {"quantity": routed["quantity"]}
    return "Measurement", routed["unit"] or "", {"unit": routed["unit"]}


# ============================================================
# PUBLIC ENTRY POINT
# ============================================================

def parse_ingredient_line(raw_text):
    """
    Parses one raw ingredient line into one or more LINE RECORDS. Each
    record is what becomes a row in the (slimmed) parsed-lines table —
    see the schema notes at the bottom of this file — and carries
    `recognized_spans` instead of an assembled ingredient name.

    A line can still expand into multiple records via:
      - or/preferably-alternative splitting (share alt_group_id)
      - plus/+ splitting (peer records)
    "A and B" name splitting no longer happens here — see
    normalize_ingredient_observations.split_multi_ingredients for where
    that decision now lives.

    Every record's `raw_text` is the WHOLE original raw_text line (not a
    slot substring): span offsets are resolved absolute within it (see
    _extract_line_spans), so a downstream stage or curator can always
    render a span directly against exactly what's stored on the row —
    no separate slot-text bookkeeping required.

    optional/alt_group_id/alt_kind semantics are unchanged from before
    this refactor: `optional` is the line's own self-declared
    optionality OR'd with whichever side of an or/preferably split is
    structurally optional (see _split_alt).
    """
    line_optional = _is_optional(raw_text)
    primary_text, alt_text, alt_kind, optional_slot = _split_alt(raw_text)
    group_id = uuid.uuid4().hex[:12] if alt_text is not None else None

    line_level_emitter = _SpanEmitter(raw_text)
    _grammar_marker_spans(raw_text, line_level_emitter)
    shared_grammar_spans = line_level_emitter.spans

    records = []
    for slot, slot_text in enumerate([primary_text, alt_text]):
        if slot_text is None:
            continue
        slot_optional = line_optional or (slot == optional_slot)

        for sub in _split_on_plus(slot_text):
            loc = _locate(raw_text, sub.strip(), 0)
            base_cursor = loc[0] if loc else 0
            spans = _extract_line_spans(sub, raw_text, base_cursor=base_cursor)
            spans = sorted(
                spans + list(shared_grammar_spans),
                key=lambda s: (s.start_offset if s.start_offset is not None else 10**9, s.parser_order)
            )
            records.append({
                "raw_text": raw_text,
                "optional": 1 if slot_optional else 0,
                "alt_group_id": group_id,
                "alt_kind": alt_kind,
                "recognized_spans": [s.to_dict() for s in spans],
            })
    return records


# ============================================================
# SCHEMA VERIFICATION
# Check-only — this module does not create or alter tables; that is
# db/init_db.py's exclusive responsibility. See the DB SCHEMA CHANGES
# comment block at the end of this file for the exact DDL this refactor
# requires there.
# ============================================================

_REQUIRED_LINE_COLUMNS = {
    "ingredient_block_id", "recipe_id", "recipe_section_id",
    "recipe_name", "section_name", "line_index", "raw_text",
    "alt_group_id", "alt_kind", "optional",
}

_REQUIRED_SPAN_COLUMNS = {
    "recipe_ingredient_id", "start_offset", "end_offset", "raw_text",
    "normalized_text", "span_type", "vocabulary_id", "ingredient_id",
    "metadata_json", "parser_order",
}


def _verify_schema(conn):
    existing_line_cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(recipe_ingredient_lines_parsed)"
    )}
    if not existing_line_cols:
        raise RuntimeError(
            "recipe_ingredient_lines_parsed does not exist. "
            "Run db/init_db.py first — this module does not create tables."
        )
    missing = _REQUIRED_LINE_COLUMNS - existing_line_cols
    if missing:
        raise RuntimeError(
            "recipe_ingredient_lines_parsed is missing column(s): "
            f"{sorted(missing)}. This refactor SHRINKS this table (dropped: "
            "quantity_value, quantity_unit, imperial_weight_value/unit, "
            "imperial_volume_value/unit, grams, ml, scaling, preparation, "
            "notes, ingredient_name_raw, ingredient_name_original — all now "
            "live in recipe_ingredient_spans instead). See the DB SCHEMA "
            "CHANGES block at the end of this file for the exact DDL to "
            "apply in db/init_db.py. This module does not create or alter "
            "tables."
        )

    existing_span_cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(recipe_ingredient_spans)"
    )}
    if not existing_span_cols:
        raise RuntimeError(
            "recipe_ingredient_spans does not exist. This is a NEW table "
            "this refactor requires — see the DB SCHEMA CHANGES block at "
            "the end of this file for the exact CREATE TABLE DDL. Add it "
            "to db/init_db.py; this module does not create tables."
        )
    missing = _REQUIRED_SPAN_COLUMNS - existing_span_cols
    if missing:
        raise RuntimeError(
            f"recipe_ingredient_spans is missing column(s): {sorted(missing)}. "
            "See the DB SCHEMA CHANGES block at the end of this file."
        )


# ============================================================
# DB EXECUTION
# ============================================================

_PROGRESS_MIN_INTERVAL = 0.2  # seconds between progress-line redraws


def _print_progress(done, total, start_time):
    now = time.monotonic()
    is_last = done >= total
    if not is_last and now - _print_progress._last_draw < _PROGRESS_MIN_INTERVAL:
        return
    _print_progress._last_draw = now
    pct = int(done / total * 100) if total else 100
    elapsed = now - start_time
    sys.stdout.write(f"\r  parsing... {done} of {total} lines ({pct}%, {elapsed:.0f}s)")
    sys.stdout.flush()


_print_progress._last_draw = 0.0


def _run(conn):
    _verify_schema(conn)
    c = conn.cursor()

    c.execute("""
        SELECT
            ib.id               AS ingredient_block_id,
            ib.recipe_id,
            ib.recipe_section_id,
            ib.recipe_name,
            ib.section_name,
            ib.raw_text
        FROM recipe_ingredient_blocks ib
        WHERE ib.raw_text IS NOT NULL AND ib.raw_text != ''
        ORDER BY ib.id
    """)
    blocks = c.fetchall()

    prepared_blocks = []
    total_input_lines = 0
    for ingredient_block_id, recipe_id, recipe_section_id, recipe_name, section_name, block_text in blocks:
        raw_lines = [ln.strip() for ln in block_text.splitlines() if ln.strip()]
        prepared_blocks.append(
            (ingredient_block_id, recipe_id, recipe_section_id, recipe_name, section_name, raw_lines)
        )
        total_input_lines += len(raw_lines)

    print(f"Preparing to parse {total_input_lines} lines from recipe_ingredient_blocks")

    set_vocabulary(DatabaseVocabularyProvider(conn), CulinaryVocabulary())

    print("Parsing, this could take a few minutes...")
    start_time = time.monotonic()

    total_lines = 0
    total_spans = 0
    lines_done = 0
    for ingredient_block_id, recipe_id, recipe_section_id, recipe_name, section_name, raw_lines in prepared_blocks:
        c.execute(
            "DELETE FROM recipe_ingredient_spans WHERE recipe_ingredient_id IN ("
            "SELECT id FROM recipe_ingredient_lines_parsed WHERE ingredient_block_id = ?)",
            (ingredient_block_id,)
        )
        c.execute(
            "DELETE FROM recipe_ingredient_lines_parsed WHERE ingredient_block_id = ?",
            (ingredient_block_id,)
        )

        for line_index, raw_line in enumerate(raw_lines):
            for record in parse_ingredient_line(raw_line):
                c.execute("""
                    INSERT INTO recipe_ingredient_lines_parsed (
                        ingredient_block_id, recipe_id, recipe_section_id,
                        recipe_name, section_name, line_index, raw_text,
                        alt_group_id, alt_kind, optional
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    ingredient_block_id, recipe_id, recipe_section_id,
                    recipe_name, section_name, line_index, record["raw_text"],
                    record["alt_group_id"], record["alt_kind"], record["optional"],
                ))
                recipe_ingredient_id = c.lastrowid
                total_lines += 1

                for span in record["recognized_spans"]:
                    c.execute("""
                        INSERT INTO recipe_ingredient_spans (
                            recipe_ingredient_id, start_offset, end_offset,
                            raw_text, normalized_text, span_type,
                            vocabulary_id, ingredient_id, metadata_json,
                            parser_order
                        ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (
                        recipe_ingredient_id, span["start_offset"], span["end_offset"],
                        span["raw_text"], span["normalized_text"], span["span_type"],
                        span["vocabulary_id"], span["ingredient_id"], span["metadata_json"],
                        span["parser_order"],
                    ))
                    total_spans += 1

            lines_done += 1
            _print_progress(lines_done, total_input_lines, start_time)

    sys.stdout.write("\n")
    conn.commit()
    print("parse_ingredient_lines: %d blocks → %d parsed rows → %d recognized spans"
          % (len(blocks), total_lines, total_spans))


def parse_ingredient_lines():
    with sqlite3.connect(DB_PATH) as conn:
        _run(conn)


def main():
    parse_ingredient_lines()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT count(*) FROM recipe_ingredient_lines_parsed")
        n = c.fetchone()[0]
    print(f"recipe_ingredient_lines_parsed populated with {n} ingredient lines")


if __name__ == "__main__":
    main()


# ============================================================
# DB SCHEMA CHANGES REQUIRED (apply in db/init_db.py — this module only
# verifies schema, per _verify_schema above; it never creates or alters
# tables).
#
# 1. recipe_ingredient_lines_parsed SHRINKS. Drop these columns (their
#    data now lives in recipe_ingredient_spans' metadata_json instead):
#        quantity_value, quantity_unit,
#        imperial_weight_value, imperial_weight_unit,
#        imperial_volume_value, imperial_volume_unit,
#        grams, ml, scaling, preparation, notes,
#        ingredient_name_raw, ingredient_name_original
#    KEEP: id, ingredient_block_id, recipe_id, recipe_section_id,
#    recipe_name, section_name, line_index, raw_text, alt_group_id,
#    alt_kind, optional, parsed_at.
#
# 2. NEW TABLE recipe_ingredient_spans:
#
#    CREATE TABLE recipe_ingredient_spans (
#        span_id              INTEGER PRIMARY KEY AUTOINCREMENT,
#        recipe_ingredient_id INTEGER NOT NULL
#            REFERENCES recipe_ingredient_lines_parsed(id),
#        start_offset         INTEGER,   -- NULL when _locate couldn't
#                                        -- find the match verbatim in
#                                        -- raw_text (best-effort; see
#                                        -- _locate's docstring)
#        end_offset           INTEGER,
#        raw_text             TEXT NOT NULL,
#        normalized_text      TEXT NOT NULL,
#        span_type            TEXT NOT NULL,  -- one of SPAN_TYPES
#        vocabulary_id        TEXT,  -- NULL: CulinaryVocabulary's public
#                                    -- API exposes terms, not row ids —
#                                    -- see note below
#        ingredient_id        TEXT,  -- NULL: VocabularyProvider's public
#                                    -- API exposes phrase strings, not
#                                    -- row ids — see note below
#        metadata_json         TEXT,
#        parser_order          INTEGER NOT NULL
#    );
#    CREATE INDEX idx_recipe_ingredient_spans_line
#        ON recipe_ingredient_spans(recipe_ingredient_id);
#
# KNOWN GAP — vocabulary_id / ingredient_id are always NULL right now.
# Neither CulinaryVocabulary nor VocabularyProvider's public API exposes
# a term/phrase -> row id lookup (CulinaryVocabulary.by_class-style
# accessors return term strings; VocabularyProvider.ingredient_identities()
# returns phrase strings). Populating these would need a small addition
# to each — e.g. CulinaryVocabulary.vocabulary_id(term) and a
# VocabularyProvider method returning (phrase, ingredient_id) pairs
# instead of bare phrases — not guessed at here since I don't own those
# modules' schemas. Everything else in a span (raw_text, normalized_text,
# span_type, metadata_json) does not depend on this gap.
# ============================================================