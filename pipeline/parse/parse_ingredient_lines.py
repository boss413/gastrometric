# pipeline/parse/parse_ingredient_lines.py
#
# Pipeline stage: PARSE
#
#   RAW RECIPE -> INGEST -> PARSE -> NORMALIZE -> IDENTITY RESOLUTION -> ...
#                            ^^^^^
#                         this file
#
#   Reads  : recipe_ingredient_blocks  (one blob per section, written by ingest_markdown)
#   Writes : recipe_ingredient_lines_parsed
#
# RESPONSIBILITY
# ---------------
# This stage answers exactly one question: "what structural information
# is present in this recipe ingredient line?" It extracts quantity, unit,
# preparation, and leftover ingredient text using grammar, punctuation,
# and known unit/technique vocabulary. It does NOT decide what ingredient
# the leftover text refers to, does not treat two different ingredient
# names as interchangeable, and does not consult the ingredient identity
# resource. That is the job of the NORMALIZE and IDENTITY RESOLUTION
# stages that run after this one. See parser_vocabulary.py for the full
# accounting of what is and isn't a parse-time concern.
#
# Each blob is split on newlines into individual lines.
# Each line is then parsed into:
#
#   quantity_value              numeric count/amount
#   quantity_unit                non-standard unit (clove, can, bunch, …)
#   imperial_weight_value/unit  oz / lb
#   imperial_volume_value/unit  cup / tbsp / tsp / …
#   grams                       metric weight
#   ml                          metric volume
#   preparation                 ordered JSON list of technique/state phrases,
#                                in the order they appear in the source line
#                                (e.g. ["peeled", "quartered lengthwise",
#                                "cut crosswise into 0.25-inch-thick slices"]).
#                                Each element is meant to resolve 1:1 against
#                                the technique graph downstream — this column
#                                intentionally holds NOTHING that isn't a
#                                cooking technique or state.
#   notes                       free-text asides that are useful context but
#                                are NOT techniques, so they must not be fed
#                                to the technique-graph lookup: parenthetical
#                                yield/count notes ("about 3"), secondary
#                                measures not chosen as primary, can/jar size
#                                notes, "% by weight" notes, etc.
#   ingredient_name_raw         everything left after all structural
#                                extraction — still parser output, NOT a
#                                normalized or identity-resolved name.
#   ingredient_name_original    the same span, but sliced verbatim out of
#                                the ORIGINAL source line, preserving its
#                                original casing/wording/hyphenation. This
#                                is the evidence a human (or the identity
#                                resolver) should look at if they want to
#                                see exactly what the recipe said, since
#                                ingredient_name_raw has already been
#                                lowercased and had numbers rewritten.
#                                Best-effort: recovered by locating the
#                                surviving name words back in the source
#                                text (see _recover_original_span).
#   alt_group_id                 non-NULL and shared between rows when the
#                                source line expressed an "X or Y"
#                                alternative that this stage split into
#                                multiple rows. NULL for ordinary lines.
#                                Lets downstream see the mutual-exclusion
#                                relationship instead of it being silently
#                                lost. See alt_kind for what kind of
#                                alternative it is.
#   alt_kind                     one of "ingredient", "measure", "scale",
#                                or NULL. Records WHICH GRAMMATICAL PATTERN
#                                matched when the line was split on "or" —
#                                this is a syntactic classification (which
#                                regex matched), not a claim about whether
#                                the two sides are truly different
#                                ingredients. "ingredient" means the split
#                                was the generic "<name> or <name>"
#                                fallback; "measure" means a "<qty> <unit>"
#                                alternative measure of presumably the same
#                                ingredient; "scale" means a relative-amount
#                                alternative ("or double the amount of X").
#   optional                    1 if the line is self-declared optional
#                                (e.g. "(optional)", "if desired"). This is
#                                independent of alt_group_id — an
#                                either/or alternative is not the same
#                                thing as an omittable ingredient, and is
#                                no longer conflated with it (see below).
#
# The source table (recipe_ingredient_blocks) is NEVER modified.
# Re-running is safe: all parsed rows for a given block_id are deleted
# before re-insertion.

import sqlite3
import json
import re
import sys
import time
import uuid
import logging

from gastrometric.config.paths import DB_PATH
from gastrometric.config.parser_vocabulary import (
    NOISE_PHRASES,
    CUT_TEMPLATE_PATTERNS,
    GRAMMAR_ONLY_PREP_PATTERNS,
    GRAM_UNITS,
    ML_UNITS,
    IMPERIAL_WEIGHT_UNITS,
    IMPERIAL_VOLUME_UNITS,
)
from gastrometric.config.vocabulary_provider import (
    StaticVocabularyProvider,
    DatabaseVocabularyProvider,
)
from gastrometric.knowledge.loader import CulinaryVocabulary

_logger = logging.getLogger(__name__)


# ============================================================
# VOCABULARY INJECTION SEAM
#
# The parsing ALGORITHM below never reads culinary vocabulary from a
# module constant. It comes from two independent runtime sources,
# compiled once into the regex/set shapes the algorithm actually needs:
#
#   VocabularyProvider    Ingredient identity ONLY (protected multi-word
#                          ingredient names/aliases). Backed by
#                          `ingredients`/`ingredient_aliases`.
#
#   CulinaryVocabulary     Everything else, read exclusively through its
#                          public API — per-category accessor methods
#                          (measurements(), packaging(), state(), ...),
#                          never a Python list. It does NOT expose a
#                          single generic by_class(name) method; this
#                          module's `_fetch_vocab_class` bridges
#                          _VOCAB_CLASSES to whichever concrete accessor
#                          each class actually has (see
#                          _VOCAB_CLASS_ACCESSORS), so the rest of the
#                          parser only ever deals with one uniform
#                          `self.by_class[<class>]` shape. Backed by
#                          `culinary_vocabulary`/`culinary_aliases`
#                          (gastrometric/knowledge/loader.py). This is
#                          the sole source of truth for these — there is
#                          no Python fallback/bootstrap for it, by
#                          design (an unseeded table yields an empty
#                          set, not a hardcoded guess). The classes this
#                          parser consumes are enumerated in
#                          `_VOCAB_CLASSES` below: measurement,
#                          packaging, natural_portion, preparation,
#                          ingredient_form, size, descriptor, shape,
#                          state, temperature, modifier, seasoning,
#                          brand.
#
# `_ACTIVE` is built LAZILY, not at import time: CulinaryVocabulary()
# does real I/O (opens a DB connection) and can raise if the schema is
# missing, so importing this module must never have that side effect.
# The real pipeline entry point (`_run`) calls `set_vocabulary(...)`
# explicitly, with a DatabaseVocabularyProvider + CulinaryVocabulary
# bound to the run. Any call site that doesn't (ad hoc scripts, tests)
# gets a lazily-constructed default on first use instead.
# ============================================================

# The complete set of CulinaryVocabulary classes this parser draws on.
# Every one of these is fetched ONLY via _fetch_vocab_class(culinary,
# name) — see CompiledVocabulary.__init__. Nothing here is a word list;
# it's the list of *category names* the loader's schema defines, which
# is itself a permanent, grammar-adjacent fact about the vocabulary's
# shape, not culinary knowledge.
_VOCAB_CLASSES = [
    "measurement", "packaging", "natural_portion", "preparation",
    "ingredient_form", "size", "descriptor", "shape", "state",
    "temperature", "modifier", "seasoning", "brand",
]


def _sorted_fragments(terms, flexible_separator=False):
    """Shared fragment-building step behind _word_pattern and the other
    by-class alternations below: escape each term's tokens, join multi-
    word terms with a whitespace (or, if flexible_separator, whitespace-
    or-hyphen) gap, and order longest-first (by token count, then
    character length) so multi-word terms outrank single-word substrings
    in the resulting alternation."""
    sep = r'[\s-]+' if flexible_separator else r'\s+'
    fragments = []
    for term in terms:
        tokens = term.split()
        escaped = sep.join(re.escape(t) for t in tokens)
        fragments.append((len(tokens), len(term), escaped))
    fragments.sort(key=lambda f: (f[0], f[1]), reverse=True)
    return fragments


def _word_pattern(terms):
    """Build a word-boundary-safe, case-insensitive alternation from a
    set of vocabulary terms. Multi-word terms match with flexible
    internal whitespace; terms are ordered longest-first (by token
    count, then character length) so multi-word terms take priority
    over single-word substrings in the alternation, the same convention
    the old hand-ordered pattern lists relied on."""
    if not terms:
        # Matches nothing. An empty vocabulary set is a seeding gap,
        # surfaced by _log_vocabulary_diagnostics's empty-set warning —
        # not something this function should paper over by matching too
        # broadly (or too narrowly) instead.
        return re.compile(r'(?!)')
    fragments = _sorted_fragments(terms)
    return re.compile(
        r'\b(?:' + '|'.join(f[2] for f in fragments) + r')\b',
        re.IGNORECASE
    )


# Maps each CulinaryVocabulary class name to the candidate public
# accessor method name(s) on CulinaryVocabulary, tried in order. Most
# classes have exactly one; "temperature" lists both the correctly
# spelled name and the loader's current typo (temperatuure()) so this
# keeps working unmodified if/when that gets fixed. "shape" and
# "ingredient_form" have NO entries — CulinaryVocabulary defines no
# public accessor for either; see _fetch_vocab_class's fallback.
_VOCAB_CLASS_ACCESSORS = {
    "measurement": ("measurements",),
    "packaging": ("packaging",),
    "natural_portion": ("natural_portions",),
    "preparation": ("preparations",),
    "size": ("sizes",),
    "descriptor": ("descriptors",),
    "state": ("state",),
    "temperature": ("temperature", "temperatuure"),
    "modifier": ("modifier",),
    "seasoning": ("seasoning",),
    "brand": ("brand",),
}


def _fetch_vocab_class(culinary, cls):
    """Fetch one CulinaryVocabulary class's term set through its public
    API. Tries each candidate accessor name in _VOCAB_CLASS_ACCESSORS in
    order and calls the first one that exists.

    For classes with no candidate list at all, or where none of the
    candidates exist on this instance (currently: "shape" and
    "ingredient_form" — CulinaryVocabulary has no shapes()/
    ingredient_forms() method as of this writing), this falls back to
    the loader's private `_class_members(cls)` rather than hard-failing,
    since the underlying data is there even though no public accessor
    was written for it yet. That fallback logs a warning every time, on
    purpose: reaching past a module's public contract should never be
    silent. The real fix is a one-line accessor added to
    gastrometric/knowledge/loader.py, matching the existing pattern,
    e.g. `def shapes(self): return self._class_members("shape")`.
    """
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


class CompiledVocabulary:
    def __init__(self, provider, culinary):
        self.culinary = culinary  # kept for diagnostics logging

        # -- ingredient identity (unchanged: VocabularyProvider only) --
        self.protected_phrases = provider.protected_phrases()

        # -- every CulinaryVocabulary class this parser draws on --
        # CulinaryVocabulary (gastrometric/knowledge/loader.py) exposes
        # per-category public methods (measurements(), packaging(),
        # state(), ...) rather than a single generic by_class(name).
        # _fetch_vocab_class bridges _VOCAB_CLASSES -> those accessors so
        # the rest of this module can keep working off one uniform
        # `self.by_class[<class>]` shape regardless of which concrete
        # method the loader happens to expose. See
        # _VOCAB_CLASS_ACCESSORS and _fetch_vocab_class below for the two
        # known gaps this bridges around (no accessor at all for "shape"
        # / "ingredient_form"; "temperature"'s accessor is misspelled
        # temperatuure() as of this writing).
        self.by_class = {
            cls: _fetch_vocab_class(culinary, cls) for cls in _VOCAB_CLASSES
        }

        # -- units: measurement + packaging + natural portions --
        # All three function as "extractable unit words" for quantity
        # routing (_extract_unit / _split_on_or), regardless of which
        # CulinaryVocabulary class a given term is tagged under.
        self.unit_vocab = (
            self.by_class["measurement"]
            | self.by_class["packaging"]
            | self.by_class["natural_portion"]
        )
        self.unit_pattern = _word_pattern(self.unit_vocab)

        # -- preparation / descriptor / state / temperature / form /
        # modifier / seasoning combined pattern --
        # Cut-pattern GRAMMAR (parser_vocabulary.py) has its {shapes}
        # placeholder filled in from self.by_class["shape"] here
        # here, at compile time — the template itself never changes.
        shapes = sorted(self.by_class["shape"], key=len, reverse=True)
        shape_alternation = (
            '|'.join(re.escape(s) for s in shapes) if shapes else r'(?!)'
        )
        cut_patterns = [
            template.format(shapes=shape_alternation)
            for template in CUT_TEMPLATE_PATTERNS
        ]

        # Every class whose words describe a technique, form, or state
        # of the ingredient rather than its quantity or identity — all
        # of these are "leftover descriptive text" that prep extraction
        # pulls out of the name, regardless of which CulinaryVocabulary
        # class a given term happens to be tagged under. Semantic
        # distinctions between e.g. "preparation" and "state" are the
        # analyzer's concern, not this parser's.
        prep_terms = (
            self.by_class["preparation"]
            | self.by_class["descriptor"]
            | self.by_class["ingredient_form"]
            | self.by_class["state"]
            | self.by_class["temperature"]
            | self.by_class["modifier"]
            | self.by_class["seasoning"]
        )
        prep_fragments = _sorted_fragments(prep_terms)

        # Order: dimensioned/shaped cut grammar, then open-capture cut
        # grammar, then flat vocabulary terms (multi-word first) — see
        # _extract_prep for why this ordering matters. Every fragment
        # gets its own \b...\b: without it, a bare word like "cut" would
        # match inside "cutting", since these fragments come from plain
        # DB terms with no boundary markers built in (unlike the old
        # hand-written pattern strings, which had \b baked into each
        # literal).
        all_prep_patterns = (
            cut_patterns
            + list(GRAMMAR_ONLY_PREP_PATTERNS)
            + [f[2] for f in prep_fragments]
        )
        self.prep_state_pattern = re.compile(
            '|'.join(r'\b(?:%s)\b' % pat for pat in all_prep_patterns),
            re.IGNORECASE
        )

        # -- size / clove-descriptor patterns --
        self.size_pattern = _word_pattern(self.by_class["size"])
        self.clove_bare_descriptor_pattern = _word_pattern(
            self.by_class["size"] | self.by_class["descriptor"]
        )

        # -- "or <prep-method/modifier alternative>" grammar --
        # Formerly a hardcoded _OR_PREP_WORDS regex fragment
        # (pressed/mashed/minced/..., low-sodium/unsalted/store-bought/
        # ...) — those are culinary terms, not grammar, so the word list
        # now comes from the same preparation+modifier vocabulary used
        # above. flexible_separator=True preserves the old behavior of
        # matching "low sodium", "low-sodium", and "lowsodium"-adjacent
        # spacing/hyphenation variants alike. See _split_on_or and
        # _clean_name for the two call sites.
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

        # -- truncation/corruption repair vocabulary --
        # Flat, lowercase set of every whole word available at runtime,
        # from both vocabulary sources: every CulinaryVocabulary class
        # (grammar-adjacent words like units/preparations) AND the
        # ingredient-identity phrases from VocabularyProvider (plain
        # ingredient nouns like "chive", "sausage", "noodle" — a
        # truncated ingredient noun needs THIS side, not the culinary
        # side). Multi-word canonical phrases are exploded into their
        # individual words, since prefix expansion below operates one
        # token at a time. See _expand_truncated_tokens.
        canonical_words = set()
        for terms in self.by_class.values():
            for term in terms:
                canonical_words.update(term.lower().split())
        for phrase in self.protected_phrases:
            canonical_words.update(phrase.lower().split())
        self.canonical_words = canonical_words


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
    """Startup summary confirming the runtime vocabulary actually loaded.

    Deliberately NOT one line per vocabulary class anymore, and
    deliberately NOT warning on an empty class: whether a given class
    (e.g. "shape"/"ingredient_form", which the current seed folds into
    "preparation") is expected to be empty is a seeding decision, not
    something this parser can judge — a blanket per-class check was
    producing false-alarm warnings on every run rather than catching
    real problems. Two aggregate counts are what actually confirms the
    vocabulary loaded at all; per-class counts are still available to
    anyone who wants them (compiled.by_class["<class>"]), just not
    printed unconditionally on every run.
    """
    total_terms = sum(len(terms) for terms in compiled.by_class.values())
    print(f"Vocabulary has {total_terms} terms")
    print(f"Ingredients has {len(compiled.protected_phrases)} foods")


# CLASSIFICATION: genuinely syntactic / intentional deferral.
# Whether "large"/"medium"/"small" belong in a final ingredient identity
# is a normalization question (it's already handled by
# QUALIFIER_STRIP_PATTERNS downstream), not something this stage should
# decide. Keeping them here isn't the parser making an identity choice —
# it's the parser correctly declining to make one, and leaving the word
# in place as evidence for the stage that should.
#
# Set True to preserve large/medium/small in ingredient_name_raw.
# They are stripped during normalization regardless.
KEEP_SIZE_ADJECTIVES = True


# ============================================================
# PHRASE PROTECTION
# Temporarily replaces multi-word phrases with opaque tokens so
# their internal words cannot be mis-extracted (e.g. "ground" in
# "ground beef" must not become a prep word).
# ============================================================

def _protect_phrases(text, protected):
    mapping = {}
    for i, phrase in enumerate(sorted(protected, key=len, reverse=True)):
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


# ============================================================
# EVIDENCE PRESERVATION
# Recovers the original-cased/original-wording substring of the source
# line corresponding to the final parsed ingredient name, so parsing's
# lowercasing and number-rewriting never destroys what the recipe
# actually said. Parsing only removes/tokenizes text — it does not
# reorder or substitute ingredient words (that is normalization's job,
# which this stage does not perform) — so the surviving name words
# should still appear, in the same order, in the original raw text.
# This is intentionally best-effort: if a word cannot be located (e.g.
# _expand_truncated_tokens repaired a corrupted word, so its "original"
# form literally isn't in the source), that word is skipped rather than
# guessed at.
# ============================================================

def _recover_original_span(original_text, name):
    if not name or not original_text:
        return name
    words = re.findall(r"[a-zA-Z']+", name)
    if not words:
        return name
    positions = []
    search_from = 0
    for w in words:
        m = re.search(r'\b' + re.escape(w) + r'\b', original_text[search_from:], re.IGNORECASE)
        if m:
            positions.append((search_from + m.start(), search_from + m.end()))
            search_from += m.end()
    if not positions:
        # No surviving word could be located verbatim in the source
        # (e.g. every word went through _expand_truncated_tokens) — fall
        # back to the parsed name rather than fabricate a span.
        return name
    return original_text[positions[0][0]:positions[-1][1]].strip()


# ============================================================
# TEXT NORMALIZATION
# Converts abbreviations, Unicode fractions, and word-numbers
# to canonical forms used by all downstream extractors.
# ============================================================

def normalize_text(text):
    if not text:
        return text

    # Case-sensitive abbreviations before lowercasing:
    # capital T = tablespoon, lowercase t = teaspoon
    text = re.sub(r'\bTbsp\b|\bTBSP\b', 'tbsp', text)
    text = re.sub(r'\bTSP\b', 'tsp', text)
    text = re.sub(r'(?<![a-zA-Z])T(?![a-zA-Z])', 'tbsp', text)
    text = re.sub(r'(?<![a-zA-Z])t(?![a-zA-Z])', 'tsp', text)

    text = text.lower()
    text = re.sub(r'^[\u2022\u2013•\-–]\s*', '', text)  # leading bullets
    text = text.replace('&', ' and ')

    text, phrase_map = _protect_phrases(text, _active().protected_phrases)

    # Protect words that clash with number word substitutions
    text = text.replace("weight", "__weight__")
    text = text.replace("eighth", "__eighth__")

    # "N and M/D" → decimal  e.g. "1 and 1/2" → "1.5"
    def _and_frac(m):
        return str(float(m.group(1)) + float(m.group(2)) / float(m.group(3)))
    text = re.sub(r'(\d+)\s+and\s+(\d+)\s*/\s*(\d+)', _and_frac, text)
    text = re.sub(r'(\d+)\s+and\s+a\s+half',
                lambda m: str(float(m.group(1)) + 0.5), text)

    # Unicode + ASCII fractions → decimal (longest strings first)
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

    # Inch mark: 1/2" or 0.5" -> 0.5-inch, so it behaves like the written-out
    # "inch" form for every downstream -inch pattern (size descriptors,
    # prep phrases like "cut into 1/2-inch pieces").
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

    # Unit abbreviation cleanup
    text = text.replace("tsp.", "tsp").replace("tbsp.", "tbsp").replace("oz.", "oz")
    text = text.replace("lbs.", "lb").replace("lbs", "lb")
    text = re.sub(r'(?<![a-zA-Z])c(?![a-zA-Z])', 'cup', text)
    text = re.sub(r'\bfrom\s+\d+\s+', 'from ', text)

    return text.strip()


# ============================================================
# PLUS / + SPLITTING
# Only split when the second segment starts with a quantity.
# "one egg plus one yolk" → two segments
# "plus more for dusting" → NOT split (noise phrase, handled later)
# ============================================================

_PLUS_SPLIT = re.compile(
    r'(?<!\w)\+(?!\w)|'
    r'\bplus\b(?!\s+(?:more|additional)\b)',
    re.IGNORECASE
)
_WORD_NUMBERS_RE = r'(?:one|two|three|four|five|six|seven|eight|nine|ten|half|a)\b'


def _split_on_plus(text):
    paren_masked = re.sub(r'\([^)]*\)', lambda m: 'X' * len(m.group(0)), text)
    parts_masked = _PLUS_SPLIT.split(paren_masked)
    if len(parts_masked) < 2:
        return [text]
    # Reconstruct positions in the original text
    positions, pos = [], 0
    for part in parts_masked:
        positions.append((pos, pos + len(part)))
        pos += len(part)
        m = _PLUS_SPLIT.match(paren_masked[pos:])
        if m:
            pos += len(m.group(0))
    parts = [text[s:e].strip() for s, e in positions if text[s:e].strip()]
    if len(parts) >= 2:
        second = parts[1].strip()
        if re.match(r'^\d', second) or re.match(_WORD_NUMBERS_RE, second, re.IGNORECASE):
            return parts
    return [text]


# ============================================================
# OR-ALTERNATIVE SPLITTING
# Splits only when "or" introduces a distinct new ingredient
# (different quantity, or a relative alternative like "double the amount").
# Prep-method "or" clauses are NOT split.
# ============================================================

_OR_QTY_UNIT = re.compile(
    r'^(.+?)\s+or\s+(\d[\d./]*(?:\s*[-–]\s*\d[\d./]*)?)\s+(\w+)\s+(.+)$',
    re.IGNORECASE
)
_OR_RELATIVE = re.compile(
    r'^(.+?)\s+or\s+(double|twice|triple|half)\s+the\s+(?:amount\s+of\s+)?(.+)$',
    re.IGNORECASE
)
# The "or <prep-method/modifier alternative>" alternation used to live
# here as _OR_PREP_WORDS — a hardcoded string of culinary terms
# (pressed/mashed/minced/..., low-sodium/unsalted/store-bought/...).
# That's vocabulary, not grammar, so it has moved to
# CompiledVocabulary.or_prep_pattern (runtime-sourced from
# self.by_class["preparation"] | self.by_class["modifier"]) —
# see _active().or_prep_pattern below and .or_prep_cleanup_pattern,
# shared with _clean_name's leaked-prep-clause cleanup, so an "or" that
# introduces a prep-method / qualifier alternative (rather than a
# genuine second ingredient) is recognized consistently in both places.

# A bare "or" whose right side isn't a real alternative ingredient — noise
# phrases / hedges that would otherwise become a bogus, empty optional row.
_OR_ALT_BLOCK = re.compile(
    r'^(?:as\s+needed|as\s+desired|if\s+desired|desired|needed|required|'
    r'to\s+taste|more|additional|so\s+desired|'
    r'\d+(?:\.\d+)?%\s+by\s+weight)\b',
    re.IGNORECASE
)

_OR_TOKEN = re.compile(r'\bor\b', re.IGNORECASE)


def _split_on_or(text):
    """Returns (primary_text, alt_text, alt_kind).

    alt_kind records WHICH GRAMMATICAL PATTERN matched — this is a
    syntactic classification (which regex fired), not a semantic claim
    about whether the two sides name different ingredients. Downstream
    normalization/identity-resolution stages decide that; this function
    only preserves the structural evidence of how the source line was
    written.

      "measure"    — "<primary> or <qty> <unit> <rest>" (an alternate
                      measure, e.g. "2 cups pumpkin puree or one 15-oz
                      can pumpkin puree")
      "scale"      — "<primary> or double/triple/half the amount of X"
      "ingredient" — generic single unparenthesized "or" fallback split,
                      e.g. "parmesan or pecorino"
      None         — no split occurred
    """
    stripped = text.strip()
    if _active().or_prep_pattern.search(stripped):
        return stripped, None, None

    m = _OR_QTY_UNIT.match(stripped)
    if m and m.group(3).strip().lower() in _active().unit_vocab:
        alt = "%s %s %s" % (m.group(2).strip(), m.group(3).strip(), m.group(4).strip())
        return m.group(1).strip(), alt, "measure"

    m = _OR_RELATIVE.match(stripped)
    if m:
        return m.group(1).strip(), "%s the amount of %s" % (m.group(2), m.group(3)), "scale"

    # Generic ingredient-swap split: "<primary> or <alternative>", e.g.
    # "chicken thighs (about 3) or wings (...)" or "chicken broth or water".
    # Only fires on a single, unambiguous "or" outside any parentheses —
    # multiple "or"s (e.g. "2 large or 3 medium" inside a paren aside) are
    # left alone, since it's unclear which one is the real split point.
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


# ============================================================
# EXPLICIT MEASURE EXTRACTION
# Pulls gram, ml, and percent values from the text.
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
    """Strip a loose, non-parenthetical 'about N word...' aside (its
    original purpose) WITHOUT reaching inside parentheses — content inside
    parens, e.g. "(about 1/2 cup)", is deliberately preserved here so
    _extract_parentheticals downstream can capture it as a note instead of
    it being silently discarded before it's ever seen."""
    parens_found = []

    def _mask(m):
        parens_found.append(m.group(0))
        return "__PARENTOK_%d__" % (len(parens_found) - 1)

    masked = re.sub(r'\([^)]*\)', _mask, text)
    masked = _APPROX_SECONDARY.sub('', masked).strip().rstrip(',').strip()
    for i, p in enumerate(parens_found):
        masked = masked.replace("__PARENTOK_%d__" % i, p)
    return masked


def _extract_explicit_measures(text):
    grams = ml = pct = plus_additional_note = None
    m = _MASS_PAT.search(text)
    if m:
        grams = m.group(2)
        text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    m = _ML_PAT.search(text)
    if m:
        ml = m.group(2)
        text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    else:
        m = _LITER_PAT.search(text)
        if m:
            ml = str(float(m.group(1)) * 1000)
            text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    m = _PCT_PAT.search(text)
    if m:
        pct = m.group(1)
        text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    text = _strip_approx_secondary_outside_parens(text)
    m = _PLUS_ADDITIONAL.search(text)
    if m:
        plus_additional_note = m.group(0).strip().lstrip(',').strip()
        text = (text[:m.start()] + text[m.end():]).strip().rstrip(',').strip()
    return text, grams, ml, pct, plus_additional_note


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


def _extract_parentheticals(text):
    matches = re.findall(r'\((.*?)\)', text)
    text = re.sub(r'\(.*?\)', '', text).strip()
    kept = [m for m in matches
            if not any(p.search(m) for p in _DROP_PAREN)
            and re.search(r'[a-zA-Z0-9]', m)]
    return text, kept


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


# ============================================================
# CAN / JAR SIZE EXTRACTION
# "1 28-oz can" → count=1, unit="can", size_note="28 oz"
# ============================================================

_CAN_PAT = re.compile(
    r'(?:(\d+(?:\.\d+)?)\s+)?'
    r'(\d+(?:\.\d+)?)\s*[-\s]?(?:ounce|oz)\.?\s+'
    r'(cans?|jars?|bottles?|packages?|bags?|boxes?)',
    re.IGNORECASE
)


def _extract_can_size(text):
    m = _CAN_PAT.search(text)
    if m:
        count     = m.group(1) or "1"
        size_note = "%s oz" % m.group(2)
        container = m.group(3).lower()
        text = (text[:m.start()] + text[m.end():]).strip().lstrip(',').strip()
        return text, count, container, size_note
    return text, None, None, None


# ============================================================
# JUICE FORM
# ============================================================

_JUICE_PAT = re.compile(r'^(juice(?:\s+(?:from|of))?\s+)', re.IGNORECASE)


def _extract_juice_form(text):
    m = _JUICE_PAT.match(text)
    if m:
        return text[m.end():].strip(), "juice"
    return text, None


# ============================================================
# PERCENT-BY-WEIGHT NOTE
# ============================================================

_PCT_WEIGHT_PAT = re.compile(
    r',?\s*or\s+\d+(?:\.\d+)?%\s+by\s+weight\s+of\s+[^,)]+', re.IGNORECASE
)


def _extract_pct_weight(text):
    m = _PCT_WEIGHT_PAT.search(text)
    if m:
        note = m.group(0).strip().lstrip(',').strip()
        text = (text[:m.start()] + text[m.end():]).strip()
        return text, note
    return text, None


# ============================================================
# SIZE DESCRIPTOR PROTECTION
# e.g. "1/4-inch-thick" — a measurement of cut size, not the ingredient.
# It must NOT be visible to quantity/unit extraction (otherwise a trailing
# word like "slices" or "strips" gets misread as a quantity_unit), but it
# also must NOT be discarded: prep extraction (the compiled cut-pattern
# grammar) needs the literal
# "N-inch..." text later to build phrases like "cut into 1/4-inch-thick
# slices". So it's tokenized here and restored just before prep extraction.
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


def _extract_quantity(text):
    m = _QTY_PAT.match(text)
    if m:
        full = m.group(0)
        if m.group(4):
            qty = float(m.group(4))          # range: take upper bound
        elif m.group(2):
            qty = float(m.group(1)) + float(m.group(2))   # whole + fraction
        else:
            qty = float(m.group(1))
        text = text[len(full):].strip()
        qty_str = str(int(qty)) if qty == int(qty) else str(qty)
        return text, qty_str
    return text, None


# ============================================================
# UNIT EXTRACTION
# ============================================================

# CLASSIFICATION: genuinely syntactic.
# "Is 'cloves' functioning as a counting unit (like 'heads' in '2 heads
# of lettuce') or as the head noun of the phrase itself?" is answered by
# sentence structure alone — is there a noun phrase after it for it to be
# counting ("N cloves garlic"/"N cloves of garlic"), or is what follows
# only pre-nominal adjectives with no head noun ("N whole cloves")? This
# does not require knowing that cloves-the-spice and garlic-cloves are
# different ingredients — only English grammar. Contrast with
# TYPO_FIXES-style rules that rename one ingredient to another; nothing
# here renames "cloves" to anything.
_GARLIC_CONTEXT = re.compile(r'\bcloves?\s+(?:of\s+)?(garlic|shallot)\b', re.IGNORECASE)
_CLOVES_OF = re.compile(r'\bcloves?\s+of\b', re.IGNORECASE)
# Descriptors that can precede "cloves" as the ingredient itself (e.g. "10
# whole cloves" = the spice), as opposed to "cloves" counting some other
# named ingredient (garlic, shallot). If, after removing "cloves" and any
# recognized unit word, only these descriptors are left, "cloves" is the
# ingredient, not a unit. Sourced from CulinaryVocabulary.by_class
# ("size") | self.by_class["descriptor"] via
# CompiledVocabulary — see _active().clove_bare_descriptor_pattern
# below, not a hardcoded list.

def _extract_unit(text):
    unit_pat = _active().unit_pattern
    if re.search(r'\bcloves?\b', text, re.IGNORECASE):
        if _GARLIC_CONTEXT.search(text):
            # "N cloves of garlic" — remove "cloves of" as a unit, so "of"
            # isn't left stranded once "garlic"'s neighboring adjective
            # (e.g. "medium") is later stripped at normalize time.
            if _CLOVES_OF.search(text):
                text = _CLOVES_OF.sub('', text, count=1).strip()
                return text, "clove"
            # "N cloves garlic" (no "of") falls through to the standard
            # unit-word extraction below, which already handles this fine.
        else:
            without = re.sub(r'\bcloves?\b', '', text, flags=re.IGNORECASE).strip()
            without_units = unit_pat.sub('', without).strip()
            if not without_units:
                # Nothing but "cloves" (+ maybe a real unit) is here — e.g.
                # "1/4 tsp cloves" (the spice). Pull out the real unit if
                # there is one, but leave "cloves" itself in the name.
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
                # Only size/state descriptors (e.g. "whole") remain once
                # "cloves" is removed — "cloves" is the ingredient itself
                # (whole cloves, the spice), not a counting unit.
                return text, None
    m = unit_pat.search(text)
    if m:
        unit = m.group(0)
        text = re.sub(r'\b' + re.escape(unit) + r'\b', '', text, count=1, flags=re.IGNORECASE).strip()
        return text, unit
    return text, None


# ============================================================
# UNIT ROUTING
# Standard measurement units are removed from quantity_value/unit
# and placed in dedicated columns.
# quantity_unit is reserved for count/container units only.
# ============================================================

_KG_TO_G = 1000.0
_L_TO_ML  = 1000.0


def _route_unit(quantity, unit, grams, ml):
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
# PREP + STATE EXTRACTION
# ============================================================

# ============================================================
# PREP + STATE EXTRACTION
#
# Returns phrases in the ORDER THEY APPEAR IN THE SOURCE TEXT, not in
# the provider's prep_patterns() list order. This matters because
# `preparation` is meant to feed a technique-graph lookup downstream, and
# technique sequence is meaningful ("peeled, then quartered, then cut
# into slices" is a real order a cook follows).
#
# Implementation: provider.prep_patterns() (multi-word / more-specific
# first) and provider.temperature_state_patterns() are combined into one
# alternation (_active().prep_state_pattern, see CompiledVocabulary) and
# scanned with a single finditer pass. re.finditer walks left-to-right
# and returns non-overlapping matches, so the match order IS the text
# order. Priority between overlapping candidates at the same starting
# position is still governed by alternation order (multi-word patterns
# listed first), exactly as it was for the old per-pattern-loop approach.
# ============================================================

def _extract_prep(text):
    pat = _active().prep_state_pattern
    found = [m.group(0).strip() for m in pat.finditer(text)]
    text = pat.sub('', text).strip()
    return text, found


# ============================================================
# NOISE PHRASE EXTRACTION
# These phrases ("plus more", "to taste", "as needed", "if desired") carry
# real cooking guidance — how flexible the amount is — so they're captured
# as notes rather than deleted outright. "optional" is the one exception:
# that's already represented structurally by the `optional` column, so
# recording it again as a note would just be redundant noise.
# ============================================================

def _extract_noise(text):
    found = []
    for phrase in NOISE_PHRASES:
        pat = re.compile(r'\b' + re.escape(phrase) + r'\b', re.IGNORECASE)
        if pat.search(text):
            if phrase.lower() != "optional":
                found.append(phrase)
            text = pat.sub('', text)
    return text.strip(), found


_LEADING_SYMBOLS = re.compile(r'^[\-\u2013\u2022\+\*•\s]+')


def _remove_leading_symbols(text):
    return _LEADING_SYMBOLS.sub('', text).strip()


_ACTION_WORDS = {"washed", "separated", "into"}


def _remove_action_words(text):
    tokens, result = text.split(), []
    for i, w in enumerate(tokens):
        if w.lower() == "and":
            prev_ok = i > 0 and tokens[i-1].lower() not in _ACTION_WORDS
            next_ok = i < len(tokens)-1 and tokens[i+1].lower() not in _ACTION_WORDS
            if not (prev_ok and next_ok):
                continue
        elif w.lower() in _ACTION_WORDS:
            continue
        result.append(w)
    return " ".join(result)


def _remove_size_adjectives(text):
    if KEEP_SIZE_ADJECTIVES:
        return text
    text = _active().size_pattern.sub('', text)
    return " ".join(text.split())


# ============================================================
# NAME CLEANUP
# ============================================================

# ============================================================
# NAME CLEANUP
# Two of these strips ("from <N> <word>" source phrases, trailing
# "for <purpose>" clauses) remove real information from the name text —
# they're captured and returned as notes instead of being discarded, so a
# line like "zest, from 2 lemons" or "butter, for greasing the pan" doesn't
# silently lose that context.
# ============================================================

def _clean_name(text):
    notes = []
    text = re.sub(r'[,;]', ' ', text)
    text = re.sub(r'[()]', ' ', text)
    text = " ".join(text.split())
    # Strip leading connectors
    text = re.sub(r'^(?:of|from|and|or|\*|:|with)\b\s*|^[-–]\s*', '', text, flags=re.IGNORECASE)
    # Strip trailing connectors
    text = re.sub(r'\s+(and|or|\*|with|in)$', '', text, flags=re.IGNORECASE)
    # "from <N> <word>" source phrases (e.g. "from 1 lime") — capture before
    # discarding, since it's the count of source items, not junk.
    m = re.search(r'\bfrom\s+\d*\s*(?:large|small|medium|fresh|whole)?\s*\w+\s*$', text, re.IGNORECASE)
    if m:
        notes.append(m.group(0).strip())
        text = text[:m.start()].strip()
    text = re.sub(r'\bfrom\s+', '', text, flags=re.IGNORECASE).strip()
    # Prep-method "or" clauses that leaked through
    text = _active().or_prep_cleanup_pattern.sub('', text).strip()
    # Trailing "for <purpose>" — a usage note ("for garnish", "for greasing
    # the pan"), not a technique, so it goes to notes rather than vanishing.
    m = re.search(r'\s+for\s+\S.*$', text, re.IGNORECASE)
    if m:
        notes.append(m.group(0).strip())
        text = text[:m.start()].strip()
    # Unresolved "N/N" count ranges that aren't recognized fractions (e.g.
    # "16/20" shrimp-per-pound sizing) — the digits get stripped below but
    # the "/" would otherwise be left stranded.
    text = re.sub(r'(?<!\w)\d+/\d+(?!\w)', '', text).strip()
    # Stray numbers, isolated punctuation, isolated single letters
    text = re.sub(r'(?<!\w)\d+(?:\.\d+)?(?!\w)', '', text).strip()
    # A hyphen left dangling after its neighboring word was extracted as
    # prep (e.g. "medium-diced" -> "medium-" once "diced" is pulled out).
    text = re.sub(r'(?<=[a-z])-(?=\s|$)', '', text).strip()
    text = re.sub(r'(?:^|\s)-(?=[a-z])', ' ', text).strip()
    text = re.sub(r'(?<!\w)\.(?!\w)', '', text).strip()
    text = re.sub(r'(?<!\w)[a-z](?!\w)', '', text, flags=re.IGNORECASE).strip()
    return " ".join(text.split()).strip('*').strip(), notes


# ============================================================
# TRUNCATION / CORRUPTION REPAIR
# Replaces the old TRUNCATION_FIXES dict (pattern -> replacement,
# hardcoded in parser_vocabulary.py). That dict is gone — several of its
# entries were plain culinary/ingredient nouns, not grammar, so a static
# Python list was the wrong home for them. This is a general algorithm
# instead: an unrecognized word is compared against every canonical word
# available at runtime (CompiledVocabulary.canonical_words — see its
# construction for what feeds it); if EXACTLY ONE canonical word begins
# with it, it's expanded to that word. Zero matches or multiple matches
# both leave the word untouched — ambiguity is not a license to guess.
# A short minimum length guards against 1-character tokens matching
# huge swaths of the vocabulary and "expanding" into noise.
# ============================================================

_MIN_TRUNCATION_PREFIX_LEN = 2


def _expand_truncated_word(word, canonical_words):
    lw = word.lower()
    if len(lw) < _MIN_TRUNCATION_PREFIX_LEN or lw in canonical_words:
        return word
    matches = {w for w in canonical_words if w != lw and w.startswith(lw)}
    if len(matches) == 1:
        return matches.pop()
    return word


def _expand_truncated_tokens(text):
    canonical_words = _active().canonical_words
    return re.sub(
        r"[a-zA-Z']+",
        lambda m: _expand_truncated_word(m.group(0), canonical_words),
        text
    )


_BARE_SIZE_WORDS = {
    "large", "extra-large", "extra large", "medium", "small", "jumbo",
    "very", "quite", "rather", "fairly", "slightly",
}


def _split_multi_ingredients(text):
    """Split "salt and pepper" → ["salt", "pepper"] only for short, digit-free names."""
    if " and " in text:
        parts = [p.strip() for p in text.split(" and ", 1)]
        if (all(not re.search(r'\d', p) for p in parts) and len(text.split()) <= 6
                and all(p.lower() not in _BARE_SIZE_WORDS and p for p in parts)):
            return parts
    return [text]


# ============================================================
# OPTIONAL FLAG
# ============================================================

def _is_optional(text):
    stripped = text.strip().lstrip('•-–*').strip()
    return bool(
        re.match(r'^optional\s*:', stripped, re.IGNORECASE)
        or re.search(r'\(optional\)', stripped, re.IGNORECASE)
        or re.search(r'\boptional\b', stripped, re.IGNORECASE)
    )


# ============================================================
# CORE LINE PARSER
# Returns a list of result dicts for one normalized sub-line.
# ============================================================

def _parse_one_line(text, optional=False, original_scope=None,
                     alt_group_id=None, alt_kind=None):
    text = normalize_text(text)
    text = _remove_leading_symbols(text)

    text, juice_prep      = _extract_juice_form(text)
    text, pct_note        = _extract_pct_weight(text)
    text, grams, ml, pct, plus_additional_note = _extract_explicit_measures(text)
    text, parens          = _extract_parentheticals(text)

    # Check parentheticals for a secondary imperial measure e.g. "(1 stick)"
    paren_qty = paren_unit = None
    for pc in parens:
        pq, pu = _paren_measure(pc)
        if pq and pu:
            paren_qty, paren_unit = pq, pu
            break

    text, can_qty, can_unit, can_size_note = _extract_can_size(text)
    text, size_map = _protect_size_descriptors(text)

    # Strip leftover secondary measures when a gram weight was already extracted
    if grams is not None:
        text = re.sub(
            r'(?:^|(?<=\s))(\d+(?:\.\d+)?)\s*'
            r'(cup|cups|tbsp|tsp|tablespoon|tablespoons|teaspoon|teaspoons|'
            r'oz|ounce|ounces|lb|pound|pounds|pint|pints|quart|quarts|'
            r'ml|liter|litre|g|kg)(?!\w)',
            '', text, flags=re.IGNORECASE
        ).strip().strip(',').strip()

    text, quantity = _extract_quantity(text)

    # Resolve quantity/unit priority:
    #   1. Explicit can/jar size
    #   2. Parenthetical imperial measure
    #   3. Gram weight as sole primary measure
    unit = None
    paren_measure_used = False
    if can_qty is not None:
        quantity, unit = can_qty, can_unit
        if can_size_note:
            parens = list(parens) + [can_size_note]
    elif grams is not None:
        if paren_qty and paren_unit:
            quantity, unit = paren_qty, paren_unit
            paren_measure_used = True
        elif quantity is None:
            quantity, unit = grams, "g"

    text, phrase_map = _protect_phrases(text, _active().protected_phrases)
    if unit is None:
        text, unit = _extract_unit(text)

    # Restore size descriptors now, before prep extraction, so cut-pattern
    # grammar entries that reference literal "N-inch..." text can match them.
    text = _restore_phrases(text, size_map)

    text, noise_notes = _extract_noise(text)
    text, prep_phrases = _extract_prep(text)
    text = _remove_action_words(text)
    text = _remove_size_adjectives(text)
    text = _restore_phrases(text, phrase_map)

    name, name_notes = _clean_name(text)
    name = _expand_truncated_tokens(name)

    # `preparation` is an ordered list of technique/state phrases, in the
    # order they appeared in the source line, meant to resolve 1:1 against
    # the technique graph downstream (e.g. ["peeled", "quartered lengthwise",
    # "cut crosswise into 0.25-inch-thick slices"]). Juice form ("juice of")
    # is always extracted from the very front of the line, so it always
    # belongs at the front of the sequence.
    preparation = list(prep_phrases)
    if juice_prep:
        preparation.insert(0, juice_prep)
    final_prep = preparation if preparation else None

    # `notes` is free text that is NOT a technique — yield/count asides,
    # secondary measures not chosen as the primary one, can/jar size,
    # "% by weight" notes, source-quantity phrases ("from 2 lemons"), usage
    # phrases ("for greasing the pan"), and flexible-amount phrases ("plus
    # more", "to taste", "as needed"). Kept out of `preparation` so the
    # technique-graph lookup downstream never has to fail-match against
    # non-technique text — but nothing here is simply thrown away.
    notes = []
    for pc in parens:
        pq, pu = _paren_measure(pc)
        if pq and pu and pq == paren_qty and pu == paren_unit and paren_measure_used:
            continue
        notes.append(pc)
    if pct_note:
        notes.append(pct_note.strip())
    if plus_additional_note:
        notes.append(plus_additional_note)
    notes.extend(noise_notes)
    notes.extend(name_notes)
    final_notes = "; ".join(notes) if notes else None

    routed = _route_unit(quantity, unit, grams, ml)

    results = []
    for ingredient_name in _split_multi_ingredients(name):
        results.append({
            "quantity":              routed["quantity"],
            "unit":                  routed["unit"],
            "preparation":           final_prep,
            "notes":                 final_notes,
            "grams":                 routed["grams"],
            "ml":                    routed["ml"],
            "imperial_weight_value": routed["imperial_weight_value"],
            "imperial_weight_unit":  routed["imperial_weight_unit"],
            "imperial_volume_value": routed["imperial_volume_value"],
            "imperial_volume_unit":  routed["imperial_volume_unit"],
            "scaling":               pct,
            "optional":              1 if optional else 0,
            "ingredient_name_raw":      ingredient_name,
            "ingredient_name_original": _recover_original_span(original_scope, ingredient_name),
            "alt_group_id":          alt_group_id,
            "alt_kind":              alt_kind,
        })
    return results


# ============================================================
# PUBLIC ENTRY POINT
# Parses one raw ingredient line through all splitting logic.
# ============================================================

def parse_ingredient_line(raw_text):
    """
    Parse a single raw ingredient line.  Returns a list of dicts
    (one per resulting row).  A line can expand via:
      - or-alternative splitting  → rows share a common alt_group_id
      - plus/+ splitting          → peer rows
      - "A and B" names           → separate name rows

    NOTE on optional vs. alternative: these are different relationships
    and are no longer conflated. `optional` reflects the line's own
    self-declared optionality ("(optional)", "if desired"). An "or"
    split (e.g. "parmesan or pecorino") means the two rows are mutually
    exclusive ALTERNATIVES for the same slot — that is NOT the same as
    either one being individually omittable, so it is recorded via
    alt_group_id/alt_kind instead of by forcing optional=1.
    """
    results = []
    line_optional = _is_optional(raw_text)
    primary_text, alt_text, alt_kind = _split_on_or(raw_text)

    group_id = uuid.uuid4().hex[:12] if alt_text is not None else None

    _MEASURE_KEYS = (
        "quantity", "unit", "grams", "ml",
        "imperial_weight_value", "imperial_weight_unit",
        "imperial_volume_value", "imperial_volume_unit",
    )
    primary_measure = None

    for slot, text in enumerate([primary_text, alt_text]):
        if text is None:
            continue
        # `text` here is still the case-preserved slice of raw_text that
        # _split_on_or returned (normalize_text/lowercasing happens next,
        # below) — keep it as the evidence scope for recovering
        # ingredient_name_original for every row this slot expands into.
        original_slot_text = text

        sub_lines = _split_on_plus(normalize_text(text))
        # Parallel split of the case-preserved slot text, so each
        # normalized sub-line has a matching original-cased scope to
        # recover its name from. "+"/"plus" aren't touched by
        # normalize_text, so split points line up in the common case;
        # if the counts ever disagree, fall back to the whole slot as
        # the scope for every sub-line (always correct, just coarser).
        original_sub_lines = _split_on_plus(original_slot_text)
        if len(original_sub_lines) != len(sub_lines):
            original_sub_lines = [original_slot_text] * len(sub_lines)

        sub_results = []
        for sub, orig_sub in zip(sub_lines, original_sub_lines):
            sub_results.extend(_parse_one_line(
                sub,
                optional=line_optional,
                original_scope=orig_sub,
                alt_group_id=group_id,
                alt_kind=alt_kind,
            ))
        # Forward the nearest non-empty name to any empty-name rows
        if len(sub_results) > 1:
            fallback = next((r["ingredient_name_raw"] for r in reversed(sub_results)
                            if r["ingredient_name_raw"]), "")
            fallback_orig = next((r["ingredient_name_original"] for r in reversed(sub_results)
                            if r["ingredient_name_raw"]), "")
            for r in sub_results:
                if not r["ingredient_name_raw"]:
                    r["ingredient_name_raw"] = fallback
                    r["ingredient_name_original"] = fallback_orig

        if slot == 0:
            # Remember the primary's measurement so an "or" alternative that
            # states no quantity of its own ("...chicken thighs or wings",
            # "chicken broth or water") can inherit it, rather than being
            # left with every measurement column empty.
            if sub_results:
                primary_measure = sub_results[0]
        elif primary_measure is not None:
            for r in sub_results:
                if not any(r.get(k) for k in _MEASURE_KEYS):
                    for k in _MEASURE_KEYS:
                        r[k] = primary_measure.get(k)

        results.extend(sub_results)
    return results


# ============================================================
# DB SCHEMA
# ============================================================

# ============================================================
# DB SCHEMA
#
# CONTRACT: only db/init_db.py creates tables/columns/indexes. This
# module never issues CREATE TABLE, ALTER TABLE, or CREATE INDEX — it
# only verifies the schema it needs already exists, and fails loudly
# with an actionable message if it doesn't.
#
# The exact DDL this stage requires in db/init_db.py (for
# recipe_ingredient_lines_parsed) is:
#
#     c.execute("""
#         CREATE TABLE IF NOT EXISTS recipe_ingredient_lines_parsed (
#             id                      INTEGER PRIMARY KEY AUTOINCREMENT,
#             -- source
#             ingredient_block_id     INTEGER NOT NULL
#                                         REFERENCES recipe_ingredient_blocks(id),
#             recipe_id               INTEGER NOT NULL,
#             recipe_section_id       INTEGER NOT NULL,
#             recipe_name             TEXT,
#             section_name            TEXT,
#             -- position within the block
#             line_index              INTEGER NOT NULL,
#             -- original text (never modified)
#             raw_text                TEXT NOT NULL,
#             -- parsed dimensions
#             quantity_value          TEXT,
#             quantity_unit           TEXT,
#             imperial_weight_value   TEXT,
#             imperial_weight_unit    TEXT,
#             imperial_volume_value   TEXT,
#             imperial_volume_unit    TEXT,
#             grams                   REAL,
#             ml                      REAL,
#             scaling                 TEXT,
#             -- ordered JSON list of technique/state phrases, source-text
#             -- order, e.g. '["peeled", "cut crosswise into 0.25-inch-thick slices"]'
#             -- intended to resolve 1:1 against the technique graph
#             preparation             TEXT,
#             -- free-text asides that are NOT techniques (yield/count notes,
#             -- secondary measures, can/jar size, "% by weight" notes, ...)
#             -- kept separate so they are never fed to the technique lookup
#             notes                   TEXT,
#             -- name as it appears after measurement / prep extraction
#             -- (lowercased, numbers rewritten — parser output, NOT
#             -- normalized or identity-resolved)
#             ingredient_name_raw     TEXT,
#             -- the same span sliced verbatim out of the ORIGINAL source
#             -- line: original casing/wording/hyphenation preserved.
#             -- Best-effort (see _recover_original_span); falls back to
#             -- ingredient_name_raw if no source words could be located.
#             ingredient_name_original TEXT,
#             -- non-NULL and shared between rows produced from the same
#             -- "X or Y" source line (mutual-exclusion relationship).
#             -- NULL for ordinary, non-alternative lines.
#             alt_group_id            TEXT,
#             -- which grammatical pattern produced the alt_group_id split:
#             -- 'ingredient' | 'measure' | 'scale' | NULL. See module
#             -- docstring. A syntactic classification, not an identity claim.
#             alt_kind                TEXT,
#             -- flags
#             optional                INTEGER DEFAULT 0,
#             -- audit
#             parsed_at               TEXT DEFAULT (datetime('now')),
#             FOREIGN KEY(recipe_id)           REFERENCES recipes(id),
#             FOREIGN KEY(recipe_section_id)   REFERENCES recipe_sections(id),
#             FOREIGN KEY(ingredient_block_id) REFERENCES recipe_ingredient_blocks(id)
#         )
#     """)
#     c.execute("""
#         CREATE INDEX IF NOT EXISTS idx_rilp_ingredient_block_id
#             ON recipe_ingredient_lines_parsed (ingredient_block_id)
#     """)
#     c.execute("""
#         CREATE INDEX IF NOT EXISTS idx_rilp_recipe_id
#             ON recipe_ingredient_lines_parsed (recipe_id)
#     """)
#     c.execute("""
#         CREATE INDEX IF NOT EXISTS idx_rilp_recipe_section_id
#             ON recipe_ingredient_lines_parsed (recipe_section_id)
#     """)
#
# If you're updating an existing recipe_ingredient_lines_parsed table
# that predates ingredient_name_original/alt_group_id/alt_kind, add
# those three columns to init_db.py's CREATE TABLE (SQLite will no-op
# CREATE TABLE IF NOT EXISTS against an existing table, so a one-time
# ALTER TABLE ADD COLUMN belongs in init_db.py too, guarded by the same
# PRAGMA table_info check init_db.py already uses elsewhere for `notes`).
# ============================================================

_REQUIRED_COLUMNS = {
    "ingredient_block_id", "recipe_id", "recipe_section_id",
    "recipe_name", "section_name", "line_index", "raw_text",
    "quantity_value", "quantity_unit",
    "imperial_weight_value", "imperial_weight_unit",
    "imperial_volume_value", "imperial_volume_unit",
    "grams", "ml", "scaling", "preparation", "notes",
    "ingredient_name_raw", "ingredient_name_original",
    "alt_group_id", "alt_kind", "optional",
}


def _verify_schema(conn):
    """Check-only. This stage does not create or alter tables — that is
    db/init_db.py's exclusive responsibility. Fails loudly and early
    (before parsing any lines) rather than letting a missing column
    surface later as an opaque sqlite3.OperationalError mid-insert."""
    existing = {row[1] for row in conn.execute(
        "PRAGMA table_info(recipe_ingredient_lines_parsed)"
    )}
    if not existing:
        raise RuntimeError(
            "recipe_ingredient_lines_parsed does not exist. "
            "Run db/init_db.py first — this module does not create tables."
        )
    missing = _REQUIRED_COLUMNS - existing
    if missing:
        raise RuntimeError(
            "recipe_ingredient_lines_parsed is missing column(s): "
            f"{sorted(missing)}. Add them to the CREATE TABLE (and, for an "
            "already-populated table, an ALTER TABLE ADD COLUMN migration) "
            "in db/init_db.py — see the DDL in this module's DB SCHEMA "
            "comment block. This module does not create or alter tables."
        )


# ============================================================
# DB EXECUTION
# ============================================================

_PROGRESS_MIN_INTERVAL = 0.2  # seconds between progress-line redraws


def _print_progress(done, total, start_time):
    """Redraws a single status line in place (\\r, no newline) — this is
    the "please wait" signal for a batch that's slow enough to look
    hung. Throttled by wall-clock time (not by line count) so it stays
    cheap and readable regardless of how large `total` is, and always
    draws on the final line so the counter visibly reaches 100%. Caller
    is responsible for printing a newline once done (see _run)."""
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

    # Split each block's text into its non-blank lines up front. This is
    # just str.splitlines() — no parsing happens here — but doing it in
    # one pass lets us report the true total line count before any real
    # work starts, and gives the progress counter below a fixed
    # denominator instead of an estimate.
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
    lines_done = 0
    for ingredient_block_id, recipe_id, recipe_section_id, recipe_name, section_name, raw_lines in prepared_blocks:
        # Idempotent: delete previously parsed rows for this block
        c.execute(
            "DELETE FROM recipe_ingredient_lines_parsed WHERE ingredient_block_id = ?",
            (ingredient_block_id,)
        )

        for line_index, raw_line in enumerate(raw_lines):
            parsed_rows = parse_ingredient_line(raw_line)
            for r in parsed_rows:
                c.execute("""
                    INSERT INTO recipe_ingredient_lines_parsed (
                        ingredient_block_id,
                        recipe_id,
                        recipe_section_id,
                        recipe_name,
                        section_name,
                        line_index,
                        raw_text,
                        quantity_value, quantity_unit,
                        imperial_weight_value, imperial_weight_unit,
                        imperial_volume_value, imperial_volume_unit,
                        grams, ml, scaling, preparation, notes,
                        ingredient_name_raw, ingredient_name_original,
                        alt_group_id, alt_kind, optional
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    ingredient_block_id,
                    recipe_id,
                    recipe_section_id,
                    recipe_name,
                    section_name,
                    line_index,
                    raw_line,
                    r["quantity"], r["unit"],
                    r["imperial_weight_value"], r["imperial_weight_unit"],
                    r["imperial_volume_value"], r["imperial_volume_unit"],
                    r["grams"], r["ml"], r["scaling"],
                    json.dumps(r["preparation"]) if r["preparation"] else None,
                    r["notes"],
                    r["ingredient_name_raw"], r["ingredient_name_original"],
                    r["alt_group_id"], r["alt_kind"], r["optional"],
                ))
                total_lines += 1
            lines_done += 1
            _print_progress(lines_done, total_input_lines, start_time)

    sys.stdout.write("\n")  # move off the in-place progress line
    conn.commit()
    print("parse_ingredient_lines: %d blocks → %d parsed rows" % (len(blocks), total_lines))


def parse_ingredient_lines():
    with sqlite3.connect(DB_PATH) as conn:
        _run(conn)


def main():
    try:
        parse_ingredient_lines()

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT count(*) FROM recipe_ingredient_lines_parsed")
            count = c.fetchone()[0]

        print(f"recipe_ingredient_lines_parsed populated with {count} ingredient lines")

    except Exception:
        print("lines failed to parse")
        raise


if __name__ == "__main__":
    main()