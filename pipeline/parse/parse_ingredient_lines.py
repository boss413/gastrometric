# pipeline/parse/parse_ingredient_lines.py
#
# Pipeline stage: parse
#
#   Reads  : recipe_ingredient_blocks  (one blob per section, written by ingest_markdown)
#   Writes : recipe_ingredient_lines_parsed
#
# Each blob is split on newlines into individual lines.
# Each line is then parsed into:
#
#   quantity_value              numeric count/amount
#   quantity_unit               non-standard unit (clove, can, bunch, …)
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
#   ingredient_name_raw         everything left after all extraction
#   optional                    1 if the line is self-declared optional
#
# The source table (recipe_ingredient_blocks) is NEVER modified.
# Re-running is safe: all parsed rows for a given block_id are deleted
# before re-insertion.

import sqlite3
import json
import re

from gastrometric.config.paths import DB_PATH
from gastrometric.config.ingredient_vocabulary import (
    PROTECTED_PHRASES,
    NOISE_PHRASES,
    PREP_PATTERNS,
    TEMPERATURE_STATE_PATTERNS,
    GRAM_UNITS,
    ML_UNITS,
    IMPERIAL_WEIGHT_UNITS,
    IMPERIAL_VOLUME_UNITS,
    UNIT_VOCAB,
    TRUNCATION_FIXES,
)

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

    text, phrase_map = _protect_phrases(text, PROTECTED_PHRASES)

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
# Shared with _clean_name's leaked-prep-clause cleanup below, so an "or"
# that introduces a prep-method / qualifier alternative (rather than a
# genuine second ingredient) is recognized consistently in both places.
_OR_PREP_WORDS = (
    r'pressed|pushed|passed|run|rubbed|blended|pureed|mashed|'
    r'squeezed|grated|ground|minced|chopped|diced|sliced|shredded|'
    r'low[\s-]sodium|reduced[\s-]sodium|unsalted|homemade|store[\s-]bought'
)
_OR_PREP_CLUES = re.compile(
    r'\bor\s+(?:' + _OR_PREP_WORDS + r')', re.IGNORECASE
)

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
    stripped = text.strip()
    if _OR_PREP_CLUES.search(stripped):
        return stripped, None

    m = _OR_QTY_UNIT.match(stripped)
    if m and m.group(3).strip().lower() in UNIT_VOCAB:
        alt = "%s %s %s" % (m.group(2).strip(), m.group(3).strip(), m.group(4).strip())
        return m.group(1).strip(), alt

    m = _OR_RELATIVE.match(stripped)
    if m:
        return m.group(1).strip(), "%s the amount of %s" % (m.group(2), m.group(3))

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
            return primary, alt

    return stripped, None


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
# also must NOT be discarded: prep patterns (PREP_PATTERNS) need the literal
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

_GARLIC_CONTEXT = re.compile(r'\bcloves?\s+(?:of\s+)?(garlic|shallot)\b', re.IGNORECASE)
_CLOVES_OF = re.compile(r'\bcloves?\s+of\b', re.IGNORECASE)
# Descriptors that can precede "cloves" as the ingredient itself (e.g. "10
# whole cloves" = the spice), as opposed to "cloves" counting some other
# named ingredient (garlic, shallot). If, after removing "cloves" and any
# recognized unit word, only these descriptors are left, "cloves" is the
# ingredient, not a unit.
_CLOVE_BARE_DESCRIPTORS = re.compile(
    r'\b(?:whole|large|medium|small|extra[- ]large|jumbo|fresh)\b',
    re.IGNORECASE
)

_UNIT_PAT = re.compile(
    r'\b(cup|cups|quart|quarts|qt|part|pinch|pinches|handful|recipe|'
    r'sprig|sprigs|pint|pints|'
    r'tbsp|tsp|gallon|gallons|teaspoon|tablespoon|teaspoons|tablespoons|'
    r'lb|pound|pounds|oz|ounce|ounces|head|bunch|stalks|'
    r'leaf|clove|cloves|stick|sticks|strips|slices|'
    r'box|can|cans|jar|bottle|kg|g|ml|milliliter|millilitre|liter|litre)\b'
)


def _extract_unit(text):
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
            without_units = _UNIT_PAT.sub('', without).strip()
            if not without_units:
                # Nothing but "cloves" (+ maybe a real unit) is here — e.g.
                # "1/4 tsp cloves" (the spice). Pull out the real unit if
                # there is one, but leave "cloves" itself in the name.
                m = _UNIT_PAT.search(without)
                if m:
                    real_unit = m.group(0)
                    new_text = re.sub(
                        r'\b' + re.escape(real_unit) + r'\b', '', text,
                        count=1, flags=re.IGNORECASE
                    ).strip()
                    return new_text, real_unit
                return text, None
            if not _CLOVE_BARE_DESCRIPTORS.sub('', without_units).strip():
                # Only size/state descriptors (e.g. "whole") remain once
                # "cloves" is removed — "cloves" is the ingredient itself
                # (whole cloves, the spice), not a counting unit.
                return text, None
    m = _UNIT_PAT.search(text)
    if m:
        unit = m.group(0)
        text = re.sub(r'\b' + re.escape(unit) + r'\b', '', text, count=1).strip()
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
# PREP_PATTERNS list order. This matters because `preparation` is meant to
# feed a technique-graph lookup downstream, and technique sequence is
# meaningful ("peeled, then quartered, then cut into slices" is a real
# order a cook follows).
#
# Implementation: PREP_PATTERNS (multi-word / more-specific first) and
# TEMPERATURE_STATE_PATTERNS are combined into one alternation and scanned
# with a single finditer pass. re.finditer walks left-to-right and returns
# non-overlapping matches, so the match order IS the text order. Priority
# between overlapping candidates at the same starting position is still
# governed by alternation order (multi-word patterns listed first), exactly
# as it was for the old per-pattern-loop approach.
# ============================================================

_PREP_STATE_PATTERNS = list(PREP_PATTERNS) + list(TEMPERATURE_STATE_PATTERNS)
_PREP_STATE_COMBINED = re.compile(
    '|'.join('(?:%s)' % pat for pat in _PREP_STATE_PATTERNS),
    re.IGNORECASE
)


def _extract_prep(text):
    found = [m.group(0).strip() for m in _PREP_STATE_COMBINED.finditer(text)]
    text = _PREP_STATE_COMBINED.sub('', text).strip()
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


SIZE_ADJECTIVES = ["extra-large", "extra large", "large", "medium", "small"]


def _remove_size_adjectives(text):
    if KEEP_SIZE_ADJECTIVES:
        return text
    for adj in SIZE_ADJECTIVES:
        text = re.sub(r'\b' + re.escape(adj) + r'\b', '', text, flags=re.IGNORECASE)
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
    text = re.sub(
        r'\s+or\s+(?:' + _OR_PREP_WORDS + r')[\w\s-]*$',
        '', text, flags=re.IGNORECASE
    ).strip()
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


def _fix_truncations(text):
    for pat, rep in TRUNCATION_FIXES.items():
        text = re.sub(pat, rep, text)
    return text


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

def _parse_one_line(text, optional=False):
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

    text, phrase_map = _protect_phrases(text, PROTECTED_PHRASES)
    if unit is None:
        text, unit = _extract_unit(text)

    # Restore size descriptors now, before prep extraction, so PREP_PATTERNS
    # entries that reference literal "N-inch..." text can match them.
    text = _restore_phrases(text, size_map)

    text, noise_notes = _extract_noise(text)
    text, prep_phrases = _extract_prep(text)
    text = _remove_action_words(text)
    text = _remove_size_adjectives(text)
    text = _restore_phrases(text, phrase_map)

    name, name_notes = _clean_name(text)
    name = _fix_truncations(name)

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
            "ingredient_name_raw":   ingredient_name,
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
      - or-alternative splitting  → second row marked optional=1
      - plus/+ splitting          → peer rows
      - "A and B" names           → separate name rows
    """
    results = []
    line_optional = _is_optional(raw_text)
    primary_text, alt_text = _split_on_or(raw_text)

    _MEASURE_KEYS = (
        "quantity", "unit", "grams", "ml",
        "imperial_weight_value", "imperial_weight_unit",
        "imperial_volume_value", "imperial_volume_unit",
    )
    primary_measure = None

    for slot, (is_optional, text) in enumerate(
        [(line_optional, primary_text), (True, alt_text)]
    ):
        if text is None:
            continue
        sub_lines = _split_on_plus(normalize_text(text))
        sub_results = []
        for sub in sub_lines:
            sub_results.extend(_parse_one_line(sub, optional=is_optional))
        # Forward the nearest non-empty name to any empty-name rows
        if len(sub_results) > 1:
            fallback = next((r["ingredient_name_raw"] for r in reversed(sub_results)
                            if r["ingredient_name_raw"]), "")
            for r in sub_results:
                if not r["ingredient_name_raw"]:
                    r["ingredient_name_raw"] = fallback

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

def _ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredient_lines_parsed (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            -- source
            ingredient_block_id     INTEGER NOT NULL
                                        REFERENCES recipe_ingredient_blocks(id),
            recipe_id               INTEGER NOT NULL,
            recipe_section_id       INTEGER NOT NULL,
            recipe_name             TEXT,
            section_name            TEXT,
            -- position within the block
            line_index              INTEGER NOT NULL,
            -- original text (never modified)
            raw_text                TEXT NOT NULL,
            -- parsed dimensions
            quantity_value          TEXT,
            quantity_unit           TEXT,
            imperial_weight_value   TEXT,
            imperial_weight_unit    TEXT,
            imperial_volume_value   TEXT,
            imperial_volume_unit    TEXT,
            grams                   REAL,
            ml                      REAL,
            scaling                 TEXT,
            -- ordered JSON list of technique/state phrases, source-text
            -- order, e.g. '["peeled", "cut crosswise into 0.25-inch-thick slices"]'
            -- intended to resolve 1:1 against the technique graph
            preparation             TEXT,
            -- free-text asides that are NOT techniques (yield/count notes,
            -- secondary measures, can/jar size, "% by weight" notes, ...)
            -- kept separate so they are never fed to the technique lookup
            notes                   TEXT,
            -- name as it appears after measurement / prep extraction
            ingredient_name_raw     TEXT,
            -- flags
            optional                INTEGER DEFAULT 0,
            -- audit
            parsed_at               TEXT DEFAULT (datetime('now'))
        )
    """)
    # Migration: tables created before this change won't have `notes` yet.
    existing_cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(recipe_ingredient_lines_parsed)"
    )}
    if "notes" not in existing_cols:
        conn.execute(
            "ALTER TABLE recipe_ingredient_lines_parsed ADD COLUMN notes TEXT"
        )
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rilp_ingredient_block_id
        ON recipe_ingredient_lines_parsed (ingredient_block_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rilp_recipe_id
        ON recipe_ingredient_lines_parsed (recipe_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rilp_recipe_section_id
        ON recipe_ingredient_lines_parsed (recipe_section_id)
    """)
    conn.commit()


# ============================================================
# DB EXECUTION
# ============================================================

def _run(conn):
    _ensure_schema(conn)
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

    total_lines = 0
    for ingredient_block_id, recipe_id, recipe_section_id, recipe_name, section_name, block_text in blocks:
        # Idempotent: delete previously parsed rows for this block
        c.execute(
            "DELETE FROM recipe_ingredient_lines_parsed WHERE ingredient_block_id = ?",
            (ingredient_block_id,)
        )

        raw_lines = [ln.strip() for ln in block_text.splitlines() if ln.strip()]

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
                        ingredient_name_raw, optional
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                    r["ingredient_name_raw"], r["optional"],
                ))
                total_lines += 1

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