# parse_ingredient_lines.py
#
# Pipeline stage: parse
#
#   Reads  : recipe_ingredient_blocks  (raw ingredient text blocks)
#   Writes : recipe_ingredient_lines_parsed
#
# Each block may contain multiple ingredient lines separated by newlines.
# Each line is split, then parsed into:
#
#   quantity_value            numeric count/amount
#   quantity_unit             non-standard unit (clove, can, bunch, …)
#   imperial_weight_value/unit  oz / lb
#   imperial_volume_value/unit  cup / tbsp / tsp / …
#   grams                     metric weight
#   ml                        metric volume
#   preparation               how the ingredient is prepped
#   ingredient_name_raw       everything left after all extraction
#   optional                  1 if the line is self-declared optional
#
# The source table (recipe_ingredient_blocks) is NEVER modified.
# Re-running is safe: all parsed rows for a given block_id are deleted
# before re-insertion.
#
# recipe_id and recipe_name flow in from the recipes table via a JOIN
# on recipe_ingredient_blocks.

import sqlite3
import os
import re

from ingredient_vocabulary import (
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

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

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
_OR_PREP_CLUES = re.compile(
    r'\bor\s+(?:pressed|pushed|passed|run|rubbed|blended|pureed|mashed|'
    r'low[\s-]sodium|reduced[\s-]sodium|unsalted|homemade|store[\s-]bought)',
    re.IGNORECASE
)


def _split_on_or(text):
    if _OR_PREP_CLUES.search(text):
        return text, None
    m = _OR_QTY_UNIT.match(text.strip())
    if m and m.group(3).strip().lower() in UNIT_VOCAB:
        alt = "%s %s %s" % (m.group(2).strip(), m.group(3).strip(), m.group(4).strip())
        return m.group(1).strip(), alt
    m = _OR_RELATIVE.match(text.strip())
    if m:
        return m.group(1).strip(), "%s the amount of %s" % (m.group(2), m.group(3))
    return text, None


# ============================================================
# EXPLICIT MEASURE EXTRACTION
# Pulls gram, ml, and percent values from the text.
# ============================================================

_APPROX = r'(?:approximately|approx\.?|about)?\s*'

_MASS_PAT = re.compile(
    r'[\(\s,/|]*' + _APPROX + r'(\d+(?:\.\d+)?)\s*(?:grams?|g(?![a-zA-Z]))\s*\)?(?:\s*,)?',
    re.IGNORECASE
)
_ML_PAT = re.compile(
    r'\(?\s*,?\s*' + _APPROX +
    r'(\d+(?:\.\d+)?)\s*(?:ml|milliliters?|millilitres?|mls?)\s*\)?(?:\s*,)?',
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


def _extract_explicit_measures(text):
    grams = ml = pct = None
    m = _MASS_PAT.search(text)
    if m:
        grams = m.group(1)
        text = (text[:m.start()] + ' ' + text[m.end():]).strip().rstrip(',').strip()
    m = _ML_PAT.search(text)
    if m:
        ml = m.group(1)
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
    text = _APPROX_SECONDARY.sub('', text).strip().rstrip(',').strip()
    text = _PLUS_ADDITIONAL.sub('', text).strip().rstrip(',').strip()
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
# SIZE DESCRIPTOR REMOVAL
# e.g. "1/4-inch-wide" — a measurement of cut size, not the ingredient
# ============================================================

_SIZE_DESCRIPTOR = re.compile(r'\d[\d./]*-inch[a-z-]*', re.IGNORECASE)


def _remove_size_descriptors(text):
    return _SIZE_DESCRIPTOR.sub('', text).strip()


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

_GARLIC_CONTEXT = re.compile(r'\bcloves?\s+(garlic|shallot)\b', re.IGNORECASE)

_UNIT_PAT = re.compile(
    r'\b(cup|cups|quart|quarts|qt|part|pinch|pinches|handful|recipe|'
    r'sprig|sprigs|pint|pints|'
    r'tbsp|tsp|gallon|gallons|teaspoon|tablespoon|teaspoons|tablespoons|'
    r'lb|pound|pounds|oz|ounce|ounces|head|bunch|stalks|'
    r'leaf|clove|cloves|stick|sticks|strips|slices|'
    r'box|can|cans|jar|bottle|kg|g|ml|milliliter|millilitre|liter|litre)\b'
)


def _extract_unit(text):
    # "cloves" by itself is the ingredient, not a unit
    if re.search(r'\bcloves?\b', text, re.IGNORECASE):
        if not _GARLIC_CONTEXT.search(text):
            without = re.sub(r'\bcloves?\b', '', text, flags=re.IGNORECASE).strip()
            if not _UNIT_PAT.sub('', without).strip():
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

_KG_TO_G     = 1000.0
_L_TO_ML     = 1000.0


def _route_unit(quantity, unit, grams, ml):
    qty   = float(quantity) if quantity is not None else None
    key   = unit.lower().strip() if unit else None

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

def _extract_prep(text):
    found = []
    for pat in PREP_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            found.append(m.group(0).strip())
            text = re.sub(pat, '', text, flags=re.IGNORECASE).strip()
    return text, (", ".join(found) if found else None)


def _extract_state(text):
    found = []
    for pat in TEMPERATURE_STATE_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            found.append(m.group(0).strip())
            text = re.sub(pat, '', text, flags=re.IGNORECASE).strip()
    return text, (", ".join(found) if found else None)


# ============================================================
# NOISE + SYMBOL REMOVAL
# ============================================================

def _remove_noise(text):
    for phrase in NOISE_PHRASES:
        text = re.sub(r'\b' + re.escape(phrase) + r'\b', '', text, flags=re.IGNORECASE)
    return text.strip()


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

def _clean_name(text):
    text = re.sub(r'[,;]', ' ', text)
    text = re.sub(r'[()]', ' ', text)
    text = " ".join(text.split())
    # Strip leading connectors
    text = re.sub(r'^(?:of|from|and|or|\*|:|with)\b\s*|^[-–]\s*', '', text, flags=re.IGNORECASE)
    # Strip trailing connectors
    text = re.sub(r'\s+(and|or|\*|with|in)$', '', text, flags=re.IGNORECASE)
    # "from <N> <word>" source phrases (e.g. "from 1 lime")
    text = re.sub(r'\bfrom\s+\d*\s*(?:large|small|medium|fresh|whole)?\s*\w+\s*$', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'\bfrom\s+', '', text, flags=re.IGNORECASE).strip()
    # Prep-method "or" clauses that leaked through
    text = re.sub(
        r'\s+or\s+(?:pressed|pushed|passed|run|rubbed|blended|pureed|mashed|'
        r'squeezed|grated|ground|minced|chopped|diced|sliced|'
        r'low[\s-]sodium|reduced[\s-]sodium|unsalted|homemade|store[\s-]bought)[\w\s-]*$',
        '', text, flags=re.IGNORECASE
    ).strip()
    # Trailing "for <purpose>"
    text = re.sub(r'\s+for\s+\S.*$', '', text, flags=re.IGNORECASE).strip()
    # Stray numbers, isolated punctuation, isolated single letters
    text = re.sub(r'(?<!\w)\d+(?:\.\d+)?(?!\w)', '', text).strip()
    text = re.sub(r'(?<!\w)\.(?!\w)', '', text).strip()
    text = re.sub(r'(?<!\w)[a-z](?!\w)', '', text, flags=re.IGNORECASE).strip()
    return " ".join(text.split()).strip('*').strip()


def _fix_truncations(text):
    for pat, rep in TRUNCATION_FIXES.items():
        text = re.sub(pat, rep, text)
    return text


def _split_multi_ingredients(text):
    """Split "salt and pepper" → ["salt", "pepper"] only for short, digit-free names."""
    if " and " in text:
        parts = [p.strip() for p in text.split(" and ", 1)]
        if all(not re.search(r'\d', p) for p in parts) and len(text.split()) <= 6:
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

    text, juice_prep       = _extract_juice_form(text)
    text, pct_note         = _extract_pct_weight(text)
    text, grams, ml, pct  = _extract_explicit_measures(text)
    text, parens           = _extract_parentheticals(text)

    # Check parentheticals for a secondary imperial measure e.g. "(1 stick)"
    paren_qty = paren_unit = None
    for pc in parens:
        pq, pu = _paren_measure(pc)
        if pq and pu:
            paren_qty, paren_unit = pq, pu
            break

    text, can_qty, can_unit, can_size_note = _extract_can_size(text)
    text = _remove_size_descriptors(text)

    # Strip leftover secondary measures when a gram weight was already extracted
    if grams is not None:
        text = re.sub(
            r'(?<=\s)(\d+(?:\.\d+)?)\s*'
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
    if can_qty is not None:
        quantity, unit = can_qty, can_unit
        if can_size_note:
            parens = list(parens) + [can_size_note]
    elif grams is not None:
        if paren_qty and paren_unit:
            quantity, unit = paren_qty, paren_unit
        elif quantity is None:
            quantity, unit = grams, "g"

    text, phrase_map = _protect_phrases(text, PROTECTED_PHRASES)
    if unit is None:
        text, unit = _extract_unit(text)

    text = _remove_noise(text)
    text, state = _extract_state(text)
    text, prep  = _extract_prep(text)
    text = _remove_action_words(text)
    text = _remove_size_adjectives(text)
    text = _restore_phrases(text, phrase_map)

    name = _clean_name(text)
    name = _fix_truncations(name)

    # Assemble preparation string
    prep_parts = [p for p in [juice_prep, prep, state] if p]
    final_prep = ", ".join(prep_parts) if prep_parts else None
    # Append non-measure parenthetical notes
    notes = []
    for pc in parens:
        pq, pu = _paren_measure(pc)
        if pq and pu and pq == paren_qty and pu == paren_unit:
            continue
        notes.append(pc)
    if pct_note:
        notes.append(pct_note.strip())
    if notes:
        final_prep = ((final_prep or "") + " | " + "; ".join(notes)).strip(" |")

    routed = _route_unit(quantity, unit, grams, ml)

    results = []
    for ingredient_name in _split_multi_ingredients(name):
        results.append({
            "quantity":              routed["quantity"],
            "unit":                  routed["unit"],
            "prep":                  final_prep,
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
      • or-alternative splitting  → second row marked optional=1
      • plus/+ splitting          → peer rows
      • "A and B" names           → separate name rows
    """
    results = []
    line_optional = _is_optional(raw_text)
    primary_text, alt_text = _split_on_or(raw_text)

    for is_optional, text in [(line_optional, primary_text), (True, alt_text)]:
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
        results.extend(sub_results)
    return results


# ============================================================
# DB SCHEMA
# ============================================================

def _ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredient_lines_parsed (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            -- source block
            block_id              INTEGER NOT NULL
                                  REFERENCES recipe_ingredient_blocks(id),
            -- recipe identity (denormalized from recipes for query convenience)
            recipe_id             INTEGER NOT NULL,
            recipe_name           TEXT,
            -- position within the block
            line_index            INTEGER NOT NULL,
            section               TEXT,
            -- original text (never modified)
            raw_text              TEXT NOT NULL,
            -- parsed dimensions
            quantity_value        TEXT,
            quantity_unit         TEXT,
            imperial_weight_value TEXT,
            imperial_weight_unit  TEXT,
            imperial_volume_value TEXT,
            imperial_volume_unit  TEXT,
            grams                 REAL,
            ml                    REAL,
            scaling               TEXT,
            preparation           TEXT,
            -- name as it appears after measurement / prep extraction
            ingredient_name_raw   TEXT,
            -- flags
            optional              INTEGER DEFAULT 0,
            -- audit
            parsed_at             TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rilp_block_id
        ON recipe_ingredient_lines_parsed (block_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rilp_recipe_id
        ON recipe_ingredient_lines_parsed (recipe_id)
    """)
    conn.commit()


# ============================================================
# DB EXECUTION
# ============================================================

def _run(conn):
    _ensure_schema(conn)
    c = conn.cursor()

    # Fetch all blocks with recipe identity joined in from recipes.
    # Assumes recipe_ingredient_blocks has a recipe_id FK to recipes.
    c.execute("""
        SELECT
            b.id          AS block_id,
            b.recipe_id,
            r.recipe_name        AS recipe_name,
            b.raw_text    AS block_text,
            b.section_name
        FROM recipe_ingredient_blocks b
        JOIN recipes r ON r.id = b.recipe_id
        WHERE b.raw_text IS NOT NULL AND b.raw_text != ''
        ORDER BY b.id
    """)
    blocks = c.fetchall()

    total_lines = 0
    for block_id, recipe_id, recipe_name, block_text, section in blocks:
        # Idempotent: delete previously parsed rows for this block
        c.execute(
            "DELETE FROM recipe_ingredient_lines_parsed WHERE block_id = ?",
            (block_id,)
        )

        # Split block into individual lines
        raw_lines = [ln.strip() for ln in block_text.splitlines() if ln.strip()]

        for line_index, raw_line in enumerate(raw_lines):
            parsed_rows = parse_ingredient_line(raw_line)
            for r in parsed_rows:
                c.execute("""
                    INSERT INTO recipe_ingredient_lines_parsed (
                        block_id, recipe_id, recipe_name,
                        line_index, section, raw_text,
                        quantity_value, quantity_unit,
                        imperial_weight_value, imperial_weight_unit,
                        imperial_volume_value, imperial_volume_unit,
                        grams, ml, scaling, preparation,
                        ingredient_name_raw, optional
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    block_id, recipe_id, recipe_name,
                    line_index, section, raw_line,
                    r["quantity"], r["unit"],
                    r["imperial_weight_value"], r["imperial_weight_unit"],
                    r["imperial_volume_value"], r["imperial_volume_unit"],
                    r["grams"], r["ml"], r["scaling"], r["prep"],
                    r["ingredient_name_raw"], r["optional"],
                ))
                total_lines += 1

    conn.commit()
    print("parse_ingredient_lines: %d blocks → %d parsed rows"
          % (len(blocks), total_lines))


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    try:
        _run(conn)
    finally:
        conn.close()