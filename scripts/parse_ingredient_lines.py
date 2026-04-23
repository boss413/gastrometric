# scripts/parse_ingredient_lines.py
#
# See README_parse_ingredient_lines.md for instructions on extending
# unit lists, prep words, noise phrases, and protected phrases.

import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")


# ============================================================
# PROTECTED INGREDIENT PHRASES
# These phrases are temporarily tokenized before any substitution
# so that their words can't be mis-parsed (e.g. "ground" in
# "ground beef" should not become a prep word).
# Add longer/more-specific phrases before shorter ones — the list
# is already sorted by length at runtime so order here doesn't matter.
# ============================================================

PROTECTED_PHRASES = [
    # dairy
    "half-and-half", "half and half", "half & half",
    "heavy cream", "whipping cream",
    "cream of tartar", "cream of wheat", "cream of mushroom soup",
    # flour
    "all purpose flour", "all-purpose flour", "bread flour", "cake flour",
    "whole wheat flour", "self rising flour", "self-rising flour",
    # pepper
    "freshly ground black pepper", "freshly ground pepper",
    "ground black pepper", "ground white pepper",
    "crushed red pepper flakes", "crushed red pepper", "red pepper flakes",
    # ground meats
    "ground beef", "ground pork", "ground turkey",
    "ground chicken", "ground lamb",
    # ground spices
    "ground coriander", "ground cumin", "ground ginger", "ground cinnamon",
    "ground nutmeg", "ground allspice", "ground cloves", "ground cardamom",
    "ground turmeric", "ground paprika", "ground mustard", "ground fennel",
    # dried herbs
    "dried thyme", "dried oregano", "dried basil", "dried rosemary",
    "dried sage", "dried parsley", "dried dill", "dried mint", "dried chili",
    "dried rubbed sage",
    # oils
    "extra virgin olive oil", "extra-virgin olive oil",
    "olive oil", "vegetable oil", "canola oil",
    "sesame oil", "coconut oil",
    # sugars
    "brown sugar", "white sugar", "granulated sugar",
    "powdered sugar", "confectioners sugar", "confectioners' sugar",
    # sauces
    "soy sauce", "fish sauce", "hot sauce", "worcestershire sauce",
    # cheeses
    "parmesan cheese", "cheddar cheese", "mozzarella cheese",
    # onions
    "green onions", "spring onions",
    "red onion", "yellow onion", "white onion",
    # tomatoes
    "crushed tomatoes", "diced tomatoes", "tomato paste", "tomato sauce",
    # water
    "boiling water", "cold water", "ice water", "warm water",
    # chorizo
    "spanish chorizo",
]


# ============================================================
# PHRASE PROTECTION HELPERS
# ============================================================

def _protect_phrases(text, protected):
    mapping = {}
    for i, phrase in enumerate(sorted(protected, key=len, reverse=True)):
        # Case-insensitive match, preserve original casing of the token
        pattern = re.compile(re.escape(phrase), re.IGNORECASE)
        m = pattern.search(text)
        if m:
            token = "__PROTECTED_%d__" % i
            mapping[token] = m.group(0)   # preserve original capitalisation
            text = pattern.sub(token, text)
    return text, mapping


def _restore_phrases(text, mapping):
    for token, phrase in mapping.items():
        text = text.replace(token, phrase)
    return text


# ============================================================
# PLUS / + SPLITTING
# Only split when the second segment starts with a digit (quantity),
# or a word-number that implies a new measurement.
# "one egg plus one yolk" → two segments
# "plus more for dusting" → NOT split (handled as noise phrase)
# ============================================================

PLUS_SPLIT_PATTERN = re.compile(
    r'(?<!\w)\+(?!\w)|'
    r'\bplus\b(?!\s+(?:more|additional)\b)',
    re.IGNORECASE
)

_WORD_NUMBERS = r'(?:one|two|three|four|five|six|seven|eight|nine|ten|half|a)\b'

def split_on_plus(raw_text):
    # Mask parenthesised content so we don't split inside parens
    paren_masked = re.sub(r'\([^)]*\)', lambda m: 'X' * len(m.group(0)), raw_text)
    parts_masked = PLUS_SPLIT_PATTERN.split(paren_masked)
    if len(parts_masked) < 2:
        return [raw_text]
    # Recover original text at split boundaries
    positions = []
    pos = 0
    for part in parts_masked:
        positions.append((pos, pos + len(part)))
        pos += len(part)
        remainder = paren_masked[pos:]
        m = PLUS_SPLIT_PATTERN.match(remainder)
        if m:
            pos += len(m.group(0))
    parts = [raw_text[s:e].strip() for s, e in positions if raw_text[s:e].strip()]
    if len(parts) >= 2:
        second = parts[1].strip()
        # Accept if it starts with a digit or a word-number
        if re.match(r'^\d', second) or re.match(_WORD_NUMBERS, second, re.IGNORECASE):
            return parts
    return [raw_text]


# ============================================================
# OR-ALTERNATIVE SPLITTING
# "2 t dried parsley or double the amount of fresh"
#   → primary: "2 t dried parsley"
#   → alt (optional): "double the amount of fresh"   ← no qty/unit requirement
#
# "6 garlic cloves, minced or pressed through a garlic press"
#   → primary: "6 garlic cloves, minced or pressed through a garlic press"
#   → no split  (the "or" connects two prep verbs, no new ingredient)
#
# Rule: only split when "or" is followed by a clearly new ingredient,
# meaning after "or" we see:
#   (a) a number/unit → peer quantity alternative
#   (b) words like "double/twice/triple/half the amount of" → relative alternative
#   (c) a KNOWN ingredient name that has no prior mention in the line
# ============================================================

_UNIT_VOCAB = {
    'cup','cups','tbsp','tsp','tablespoon','tablespoons','teaspoon','teaspoons',
    'oz','ounce','ounces','lb','pound','pounds','g','kg','ml','liter','litre',
    'pint','quart','gallon','can','cans','jar','bottle','bunch','head',
    'clove','cloves','sprig','sprigs','stalk','stalks','slice','slices',
}

# Pattern A: "… or <qty> <unit> <ingredient>"
_OR_QTY_UNIT = re.compile(
    r'^(.+?)\s+or\s+(\d[\d./]*(?:\s*[-–]\s*\d[\d./]*)?)\s+(\w+)\s+(.+)$',
    re.IGNORECASE
)
# Pattern B: "… or double/twice/triple/half the amount of …"
_OR_RELATIVE = re.compile(
    r'^(.+?)\s+or\s+(double|twice|triple|half)\s+the\s+(?:amount\s+of\s+)?(.+)$',
    re.IGNORECASE
)
# Pattern C: "… or low-sodium chicken broth" (prep-phrase alternative, keep on primary)
# We explicitly do NOT split these — they describe the same ingredient.

def split_on_or_alternative(raw_text):
    norm = normalize_text(raw_text)

    # Hard-stop: if the "or" connects prep-method synonyms or brand synonyms,
    # do NOT split.  Check the full normalized string first.
    _OR_PREP_CLUES = re.compile(
        r'\bor\s+(?:pressed|pushed|passed|run|rubbed|blended|pureed|mashed|'
        r'low[\s-]sodium|reduced[\s-]sodium|unsalted|homemade|store[\s-]bought)',
        re.IGNORECASE
    )
    if _OR_PREP_CLUES.search(norm):
        return raw_text, None

    # Pattern A: quantified alternative
    m = _OR_QTY_UNIT.match(norm.strip())
    if m:
        alt_unit = m.group(3).strip().lower()
        if alt_unit in _UNIT_VOCAB:
            primary = m.group(1).strip()
            alt_line = "%s %s %s" % (m.group(2).strip(), alt_unit, m.group(4).strip())
            return primary, alt_line

    # Pattern B: relative-quantity alternative ("or double the amount of fresh")
    m = _OR_RELATIVE.match(norm.strip())
    if m:
        primary  = m.group(1).strip()
        alt_line = "%s the amount of %s" % (m.group(2).strip(), m.group(3).strip())
        return primary, alt_line

    return raw_text, None


# ============================================================
# NORMALIZATION
# Converts abbreviations and Unicode fractions to canonical forms.
# ============================================================

def normalize_text(text):
    if not text:
        return text

    # ---- Case-sensitive abbreviations BEFORE lowercasing ----
    # Capital T = tablespoon, lowercase t = teaspoon
    # Must be matched as isolated tokens
    text = re.sub(r'\bTbsp\b', 'tbsp', text)
    text = re.sub(r'\bTBSP\b', 'tbsp', text)
    text = re.sub(r'\bTSP\b',  'tsp',  text)
    # Standalone capital T (tablespoon)
    text = re.sub(r'(?<![a-zA-Z])T(?![a-zA-Z])', 'tbsp', text)
    # Standalone lowercase t (teaspoon) — be careful not to hit 't' inside words
    text = re.sub(r'(?<![a-zA-Z])t(?![a-zA-Z])', 'tsp',  text)

    text = text.lower()

    # Remove bullet characters that survive lowercasing
    text = re.sub(r'^[\u2022•\-–]\s*', '', text)

    # Protect compound phrases BEFORE any word substitution
    text, phrase_map = _protect_phrases(text, PROTECTED_PHRASES)

    # Protect words that clash with substitutions
    text = text.replace("weight", "__weight__")
    text = text.replace("eighth", "__eighth__")

    # "N and fraction" → decimal  e.g. "1 and 1/2"
    def _and_frac(m):
        return str(float(m.group(1)) + float(m.group(2)) / float(m.group(3)))
    text = re.sub(r'(\d+)\s+and\s+(\d+)\s*/\s*(\d+)', _and_frac, text)
    text = re.sub(r'(\d+)\s+and\s+a\s+half',
                  lambda m: str(float(m.group(1)) + 0.5), text)

    # Unicode + ASCII fractions → decimal  (longest first to avoid partial matches)
    fractions = [
        ("2 1/2", "2.5"), ("1-1/2","1.5"), ("1 1/2","1.5"),
        ("1-½",   "1.5"), ("1½",   "1.5"), ("1 ½",  "1.5"),
        ("½",     "0.5"), ("1/2",  "0.5"),
        ("⅓",    "0.333"),("1/3",  "0.333"),
        ("⅔",    "0.666"),("2/3",  "0.666"),
        ("¼",    "0.25"), ("1/4",  "0.25"),
        ("¾",    "0.75"), ("3/4",  "0.75"),
        ("⅛",   "0.125"),("1/8",  "0.125"),
        ("⅜",   "0.375"),("3/8",  "0.375"),
        ("⅝",   "0.625"),("5/8",  "0.625"),
        ("⅞",   "0.875"),("7/8",  "0.875"),
    ]
    for k, v in fractions:
        text = text.replace(k, v)

    # Word numbers (AFTER phrase protection so "half and half" is safe)
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

    # Standalone 'c' → 'cup'  (but not inside words like 'chicken')
    text = re.sub(r'(?<![a-zA-Z])c(?![a-zA-Z])', 'cup', text)

    # "from N <fruit>" → strip the "from N" numeric bridge (e.g. "from 1 lime")
    text = re.sub(r'\bfrom\s+\d+\s+', 'from ', text)

    return text.strip()


# ============================================================
# PARENTHETICAL SECONDARY MEASURE
# e.g. "(1 stick)" → qty=1, unit=stick
#      "(45ml)"    → captured by explicit ml extractor, not here
# ============================================================

_IMPERIAL_VOL_PRIORITY = {
    'oz': 1, 'ounce': 1, 'ounces': 1,
    'lb': 1, 'pound': 1, 'pounds': 1,
    'pint': 1, 'quart': 1, 'gallon': 1,
    'cup': 2, 'cups': 2,
    'tbsp': 3, 'tablespoon': 3, 'tablespoons': 3,
    'tsp': 4, 'teaspoon': 4, 'teaspoons': 4,
    'stick': 5, 'sticks': 5,
}

_PAREN_MEASURE_RE = re.compile(
    r'(?:about|approximately|approx\.?)?\s*'
    r'(\d+(?:\.\d+)?(?:\s*[-–]\s*\d+(?:\.\d+)?)?)\s*'
    r'(cup|cups|oz|ounce|ounces|tbsp|tablespoon|tablespoons|'
    r'tsp|teaspoon|teaspoons|pint|quart|gallon|lb|pound|pounds|'
    r'stick|sticks)',
    re.IGNORECASE
)

def extract_paren_secondary_measure(paren_text):
    candidates = []
    for m in _PAREN_MEASURE_RE.finditer(paren_text):
        raw_qty = m.group(1).strip()
        unit    = m.group(2).lower()
        if '-' in raw_qty or '–' in raw_qty:
            raw_qty = re.split(r'[-–]', raw_qty)[0].strip()
        priority = _IMPERIAL_VOL_PRIORITY.get(unit, 99)
        candidates.append((priority, raw_qty, unit))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1], candidates[0][2]


# ============================================================
# EXPLICIT MEASURE EXTRACTION
# Pulls out gram, ml, and % values (often in parentheses or
# appended with commas).  Gram/ml values are removed from the
# name text and stored in dedicated columns.
# ============================================================

_APPROX = r'(?:approximately|approx\.?|about)?\s*'

# Grams: may be bare or parenthesised  e.g. "50g" or "(50 g)" or "50 grams"
EXPLICIT_MASS_PATTERN = re.compile(
    r'[\(\s,]*' + _APPROX + r'(\d+(?:\.\d+)?)\s*(?:grams?|(?<!\w)g(?!\w))\s*\)?'
    r'(?:\s*,)?',
    re.IGNORECASE
)
EXPLICIT_ML_PATTERN   = re.compile(
    r'\(?\s*,?\s*' + _APPROX +
    r'(\d+(?:\.\d+)?)\s*(?:ml|milliliters?|millilitres?|mls?)\s*\)?'
    r'(?:\s*,)?',
    re.IGNORECASE
)
# Note: liters/litres intentionally kept separate below to avoid false matches
# on ingredient names.  Bare "l" is NOT matched here (too ambiguous).
EXPLICIT_LITER_PATTERN = re.compile(
    r',?\s*' + _APPROX +
    r'(\d+(?:\.\d+)?)\s*(?:liters?|litres?)\b',
    re.IGNORECASE
)
EXPLICIT_PCT_PATTERN  = re.compile(
    r',?\s*(\d+(?:\.\d+)?)\s*%(?:\s+(?:total|by\s+weight(?:\s+of\s+[^,)]+)?))?',
    re.IGNORECASE
)
APPROX_SECONDARY_PATTERN = re.compile(
    r',?\s*(?:approximately|approx\.?|about)\s+\d+(?:\.\d+)?\s+\w+[^,)]*',
    re.IGNORECASE
)
PLUS_ADDITIONAL_PATTERN = re.compile(
    r',?\s*plus\s+additional(?:\s+for\s+\w+)?', re.IGNORECASE
)


def extract_explicit_measures(text):
    grams_val = ml_val = pct_val = None

    m = EXPLICIT_MASS_PATTERN.search(text)
    if m:
        grams_val = m.group(1)
        text = (text[:m.start()] + text[m.end():]).strip().rstrip(',').strip()

    m = EXPLICIT_ML_PATTERN.search(text)
    if m:
        ml_val = m.group(1)
        text = (text[:m.start()] + text[m.end():]).strip().rstrip(',').strip()
    else:
        m = EXPLICIT_LITER_PATTERN.search(text)
        if m:
            ml_val = str(float(m.group(1)) * 1000)
            text = (text[:m.start()] + text[m.end():]).strip().rstrip(',').strip()

    m = EXPLICIT_PCT_PATTERN.search(text)
    if m:
        pct_val = m.group(1)
        text = (text[:m.start()] + text[m.end():]).strip().rstrip(',').strip()

    text = APPROX_SECONDARY_PATTERN.sub('', text).strip().rstrip(',').strip()
    text = PLUS_ADDITIONAL_PATTERN.sub('', text).strip().rstrip(',').strip()

    return text, grams_val, ml_val, pct_val


# ============================================================
# CAN / JAR SIZE EXTRACTION
# e.g. "1 28-oz can" → count=1, unit="can", size_note="28 oz"
#      "28-oz crushed tomatoes" → count=1, unit=None, size_note="28 oz"
# ============================================================

CAN_SIZE_PATTERN = re.compile(
    r'(?:(\d+(?:\.\d+)?)\s+)?'         # optional count of containers
    r'(\d+(?:\.\d+)?)\s*[-\s]?'        # container size in oz (space/hyphen optional)
    r'(?:ounce|oz)\.?\s+'
    r'(cans?|jars?|bottles?|packages?|bags?|boxes?)',
    re.IGNORECASE
)
# Also capture bare "28-oz" or "28oz" before a non-can ingredient
BARE_OZ_SIZE_PATTERN = re.compile(
    r'(?:^|\s)(\d+(?:\.\d+)?)\s*[-\s]?oz\.?\s+(?!can|jar|bottle)',
    re.IGNORECASE
)

def extract_can_size(text):
    m = CAN_SIZE_PATTERN.search(text)
    if m:
        count      = m.group(1) or "1"
        size_oz    = m.group(2)
        container  = m.group(3).lower()
        size_note  = "%s oz" % size_oz
        text = (text[:m.start()] + text[m.end():]).strip().lstrip(',').strip()
        return text, count, container, size_note
    return text, None, None, None


# ============================================================
# JUICE FORM EXTRACTION
# ============================================================

JUICE_PATTERN = re.compile(r'^(juice(?:\s+(?:from|of))?\s+)', re.IGNORECASE)

def extract_juice_form(text):
    m = JUICE_PATTERN.match(text)
    if m:
        return text[m.end():].strip(), "juice"
    return text, None


# ============================================================
# PERCENT-BY-WEIGHT
# ============================================================

PERCENT_BY_WEIGHT_PATTERN = re.compile(
    r',?\s*or\s+\d+(?:\.\d+)?%\s+by\s+weight\s+of\s+[^,)]+',
    re.IGNORECASE
)

def extract_percent_by_weight(text):
    m = PERCENT_BY_WEIGHT_PATTERN.search(text)
    if m:
        note = m.group(0).strip().lstrip(',').strip()
        text = text[:m.start()] + text[m.end():]
        return text.strip(), note
    return text, None


# ============================================================
# SIZE DESCRIPTOR STRIPPING
# Removes measurement-qualified size phrases like "1/4-inch-wide"
# from the ingredient name, but NOT from prep instructions.
# ============================================================

SIZE_DESCRIPTOR_PATTERN = re.compile(r'\d[\d./]*-inch[a-z-]*', re.IGNORECASE)

def remove_size_descriptors(text):
    return SIZE_DESCRIPTOR_PATTERN.sub('', text).strip()


# ============================================================
# CLEANING RULES
# ============================================================

def remove_leading_symbols(text):
    # Remove leading bullet characters, dashes, asterisks, "• ", "- "
    # Also handles the literal hyphen-space that some bullet normalisation leaves.
    return re.sub(r'^[\-\u2013\u2022\+\*•\s]+', '', text).strip()


# Phrases that add no information to the ingredient name.
# These are removed from the name field entirely.
# To add more: append to this list.
NOISE_PHRASES = [
    "plus more", "as needed", "to taste", "if desired",
    "as desired", "optional",
]

def remove_noise_phrases(text):
    for phrase in NOISE_PHRASES:
        text = re.sub(r'\b' + re.escape(phrase) + r'\b', '', text, flags=re.IGNORECASE)
    return text.strip()


# Size adjectives are KEPT in the name (they help USDA matching)
# but removed only from the final clean-up step if they appear
# as isolated tokens that would confuse the canonical name.
# Set KEEP_SIZE_ADJECTIVES = True to preserve them (default).
# To strip them instead, set to False.
KEEP_SIZE_ADJECTIVES = True

SIZE_ADJECTIVES = ["extra-large", "extra large", "large", "medium", "small"]

def remove_size_adjectives(text):
    if KEEP_SIZE_ADJECTIVES:
        return text
    for adj in SIZE_ADJECTIVES:
        text = re.sub(r'\b' + re.escape(adj) + r'\b', '', text, flags=re.IGNORECASE)
    return " ".join(text.split())


# Words that describe actions done TO the ingredient.
# These go to the preparation column, not the name.
# To add more action words: add to ACTION_WORDS set.
ACTION_WORDS = {
    "washed", "separated", "into", "and",
}

def remove_actions(text):
    # Only remove "and" if it's now isolated (prep words around it were removed)
    words = text.split()
    result = []
    for i, w in enumerate(words):
        if w.lower() == "and":
            # Drop "and" only if adjacent words were removed (it's now dangling)
            prev_ok = i > 0 and words[i-1].lower() not in ACTION_WORDS
            next_ok = i < len(words)-1 and words[i+1].lower() not in ACTION_WORDS
            if not (prev_ok and next_ok):
                continue
        elif w.lower() in ACTION_WORDS - {"and"}:
            continue
        result.append(w)
    return " ".join(result)


FIXES = {
    r'\boi\b':           "oil",
    r'\bsausag\b':       "sausage",
    r'\bleav\b':         "leaves",
    r'\bnoodl\b':        "noodle",
    r'\bchiv\b':         "chive",
    r'\bparsley leav\b': "parsley leaves",
}

def fix_truncations(text):
    for pattern, replacement in FIXES.items():
        text = re.sub(pattern, replacement, text)
    return text


# ============================================================
# EXTRACTION — PARENTHETICALS
# Parentheticals are removed from the name.  Those that contain
# measurement data feed into the secondary-measure logic.
# Non-measurement parentheticals that look like page references,
# see-note markers, or pure descriptive asides are dropped.
# Others become part of the prep column.
# ============================================================

# Parentheticals that are always dropped (no useful info)
_DROP_PAREN_PATTERNS = [
    re.compile(r'see\s+note', re.IGNORECASE),
    re.compile(r'page\s+\d+', re.IGNORECASE),
    re.compile(r'note\s*\d*', re.IGNORECASE),
    re.compile(r'^\s*\*+\s*$'),          # just asterisks
    re.compile(r'^\s*optional\s*$', re.IGNORECASE),
]

def _should_drop_paren(paren_text):
    for pat in _DROP_PAREN_PATTERNS:
        if pat.search(paren_text):
            return True
    # Drop if it contains no letters and no digits (pure punctuation)
    if not re.search(r'[a-zA-Z0-9]', paren_text):
        return True
    return False


def extract_parentheticals(text):
    matches = re.findall(r'\((.*?)\)', text)
    text = re.sub(r'\(.*?\)', '', text).strip()
    # Filter: keep only parens with real content
    kept = [m for m in matches if not _should_drop_paren(m)]
    return text, kept


# ============================================================
# QUANTITY EXTRACTION
# ============================================================

QUANTITY_PATTERN = re.compile(
    r'^(\d+(?:\.\d+)?)'
    r'(?:\s+(\d+(?:\.\d+)?)(?!\s*[-–]?\s*(?:oz|g|ml|lb|cup|tsp|tbsp)))?'  # whole + fraction, but not "1 12-oz"
    r'(?:\s*(to|-)\s*(\d+(?:\.\d+)?))?'
)

def extract_quantity(text):
    m = QUANTITY_PATTERN.match(text)
    if m:
        full = m.group(0)
        if m.group(4):                          # range → take upper
            qty = float(m.group(4))
        elif m.group(2):                        # leading integer + fraction
            qty = float(m.group(1)) + float(m.group(2))
        else:
            qty = float(m.group(1))
        text = text[len(full):].strip()
        return text, (str(qty) if qty != int(qty) else str(int(qty)))
    return text, None


# ============================================================
# UNIT EXTRACTION
# To add a new unit: add it to the alternation in UNIT_PATTERN.
# ============================================================

_GARLIC_UNIT_CONTEXT = re.compile(r'\bcloves?\s+(garlic|shallot)\b', re.IGNORECASE)

UNIT_PATTERN = re.compile(
    r'\b(cup|cups|quart|quarts|qt|part|pinch|pinches|handful|recipe|'
    r'sprig|sprigs|pint|pints|'
    r'tbsp|tsp|gallon|gallons|teaspoon|tablespoon|teaspoons|tablespoons|'
    r'lb|pound|pounds|oz|ounce|ounces|head|bunch|stalks|'
    r'leaf|clove|cloves|stick|sticks|strips|slices|'
    r'box|can|cans|jar|bottle|kg|g|ml|milliliter|millilitre|liter|litre)\b'
)
# Note: "leaves" deliberately excluded — it is nearly always an ingredient word
# (bay leaves, basil leaves, etc.).  "leaf" is retained for singular use.

def extract_unit(text):
    # Disambiguate "clove(s)": only treat as unit when followed by garlic/shallot
    if re.search(r'\bcloves?\b', text, re.IGNORECASE):
        if not _GARLIC_UNIT_CONTEXT.search(text):
            without_clove = re.sub(r'\bcloves?\b', '', text, flags=re.IGNORECASE).strip()
            if not UNIT_PATTERN.sub('', without_clove).strip():
                return text, None   # bare "cloves" is the ingredient

    m = UNIT_PATTERN.search(text)
    if m:
        unit = m.group(0)
        text = re.sub(r'\b' + re.escape(unit) + r'\b', '', text, count=1).strip()
        return text, unit
    return text, None


# ============================================================
# PREPARATION EXTRACTION
# Prep patterns are extracted from the ingredient text and moved
# to the preparation column.  Multi-word patterns must come before
# their single-word components.
# To add a new prep phrase: add a raw string to PREP_PATTERNS.
# To add a single prep word: add it to PREP_PATTERNS as r'\bword\b'.
# ============================================================

PREP_PATTERNS = [
    r'chopped into large chunks',
    r'chopped into small chunks',
    r'chopped into bite[- ]sized chunks',
    r'chopped into bite[- ]sized pieces',
    r'chopped into chunks',
    r'chopped into pieces',
    r'sliced into \d[\d./]*-inch[a-z-]* rounds',
    r'sliced into \d[\d./]*-inch[a-z-]* pieces',
    # Multi-word patterns first (most-specific → least-specific)
    r'very thinly sliced',
    r'thinly sliced',
    r'roughly chopped',
    r'finely chopped',
    r'coarsely chopped',
    r'chopped fine',
    r'sliced thinly',
    r'sliced thin',
    r'finely sliced',
    r'finely minced',
    r'diced fine',
    r'cut into \d[\d./]*-inch[a-z-]* rounds',
    r'cut into \d[\d./]*-inch[a-z-]* pieces',
    r'cut into \d[\d./]*-inch[a-z-]* chunks',
    r'cut into \d[\d./]*-inch[a-z-]* strips',
    r'cut into \d[\d./]* pieces',
    r'cut into \d[\d./]* chunks',
    r'cut into pieces',
    r'cut into chunks',
    r'cut into strips',
    r'cut into rounds',
    r'cut in half',
    r'cut in \d+ pieces',
    r'cut in \d+',
    r'cut into bite[- ]sized chunks',
    r'cut into bite[- ]sized pieces',
    r'cut in half',
    r'sliced into bite[- ]sized chunks',
    r'sliced into bite[- ]sized pieces',
    r'sliced crosswise into [^,;]+',    # "sliced crosswise into 1/4-inch-wide pieces"
    r'for dusting',
    r'for sprinkling',
    r'for sprinklng',
    r'for greasing',
    r'loosely packed',
    r'crosswise into [^,;]+',
    # State/method adjectives that belong in prep
    r'\bde-stemmed\b',
    r'\bde-veined\b',
    r'\bboneless\b',
    r'\bskinless\b',
    # Single-word prep verbs
    r'\bdiced?\b',
    r'\bfinely\b',
    r'\bcoarsely\b',
    r'\broughly\b',
    r'\bchopped\b',
    r'\bdiced\b',
    r'\bminced\b',
    r'\bsliced\b',
    r'\bpeeled\b',
    r'\bgrated\b',
    r'\bcrushed\b',
    r'\bseeded\b',
    r'\bbeaten\b',
    r'\bwashed\b',
    r'\btrimmed\b',
    r'\bseparated\b',
    r'\bdivided\b',
    r'\bmelted\b',
    r'\bcubed\b',
    r'\bsifted\b',
    r'\bpacked\b',
    r'\bsoftened\b',
    r'\bshredded\b',
    r'\bcut\b',
    r'\bhalved\b',
    r'\bquartered\b',
    r'\bcracked\b',
    r'\bcored\b',
    r'\bdeveined\b',
    r'\bdebearded\b',
    r'\bpatted dry\b',
]

def extract_prep(text):
    found = []
    for pattern in PREP_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            found.append(m.group(0).strip())
            text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
    return text, (", ".join(found) if found else None)


# ============================================================
# TEMPERATURE / STATE EXTRACTION
# These adjectives help with USDA matching and are kept in prep,
# NOT removed from the name.
# To add more state words: add to TEMPERATURE_STATE_PATTERNS.
# ============================================================

TEMPERATURE_STATE_PATTERNS = [
    r'at\s+room[\s-]temperature',
    r'room[\s-]temperature',
    r'\bboiling\b(?!\s+water)',
    r'\bchilled\b',
    r'\bcold\b',
    r'\bwarmed?\b',
    r'\bhot\b',
    r'\bfrozen\b',
    r'\bthawed\b',
    r'\biced\b',
    r'\brefrigerated\b',
    r'\bfresh(?:ly)?\b',
]

def extract_temperature_state(text):
    found = []
    for pattern in TEMPERATURE_STATE_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            found.append(m.group(0).strip())
            text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
    return text, (", ".join(found) if found else None)


# ============================================================
# FINAL CLEAN
# ============================================================

def clean_name(text):
    text = text.replace(",", " ")
    text = text.replace(";", " ")
    # Remove stray unmatched parentheses
    text = text.replace("(", " ").replace(")", " ")
    text = " ".join(text.split())
    # Strip leading stopwords / connectors
    text = re.sub(r'^(of|from|and|or|\*|:|with|[-–])\s*', '', text, flags=re.IGNORECASE)
    # Strip trailing connectors and prepositions
    text = re.sub(r'\s+(and|or|\*|with|in)$', '', text, flags=re.IGNORECASE)
    # "from N <word>" — the source fruit/ingredient (e.g. "from 1 lime")
    # is prep context, not part of the canonical name.  Strip it here.
    text = re.sub(r'\bfrom\s+\d*\s*(?:large|small|medium|fresh|whole)?\s*\w+\s*$', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'\bfrom\s+\d+\s+(?:large|small|medium|fresh)?\s*\w+', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'\bfrom\s+', '', text, flags=re.IGNORECASE).strip()
    # Strip "or <prep-verb-phrase>" that leaked from unparsed prep alternatives
    # e.g. "garlic or pressed through garlic press" → "garlic"
    text = re.sub(
        r'\s+or\s+(?:pressed|pushed|passed|run|rubbed|blended|pureed|mashed|'
        r'squeezed|grated|ground|minced|chopped|diced|sliced|'
        r'low[\s-]sodium|reduced[\s-]sodium|unsalted|homemade|store[\s-]bought)'
        r'[\w\s-]*$',
        '', text, flags=re.IGNORECASE
    ).strip()
    text = re.sub(r'(?<!\w)\d+(?:\.\d+)?(?!\w)', '', text).strip()
    # Strip stray isolated punctuation dots
    text = re.sub(r'(?<!\w)\.(?!\w)', '', text).strip()
    # Strip stray isolated single letters (artifacts from unit removal etc.)
    text = re.sub(r'(?<!\w)[a-z](?!\w)', '', text, flags=re.IGNORECASE).strip()
    return " ".join(text.split()).strip('*').strip()


def split_multi_ingredients(text):
    """
    Only split "A and B" when BOTH sides have no digits and total tokens ≤ 6.
    This handles "salt and pepper" → ["salt", "pepper"].
    We do NOT split on "or" here — "or" alternatives are handled upstream by
    split_on_or_alternative before parse_line is called.
    """
    if " and " in text:
        parts = [p.strip() for p in text.split(" and ", 1)]
        if all(not re.search(r'\d', p) for p in parts) and len(text.split()) <= 6:
            return parts
    return [text]


# ============================================================
# OPTIONAL FLAG DETECTION
# Lines that are themselves flagged as optional via noise words
# or leading "optional:" prefix.
# ============================================================

def detect_optional_flag(raw_text):
    """Return True if the raw line is self-declared optional."""
    stripped = raw_text.strip().lstrip('•-–*').strip()
    if re.match(r'^optional\s*:', stripped, re.IGNORECASE):
        return True
    if re.search(r'\(optional\)', stripped, re.IGNORECASE):
        return True
    if re.search(r'\boptional\b', stripped, re.IGNORECASE):
        return True
    return False


# ============================================================
# MAIN PARSER  (single sub-line)
# ============================================================

def parse_line(raw_text, optional=False):
    text = normalize_text(raw_text)
    text = remove_leading_symbols(text)

    text, juice_prep        = extract_juice_form(text)
    text, pct_note          = extract_percent_by_weight(text)
    # Extract explicit gram/ml measures BEFORE parenthetical extraction so that
    # "(45ml)" and "(50g)" are captured into their columns and not forwarded
    # to prep as raw paren strings.
    text, grams_val, ml_val, pct_val = extract_explicit_measures(text)
    text, parens            = extract_parentheticals(text)

    # Parse paren content for secondary imperial measure  e.g. "(1 stick)"
    paren_qty = paren_unit = None
    for pc in parens:
        pq, pu = extract_paren_secondary_measure(pc)
        if pq and pu:
            paren_qty, paren_unit = pq, pu
            break
    text, can_qty, can_unit, can_size_note = extract_can_size(text)
    text = remove_size_descriptors(text)

    # If a gram weight was already captured, strip any remaining secondary
    # volume/count measure (e.g. "1/4 cup" after "50 g, 1/4 cup, …")
    if grams_val is not None:
        text = re.sub(
            r'(?<!\w)(\d+(?:\.\d+)?)\s*'
            r'(cup|cups|tbsp|tsp|tablespoon|tablespoons|teaspoon|teaspoons|'
            r'oz|ounce|ounces|lb|pound|pounds|pint|pints|quart|quarts|'
            r'ml|liter|litre|g|kg)(?!\w)',
            '', text, flags=re.IGNORECASE
        ).strip().strip(',').strip()

    text, quantity = extract_quantity(text)

    unit = None

    # Resolve canonical qty/unit from gram/can data
    if can_qty is not None:
        quantity = can_qty
        unit     = can_unit
        if can_size_note:
            parens = list(parens) + [can_size_note]
    elif grams_val is not None:
        # If a parenthetical imperial measure exists, prefer it for qty/unit
        if paren_qty and paren_unit:
            quantity = paren_qty
            unit     = paren_unit
        else:
            quantity = grams_val
            unit     = "g"

    # Protect phrases BEFORE unit extraction so e.g. "ground cloves" is not
    # split into unit=cloves + name=ground.
    text, phrase_map = _protect_phrases(text, PROTECTED_PHRASES)
    text, unit     = extract_unit(text) if unit is None else (text, unit)

    text = remove_noise_phrases(text)
    text, state      = extract_temperature_state(text)
    text, prep       = extract_prep(text)
    text = remove_actions(text)
    text = remove_size_adjectives(text)
    text = _restore_phrases(text, phrase_map)

    name = clean_name(text)
    name = fix_truncations(name)

    # Filter parens: drop any that were already used as paren_qty/unit
    notes = []
    for pc in parens:
        pq, pu = extract_paren_secondary_measure(pc)
        if pq and pu and pq == paren_qty and pu == paren_unit:
            continue   # already represented in qty/unit columns
        notes.append(pc)

    if pct_note:
        notes.append(pct_note.strip())

    prep_parts = [p for p in [juice_prep, prep, state] if p]
    final_prep = ", ".join(prep_parts) if prep_parts else None
    if notes:
        final_prep = ((final_prep or "") + " | " + "; ".join(notes)).strip(" |")

    return (
        quantity, unit, final_prep,
        grams_val, ml_val, pct_val,
        1 if optional else 0,
        split_multi_ingredients(name)
    )


# ============================================================
# PUBLIC ENTRY POINT
# ============================================================

def parse_ingredient_line(raw_text):
    """
    Parse one raw ingredient line and return a list of dicts, one per
    resulting ingredient row.  A single raw line can expand to multiple
    rows via:
      • "or"-alternative splitting  → second row marked optional=1
      • "plus"/"+" splitting        → peer rows (same optional flag)
      • "A and B" / "A or B" names → separate name rows within one quantity
    """
    results = []

    # Detect self-declared optional lines (e.g. "optional: tomato leaves")
    line_is_optional = detect_optional_flag(raw_text)

    primary_text, alt_text = split_on_or_alternative(raw_text)

    for is_optional, text in [(line_is_optional, primary_text), (True, alt_text)]:
        if text is None:
            continue
        sub_lines = split_on_plus(text)
        parsed_sub = []
        for sub in sub_lines:
            qty, unit, prep, grams, ml, scaling, opt, names = parse_line(
                sub, optional=is_optional
            )
            for name in names:
                parsed_sub.append({
                    "quantity": qty, "unit": unit, "prep": prep,
                    "grams": grams, "ml": ml, "scaling": scaling,
                    "optional": opt, "name": name, "raw_text": raw_text,
                })
        # If a sub-line produced an empty name, forward the nearest real name
        if len(parsed_sub) > 1:
            canonical = next((r["name"] for r in reversed(parsed_sub) if r["name"]), "")
            for r in parsed_sub:
                if not r["name"]:
                    r["name"] = canonical
        results.extend(parsed_sub)

    return results


# ============================================================
# DB EXECUTION
# ============================================================
#
# DUPLICATE-ROW SAFETY
# --------------------
# The script fetches existing rows, clears their parsed columns, then
# re-writes them.  A single raw_text line can expand to multiple DB rows
# (one per alternative/plus-split).  On subsequent runs the expanded rows
# are ALREADY in the DB, so we must only ever process the FIRST row for
# each (recipe_row_id, line_index) group — the primary row — and DELETE
# any previously-inserted secondary rows before re-inserting them.
# Without this guard the secondary rows double on every run.

def _run(conn):
    c = conn.cursor()

    # ------------------------------------------------------------------
    # Step 1: Identify the canonical (lowest id) row per
    #         (recipe_row_id, line_index).  These are the primary rows.
    # ------------------------------------------------------------------
    c.execute("""
        SELECT MIN(id) AS primary_id, recipe_id, recipe_row_id, line_index,
               raw_text, section
        FROM recipe_ingredients
        WHERE raw_text IS NOT NULL
        GROUP BY recipe_row_id, line_index
    """)
    primary_rows = c.fetchall()

    # ------------------------------------------------------------------
    # Step 2: Delete ALL non-primary rows (i.e. previously-generated
    #         secondary/alternative rows) so we start clean.
    # ------------------------------------------------------------------
    c.execute("""
        DELETE FROM recipe_ingredients
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM recipe_ingredients
            WHERE raw_text IS NOT NULL
            GROUP BY recipe_row_id, line_index
        )
        AND raw_text IS NOT NULL
    """)

    # ------------------------------------------------------------------
    # Step 3: Clear the parsed columns on the surviving primary rows.
    # ------------------------------------------------------------------
    c.execute("""
        UPDATE recipe_ingredients SET
            quantity_value=NULL, quantity_unit=NULL, preparation=NULL,
            ingredient_name=NULL, grams=NULL, ml=NULL, scaling=NULL, optional=0
        WHERE raw_text IS NOT NULL
    """)

    # ------------------------------------------------------------------
    # Step 4: Parse and write back.
    # ------------------------------------------------------------------
    for primary_id, recipe_id, recipe_row_id, line_index, raw_text, section in primary_rows:
        if not raw_text:
            continue
        parsed_rows = parse_ingredient_line(raw_text)
        for i, r in enumerate(parsed_rows):
            if i == 0:
                c.execute("""
                    UPDATE recipe_ingredients SET
                        quantity_value=?, quantity_unit=?, preparation=?,
                        ingredient_name=?, grams=?, ml=?, scaling=?, optional=?
                    WHERE id=?
                """, (r["quantity"], r["unit"], r["prep"], r["name"],
                      r["grams"], r["ml"], r["scaling"], r["optional"],
                      primary_id))
            else:
                c.execute("""
                    INSERT INTO recipe_ingredients
                    (recipe_id, recipe_row_id, line_index, raw_text, section,
                     quantity_value, quantity_unit, preparation, ingredient_name,
                     grams, ml, scaling, optional)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (recipe_id, recipe_row_id, line_index, raw_text, section,
                      r["quantity"], r["unit"], r["prep"], r["name"],
                      r["grams"], r["ml"], r["scaling"], r["optional"]))

    conn.commit()
    print("Ingredient lines parsed — %d primary rows processed." % len(primary_rows))


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    try:
        _run(conn)
    finally:
        conn.close()