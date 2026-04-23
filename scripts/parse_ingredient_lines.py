# scripts/parse_ingredient_lines.py

import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")


# ============================================================
# PROTECTED INGREDIENT PHRASES
# ============================================================

PROTECTED_PHRASES = [
    "cream of tartar", "cream of wheat", "cream of mushroom soup",
    "baking soda", "baking powder",
    "half and half", "half & half",
    "heavy cream", "whipping cream",
    "all purpose flour", "all-purpose flour", "bread flour", "cake flour",
    "whole wheat flour", "self rising flour", "self-rising flour",
    "freshly ground black pepper", "freshly ground pepper",
    "ground black pepper", "ground white pepper",
    "ground beef", "ground pork", "ground turkey",
    "ground chicken", "ground lamb",
    "ground coriander", "ground cumin", "ground ginger", "ground cinnamon",
    "ground nutmeg", "ground allspice", "ground cloves", "ground cardamom",
    "ground turmeric", "ground paprika", "ground mustard", "ground fennel",
    "crushed red pepper", "crushed red pepper flakes", "red pepper flakes",
    "dried thyme", "dried oregano", "dried basil", "dried rosemary",
    "dried sage", "dried parsley", "dried dill", "dried mint", "dried chili",
    "extra virgin olive oil", "olive oil", "vegetable oil", "canola oil",
    "sesame oil", "coconut oil",
    "brown sugar", "white sugar", "granulated sugar",
    "powdered sugar", "confectioners sugar", "confectioners' sugar",
    "soy sauce", "fish sauce", "hot sauce", "worcestershire sauce",
    "parmesan cheese", "cheddar cheese", "mozzarella cheese",
    "green onions", "spring onions",
    "red onion", "yellow onion", "white onion",
    "crushed tomatoes", "diced tomatoes", "tomato paste", "tomato sauce",
    "boiling water", "cold water", "ice water", "warm water",
]


KNOWN_INGREDIENTS = [
    "chicken breast", "chicken thighs", "whole chicken",
    "ground beef", "ground pork", "ground turkey",
    "beef chuck", "beef brisket", "pork chops", "pork shoulder",
    "andouille sausage", "italian sausage", "bacon", "sausage",
    "salmon", "tuna", "cod", "shrimp",
    "whole milk", "skim milk", "milk",
    "unsalted butter", "salted butter", "butter",
    "heavy cream", "whipping cream", "half and half",
    "cream cheese", "sour cream", "yogurt",
    "parmesan cheese", "cheddar cheese", "mozzarella cheese",
    "feta cheese", "goat cheese", "mozzarella", "parmesan",
    "egg yolk", "egg white", "egg", "eggs",
    "all-purpose flour", "all purpose flour", "bread flour",
    "white rice", "brown rice", "rice",
    "pasta", "spaghetti", "noodles", "bread crumbs",
    "black beans", "pinto beans", "kidney beans", "chickpeas", "lentils",
    "red onion", "yellow onion", "white onion", "green onions", "spring onions", "onion",
    "garlic cloves", "garlic",
    "carrot", "carrots", "celery",
    "sweet potato", "potato", "potatoes",
    "red bell pepper", "green bell pepper", "yellow bell pepper",
    "orange bell pepper", "bell pepper",
    "jalapeno", "chili pepper",
    "romaine lettuce", "iceberg lettuce", "lettuce",
    "baby spinach", "spinach", "kale",
    "crushed tomatoes", "diced tomatoes", "tomato paste", "tomato sauce",
    "tomato", "tomatoes",
    "fresh basil", "basil", "parsley", "cilantro",
    "thyme", "rosemary", "oregano",
    "kosher salt", "salt", "black pepper", "white pepper",
    "cumin", "paprika", "turmeric", "cinnamon",
    "extra virgin olive oil", "olive oil", "vegetable oil", "canola oil",
    "soy sauce", "apple cider vinegar", "vinegar",
    "brown sugar", "white sugar", "granulated sugar", "sugar",
    "honey", "maple syrup",
    "vanilla extract", "chocolate chips", "peanut butter",
]


# ============================================================
# PHRASE PROTECTION HELPERS
# ============================================================

def _protect_phrases(text, protected):
    mapping = {}
    for i, phrase in enumerate(sorted(protected, key=len, reverse=True)):
        token = "__PROTECTED_%d__" % i
        if phrase in text:
            mapping[token] = phrase
            text = text.replace(phrase, token)
    return text, mapping


def _restore_phrases(text, mapping):
    for token, phrase in mapping.items():
        text = text.replace(token, phrase)
    return text


# ============================================================
# PLUS / + SPLITTING
# ============================================================

PLUS_SPLIT_PATTERN = re.compile(
    r'(?<!\w)\+(?!\w)|'
    r'\bplus\b(?!\s+more\b)',
    re.IGNORECASE
)

def split_on_plus(raw_text):
    # Do not split on "plus" that appears inside parentheses
    # Strategy: temporarily replace paren content, split, then nothing to restore
    # because we only use this to decide split points on the outer text.
    paren_masked = re.sub(r'\([^)]*\)', lambda m: 'X' * len(m.group(0)), raw_text)
    parts_masked = PLUS_SPLIT_PATTERN.split(paren_masked)
    if len(parts_masked) < 2:
        return [raw_text]
    # Recover original text at the same split boundaries
    # by using the split positions from the masked version
    positions = []
    pos = 0
    for part in parts_masked:
        positions.append((pos, pos + len(part)))
        pos += len(part)
        # skip the matched separator in masked text
        remainder = paren_masked[pos:]
        m = PLUS_SPLIT_PATTERN.match(remainder)
        if m:
            pos += len(m.group(0))
    parts = [raw_text[s:e].strip() for s, e in positions if raw_text[s:e].strip()]
    if len(parts) >= 2 and re.match(r'^\d', parts[1].strip()):
        return parts
    return [raw_text]


# ============================================================
# OR-ALTERNATIVE SPLITTING
# ============================================================

_UNIT_VOCAB = {
    'cup','cups','tbsp','tsp','tablespoon','tablespoons','teaspoon','teaspoons',
    'oz','ounce','ounces','lb','pound','pounds','g','kg','ml','liter','litre',
    'pint','quart','gallon','can','cans','jar','bottle','bunch','head','clove','cloves',
}

OR_PEER_PATTERN = re.compile(
    r'^(.+?)\s+or\s+(\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?)\s+(\w+)\s+(.+)$',
    re.IGNORECASE
)

def split_on_or_alternative(raw_text):
    # Operate on normalized text so abbreviations are resolved (oz., T, etc.)
    norm = normalize_text(raw_text)
    m = OR_PEER_PATTERN.match(norm.strip())
    if not m:
        return raw_text, None
    primary_norm = m.group(1).strip()
    alt_qty      = m.group(2).strip()
    alt_unit     = m.group(3).strip()
    alt_name     = m.group(4).strip()
    if alt_unit.lower() not in _UNIT_VOCAB:
        return raw_text, None
    # Return raw text sliced to primary length for best downstream fidelity,
    # but fall back to normalized if lengths differ (abbreviation expansion)
    alt_line = "%s %s %s" % (alt_qty, alt_unit, alt_name)
    return primary_norm, alt_line


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_text(text):
    if not text:
        return text

    # Case-sensitive abbreviations before lowercasing
    text = re.sub(r'\bTbsp\b', 'tbsp', text)
    text = re.sub(r'\bTBSP\b', 'tbsp', text)
    text = re.sub(r'\bTSP\b',  'tsp',  text)
    text = re.sub(r'(?<!\w)T(?!\w)', 'tbsp', text)
    text = re.sub(r'(?<!\w)t(?!\w)', 'tsp',  text)

    text = text.lower()

    # Protect compound phrases before any substitution
    text, phrase_map = _protect_phrases(text, PROTECTED_PHRASES)

    # Protect words that clash with substitutions
    text = text.replace("weight", "__weight__")
    text = text.replace("eighth", "__eighth__")

    # "N and fraction" -> decimal
    def _and_frac(m):
        return str(float(m.group(1)) + float(m.group(2)) / float(m.group(3)))
    text = re.sub(r'(\d+)\s+and\s+(\d+)\s*/\s*(\d+)', _and_frac, text)
    text = re.sub(r'(\d+)\s+and\s+a\s+half',
                  lambda m: str(float(m.group(1)) + 0.5), text)

    fractions = [
        ("2 1/2","2.5"),("1-1/2","1.5"),("1 1/2","1.5"),
        ("1-½","1.5"),("1½","1.5"),("1 ½","1.5"),
        ("½","0.5"),("1/2","0.5"),
        ("⅓","0.333"),("1/3","0.333"),
        ("⅔","0.666"),("2/3","0.666"),
        ("¼","0.25"),("1/4","0.25"),
        ("¾","0.75"),("3/4","0.75"),
        ("⅛","0.125"),("1/8","0.125"),
        ("⅜","0.375"),("3/8","0.375"),
        ("⅝","0.625"),("5/8","0.625"),
        ("⅞","0.875"),("7/8","0.875"),
    ]
    for k, v in fractions:
        text = text.replace(k, v)

    # Word numbers — "half" included HERE after phrase protection
    word_numbers = {
        "half": "0.5",
        "two": "2", "three": "3", "four": "4", "five": "5",
        "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
    }
    for word, num in word_numbers.items():
        text = re.sub(r'\b' + word + r'\b', num, text)

    text = text.replace("__weight__", "weight")
    text = text.replace("__eighth__", "eighth")
    text = _restore_phrases(text, phrase_map)

    text = text.replace("tsp.", "tsp").replace("tbsp.", "tbsp").replace("oz.", "oz")
    text = re.sub(r'(?<!\w)c(?!\w)', 'cup', text)

    return text.strip()


# ============================================================
# PARENTHETICAL SECONDARY MEASURE
# ============================================================

_IMPERIAL_VOL_PRIORITY = {
    'oz': 1, 'ounce': 1, 'ounces': 1,
    'lb': 1, 'pound': 1, 'pounds': 1,
    'pint': 1, 'quart': 1, 'gallon': 1,
    'cup': 2, 'cups': 2,
    'tbsp': 3, 'tablespoon': 3, 'tablespoons': 3,
    'tsp': 4, 'teaspoon': 4, 'teaspoons': 4,
}

_PAREN_MEASURE_RE = re.compile(
    r'(?:about|approximately|approx\.?)?\s*'
    r'(\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?)\s*'
    r'(cup|cups|oz|ounce|ounces|tbsp|tablespoon|tablespoons|'
    r'tsp|teaspoon|teaspoons|pint|quart|gallon|lb|pound|pounds)',
    re.IGNORECASE
)

def extract_paren_secondary_measure(paren_text):
    candidates = []
    for m in _PAREN_MEASURE_RE.finditer(paren_text):
        raw_qty = m.group(1).strip()
        unit    = m.group(2).lower()
        if '-' in raw_qty:
            raw_qty = raw_qty.split('-')[0].strip()
        priority = _IMPERIAL_VOL_PRIORITY.get(unit, 99)
        candidates.append((priority, raw_qty, unit))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1], candidates[0][2]


# ============================================================
# EXPLICIT MEASURE EXTRACTION
# ============================================================

_APPROX = r'(?:approximately|approx\.?|about)?\s*'

EXPLICIT_MASS_PATTERN    = re.compile(r',?\s*' + _APPROX + r'(\d+(?:\.\d+)?)\s*(?:grams?|g)\b', re.IGNORECASE)
EXPLICIT_ML_PATTERN      = re.compile(r',?\s*' + _APPROX + r'(\d+(?:\.\d+)?)\s*(?:ml|milliliters?|millilitres?|mls?|liters?|litres?|l)\b', re.IGNORECASE)
EXPLICIT_PCT_PATTERN     = re.compile(r',?\s*(\d+(?:\.\d+)?)\s*%(?:\s+(?:total|by\s+weight(?:\s+of\s+[^,)]+)?))?', re.IGNORECASE)
APPROX_SECONDARY_PATTERN = re.compile(r',?\s*(?:approximately|approx\.?|about)\s+\d+(?:\.\d+)?\s+\w+[^,)]*', re.IGNORECASE)
PLUS_ADDITIONAL_PATTERN  = re.compile(r',?\s*plus\s+additional(?:\s+for\s+\w+)?', re.IGNORECASE)


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

    m = EXPLICIT_PCT_PATTERN.search(text)
    if m:
        pct_val = m.group(1)
        text = (text[:m.start()] + text[m.end():]).strip().rstrip(',').strip()

    text = APPROX_SECONDARY_PATTERN.sub('', text).strip().rstrip(',').strip()
    text = PLUS_ADDITIONAL_PATTERN.sub('', text).strip().rstrip(',').strip()

    return text, grams_val, ml_val, pct_val


# ============================================================
# CAN / JAR SIZE EXTRACTION
# ============================================================

CAN_SIZE_PATTERN = re.compile(
    r'(?:(\d+(?:\.\d+)?)\s+)?'
    r'(\d+(?:\.\d+)?)\s*[-\s]'
    r'(?:ounce|oz)\s+'
    r'(cans?|jars?|bottles?)',
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
# ============================================================

SIZE_DESCRIPTOR_PATTERN = re.compile(r'\d[\d./]*-inch[a-z-]*', re.IGNORECASE)

def remove_size_descriptors(text):
    return SIZE_DESCRIPTOR_PATTERN.sub('', text).strip()


# ============================================================
# CLEANING RULES
# ============================================================

def remove_leading_symbols(text):
    return re.sub(r'^[\-\u2013\+\u2022\*\s]+', '', text).strip()


NOISE_PHRASES = ["plus more","as needed","to taste","if desired","as desired","optional"]

def remove_noise_phrases(text):
    for phrase in NOISE_PHRASES:
        text = re.sub(r'\b' + re.escape(phrase) + r'\b', '', text, flags=re.IGNORECASE)
    return text.strip()


SIZE_ADJECTIVES = ["extra-large","extra large","large","medium","small"]

def remove_size_adjectives(text):
    for adj in SIZE_ADJECTIVES:
        text = re.sub(r'\b' + re.escape(adj) + r'\b', '', text, flags=re.IGNORECASE)
    return " ".join(text.split())


ACTION_WORDS = {
    "chopped","diced","minced","sliced","peeled","grated",
    "cut","divided","washed","trimmed","separated",
    "freshly","into",
}

def remove_actions(text):
    return " ".join(t for t in text.split() if t not in ACTION_WORDS)


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
# EXTRACTION — PARENTHETICALS, QUANTITY, UNIT, PREP
# ============================================================

def extract_parentheticals(text):
    matches = re.findall(r'\((.*?)\)', text)
    text = re.sub(r'\(.*?\)', '', text).strip()
    return text, matches


QUANTITY_PATTERN = re.compile(
    r'^(\d+(?:\.\d+)?)'
    r'(?:\s+(\d+(?:\.\d+)?))?'
    r'(?:\s*(to|-)\s*(\d+(?:\.\d+)?))?'
)

def extract_quantity(text):
    m = QUANTITY_PATTERN.match(text)
    if m:
        full = m.group(0)
        if m.group(4):
            qty = float(m.group(4))
        elif m.group(2):
            qty = float(m.group(1)) + float(m.group(2))
        else:
            qty = float(m.group(1))
        text = text[len(full):].strip()
        return text, (str(qty) if qty != int(qty) else str(int(qty)))
    return text, None


_GARLIC_UNIT_CONTEXT = re.compile(r'\bcloves?\s+(garlic|shallot)\b', re.IGNORECASE)

UNIT_PATTERN = re.compile(
    r'\b(cup|cups|quart|qt|part|pinch|pinches|handful|recipe|sprig|pint|'
    r'tbsp|tsp|gallon|teaspoon|tablespoon|teaspoons|tablespoons|'
    r'lb|pound|pounds|oz|ounce|ounces|head|bunch|sprigs|stalks|'
    r'leaves|leaf|clove|cloves|stick|sticks|strips|slices|'
    r'box|can|cans|jar|bottle|kg|g|ml|milliliter|millilitre|liter|litre)\b'
)

def extract_unit(text):
    # Disambiguate "clove(s)": only treat as unit when followed by garlic/shallot
    if re.search(r'\bcloves?\b', text, re.IGNORECASE):
        if not _GARLIC_UNIT_CONTEXT.search(text):
            # It's the spice — strip anything else via normal path but skip clove
            without_clove = re.sub(r'\bcloves?\b', '', text, flags=re.IGNORECASE).strip()
            if not UNIT_PATTERN.sub('', without_clove).strip():
                return text, None   # bare "cloves" = ingredient name

    m = UNIT_PATTERN.search(text)
    if m:
        unit = m.group(0)
        text = re.sub(r'\b' + re.escape(unit) + r'\b', '', text, count=1).strip()
        return text, unit
    return text, None


PREP_PATTERNS = [
    r'very thinly sliced', r'thinly sliced',
    r'roughly chopped', r'finely chopped', r'coarsely chopped', r'chopped fine',
    r'sliced thinly', r'sliced thin', r'finely sliced', r'finely minced', r'diced fine',
    r'cut into \d[\d./]*-inch[a-z-]* rounds', r'cut into \d[\d./]*-inch[a-z-]* pieces',
    r'cut into \d[\d./]*-inch[a-z-]* chunks', r'cut into \d[\d./]*-inch[a-z-]* strips',
    r'cut into pieces', r'cut into chunks', r'cut into strips', r'cut into rounds',
    r'for dusting', r'for sprinkling', r'for sprinklng', r'for greasing',
    r'loosely packed',
    r'\bchopped\b', r'\bdiced\b', r'\bminced\b', r'\bsliced\b',
    r'\bpeeled\b', r'\bgrated\b', r'\bcrushed\b', r'\bseeded\b',
    r'\bbeaten\b', r'\bwashed\b', r'\btrimmed\b', r'\bseparated\b',
    r'\bdivided\b', r'\bmelted\b', r'\bcubed\b', r'\bsifted\b',
    r'\bpacked\b', r'\bsoftened\b', r'\bshredded\b',
]

def extract_prep(text):
    found = []
    for pattern in PREP_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            found.append(m.group(0))
            text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
    return text, (", ".join(found) if found else None)


# ============================================================
# TEMPERATURE / STATE EXTRACTION
# ============================================================

TEMPERATURE_STATE_PATTERNS = [
    r'at\s+room[\s-]temperature',
    r'room[\s-]temperature',
    r'\bboiling\b(?!\s+water)',
    r'\bchilled\b',
    r'\bcold\b',
    r'\bwarm\b',
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
    text = " ".join(text.split())
    # Strip leading stopwords / connectors
    text = re.sub(r'^(of|from|and|or|\*)\s+', '', text, flags=re.IGNORECASE)
    # Strip trailing connectors
    text = re.sub(r'\s+(and|or|\*)$', '', text, flags=re.IGNORECASE)
    return text.strip().strip('*').strip()


def split_multi_ingredients(text):
    for connector in (" and ", " or "):
        if connector in text:
            parts = [p.strip() for p in text.split(connector, 1)]
            if all(not re.search(r'\d', p) for p in parts) and len(text.split()) <= 6:
                return parts
    return [text]


# ============================================================
# MAIN PARSER  (single sub-line)
# ============================================================

def parse_line(raw_text, optional=False):
    text = normalize_text(raw_text)
    text = remove_leading_symbols(text)

    text, juice_prep        = extract_juice_form(text)
    text, pct_note          = extract_percent_by_weight(text)
    text, parens            = extract_parentheticals(text)

    # Parse paren content for secondary imperial measure
    paren_qty = paren_unit = None
    for pc in parens:
        pq, pu = extract_paren_secondary_measure(pc)
        if pq and pu:
            paren_qty, paren_unit = pq, pu
            break

    text, grams_val, ml_val, pct_val = extract_explicit_measures(text)
    text, can_qty, can_unit, can_size_note = extract_can_size(text)
    text = remove_size_descriptors(text)
    text, quantity = extract_quantity(text)
    # Unit extraction happens after phrase protection (below) to prevent
    # "ground cloves" -> unit=cloves. Set unit=None here; filled in below.
    unit = None

    # Resolve canonical qty/unit
    if can_qty is not None:
        quantity = can_qty
        unit     = can_unit
        if can_size_note:
            parens = list(parens) + [can_size_note]
    elif grams_val is not None:
        if paren_qty and paren_unit:
            quantity = paren_qty
            unit     = paren_unit
        else:
            quantity = grams_val
            unit     = "g"

    # Protect phrases BEFORE unit extraction so e.g. "ground cloves" is not
    # split into unit=cloves + name=ground by the unit extractor.
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

    notes = list(parens)
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
    results = []
    primary_text, alt_text = split_on_or_alternative(raw_text)

    for is_optional, text in [(False, primary_text), (True, alt_text)]:
        if text is None:
            continue
        sub_lines = split_on_plus(text)
        parsed_sub = []
        for sub in sub_lines:
            qty, unit, prep, grams, ml, scaling, opt, names = parse_line(sub, optional=is_optional)
            for name in names:
                parsed_sub.append({
                    "quantity": qty, "unit": unit, "prep": prep,
                    "grams": grams, "ml": ml, "scaling": scaling,
                    "optional": opt, "name": name, "raw_text": raw_text,
                })
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

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("SELECT id, recipe_id, recipe_row_id, line_index, raw_text, section FROM recipe_ingredients")
rows = c.fetchall()

c.execute("""
UPDATE recipe_ingredients SET
    quantity_value=NULL, quantity_unit=NULL, preparation=NULL,
    ingredient_name=NULL, grams=NULL, scaling=NULL, optional=0
""")

for row_id, recipe_id, recipe_row_id, line_index, raw_text, section in rows:
    if not raw_text:
        continue
    parsed_rows = parse_ingredient_line(raw_text)
    for i, r in enumerate(parsed_rows):
        if i == 0:
            c.execute("""
            UPDATE recipe_ingredients SET
                quantity_value=?, quantity_unit=?, preparation=?, ingredient_name=?,
                grams=?, scaling=?, optional=?
            WHERE id=?
            """, (r["quantity"], r["unit"], r["prep"], r["name"],
                  r["grams"], r["scaling"], r["optional"], row_id))
        else:
            c.execute("""
            INSERT INTO recipe_ingredients
            (recipe_id, recipe_row_id, line_index, raw_text, section,
             quantity_value, quantity_unit, preparation, ingredient_name,
             grams, scaling, optional)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (recipe_id, recipe_row_id, line_index, raw_text, section,
                  r["quantity"], r["unit"], r["prep"], r["name"],
                  r["grams"], r["scaling"], r["optional"]))

conn.commit()
conn.close()
print("Ingredient lines parsed")