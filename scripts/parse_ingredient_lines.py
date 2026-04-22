# scripts/parse_ingredient_lines.py

import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

# ------------------------
# NORMALIZATION
# ------------------------

def normalize_text(text):
    if not text:
        return text

    text = text.lower()

    fractions = {
        "½": "1/2", "⅓": "1/3", "⅔": "2/3",
        "¼": "1/4", "¾": "3/4", "⅛": "1/8"
    }

    for k, v in fractions.items():
        text = text.replace(k, v)

    text = text.replace("tsp.", "tsp")
    text = text.replace("tbsp.", "tbsp")
    text = text.replace("oz.", "oz")

    text = text.replace(" t ", " tsp ")
    text = text.replace(" T ", " tbsp ")
    text = text.replace(" c ", " cup ")

    return text.strip()


# ------------------------
# CLEANING RULES
# ------------------------

def remove_leading_symbols(text):
    # remove bullets, dashes, etc. but NOT numbers
    return re.sub(r'^[\-\–\+\•\*\s]+', '', text).strip()


MEASURE_WORDS = [
    "grams", "gram", "g",
    "ounces", "ounce", "oz",
    "pints", "quart", "qt"
]

def remove_measure_words(text):
    tokens = text.split()
    tokens = [t for t in tokens if t not in MEASURE_WORDS]
    return " ".join(tokens)


NOISE_PHRASES = [
    "plus more",
    "as needed",
    "to taste",
    "if desired",
    "optional",
]

def remove_noise_phrases(text):
    for phrase in NOISE_PHRASES:
        text = text.replace(phrase, "")
    return text.strip()


ACTION_WORDS = [
    "chopped", "diced", "minced", "sliced",
    "peeled", "grated", "cut", "divided",
    "washed", "trimmed", "separated"
]

def remove_actions(text):
    tokens = text.split()
    tokens = [t for t in tokens if t not in ACTION_WORDS]
    return " ".join(tokens)


FIXES = {
    "oi": "oil",
    "sausag": "sausage",
    "leav": "leaves",
    "noodl": "noodle",
    "chiv": "chive",
    "parsley leav": "parsley leaves"
}

def fix_truncations(text):
    for bad, good in FIXES.items():
        text = text.replace(bad, good)
    return text


# ------------------------
# EXTRACTION
# ------------------------

def extract_parentheticals(text):
    matches = re.findall(r'\((.*?)\)', text)
    text = re.sub(r'\(.*?\)', '', text).strip()
    return text, matches


QUANTITY_PATTERN = r'^(\d+\s*/\s*\d+|\d+\.\d+|\d+)(\s*(to|-)\s*(\d+\s*/\s*\d+|\d+\.\d+|\d+))?'

def extract_quantity(text):
    match = re.match(QUANTITY_PATTERN, text)
    if match:
        full = match.group(0)

        # handle ranges like "1 to 2"
        if match.group(2):
            qty = match.group(4)  # take upper bound
        else:
            qty = match.group(1)

        text = text[len(full):].strip()
        return text, qty

    return text, None

UNIT_PATTERN = r'\b(cup|cups|quart|part|pinch|recipe|sprig|pint|tbsp|tsp|teaspoon|tablespoon|teaspoons|tablespoons|lb|pound|oz|ounce|clove|cloves|stick|sticks|can|cans|kg|g)\b'

def extract_unit(text):
    match = re.search(UNIT_PATTERN, text)
    if match:
        unit = match.group(0)
        text = re.sub(r'\b' + re.escape(unit) + r'\b', '', text, count=1).strip()
        return text, unit
    return text, None


PREP_PATTERNS = [
    r'roughly chopped',
    r'finely chopped',
    r'chopped',
    r'diced',
    r'minced',
    r'finely sliced',
    r'finely minced',
    r'sliced',
    r'shredded'
]

def extract_prep(text):
    for pattern in PREP_PATTERNS:
        if re.search(pattern, text):
            text = re.sub(pattern, '', text).strip()
            return text, pattern
    return text, None


# ------------------------
# FINAL CLEAN
# ------------------------

def clean_name(text):
    text = text.replace(",", " ")
    text = " ".join(text.split())
    return text.strip()


# ------------------------
# MULTI-INGREDIENT SPLIT
# ------------------------

def split_multi_ingredients(text):
    # Only split simple cases
    if " and " in text and len(text.split()) <= 4:
        return [t.strip() for t in text.split(" and ")]
    if " or " in text and len(text.split()) <= 4:
        return [t.strip() for t in text.split(" or ")]
    return [text]


# ------------------------
# MAIN PARSER
# ------------------------

def parse_line(raw_text):
    text = normalize_text(raw_text)

    # early cleanup
    text = remove_leading_symbols(text)
    text, parens = extract_parentheticals(text)

    # extract quantity FIRST while string is intact
    text, quantity = extract_quantity(text)

    text, unit = extract_unit(text)

    # mid cleanup
    text = remove_measure_words(text)
    text = remove_noise_phrases(text)

    # prep
    text, prep = extract_prep(text)
    text = remove_actions(text)

    # final name
    name = clean_name(text)
    name = fix_truncations(name)

    # merge prep
    if parens:
        prep = (prep or "") + " " + " ".join(parens)

    names = split_multi_ingredients(name)

    return quantity, unit, prep, names


# ------------------------
# DB EXECUTION
# ------------------------

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""
SELECT id, recipe_id, recipe_row_id, line_index, raw_text, section
FROM recipe_ingredients
""")

rows = c.fetchall()

# Clear parsed data first (important when re-running)
c.execute("""
UPDATE recipe_ingredients
SET quantity_value = NULL,
    quantity_unit = NULL,
    preparation = NULL,
    ingredient_name = NULL
""")

for row_id, recipe_id, recipe_row_id, line_index, raw_text, section in rows:
    if not raw_text:
        continue

    quantity, unit, prep, names = parse_line(raw_text)

    for i, name in enumerate(names):
        if i == 0:
            # update original row
            c.execute("""
            UPDATE recipe_ingredients
            SET quantity_value = ?,
                quantity_unit = ?,
                preparation = ?,
                ingredient_name = ?
            WHERE id = ?
            """, (quantity, unit, prep, name, row_id))
        else:
            # insert additional rows for split ingredients
            c.execute("""
            INSERT INTO recipe_ingredients
            (recipe_id, recipe_row_id, line_index, raw_text, section,
             quantity_value, quantity_unit, preparation, ingredient_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                recipe_id,
                recipe_row_id,
                line_index,
                raw_text,
                section,
                quantity,
                unit,
                prep,
                name
            ))

conn.commit()
conn.close()

print("Ingredient lines parsed")