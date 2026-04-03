# scripts/parse_ingredient_lines.py

import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

import re

def extract_parentheticals(text):
    matches = re.findall(r'\((.*?)\)', text)
    text = re.sub(r'\(.*?\)', '', text).strip()
    return text, matches

def normalize_text(text):
    if not text:
        return text

    text = text.lower()

    # Normalize unicode fractions
    fractions = {
        "½": "1/2",
        "⅓": "1/3",
        "⅔": "2/3",
        "¼": "1/4",
        "¾": "3/4",
        "⅛": "1/8"
    }

    for k, v in fractions.items():
        text = text.replace(k, v)

    # normalize units with dots
    text = text.replace("tsp.", "tsp")
    text = text.replace("tbsp.", "tbsp")
    text = text.replace("oz.", "oz")

    # normalize T/t ambiguity
    text = text.replace(" t ", " tsp ")
    text = text.replace(" T ", " tbsp ")

    return text.strip()

# Basic patterns
QUANTITY_PATTERN = r'(\d+\s*/\s*\d+|\d+\.\d+|\d+)'
UNIT_PATTERN = r'\b(cup|cups|c|tbsp|tsp|teaspoon|tablespoon|teaspoons|tablespoons|lb|pound|part|oz|ounce|clove|cloves|can|cans|g|kg)\b'
PREP_WORDS = [
    "chopped", "minced", "diced", "sliced", "crushed",
    "grated", "shredded", "peeled", "softened"
]

PREP_PATTERNS = [
    r'roughly chopped',
    r'finely chopped',
    r'chopped',
    r'diced',
    r'minced',
    r'sliced',
    r'shredded'
]

def extract_prep(text):
    for pattern in PREP_PATTERNS:
        if re.search(pattern, text):
            text = re.sub(pattern, '', text).strip()
            return text, pattern
    return text, None

def extract_unit(text):
    match = re.search(UNIT_PATTERN, text)
    if match:
        unit = match.group(0)
        text = re.sub(r'\b' + re.escape(unit) + r'\b', '', text, count=1).strip()
        return text, unit
    return text, None

def extract_quantity(text):
    match = re.search(QUANTITY_PATTERN, text)
    if match:
        qty = match.group(0)
        text = text.replace(qty, '', 1).strip()
        return text, qty
    return text, None

def clean_name(text):
    # remove stray commas
    text = text.replace(",", " ")

    # collapse whitespace
    text = " ".join(text.split())

    return text.strip()

def parse_line(raw_text):
    text = normalize_text(raw_text)

    text, parens = extract_parentheticals(text)
    text, quantity = extract_quantity(text)
    text, unit = extract_unit(text)
    text, prep = extract_prep(text)

    name = clean_name(text)

    # merge prep sources
    if parens:
        prep = (prep or "") + " " + " ".join(parens)

    return quantity, unit, prep, name.strip()


conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Pull all unparsed ingredient rows
c.execute("""
SELECT id, raw_text
FROM recipe_ingredients
""")

rows = c.fetchall()

for row_id, raw_text in rows:
    if not raw_text:
        continue

    quantity, unit, prep, name = parse_line(raw_text)

    c.execute("""
    UPDATE recipe_ingredients
    SET quantity_value = ?,
        quantity_unit = ?,
        preparation = ?,
        ingredient_name = ?
    WHERE id = ?
    """, (quantity, unit, prep, name, row_id))

conn.commit()
conn.close()

print("Ingredient lines parsed")