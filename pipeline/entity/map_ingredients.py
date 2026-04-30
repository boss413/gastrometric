import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Get all parsed ingredient names
c.execute("""
SELECT DISTINCT ingredient_name
FROM recipe_ingredients
WHERE ingredient_name IS NOT NULL
""")

names = c.fetchall()

def normalize_ingredient_name(name):
    if not name:
        return None

    name = name.lower().strip()

    # remove punctuation
    name = name.replace(",", "").strip()

    # remove common trailing noise
    noise_words = ["to taste", "as needed"]
    for word in noise_words:
        name = name.replace(word, "").strip()

    # singularize (very naive but effective early)
    if name.endswith("es"):
        name = name[:-2]
    elif name.endswith("s") and not name.endswith("ss"):
        name = name[:-1]

    # remove common descriptors that should NOT define identity
    remove_words = [
        "fresh", "large", "small", "medium",
        "extra", "virgin", "optional"
    ]

    tokens = name.split()
    tokens = [t for t in tokens if t not in remove_words]

    name = " ".join(tokens)

    return name.strip()

for (name,) in names:
    if not name:
        continue

    normalized = normalize_ingredient_name(name)

    # Check if alias exists
    c.execute("""
    SELECT ingredient_id FROM ingredient_aliases WHERE alias = ?
    """, (normalized,))
    result = c.fetchone()

    if result:
        continue  # already mapped

    # Check if canonical ingredient exists
    c.execute("""
    SELECT id FROM ingredients WHERE name = ?
    """, (normalized,))
    ingredient = c.fetchone()

    if ingredient:
        ingredient_id = ingredient[0]
    else:
        # create new canonical ingredient
        c.execute("""
        INSERT INTO ingredients (name) VALUES (?)
        """, (normalized,))
        ingredient_id = c.lastrowid

    # create alias mapping
    c.execute("""
    INSERT INTO ingredient_aliases (alias, ingredient_id)
    VALUES (?, ?)
    """, (normalized, ingredient_id))

conn.commit()
conn.close()

print("Ingredient mapping complete")