import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

def derive_group(name):
    if not name:
        return None

    name = name.strip()

    # manual overrides FIRST (high priority)
    SPECIAL_CASES = {
        "chicken breast": "chicken",
        "chicken thigh": "chicken",
        "chicken leg meat": "chicken",
        "garlic clove": "garlic",
        "garlic cloves": "garlic",
        "green onions": "green onion",
        "scallions": "green onion",
        "spring onions": "green onion",
        "olive oil": "oil",
        "vegetable oil": "oil",
        "canola oil": "oil",
        "half-and-half": "half & half"
    }

    if name in SPECIAL_CASES:
        return SPECIAL_CASES[name]

    tokens = name.split()

    if not tokens:
        return None

    # fallback: last word heuristic
    base = tokens[-1]

    # crude singularization
    if base.endswith("es"):
        base = base[:-2]
    elif base.endswith("s") and not base.endswith("ss"):
        base = base[:-1]

    return base


conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("SELECT id, name FROM ingredients")

rows = c.fetchall()

for ing_id, name in rows:
    group = derive_group(name)

    c.execute("""
        UPDATE ingredients
        SET canonical_group = ?
        WHERE id = ?
    """, (group, ing_id))

conn.commit()
conn.close()

print("Canonical groups generated")