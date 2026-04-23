import sqlite3
import os
import re
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")


# ============================================================
# NORMALIZATION
# ============================================================

def normalize(text):
    if not text:
        return ""

    text = text.lower()

    # unify punctuation
    text = re.sub(r'[^a-z0-9\s\-]', ' ', text)

    # collapse whitespace
    text = " ".join(text.split())

    return text


# ============================================================
# STRIP JUNK TOKENS
# ============================================================

JUNK_WORDS = {
    "chunks", "chunk", "pieces", "piece", "finely", "rough",
    "halved", "lengthwise", "crosswise", "bite", "size",
    "sized", "cubed", "cubes", "dice", "diced",
    "thin", "thick", "extra", "virgin", "pure",
    "fresh", "freshly", "large", "small",
    "boneless", "skinless", "removed", "reserved",
    "plus", "minus", "or", "and", "with", "from",
    "such", "as", "for", "serving", "garnish",
}


def strip_junk_tokens(text):
    tokens = text.split()
    tokens = [t for t in tokens if t not in JUNK_WORDS]
    return " ".join(tokens)


# ============================================================
# HIGH CONFIDENCE CANONICAL RULES
# ============================================================

# These solve your most important real-world errors
CANONICAL_RULES = [
    # --- fats / oils ---
    (r'.*olive oil.*', "olive oil"),
    (r'.*vegetable oil.*', "vegetable oil"),
    (r'.*canola oil.*', "canola oil"),
    (r'.*sesame oil.*', "sesame oil"),
    (r'.*oil.*', "oil"),

    # --- butter ---
    (r'.*butter.*', "butter"),

    # --- chicken ---
    (r'.*chicken.*(stock|broth).*', "chicken stock"),
    (r'.*chicken.*', "chicken"),

    # --- beef ---
    (r'.*beef.*', "beef"),

    # --- pork ---
    (r'.*pork.*', "pork"),
    (r'.*bacon.*', "bacon"),

    # --- dairy ---
    (r'.*milk.*', "milk"),
    (r'.*cream.*', "cream"),
    (r'.*cheese.*', "cheese"),

    # --- eggs ---
    (r'.*egg.*', "egg"),

    # --- vegetables ---
    (r'.*onion.*', "onion"),
    (r'.*garlic.*', "garlic"),
    (r'.*carrot.*', "carrot"),
    (r'.*celery.*', "celery"),
    (r'.*potato.*', "potato"),

    # --- tomatoes ---
    (r'.*tomato.*', "tomato"),

    # --- grains ---
    (r'.*flour.*', "flour"),
    (r'.*rice.*', "rice"),
    (r'.*pasta.*|.*spaghetti.*|.*noodle.*', "pasta"),

    # --- legumes ---
    (r'.*bean.*', "beans"),

    # --- liquids ---
    (r'.*vinegar.*', "vinegar"),
    (r'.*wine.*', "wine"),

    # --- sugar ---
    (r'.*sugar.*', "sugar"),
    (r'.*honey.*', "honey"),

    # --- fallback stock ---
    (r'.*stock.*', "stock"),
    (r'.*broth.*', "stock"),
]


# ============================================================
# DERIVE CANONICAL
# ============================================================

def derive_canonical(name):
    if not name:
        return None, "empty"

    original = name
    text = normalize(name)

    if not text or len(text) < 2:
        return None, "garbage"

    text = strip_junk_tokens(text)

    # --- apply rules ---
    for pattern, canonical in CANONICAL_RULES:
        if re.search(pattern, text):
            return canonical, "rule"

    # --- fallback: last meaningful token ---
    tokens = text.split()

    if not tokens:
        return None, "garbage"

    base = tokens[-1]

    # crude singularization
    if base.endswith("es"):
        base = base[:-2]
    elif base.endswith("s") and not base.endswith("ss"):
        base = base[:-1]

    return base, "fallback"


# ============================================================
# RUN + METRICS
# ============================================================

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("SELECT id, name FROM ingredients")
rows = c.fetchall()

stats = defaultdict(int)
fail_examples = []

for ing_id, name in rows:
    canonical, method = derive_canonical(name)

    stats["total"] += 1
    stats[method] += 1

    if method in ("garbage", "empty"):
        fail_examples.append((ing_id, name))

    c.execute("""
        UPDATE ingredients
        SET canonical_group = ?
        WHERE id = ?
    """, (canonical, ing_id))


conn.commit()
conn.close()


# ============================================================
# REPORT
# ============================================================

print("\n=== CANONICALIZATION REPORT ===")
print(f"Total: {stats['total']}")
print(f"Rule-based: {stats['rule']}")
print(f"Fallback: {stats['fallback']}")
print(f"Garbage: {stats['garbage']}")
print(f"Empty: {stats['empty']}")

print("\n--- Needs Review (sample) ---")
for row in fail_examples[:20]:
    print(row)

print("\nDone.")