import sqlite3
import os
import re
import unicodedata

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
DB_PATH = os.path.join(DATA_DIR, "gastrometric.db")

def normalize_unicode(text):
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )

def split_compound(raw):
    parts = re.split(r"\band\b|\bor\b|,", raw)
    return [p.strip() for p in parts if p.strip()]

def normalize_raw(raw):
    raw = normalize_unicode(raw.lower().strip())

    # remove leading/trailing parentheses
    if raw.startswith("(") and raw.endswith(")"):
        return None

    # remove standalone location lines
    if re.match(r"^\(.+\)$", raw):
        return None

    # remove chef/restaurant lines
    if "," in raw and "(" in raw and ")" in raw:
        return None

    # normalize cuisine formatting
    raw = raw.replace("(", "").replace(")", "")

    # normalize "x (y) cuisine" → "x cuisine y"
    raw = re.sub(r"(.+?) cuisine (\w+)", r"\1 cuisine \2", raw)

    # remove "see also" junk if it appears later
    raw = re.sub(r"see also.*", "", raw).strip()

    return raw

def tokenize(text):
    return re.findall(r"[a-z]+", text)

MODIFIERS = {
    "juice", "puree", "sauce", "jam", "paste",
    "dried", "fresh", "tart", "sweet",
    "cider", "brandy", "liqueur",
    "peak", "season", "fruit"
}

def extract_core(tokens):
    # remove modifiers
    core = [t for t in tokens if t not in MODIFIERS]

    if not core:
        return tokens  # fallback

    return core

def auto_map(raw, canonical_groups):
    raw_tokens = tokenize(raw)
    raw_core = extract_core(raw_tokens)

    best_match = None
    best_score = 0

    for group in canonical_groups:
        group_tokens = tokenize(group)

        overlap = len(set(raw_core) & set(group_tokens))

        if overlap > best_score:
            best_score = overlap
            best_match = group

    if best_score >= 1:
        return best_match

    return None


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # get all canonical groups
    cur.execute("SELECT DISTINCT canonical_group FROM ingredients")
    canonical_groups = [row[0] for row in cur.fetchall() if row[0]]

    # get all unique raw terms
    cur.execute("""
        SELECT DISTINCT source_text FROM flavor_bible_raw
        UNION
        SELECT DISTINCT target_text FROM flavor_bible_raw
    """)
    raw_terms = [row[0] for row in cur.fetchall()]

    mapped = 0

    for raw in raw_terms:
        clean = normalize_raw(raw)

        if not clean:
            continue

        parts = split_compound(clean)

        mapped_group = None

        for part in parts:
            mapped_group = auto_map(part, canonical_groups)
            if mapped_group:
                break 

        cur.execute("""
            INSERT OR IGNORE INTO ingredient_aliases (raw_text, canonical_group, confidence, source)
            VALUES (?, ?, ?, ?)
        """, (
            clean,
            mapped_group,
            1 if mapped_group else 0,
            "auto"
        ))

        if mapped_group:
            mapped += 1

    conn.commit()
    conn.close()

    print(f"Mapped {mapped}/{len(raw_terms)} terms")


if __name__ == "__main__":
    main()