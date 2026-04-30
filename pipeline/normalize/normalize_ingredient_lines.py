# pipeline/normalize/normalize_ingredient_lines.py
#
# Pipeline stage: normalize
#
#   Reads  : recipe_ingredient_lines_parsed
#   Writes : ingredient_normalizations
#
# This stage produces a clean "core ingredient" name suitable for a
# future ingredient_id lookup.  It runs two ordered passes on
# ingredient_name_raw:
#
#   Pass 1 — TYPO FIXES (from config.ingredient_vocabulary.TYPO_FIXES)
#     Correct spelling variants, regional synonyms, and brand names.
#     "scallions" → "green onion",  "calamari" → "squid"
#     These unify surface forms without changing ingredient identity.
#
#   Pass 2 — QUALIFIER STRIPPING (config.ingredient_vocabulary.QUALIFIER_STRIP_PATTERNS)
#     Remove words that describe HOW the ingredient was prepared,
#     its freshness, size, cut style, or diet classification.
#     "boneless skinless chicken breast" → "chicken breast"
#     "slivered almonds"                 → "almonds"
#     "extra-virgin olive oil"           → "olive oil"
#
# What this stage does NOT do:
#   • Semantic grouping  ("olive oil" → "oil" is canonicalization, done downstream)
#   • ingredient_id assignment  (no ingredient_id file available yet)
#
# ingredient_normalizations is a dedicated normalization log.  It holds
# only the FK to the parsed row and the name transformation — it does not
# duplicate measurement columns.  Join back to recipe_ingredient_lines_parsed
# on parsed_line_id to get quantity/unit/prep/etc.
#
# Re-running is safe: the existing row for a given parsed_line_id is
# deleted before re-insertion.

import sqlite3
import re
from collections import defaultdict

from gastrometric.config.paths import DB_PATH
from gastrometric.config.ingredient_vocabulary import TYPO_FIXES, QUALIFIER_STRIP_PATTERNS


# ============================================================
# PASS 1 — TYPO FIXES
# ============================================================

def _apply_typo_fixes(text):
    """
    Apply spelling / synonym corrections to lowercased ingredient_name_raw.
    Returns the corrected string (still lowercased).
    """
    for pattern, replacement in TYPO_FIXES:
        text = pattern.sub(replacement, text)
    return text


# ============================================================
# PASS 2 — QUALIFIER STRIPPING
# ============================================================

def _strip_qualifiers(text):
    """
    Remove qualifier words to expose the core ingredient.
    Applied in order; whitespace is collapsed after each removal.

    Special case handled here (not in the pattern list because it
    needs context): "juice of/from <ingredient>" → "<ingredient> juice"
    e.g. "juice of lemon" → "lemon juice"
    """
    # Reorder "juice of X" → "X juice" before stripping anything else
    m = re.match(r'^juice\s+(?:of|from)\s+(.+)$', text, re.IGNORECASE)
    if m:
        text = m.group(1).strip() + " juice"

    for pattern in QUALIFIER_STRIP_PATTERNS:
        text = pattern.sub('', text)
        text = " ".join(text.split())   # collapse whitespace after each removal

    # Drop any dangling connectors left by stripping
    text = re.sub(r'^(?:and|or|of|with|the)\b\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+(?:and|or|of|with)$', '', text, flags=re.IGNORECASE)
    text = " ".join(text.split()).strip()

    return text


# ============================================================
# FULL NORMALIZATION PIPELINE
# ============================================================

def normalize_ingredient_name(ingredient_name_raw):
    """
    Run the full two-pass normalization on a raw ingredient name.
    Returns (normalized_name, str: 'ok' | 'empty' | 'reduced_to_nothing').
    normalized_name is None on failure.
    """
    if not ingredient_name_raw or not ingredient_name_raw.strip():
        return None, "empty"

    text = ingredient_name_raw.lower().strip()

    fixed = _apply_typo_fixes(text)
    core  = _strip_qualifiers(fixed)

    if not core or len(core) < 2:
        return None, "reduced_to_nothing"

    return core.strip(), "ok"


# ============================================================
# DB SCHEMA
#
# ingredient_normalizations is a normalization log, not a row copy.
# It records:
#   - which parsed line was normalized (FK)
#   - the recipe + raw text for human-readable audit
#   - the before/after name transformation
#   - the outcome status
#
# Join to recipe_ingredient_lines_parsed on parsed_line_id to get
# quantity, unit, prep, and all other parsed dimensions.
# ============================================================

def _ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingredient_normalizations (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            -- lineage
            parsed_line_id      INTEGER NOT NULL UNIQUE
                                REFERENCES recipe_ingredient_lines_parsed(id),
            -- denormalized for readable audit queries without joins
            recipe_id           INTEGER NOT NULL,
            recipe_name         TEXT,
            raw_text            TEXT,
            -- name transformation
            ingredient_name_raw TEXT,       -- as arrived from parse stage
            ingredient_name     TEXT,       -- core ingredient after both passes
            -- outcome
            status              TEXT NOT NULL,  -- 'ok' | 'empty' | 'reduced_to_nothing'
            -- audit
            normalized_at       TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ingn_parsed_line_id
        ON ingredient_normalizations (parsed_line_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ingn_recipe_id
        ON ingredient_normalizations (recipe_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ingn_ingredient_name
        ON ingredient_normalizations (ingredient_name)
    """)
    conn.commit()


# ============================================================
# DB EXECUTION
# ============================================================

def normalize_ingredient_lines():
    conn = sqlite3.connect(DB_PATH)
    try:
        _normalize(conn)
    finally:
        conn.close()


def _normalize(conn):
    _ensure_schema(conn)
    c = conn.cursor()

    c.execute("""
        SELECT id, recipe_id, recipe_name, raw_text, ingredient_name_raw
        FROM   recipe_ingredient_lines_parsed
        WHERE  ingredient_name_raw IS NOT NULL
        ORDER  BY id
    """)
    parsed_rows = c.fetchall()

    stats = defaultdict(int)
    review = []

    for parsed_id, recipe_id, recipe_name, raw_text, ingredient_name_raw in parsed_rows:
        # Idempotent: replace existing normalization for this parsed line
        c.execute(
            "DELETE FROM ingredient_normalizations WHERE parsed_line_id = ?",
            (parsed_id,)
        )

        ingredient_name, status = normalize_ingredient_name(ingredient_name_raw)

        stats["total"] += 1
        stats[status]  += 1

        if status != "ok":
            review.append((parsed_id, recipe_name, raw_text, ingredient_name_raw, status))

        c.execute("""
            INSERT INTO ingredient_normalizations
                (parsed_line_id, recipe_id, recipe_name, raw_text,
                 ingredient_name_raw, ingredient_name, status)
            VALUES (?,?,?,?,?,?,?)
        """, (
            parsed_id, recipe_id, recipe_name, raw_text,
            ingredient_name_raw, ingredient_name, status,
        ))

    conn.commit()

    print("\n=== normalize_ingredient_lines REPORT ===")
    print("Total             : %d" % stats["total"])
    print("ok                : %d" % stats["ok"])
    print("empty             : %d" % stats["empty"])
    print("reduced_to_nothing: %d" % stats["reduced_to_nothing"])

    if review:
        print("\n--- Rows needing review (up to 30) ---")
        print("  %-6s  %-30s  %-40s  %-25s  %s"
              % ("id", "recipe", "raw_text", "name_raw", "status"))
        for parsed_id, rname, rtxt, nraw, st in review[:30]:
            print("  %-6d  %-30s  %-40s  %-25s  %s" % (
                parsed_id,
                (rname or "")[:30],
                (rtxt  or "")[:40],
                (nraw  or "")[:25],
                st,
            ))

    print("\nnormalize_ingredient_lines: done → ingredient_normalizations")


if __name__ == "__main__":
    normalize_ingredient_lines()