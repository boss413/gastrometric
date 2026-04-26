# normalize_ingredient_lines.py
#
# Pipeline stage: normalize
#
#   Reads  : recipe_ingredient_lines_parsed
#   Writes : recipe_ingredient_lines_normalized
#
# This stage produces a clean "core ingredient" name suitable for a
# future ingredient_id lookup.  It runs two ordered passes on
# ingredient_name_raw:
#
#   Pass 1 — TYPO FIXES (from ingredient_vocabulary.TYPO_FIXES)
#     Correct spelling variants, regional synonyms, and brand names.
#     "scallions" → "green onion",  "calamari" → "squid"
#     These unify surface forms without changing ingredient identity.
#
#   Pass 2 — QUALIFIER STRIPPING (from ingredient_vocabulary.QUALIFIER_STRIP_PATTERNS)
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
# recipe_id and recipe_name are carried forward from the parsed table.
# All other parsed columns (quantity, unit, prep, etc.) are preserved verbatim.
#
# Re-running is safe: existing normalized rows for a given parsed_line_id
# are deleted before re-insertion.

import sqlite3
import os
import re
from collections import defaultdict

from ingredient_vocabulary import TYPO_FIXES, QUALIFIER_STRIP_PATTERNS

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")


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
    Returns (normalized_name, list_of_passes_applied).

    normalized_name is None if the input is blank or reduces to nothing.
    """
    if not ingredient_name_raw or not ingredient_name_raw.strip():
        return None, []

    text = ingredient_name_raw.lower().strip()

    # --- Pass 1: typo fixes ---
    fixed = _apply_typo_fixes(text)

    # --- Pass 2: qualifier stripping ---
    core = _strip_qualifiers(fixed)

    if not core or len(core) < 2:
        return None, ["typo_fix", "qualifier_strip"]

    # Title-case the final result for consistency with display
    core = core.strip()

    return core, ["typo_fix", "qualifier_strip"]


# ============================================================
# DB SCHEMA
# ============================================================

def _ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredient_lines_normalized (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            -- lineage
            parsed_line_id        INTEGER NOT NULL
                                  REFERENCES recipe_ingredient_lines_parsed(id),
            block_id              INTEGER,
            -- recipe identity (carried forward from parsed stage)
            recipe_id             INTEGER NOT NULL,
            recipe_name           TEXT,
            -- position
            line_index            INTEGER,
            section               TEXT,
            -- original text preserved for audit
            raw_text              TEXT,
            -- all parsed dimensions (carried forward verbatim)
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
            optional              INTEGER DEFAULT 0,
            -- normalization outputs
            ingredient_name_raw   TEXT,     -- as it arrived from parsed stage
            ingredient_name       TEXT,     -- core ingredient after both passes
            -- audit
            normalized_at         TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_riln_parsed_line_id
        ON recipe_ingredient_lines_normalized (parsed_line_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_riln_recipe_id
        ON recipe_ingredient_lines_normalized (recipe_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_riln_ingredient_name
        ON recipe_ingredient_lines_normalized (ingredient_name)
    """)
    conn.commit()


# ============================================================
# DB EXECUTION
# ============================================================

def _run(conn):
    _ensure_schema(conn)
    c = conn.cursor()

    c.execute("""
        SELECT
            id, block_id,
            recipe_id, recipe_name,
            line_index, section, raw_text,
            quantity_value, quantity_unit,
            imperial_weight_value, imperial_weight_unit,
            imperial_volume_value, imperial_volume_unit,
            grams, ml, scaling, preparation, optional,
            ingredient_name_raw
        FROM recipe_ingredient_lines_parsed
        WHERE ingredient_name_raw IS NOT NULL
        ORDER BY id
    """)
    parsed_rows = c.fetchall()

    stats = defaultdict(int)
    review = []   # rows that normalized to None — need attention

    for row in parsed_rows:
        (parsed_id, block_id,
         recipe_id, recipe_name,
         line_index, section, raw_text,
         qty_val, qty_unit,
         imp_wt_val, imp_wt_unit, imp_vol_val, imp_vol_unit,
         grams, ml, scaling, prep, optional,
         ingredient_name_raw) = row

        # Idempotent: delete existing normalized row for this parsed line
        c.execute(
            "DELETE FROM recipe_ingredient_lines_normalized WHERE parsed_line_id = ?",
            (parsed_id,)
        )

        ingredient_name, _ = normalize_ingredient_name(ingredient_name_raw)

        stats["total"] += 1
        if ingredient_name is None:
            stats["failed"] += 1
            review.append((parsed_id, recipe_name, raw_text, ingredient_name_raw))
        else:
            stats["ok"] += 1

        c.execute("""
            INSERT INTO recipe_ingredient_lines_normalized (
                parsed_line_id, block_id,
                recipe_id, recipe_name,
                line_index, section, raw_text,
                quantity_value, quantity_unit,
                imperial_weight_value, imperial_weight_unit,
                imperial_volume_value, imperial_volume_unit,
                grams, ml, scaling, preparation, optional,
                ingredient_name_raw, ingredient_name
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            parsed_id, block_id,
            recipe_id, recipe_name,
            line_index, section, raw_text,
            qty_val, qty_unit,
            imp_wt_val, imp_wt_unit, imp_vol_val, imp_vol_unit,
            grams, ml, scaling, prep, optional,
            ingredient_name_raw, ingredient_name,
        ))

    conn.commit()

    print("\n=== normalize_ingredient_lines REPORT ===")
    print("Total rows : %d" % stats["total"])
    print("Normalized : %d" % stats["ok"])
    print("Failed     : %d" % stats["failed"])

    if review:
        print("\n--- Rows needing review (up to 30) ---")
        print("  %-6s  %-30s  %-40s  %s" % ("id", "recipe", "raw_text", "name_raw"))
        for parsed_id, rname, rtxt, nraw in review[:30]:
            print("  %-6d  %-30s  %-40s  %r" % (
                parsed_id,
                (rname or "")[:30],
                (rtxt or "")[:40],
                nraw,
            ))

    print("\nnormalize_ingredient_lines: done → recipe_ingredient_lines_normalized")


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    try:
        _run(conn)
    finally:
        conn.close()