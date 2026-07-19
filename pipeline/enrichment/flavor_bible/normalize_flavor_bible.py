"""
pipeline/enrichment/flavor_bible/normalize_flavor_bible.py

Reads flavor_bible_curated (source, target_cleaned, score, …) and writes
flavor_bible_normalized with human-readable singular ingredient names.

Normalization pipeline (in order)
-----------------------------------
  1. Unicode → ASCII
  2. Reject non-ingredients (cuisines, techniques, "X with Y" pairings)
  3. Strip "esp. …" annotations
  4. Resolve comma phrases  ("vinegar, balsamic" → "balsamic vinegar",
                             "mushrooms, wild"   → "wild mushrooms",
                             "onions, sweet"     → "sweet onions")
  5. Check PROTECTED_PREP_PHRASES — if matched, return as-is
     ("sour cream", "sweet potato", "ground ginger" are untouched)
  6. Strip STATE_ONLY_PREP_WORDS  ("fresh", "raw", "frozen", "canned")
     — with DRIED_DISTINCT exception: "dried apricot" stays intact
  7. Apply PREP_INFLECTIONS       ("juiced" → "juice")
  8. Singularize                  ("apricots" → "apricot")

What this step deliberately does NOT do
-----------------------------------------
  - Strip "sour", "sweet", "wild", "aged" — these are variety/taxonomy
    qualifiers or parts of proper names; the graph resolves them
  - Collapse parts to parents     ("lime juice" stays "lime juice")
  - Resolve technique affinity    ("roasted", "smoked" stay in name)
  - Decide "coriander seeds" == "coriander"
  - Collapse "pickled X" to "X"

Output columns
--------------
  ingredient    TEXT  — source ingredient
  pairing       TEXT  — normalized target ingredient name
  score         INT   — 1–4 pairing strength
  key_ingredient TEXT
  seasonality   TEXT
  accompaniment TEXT
  preparation   TEXT  — state words stripped from the name
"""

import unicodedata
import sqlite3
from collections import defaultdict

from gastrometric.config.paths import DB_PATH
from gastrometric.config.ingredient_vocabulary import (
    NON_INGREDIENT_HINTS,
    LEADING_CATEGORY_REVERSALS,
    PLURAL_CATEGORIES,
    COLOR_QUALIFIERS,
    VARIETY_QUALIFIERS,
    COMMA_RIGHT_QUALIFIERS,
    PROTECTED_PREP_PHRASES,
    DRIED_DISTINCT,
    STATE_ONLY_PREP_WORDS,
    PREP_INFLECTIONS,
    PLURAL_IRREGULAR,
    PLURAL_SUFFIX_RULES,
    PLURAL_EXCEPTIONS,
)

# Sorted longest-first: "ground black pepper" must shadow "ground pepper"
_PROTECTED = sorted(PROTECTED_PREP_PHRASES, key=len, reverse=True)


# ---------------------------------------------------------------------------
# Skip-reason labels
# ---------------------------------------------------------------------------

SKIP_WITH     = "compound_pairing (with)"
SKIP_HINT     = "non_ingredient_hint"
SKIP_LOCATION = "location_pattern"
SKIP_EMPTY    = "empty_after_normalization"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def load_flavor_bible_normalized():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    inserted = 0
    raw_targets_seen        = set()
    normalized_targets_seen = set()
    skip_reasons            = defaultdict(list)
    emptied_rows            = []

    source_rows = cur.execute("""
        SELECT source, target_cleaned, score,
               key_ingredient, seasonality, accompaniment
        FROM flavor_bible_curated
    """).fetchall()

    for (source, raw_target, score,
         key_ingredient, seasonality, accompaniment) in source_rows:

        source     = (source     or "").strip().lower()
        raw_target = (raw_target or "").strip().lower()

        if not raw_target:
            continue

        # "/" means synonym/alternate name — keep only the first (primary) form.
        # "coffee / espresso" → "coffee", "melon / musk melon" → "melon"
        source     = _resolve_slash(source)
        raw_target = _resolve_slash(raw_target)

        # Apply the same comma normalization to source that we apply to pairings.
        source = _normalize_unicode(source)
        source = _strip_esp(source)
        source = _resolve_comma_phrase(source)
        source = " ".join(source.split())

        if not _is_ingredient(source):
            skip_reasons[_skip_reason(source)].append(source)
            continue

        raw_targets_seen.add(raw_target)

        result = normalize_target(raw_target)

        if result is None:
            reason = _skip_reason(raw_target)
            skip_reasons[reason].append(raw_target)
            continue

        pairing, preparation = result

        if not pairing:
            emptied_rows.append(raw_target)
            skip_reasons[SKIP_EMPTY].append(raw_target)
            continue

        normalized_targets_seen.add(pairing)

        cur.execute("""
            INSERT INTO flavor_bible_normalized (
                ingredient,
                pairing,
                score,
                key_ingredient,
                seasonality,
                accompaniment,
                preparation
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            source,
            pairing,
            score,
            key_ingredient,
            seasonality,
            accompaniment,
            preparation or None,
        ))

        inserted += 1

    conn.commit()
    conn.close()

    _print_report(
        inserted=inserted,
        raw_count=len(raw_targets_seen),
        normalized_count=len(normalized_targets_seen),
        skip_reasons=skip_reasons,
        emptied_rows=emptied_rows,
    )


# ---------------------------------------------------------------------------
# normalize_target — public, also used by tests
# ---------------------------------------------------------------------------

def normalize_target(raw: str):
    """
    Returns (pairing_name, preparation) or None.

    None    → skip entirely (non-ingredient row).
    ("", …) → ingredient that became empty — caller logs as a vocabulary gap.
    """
    t = raw.strip().lower()
    t = _normalize_unicode(t)

    if not _is_ingredient(t):
        return None

    t = _strip_esp(t)
    t = _resolve_comma_phrase(t)

    # Protection check after comma resolution: "ginger, ground" → "ground ginger"
    # must be resolved before we can match against PROTECTED_PREP_PHRASES.
    if _is_protected(t):
        return t, ""

    t, preparation = _strip_state_words(t)
    t = _apply_inflections(t)
    t = " ".join(t.split())

    return t, preparation


# ---------------------------------------------------------------------------
# Rejection helpers
# ---------------------------------------------------------------------------

def _is_ingredient(text: str) -> bool:
    if " with " in text:
        return False
    if any(hint in text for hint in NON_INGREDIENT_HINTS):
        return False
    if "(" in text and ")" in text and "," in text:
        return False
    return True


def _skip_reason(text: str) -> str:
    if " with " in text:
        return SKIP_WITH
    if any(hint in text for hint in NON_INGREDIENT_HINTS):
        return SKIP_HINT
    if "(" in text and ")" in text and "," in text:
        return SKIP_LOCATION
    return SKIP_EMPTY


# ---------------------------------------------------------------------------
# Normalization steps
# ---------------------------------------------------------------------------

def _normalize_unicode(text: str) -> str:
    return (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )


def _strip_esp(text: str) -> str:
    if "esp." in text:
        return text.split("esp.")[0].strip()
    return text


def _resolve_slash(text: str) -> str:
    """
    "/" marks synonym or alternate regional name — take the first (primary) form.
    "coffee / espresso"   → "coffee"
    "chocolate / cocoa"   → "chocolate"
    "melon / musk melon"  → "melon"
    """
    if "/" in text:
        return text.split("/")[0].strip()
    return text


def _is_protected(text: str) -> bool:
    return any(
        text == phrase or text.startswith(phrase + " ")
        for phrase in _PROTECTED
    )


def _resolve_comma_phrase(text: str) -> str:
    """
    Resolve "category, qualifier" comma patterns into natural English order.

    Rules (applied in order for each two-part phrase):
      A. Right is in COMMA_RIGHT_QUALIFIERS (colors + variety words)
             "pepper, black"    → "black pepper"
             "mushrooms, wild"  → "wild mushrooms"
             "onions, sweet"    → "sweet onions"
      B. Left is in LEADING_CATEGORY_REVERSALS
             "vinegar, balsamic" → "balsamic vinegar"
      C. Left is in PLURAL_CATEGORIES
             "berries, strawberry" → "strawberry"
      D. Fallback: space-join
             "lime, juice" → "lime juice"

    Three-or-more parts fold left-to-right:
      "spices, coriander, seeds" → "coriander seeds"
    """
    if "," not in text:
        return text

    parts = [p.strip() for p in text.split(",")]

    if len(parts) > 2:
        left_resolved = _resolve_comma_phrase(f"{parts[0]}, {parts[1]}")
        remainder = ", ".join([left_resolved] + parts[2:])
        return _resolve_comma_phrase(remainder)

    left, right = parts

    if right in COMMA_RIGHT_QUALIFIERS:
        return f"{right} {left}"
    if left in LEADING_CATEGORY_REVERSALS:
        return f"{right} {left}"
    if left in PLURAL_CATEGORIES:
        return right

    return f"{left} {right}"


def _strip_state_words(text: str):
    """
    Strip STATE_ONLY_PREP_WORDS, recording them in preparation.

    Special case for "dried": keeps it when the base ingredient is in
    DRIED_DISTINCT (singularized for the lookup).

    Returns (cleaned_name, preparation_str).
    """
    tokens = text.split()
    name_tokens = []
    prep_tokens = []

    if "dried" in tokens:
        base_tokens  = [t for t in tokens if t != "dried"]
        base_singular = [_singularize_token(t) for t in base_tokens]
        if any(t in DRIED_DISTINCT for t in base_singular):
            # "dried" is identity-bearing — keep in name
            name_tokens = ["dried"] + [
                t for t in base_tokens
                if t not in STATE_ONLY_PREP_WORDS
            ]
            prep_tokens = [t for t in base_tokens if t in STATE_ONLY_PREP_WORDS]
        else:
            prep_tokens.append("dried")
            tokens = base_tokens
            for tok in tokens:
                if tok in STATE_ONLY_PREP_WORDS:
                    prep_tokens.append(tok)
                else:
                    name_tokens.append(tok)
    else:
        for tok in tokens:
            if tok in STATE_ONLY_PREP_WORDS:
                prep_tokens.append(tok)
            else:
                name_tokens.append(tok)

    return (
        " ".join(name_tokens),
        " ".join(prep_tokens) if prep_tokens else "",
    )


def _apply_inflections(text: str) -> str:
    tokens = text.split()
    return " ".join(PREP_INFLECTIONS.get(tok, tok) for tok in tokens)


def _singularize(text: str) -> str:
    """
    Singularize each token in the name independently.
    Preserves protected multi-word names (already returned early above).
    """
    return " ".join(_singularize_token(tok) for tok in text.split())


def _singularize_token(word: str) -> str:
    """
    Singularize a single word.
    Order: exception list → irregular map → suffix rules → unchanged.
    """
    if word in PLURAL_EXCEPTIONS:
        return word
    if word in PLURAL_IRREGULAR:
        return PLURAL_IRREGULAR[word]
    for suffix, replacement in PLURAL_SUFFIX_RULES:
        if word.endswith(suffix):
            stem = word[: -len(suffix)] + replacement
            if len(stem) >= 3:
                return stem
    return word


# ---------------------------------------------------------------------------
# End-of-run report
# ---------------------------------------------------------------------------

def _print_report(
    inserted: int,
    raw_count: int,
    normalized_count: int,
    skip_reasons: dict,
    emptied_rows: list,
):
    total_skipped = sum(len(v) for v in skip_reasons.values())

    print("\n" + "=" * 60)
    print("  flavor_bible_normalized — load report")
    print("=" * 60)
    print(f"  Rows inserted             : {inserted}")
    print(f"  Rows skipped              : {total_skipped}")
    print(f"  Distinct raw targets      : {raw_count}")
    print(f"  Distinct normalized       : {normalized_count}")
    print(f"  Grouping ratio            : {raw_count} raw → {normalized_count} normalized"
          f"  ({raw_count - normalized_count:+d})")
    print()

    if skip_reasons:
        print("  Skip breakdown:")
        for reason, rows in sorted(skip_reasons.items(), key=lambda x: -len(x[1])):
            print(f"    {reason:<38} {len(rows):>5}")
        print()

    if emptied_rows:
        print(f"  WARNING: {len(emptied_rows)} row(s) reduced to empty string.")
        print("  Review and extend PROTECTED_PREP_PHRASES, LEADING_CATEGORY_REVERSALS,")
        print("  or PLURAL_CATEGORIES as needed:")
        for r in emptied_rows[:20]:
            print(f"    {r!r}")
        if len(emptied_rows) > 20:
            print(f"    … and {len(emptied_rows) - 20} more")
    else:
        print("  OK: No rows reduced to empty string.")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    load_flavor_bible_normalized()