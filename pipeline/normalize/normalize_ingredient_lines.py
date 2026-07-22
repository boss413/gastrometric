"""
normalize_ingredient_lines.py — pipeline stage: parse -> [normalize] -> entity resolution

Reads recipe_ingredient_lines_parsed, resolves each ingredient_name_raw against
the curated ingredients/ingredient_aliases tables, and writes:

  - ingredient_normalizations : audit log of the NAME transformation pass only
                                 (status = 'ok' | 'empty' | 'reduced_to_nothing')
  - recipe_ingredients        : one row per parsed line, ingredient_id NULL if
                                 unresolved (left for later triage)
  - instance_attribute_value  : attribute values recognized in the parsed
                                 preparation list / notes / stripped name
                                 qualifiers, scoped against each ingredient's
                                 own identity_attribute_rule set

Schema ownership: this module only INSERTs into tables gastrometric/db/init_db.py
already created. No CREATE TABLE / ALTER TABLE here.

Idempotent: re-running skips any recipe_ingredient_lines_parsed row that
already has a corresponding ingredient_normalizations row (parsed_line_id is
UNIQUE), so partial/failed runs can simply be re-invoked.

DESIGN NOTES (decided with the project owner; don't relitigate without cause)
-------------------------------------------------------------------------
Name matching (2-pass, matches ingredient_vocabulary.py's own PASS 1 / PASS 2
labeling, which documents itself as being for this stage):
  1. Try resolve_alias() on the raw name as-is.
  2. Apply TYPO_FIXES (spelling/synonym/brand normalization that preserves
     identity), retry resolve_alias().
  3. Apply QUALIFIER_STRIP_PATTERNS (strips decorative/attribute-bearing
     qualifiers down to the core ingredient), retry resolve_alias().
  4. Still no match -> recipe_ingredients gets ingredient_id = NULL for triage.
  ingredient_normalizations.status describes whether this pass produced a
  usable *name*, independent of whether an identity was found:
    'empty'            -> ingredient_name_raw was blank to begin with
    'reduced_to_nothing' -> qualifier stripping consumed the entire name
    'ok'                 -> a usable core name resulted (matched or not)

Attribute extraction, once an ingredient_id IS resolved:
  Text sources: the parsed `preparation` list, the parsed `notes` string, and
  any qualifier phrases OUR OWN Pass-2 stripping removed from the name (e.g.
  parse stage left "skin-on bone-in chicken thighs" untouched in
  ingredient_name_raw; Pass 2 strips "skin-on"/"bone-in" to resolve the
  identity, and those phrases still carry real attribute info, so they're
  fed into extraction rather than discarded).

  Matching is RULE-SCOPED FIRST: only attribute types this ingredient
  actually has an identity_attribute_rule for are checked (chicken breast:
  bone/skin/state/size/preparation; kosher salt: brand). This avoids
  spurious cross-ingredient matches and mirrors what identity_attribute_rule
  is for. Text is matched to attribute_value.value via a word-boundary
  phrase match after normalizing hyphens/underscores/whitespace to single
  spaces on both sides (so "skin-on" text matches a "skin_on" value).

  If no rule-scoped match is found for a rule that has a default_value_id,
  write that default with source='defaulted' (values like chicken breast's
  state defaulting to raw when nothing says otherwise).

  If a rule-scoped attribute type has no match and no default: nothing is
  written for it. This is normal and expected -- most preparation phrases
  ("cut into 0.5-inch pieces") aren't meant to map to anything.

  GLOBAL FALLBACK, gap detection only: after rule-scoped matching, text is
  additionally checked against the full global attribute_value vocabulary.
  If it matches a value whose attribute_type has NO identity_attribute_rule
  at all for this ingredient, that's flagged via logging.warning() as a
  likely curation gap (the value IS still written, since the information is
  real -- just noted for someone to add the missing rule). This fallback is
  deliberately NOT triggered by ordinary unmatched free text -- only by text
  that matches *some* curated attribute value somewhere, just not one this
  ingredient has a rule for.
"""

import json
import logging
import re
import sqlite3

from gastrometric.config.paths import DB_PATH
from gastrometric.config.ingredient_vocabulary import (
    TYPO_FIXES,
    QUALIFIER_STRIP_PATTERNS,
)
from gastrometric.pipeline.normalize.ingredient_identity import resolve_alias

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("normalize_ingredient_lines")


# ============================================================
# text helpers
# ============================================================

_WS_RE = re.compile(r"\s+")


def _collapse_ws(text):
    return _WS_RE.sub(" ", text).strip()


def normalize_phrase(text):
    """Lowercase, fold hyphens/underscores to spaces, collapse whitespace.
    Used on BOTH sides of an attribute-value match (curated values use
    underscores, e.g. 'skin_on'; parsed text uses hyphens, e.g. 'skin-on')."""
    if not text:
        return ""
    text = text.lower().replace("-", " ").replace("_", " ")
    return _collapse_ws(text)


def _phrase_in_corpus(phrase, normalized_corpus):
    if not phrase or not normalized_corpus:
        return False
    return re.search(r"\b" + re.escape(phrase) + r"\b", normalized_corpus) is not None


def _clean_optional_text(value):
    """NULL/blank-safe strip for notes and other free-text columns."""
    if not value or not value.strip():
        return ""
    return value.strip()


def _parse_preparation(raw_prep_json):
    """preparation is json.dumps([...]) or NULL. Never crash the pipeline
    on malformed JSON -- treat it as 'no preparation phrases' and move on."""
    if not raw_prep_json or not raw_prep_json.strip():
        return []
    try:
        parsed = json.loads(raw_prep_json)
    except (ValueError, TypeError):
        log.warning("could not parse preparation JSON: %r", raw_prep_json)
        return []
    if not isinstance(parsed, list):
        return []
    return [str(p) for p in parsed if p]


# ============================================================
# PASS 1 / PASS 2 name resolution
# ============================================================

def _apply_typo_fixes(text):
    for pattern, replacement in TYPO_FIXES:
        text = pattern.sub(replacement, text)
    return _collapse_ws(text)


def _apply_qualifier_strip(text):
    """Returns (stripped_text, removed_phrases). removed_phrases preserves
    the original matched surface form (e.g. 'skin-on', not 'skin_on') so it
    can be fed into attribute-phrase matching alongside preparation/notes."""
    removed = []
    for pattern in QUALIFIER_STRIP_PATTERNS:
        found = pattern.findall(text)
        if found:
            removed.extend(found)
        text = pattern.sub(" ", text)
    return _collapse_ws(text), removed


def normalize_and_resolve_name(conn, name_raw):
    """Returns (ingredient_id_or_None, final_name, status, stripped_qualifiers).

    status: 'empty' | 'reduced_to_nothing' | 'ok'
    final_name: the name text ingredient_normalizations.ingredient_name gets;
                may be non-empty even when ingredient_id is None (unmatched,
                but a plausible core name was produced -- fine for triage).
    stripped_qualifiers: qualifier phrases Pass 2 removed, for attribute
                         extraction by the caller.
    """
    if not name_raw or not name_raw.strip():
        return None, "", "empty", []

    lowered = _collapse_ws(name_raw.strip().lower())

    ingredient_id = resolve_alias(conn, lowered)
    if ingredient_id:
        return ingredient_id, lowered, "ok", []

    typo_fixed = _apply_typo_fixes(lowered)
    if typo_fixed != lowered:
        ingredient_id = resolve_alias(conn, typo_fixed)
        if ingredient_id:
            return ingredient_id, typo_fixed, "ok", []

    stripped, removed_qualifiers = _apply_qualifier_strip(typo_fixed)
    if not stripped:
        return None, "", "reduced_to_nothing", removed_qualifiers

    if stripped != typo_fixed:
        ingredient_id = resolve_alias(conn, stripped)
        if ingredient_id:
            return ingredient_id, stripped, "ok", removed_qualifiers

    # Unmatched, but we have a usable core name for triage.
    return None, stripped, "ok", removed_qualifiers


# ============================================================
# attribute extraction
# ============================================================

def build_global_attribute_lookup(conn):
    """normalized phrase -> [(attribute_type_id, value_id), ...], across the
    ENTIRE curated attribute vocabulary. Used only for gap detection."""
    lookup = {}
    rows = conn.execute(
        "SELECT id, attribute_type_id, value FROM attribute_value"
    ).fetchall()
    for value_id, attribute_type_id, value in rows:
        phrase = normalize_phrase(value)
        if not phrase:
            continue
        lookup.setdefault(phrase, []).append((attribute_type_id, value_id))
    return lookup


def get_rule_scope(conn, ingredient_id, cache):
    """Returns (phrase_map, meta) for one ingredient, cached across the run.

    phrase_map: {attribute_type_id: {normalized_phrase: value_id}}
                (respects identity_attribute_allowed_value restriction when
                present, else falls back to the full global value list for
                that attribute type)
    meta:       {attribute_type_id: {'rule_id':, 'default_value_id':}}
                present for EVERY attribute_type this ingredient has a rule
                for, even if phrase_map ends up empty for it.
    """
    if ingredient_id in cache:
        return cache[ingredient_id]

    phrase_map = {}
    meta = {}
    rules = conn.execute(
        "SELECT id, attribute_type_id, default_value_id "
        "FROM identity_attribute_rule WHERE ingredient_id = ?",
        (ingredient_id,),
    ).fetchall()

    for rule_id, attribute_type_id, default_value_id in rules:
        meta[attribute_type_id] = {
            "rule_id": rule_id,
            "default_value_id": default_value_id,
        }
        allowed = conn.execute(
            "SELECT av.id, av.value FROM identity_attribute_allowed_value iav "
            "JOIN attribute_value av ON av.id = iav.value_id "
            "WHERE iav.identity_attribute_rule_id = ?",
            (rule_id,),
        ).fetchall()
        if not allowed:
            allowed = conn.execute(
                "SELECT id, value FROM attribute_value WHERE attribute_type_id = ?",
                (attribute_type_id,),
            ).fetchall()
        phrase_dict = {}
        for value_id, value in allowed:
            phrase = normalize_phrase(value)
            if phrase:
                phrase_dict[phrase] = value_id
        phrase_map[attribute_type_id] = phrase_dict

    result = (phrase_map, meta)
    cache[ingredient_id] = result
    return result


def _insert_instance_attribute(conn, recipe_ingredient_id, attribute_type_id,
                                value_id, source):
    conn.execute(
        "INSERT OR IGNORE INTO instance_attribute_value "
        "(recipe_ingredient_id, attribute_type_id, value_id, source) "
        "VALUES (?, ?, ?, ?)",
        (recipe_ingredient_id, attribute_type_id, value_id, source),
    )


def extract_and_write_attributes(conn, recipe_ingredient_id, ingredient_id,
                                  corpus_phrases, rule_cache, global_lookup):
    """corpus_phrases: raw (pre-normalization) phrases from preparation list
    + notes + Pass-2 stripped qualifiers. Writes instance_attribute_value
    rows directly; returns nothing."""
    normalized_corpus = " | ".join(
        normalize_phrase(p) for p in corpus_phrases if p and p.strip()
    )
    phrase_map, meta = get_rule_scope(conn, ingredient_id, rule_cache)

    matched_types = set()

    # Rule-scoped pass.
    for attribute_type_id, phrase_dict in phrase_map.items():
        found_value_id = None
        for phrase, value_id in phrase_dict.items():
            if _phrase_in_corpus(phrase, normalized_corpus):
                found_value_id = value_id
                break
        if found_value_id is not None:
            _insert_instance_attribute(
                conn, recipe_ingredient_id, attribute_type_id, found_value_id, "parsed"
            )
            matched_types.add(attribute_type_id)
        else:
            default_value_id = meta[attribute_type_id]["default_value_id"]
            if default_value_id is not None:
                _insert_instance_attribute(
                    conn, recipe_ingredient_id, attribute_type_id,
                    default_value_id, "defaulted",
                )
                matched_types.add(attribute_type_id)

    if not normalized_corpus:
        return

    # Global fallback: gap detection only. Only writes when the matched
    # attribute_type has NO rule for this ingredient at all. No per-row
    # logging here -- these rows are still written to instance_attribute_value
    # (source='parsed'), so the full list is queryable after the fact via
    # GAP_QUERY below instead of being blasted to the console mid-run.
    for phrase, candidates in global_lookup.items():
        if not _phrase_in_corpus(phrase, normalized_corpus):
            continue
        for attribute_type_id, value_id in candidates:
            if attribute_type_id in matched_types or attribute_type_id in meta:
                continue
            _insert_instance_attribute(
                conn, recipe_ingredient_id, attribute_type_id, value_id, "parsed"
            )
            matched_types.add(attribute_type_id)


# ============================================================
# main pass
# ============================================================

# Retroactive gap query -- run this any time in datasette/sqlite3 to see
# the FULL list of attribute values that were recorded via the global
# fallback (i.e. matched a curated attribute_value, but the ingredient in
# question has no identity_attribute_rule for that attribute_type). This
# is the debugging view; the console print at the end of a run is a
# summary only.
GAP_QUERY = """
    SELECT ing.ingredient_name, at.name AS attribute_type, av.value,
           ri.raw_text, ri.id AS recipe_ingredient_id
    FROM instance_attribute_value iav
    JOIN recipe_ingredients ri ON ri.id = iav.recipe_ingredient_id
    JOIN ingredients ing       ON ing.id = ri.ingredient_id
    JOIN attribute_type at     ON at.id = iav.attribute_type_id
    JOIN attribute_value av    ON av.id = iav.value_id
    LEFT JOIN identity_attribute_rule iar
           ON iar.ingredient_id = ri.ingredient_id
          AND iar.attribute_type_id = iav.attribute_type_id
    WHERE iar.id IS NULL AND iav.source = 'parsed'
    ORDER BY at.name, ing.ingredient_name
"""


def _print_gap_summary(conn, example_limit=8):
    rows = conn.execute(GAP_QUERY).fetchall()
    if not rows:
        return
    print(
        "normalize_ingredient_lines: %d attribute value(s) matched via the "
        "global fallback with no identity_attribute_rule for that "
        "ingredient -- likely curation gaps. First %d:"
        % (len(rows), min(example_limit, len(rows)))
    )
    for ingredient_name, attribute_type, value, raw_text, _ in rows[:example_limit]:
        print(f"    {ingredient_name!r}: {attribute_type}={value}  ({raw_text!r})")
    if len(rows) > example_limit:
        print(
            "    ... %d more. Full list: run GAP_QUERY from "
            "normalize_ingredient_lines.py, or in datasette/sqlite3:\n"
            "%s" % (len(rows) - example_limit, GAP_QUERY)
        )


def get_unprocessed_lines(conn):
    return conn.execute(
        """
        SELECT p.id, p.recipe_id, p.recipe_section_id, p.section_name,
               p.line_index, p.raw_text, p.preparation, p.ingredient_name_raw,
               p.notes, p.recipe_name
        FROM recipe_ingredient_lines_parsed p
        LEFT JOIN ingredient_normalizations n ON n.parsed_line_id = p.id
        WHERE n.id IS NULL
        ORDER BY p.id
        """
    ).fetchall()


def _canonical_ingredient_name(conn, ingredient_id):
    row = conn.execute(
        "SELECT ingredient_name FROM ingredients WHERE id = ?", (ingredient_id,)
    ).fetchone()
    return row[0] if row else None


def normalize_ingredient_lines(db_path=None):
    db_path = db_path or DB_PATH
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    rows = get_unprocessed_lines(conn)
    if not rows:
        print("normalize_ingredient_lines: nothing new to process")
        conn.close()
        return

    global_lookup = build_global_attribute_lookup(conn)
    rule_cache = {}

    ok_count = 0
    empty_count = 0
    reduced_count = 0
    unmatched_count = 0

    for (parsed_line_id, recipe_id, recipe_section_id, section_name,
         line_index, raw_text, prep_json, name_raw, notes_raw,
         recipe_name) in rows:

        ingredient_id, final_name, status, stripped_qualifiers = (
            normalize_and_resolve_name(conn, name_raw)
        )

        conn.execute(
            """
            INSERT INTO ingredient_normalizations
                (parsed_line_id, recipe_id, recipe_name, raw_text,
                 ingredient_name_raw, ingredient_name, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (parsed_line_id, recipe_id, recipe_name, raw_text,
             name_raw, final_name, status),
        )

        if status == "empty":
            empty_count += 1
        elif status == "reduced_to_nothing":
            reduced_count += 1
        else:
            ok_count += 1
        if status == "ok" and ingredient_id is None:
            unmatched_count += 1

        display_name = (
            _canonical_ingredient_name(conn, ingredient_id)
            if ingredient_id is not None
            else final_name
        )

        cur = conn.execute(
            """
            INSERT INTO recipe_ingredients
                (parsed_line_id, ingredient_name, preparation, recipe_id,
                 recipe_section_id, line_index, raw_text, section_name,
                 ingredient_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (parsed_line_id, display_name, prep_json, recipe_id, recipe_section_id,
             line_index, raw_text, section_name, ingredient_id),
        )
        recipe_ingredient_id = cur.lastrowid

        if ingredient_id is not None:
            corpus_phrases = list(_parse_preparation(prep_json))
            corpus_phrases.extend(stripped_qualifiers)
            notes_text = _clean_optional_text(notes_raw)
            if notes_text:
                corpus_phrases.append(notes_text)
            extract_and_write_attributes(
                conn, recipe_ingredient_id, ingredient_id,
                corpus_phrases, rule_cache, global_lookup,
            )

    conn.commit()

    print(
        "normalize_ingredient_lines: %d rows processed "
        "(%d ok / %d empty / %d reduced_to_nothing, %d unmatched for triage)"
        % (len(rows), ok_count, empty_count, reduced_count, unmatched_count)
    )
    _print_gap_summary(conn)

    conn.close()


if __name__ == "__main__":
    normalize_ingredient_lines()