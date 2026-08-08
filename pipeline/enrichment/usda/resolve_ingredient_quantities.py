# pipeline/enrichment/usda/resolve_ingredient_quantities.py
#
# Pipeline stage: enrichment / usda / resolve
#
#   Reads  : recipe_ingredients
#            recipe_ingredient_lines_parsed
#            nutrition_ingredient_mappings
#            nutrition_mapping_portions   (+ usda_food_portions)
#   Writes : recipe_ingredient_line_nutrition
#
# This stage resolves each normalized recipe ingredient line to a gram
# quantity where possible, using the ingredient's approved nutrition
# mapping and, where a USDA nominal portion is required, the mapping's
# associated `nutrition_mapping_portions` rows (see
# gastrometric.pipeline.enrichment.usda.ingest_mappings).
#
# It does NOT calculate nutrients, aggregate sections, or aggregate
# recipes. Nutrient columns on the output table are always left NULL;
# a later stage is responsible for them.
#
# ------------------------------------------------------------------
# CONTRACT BOUNDARY
#
#   recipe ingredient line -> ingredient identity -> approved nutrition
#   mapping -> selected USDA food/state -> resolved grams
#
# This stage's output ends at `resolved_fdc_id` / `resolved_state` /
# `resolved_grams`. It never computes a nutrient value, never decides
# which nutrients matter, and never discards or transforms USDA
# nutrient data -- it doesn't look at USDA nutrient data at all. A
# planned downstream stage will add a normalized child table storing
# every available USDA nutrient keyed by nutrient_id; once that lands,
# the fixed nutrient columns below (calories, protein, ...) stop being
# the authoritative nutrition store. This stage doesn't need to change
# for that: it was already never writing to them (see `_persist` --
# they're absent from the INSERT column list, so SQLite leaves them at
# their real NULL default rather than this code zero-filling them).
# ------------------------------------------------------------------
#
# ------------------------------------------------------------------
# STATE SELECTION WITHOUT A `state` COLUMN
#
# `nutrition_mapping_portions` (id, mapping_id, usda_portion_id, modifier,
# notes) has no column recording whether a given portion applies to the
# raw or cooked version of an ingredient. It doesn't need one: ingestion
# (ingest_mappings.py) already only links a portion to a mapping when the
# portion's own `usda_food_portions.fdc_id` matches one of that mapping's
# specific fdc_ids (default/raw/cooked) -- see its `valid_fdc_ids` check.
#
# So once this resolver has already picked which single fdc_id applies to
# a line (raw vs. cooked vs. default -- see `_select_state_and_fdc`), it
# only needs to join `nutrition_mapping_portions` -> `usda_food_portions`
# and filter to portions whose `fdc_id` equals that chosen fdc_id. That
# automatically yields only raw portions for a raw-resolved line and only
# cooked portions for a cooked-resolved line, with no extra schema needed.
# ------------------------------------------------------------------

import json
import re
import sqlite3
from collections import Counter, OrderedDict

from gastrometric.config.paths import DB_PATH


# ============================================================
# STATUS VOCABULARY
# ============================================================

STATUS_RESOLVED = "resolved"
STATUS_UNRESOLVED = "unresolved"
STATUS_MISSING_MAPPING = "skipped_missing_mapping"
STATUS_UNAPPROVED_MAPPING = "skipped_unapproved_mapping"
STATUS_EXCLUDED_ALTERNATIVE = "excluded_alternative"

_SKIPPED_MAPPING_STATUSES = (STATUS_MISSING_MAPPING, STATUS_UNAPPROVED_MAPPING)
_FAILURE_STATUSES = (STATUS_UNRESOLVED,) + _SKIPPED_MAPPING_STATUSES


# ============================================================
# UNIT NORMALIZATION
#
# Bridges three independently-authored vocabularies: the parser's
# stored unit strings (e.g. "cup" or "cups", "tbsp"), the
# nutrition_mappings.json authors' free-text `modifier` strings (e.g.
# "tablespoon"), and usda_food_portions.unit/modifier. This is
# intentionally narrow -- it only canonicalizes spelling/plural
# variants for the purpose of matching a recipe quantity's unit
# against a mapped USDA portion. It is not a replacement for, or a
# competitor to, the parser's own unit vocabulary
# which is not visible to this module.
# ============================================================

_UNIT_SYNONYMS = {
    "tbsp": "tbsp", "tablespoon": "tbsp", "tablespoons": "tbsp",
    "tsp": "tsp", "teaspoon": "tsp", "teaspoons": "tsp",
    "cup": "cup", "cups": "cup",
    "oz": "oz", "ounce": "oz", "ounces": "oz",
    "lb": "lb", "pound": "lb", "pounds": "lb",
    "pint": "pint", "pints": "pint",
    "quart": "quart", "quarts": "quart", "qt": "quart",
    "gallon": "gallon", "gallons": "gallon",
    "floz": "floz", "fl oz": "floz", "fluid ounce": "floz", "fluid ounces": "floz",
    "g": "g", "gram": "g", "grams": "g",
    "kg": "kg", "kilogram": "kg", "kilograms": "kg",
    "ml": "ml", "milliliter": "ml", "millilitre": "ml", "milliliters": "ml", "millilitres": "ml",
    "liter": "l", "litre": "l", "liters": "l", "litres": "l", "l": "l",
    "clove": "clove", "cloves": "clove",
    "sprig": "sprig", "sprigs": "sprig",
    "stalk": "stalk", "stalks": "stalk",
    "leaf": "leaf", "leaves": "leaf",
    "head": "head", "heads": "head",
    "bunch": "bunch", "bunches": "bunch",
    "stick": "stick", "sticks": "stick",
    "can": "can", "cans": "can",
    "jar": "jar", "jars": "jar",
    "bottle": "bottle", "bottles": "bottle",
    "box": "box", "boxes": "box",
    "slice": "slice", "slices": "slice",
    "strip": "strip", "strips": "strip",
    "pinch": "pinch", "pinches": "pinch",
    "handful": "handful", "handfuls": "handful",
    "sprout": "sprout", "sprouts": "sprout",
    "egg": "egg", "eggs": "egg",
}


def _canon_unit(unit):
    if not unit:
        return None
    key = str(unit).strip().lower()
    return _UNIT_SYNONYMS.get(key, key)


_IMPERIAL_WEIGHT_TO_G = {
    "oz": 28.349523125,
    "lb": 453.59237,
}

_VOLUME_PRIORITY = ["cup", "tbsp", "tsp", "floz", "pint", "quart", "gallon"]
_STANDARD_VOLUME_ML = {
    "cup": 240.0,
    "tbsp": 14.7868,
    "tsp": 4.92892,
    "floz": 29.5735,
    "pint": 473.176,
    "quart": 946.353,
    "gallon": 3785.41,
}


# ============================================================
# COOKED-STATE DETECTION
#
# MVP rule (task section 6): raw by default; cooked only on explicit
# in-line language. We look at the parser's own `preparation` phrase
# list first (it already isolates technique/state language from the
# rest of the line), and fall back to a whole-word search of the raw
# text. We deliberately never look past this single ingredient line.
# ============================================================

_COOKED_WORD = re.compile(r"\bcooked\b", re.IGNORECASE)


def _is_cooked_indicated(preparation_json, raw_text):
    if preparation_json:
        try:
            phrases = json.loads(preparation_json)
        except (TypeError, ValueError):
            phrases = []
        for phrase in phrases or []:
            if isinstance(phrase, str) and _COOKED_WORD.search(phrase):
                return True
    if raw_text and _COOKED_WORD.search(raw_text):
        return True
    return False


# ============================================================
# SCHEMA (output table + minimal migration of the measurements table)
# ============================================================

def _ensure_output_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredient_line_nutrition (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_ingredient_id INTEGER NOT NULL REFERENCES recipe_ingredients(id),
            recipe_id INTEGER NOT NULL,
            recipe_section_id INTEGER,
            ingredient_id INTEGER REFERENCES ingredients(id),
            mapping_id INTEGER REFERENCES nutrition_ingredient_mappings(id),
            resolved_fdc_id INTEGER REFERENCES usda_foods(fdc_id),
            resolved_state TEXT CHECK (resolved_state IN ('raw', 'cooked')),
            resolved_grams REAL,
            calories REAL,
            protein REAL,
            total_fat REAL,
            saturated_fat REAL,
            monounsaturated_fat REAL,
            polyunsaturated_fat REAL,
            carbohydrates REAL,
            sugars REAL,
            fiber REAL,
            sodium REAL,
            status TEXT NOT NULL,
            source TEXT,
            diagnostic_notes TEXT,
            calculated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_riln_recipe_ingredient_id
        ON recipe_ingredient_line_nutrition (recipe_ingredient_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_riln_recipe_id
        ON recipe_ingredient_line_nutrition (recipe_id)
    """)


# ============================================================
# MAPPING SELECTION
# ============================================================

def _select_mapping(cur, ingredient_id):
    if ingredient_id is None:
        return None, (STATUS_MISSING_MAPPING, "missing nutrition mapping: ingredient_id not set")
    cur.execute("""
        SELECT id, status, source, default_fdc_id, raw_fdc_id, cooked_fdc_id
        FROM nutrition_ingredient_mappings
        WHERE ingredient_id = ?
        ORDER BY id
        LIMIT 1
    """, (ingredient_id,))
    row = cur.fetchone()
    if row is None:
        return None, (STATUS_MISSING_MAPPING, "missing nutrition mapping for ingredient_id=%s" % ingredient_id)
    mapping = {
        "id": row[0],
        "status": row[1],
        "source": row[2],
        "default_fdc_id": row[3],
        "raw_fdc_id": row[4],
        "cooked_fdc_id": row[5],
    }
    if mapping["status"] != "approved":
        return None, (STATUS_UNAPPROVED_MAPPING, "unapproved nutrition mapping (status=%s)" % mapping["status"])
    return mapping, None


def _select_state_and_fdc(mapping, cooked_indicated):
    if mapping["default_fdc_id"] is not None:
        return None, mapping["default_fdc_id"]
    if cooked_indicated and mapping["cooked_fdc_id"] is not None:
        return "cooked", mapping["cooked_fdc_id"]
    if mapping["raw_fdc_id"] is not None:
        return "raw", mapping["raw_fdc_id"]
    if mapping["cooked_fdc_id"] is not None:
        return "cooked", mapping["cooked_fdc_id"]
    return None, None


# ============================================================
# USDA PORTION MATCHING
#
# usda_food_portions' real columns (confirmed against the live DB, which
# differs from the task's original description in two ways):
#   - primary key is `usda_portion_id`, not `id`
#   - there is no single `unit` column; unit information is split across
#     `measure_unit_name`, `measure_unit_abbr`, and `modifier`
#   - there is an `amount` column: `gram_weight` is the weight of
#     `amount` units of the portion, not necessarily of a single unit
#     (e.g. amount=1.0, gram_weight=34.0, modifier='serving' means
#     "1 serving = 34g", but a row with amount=2.0 would mean
#     "2 <unit> = gram_weight g", i.e. gram_weight/amount per unit).
#     Grams-per-unit is therefore gram_weight / amount, not gram_weight
#     alone -- dividing by amount is required for correctness even
#     though it is frequently 1.
# ============================================================

def _grams_per_unit(portion):
    amount = portion["amount"]
    if not amount:
        # Defensive: some rows may have amount NULL/0 rather than 1; a
        # missing amount most plausibly means "this gram_weight already
        # describes a single unit", so treat it as 1 rather than divide
        # by zero or silently drop the portion.
        amount = 1.0
    return portion["gram_weight"] / amount


def _eligible_portions(cur, mapping_id, fdc_id):
    """Portions associated with this mapping via nutrition_mapping_portions,
    restricted to the specific fdc_id already chosen for this line (see
    the STATE SELECTION note at the top of this file -- this is what
    keeps raw and cooked portions from being conflated, with no separate
    state column needed)."""
    cur.execute("""
        SELECT nmp.usda_portion_id, ufp.gram_weight, ufp.amount,
               ufp.measure_unit_name, ufp.measure_unit_abbr, ufp.modifier,
               nmp.modifier
        FROM nutrition_mapping_portions nmp
        JOIN usda_food_portions ufp ON ufp.usda_portion_id = nmp.usda_portion_id
        WHERE nmp.mapping_id = ? AND ufp.fdc_id = ?
    """, (mapping_id, fdc_id))
    out = []
    for (portion_id, gram_weight, amount, measure_unit_name,
         measure_unit_abbr, portion_modifier, mapping_modifier) in cur.fetchall():
        out.append({
            "portion_id": portion_id,
            "gram_weight": gram_weight,
            "amount": amount,
            "measure_unit_name": measure_unit_name,
            "measure_unit_abbr": measure_unit_abbr,
            "portion_modifier": portion_modifier,
            "mapping_modifier": mapping_modifier,
        })
    return out


def _candidate_unit_strings(c):
    return (c["measure_unit_name"], c["measure_unit_abbr"], c["portion_modifier"], c["mapping_modifier"])


def _match_unit(candidates, target_unit):
    target = _canon_unit(target_unit)
    if not target:
        return None
    for c in candidates:
        for u in _candidate_unit_strings(c):
            if _canon_unit(u) == target:
                return c
    return None


def _match_volume_for_ml(candidates):
    by_canon = {}
    for c in candidates:
        for u in _candidate_unit_strings(c):
            canon = _canon_unit(u)
            if canon in _STANDARD_VOLUME_ML and canon not in by_canon:
                by_canon[canon] = c
                break
    for canon in _VOLUME_PRIORITY:
        if canon in by_canon:
            return by_canon[canon], canon
    return None, None


# ============================================================
# QUANTITY RESOLUTION (priority order per task section 7)
# `scaling` is never consulted.
# ============================================================

def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _grams_from_volume_ml(candidates, ml_amount):
    """Resolve a volume amount (already expressed in ml) to grams using
    any mapped volume-type portion available for this mapping/fdc_id.
    Shared by the metric-volume path and the imperial-volume fallback
    below -- converting between volume units (tsp/tbsp/cup/floz/pint/
    quart/gallon) via the fixed ratios in _STANDARD_VOLUME_ML is plain
    deterministic unit conversion, not a food-specific density, so it's
    fine to use even when the recipe's exact unit isn't itself mapped."""
    match, canon = _match_volume_for_ml(candidates)
    if match is None or canon is None:
        return None
    return (ml_amount / _STANDARD_VOLUME_ML[canon]) * _grams_per_unit(match)


def _resolve_quantity(cur, mapping_id, fdc_id, parsed):
    grams = _to_float(parsed["grams"])
    if grams is not None and grams > 0:
        return grams, "parsed_grams", None

    iw_val = _to_float(parsed["imperial_weight_value"])
    iw_unit = parsed["imperial_weight_unit"]
    if iw_val is not None and iw_unit:
        canon_iw = _canon_unit(iw_unit)
        factor = _IMPERIAL_WEIGHT_TO_G.get(canon_iw) if canon_iw is not None else None
        if factor is None:
            return None, None, "invalid quantity: unsupported imperial weight unit '%s'" % iw_unit
        return iw_val * factor, "imperial_weight", None

    ml = _to_float(parsed["ml"])
    if ml is not None and ml > 0:
        candidates = _eligible_portions(cur, mapping_id, fdc_id)
        grams_result = _grams_from_volume_ml(candidates, ml)
        if grams_result is not None:
            return grams_result, "metric_volume", None
        # No usable volume portion for this mapping/fdc_id; fall through
        # to lower-priority methods rather than failing outright here.

    iv_val = _to_float(parsed["imperial_volume_value"])
    iv_unit = parsed["imperial_volume_unit"]
    if iv_val is not None and iv_unit:
        candidates = _eligible_portions(cur, mapping_id, fdc_id)
        match = _match_unit(candidates, iv_unit)
        if match:
            return iv_val * _grams_per_unit(match), "imperial_volume", None
        # No portion mapped for this exact unit (e.g. mapping has tsp
        # and cup but the recipe asks for tbsp): derive it from whatever
        # volume-type portion IS mapped, via the fixed tsp/tbsp/cup/...
        # ratios -- still not a food-specific density, just volume-unit
        # conversion applied on top of the mapping's own gram_weight.
        canon_iv = _canon_unit(iv_unit)
        if canon_iv in _STANDARD_VOLUME_ML:
            target_ml = iv_val * _STANDARD_VOLUME_ML[canon_iv]
            grams_result = _grams_from_volume_ml(candidates, target_ml)
            if grams_result is not None:
                return grams_result, "imperial_volume", None

    q_val = _to_float(parsed["quantity_value"])
    q_unit = parsed["quantity_unit"]
    if q_val is not None and q_unit:
        candidates = _eligible_portions(cur, mapping_id, fdc_id)
        match = _match_unit(candidates, q_unit)
        if match:
            return q_val * _grams_per_unit(match), "nominal_count", None
        return None, None, "no matching mapped portion for unit '%s'" % q_unit

    if any(v is not None for v in (grams, iw_val, ml, iv_val, q_val)):
        return None, None, "no matching mapped portion"
    return None, None, "no quantity"


# ============================================================
# PER-LINE RESOLUTION
# ============================================================

_ROW_FIELDS = (
    "recipe_ingredient_id", "recipe_id", "recipe_section_id", "ingredient_id",
    "parsed_line_id", "recipe_name", "line_index", "raw_text",
    "quantity_value", "quantity_unit",
    "imperial_weight_value", "imperial_weight_unit",
    "imperial_volume_value", "imperial_volume_unit",
    "grams", "ml", "preparation", "optional",
)


def _row_dict(row):
    return dict(zip(_ROW_FIELDS, row))


def _base_result(rd):
    return {
        "recipe_ingredient_id": rd["recipe_ingredient_id"],
        "recipe_id": rd["recipe_id"],
        "recipe_section_id": rd["recipe_section_id"],
        "ingredient_id": rd["ingredient_id"],
        "mapping_id": None,
        "resolved_fdc_id": None,
        "resolved_state": None,
        "resolved_grams": None,
        "status": None,
        "source": None,
        "diagnostic_notes": None,
        "method": None,
    }


def _resolve_single(cur, row):
    rd = _row_dict(row)
    result = _base_result(rd)

    mapping, err = _select_mapping(cur, rd["ingredient_id"])
    if mapping is None:
        assert err is not None
        result["status"], result["diagnostic_notes"] = err
        return result

    cooked = _is_cooked_indicated(rd["preparation"], rd["raw_text"])
    state, fdc_id = _select_state_and_fdc(mapping, cooked)
    result["mapping_id"] = mapping["id"]
    result["source"] = mapping["source"]

    if fdc_id is None:
        result["status"] = STATUS_UNRESOLVED
        result["diagnostic_notes"] = "invalid mapping: no fdc id available for resolved state"
        return result

    parsed = {
        "grams": rd["grams"],
        "imperial_weight_value": rd["imperial_weight_value"],
        "imperial_weight_unit": rd["imperial_weight_unit"],
        "ml": rd["ml"],
        "imperial_volume_value": rd["imperial_volume_value"],
        "imperial_volume_unit": rd["imperial_volume_unit"],
        "quantity_value": rd["quantity_value"],
        "quantity_unit": rd["quantity_unit"],
    }
    grams, method, diag = _resolve_quantity(cur, mapping["id"], fdc_id, parsed)

    result["resolved_state"] = state
    result["resolved_fdc_id"] = fdc_id

    if grams is None:
        result["status"] = STATUS_UNRESOLVED
        result["diagnostic_notes"] = diag or "no matching mapped portion"
        return result

    result["status"] = STATUS_RESOLVED
    result["resolved_grams"] = grams
    result["method"] = method
    return result


def _excluded_result(row, note):
    rd = _row_dict(row)
    result = _base_result(rd)
    result["status"] = STATUS_EXCLUDED_ALTERNATIVE
    result["diagnostic_notes"] = note
    return result


# ============================================================
# PERSISTENCE + SUMMARY
# ============================================================

def _persist(cur, result):
    cur.execute("""
        INSERT INTO recipe_ingredient_line_nutrition (
            recipe_ingredient_id, recipe_id, recipe_section_id, ingredient_id,
            mapping_id, resolved_fdc_id, resolved_state, resolved_grams,
            status, source, diagnostic_notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        result["recipe_ingredient_id"], result["recipe_id"], result["recipe_section_id"],
        result["ingredient_id"], result["mapping_id"], result["resolved_fdc_id"],
        result["resolved_state"], result["resolved_grams"], result["status"],
        result["source"], result["diagnostic_notes"],
    ))


def _record(cur, stats, result):
    _persist(cur, result)
    stats["processed"] += 1
    if result["status"] == STATUS_RESOLVED:
        stats["resolved"] += 1
        stats["method:%s" % result["method"]] += 1
    elif result["status"] in _SKIPPED_MAPPING_STATUSES:
        stats["skipped_mapping"] += 1
    elif result["status"] == STATUS_UNRESOLVED:
        stats["unresolved"] += 1


def _maybe_record_failure(failures, row, result):
    if result["status"] in _FAILURE_STATUSES and len(failures) < 8:
        rd = _row_dict(row)
        failures.append((rd["recipe_name"], rd["raw_text"], result["diagnostic_notes"] or result["status"]))


def _print_summary(stats, failures):
    print("Recipe nutrition quantity resolution complete")
    print()
    print("Ingredient lines processed: %d" % stats["processed"])
    print()
    print("Resolved to grams: %d" % stats["resolved"])
    print("  Parsed grams: %d" % stats["method:parsed_grams"])
    print("  Imperial weight: %d" % stats["method:imperial_weight"])
    print("  Metric volume via USDA portion: %d" % stats["method:metric_volume"])
    print("  Imperial volume via USDA portion: %d" % stats["method:imperial_volume"])
    print("  Nominal/count/container via USDA portion: %d" % stats["method:nominal_count"])
    print()
    print("Unresolved: %d" % stats["unresolved"])
    print("Skipped due to missing/unapproved mapping: %d" % stats["skipped_mapping"])
    if failures:
        print()
        print("Representative failures:")
        for recipe_name, raw_text, reason in failures:
            print("  - %s | %s | %s" % (recipe_name, raw_text, reason))


# ============================================================
# GROUPING / OR-ALTERNATIVE HANDLING (task section 9)
#
# recipe_ingredient_lines_parsed does not carry a column that records
# which "slot" (primary vs. or-alternative) a row was produced from --
# only `optional`, which is 1 both for genuinely self-declared-optional
# ingredients and for every row on the or-alternative side of a split.
# Rows sharing (recipe_id, recipe_section_id, line_index) came from the
# same raw source line, and within that group the parser always writes
# primary-slot rows before alternative-slot rows (see
# parse_ingredient_lines.py: primary is emitted at slot 0, alt at slot
# 1), so ordering by id within the group preserves that.
#
# This lets us reliably detect the common case: a group containing both
# optional=0 row(s) (the primary, possibly several via "+"/"and"
# splitting -- those are peers to be summed, not alternatives to each
# other) and optional=1 row(s) (the "or" alternative). We apply the
# preference rule from section 9 to that case.
#
# KNOWN LIMITATION: if the primary side of an "A or B" line was *itself*
# self-declared optional (e.g. "broth (optional) or water"), both sides
# end up with optional=1 and are indistinguishable from two independent
# optional single-ingredient lines. The schema gives us no way to tell
# these apart, so in that situation we do not infer an alternative
# relationship -- each row is resolved independently, which risks
# double-counting a mutually exclusive pair. Fixing this would require
# the parser to persist which slot/alt-group a row came from; that is
# outside this task's scope (do not redesign the parser) and is
# reported here rather than worked around with a fragile heuristic.
# ============================================================

def _process_group(cur, stats, failures, group_rows):
    primary_rows = [r for r in group_rows if not _row_dict(r)["optional"]]
    alt_rows = [r for r in group_rows if _row_dict(r)["optional"]]

    if not (primary_rows and alt_rows):
        for row in group_rows:
            result = _resolve_single(cur, row)
            _record(cur, stats, result)
            _maybe_record_failure(failures, row, result)
        return

    primary_results = [_resolve_single(cur, r) for r in primary_rows]
    if all(res["status"] == STATUS_RESOLVED for res in primary_results):
        for row, result in zip(primary_rows, primary_results):
            _record(cur, stats, result)
        for row in alt_rows:
            _record(cur, stats, _excluded_result(
                row, "or-alternative not used; primary ingredient resolved"))
        return

    alt_results = [_resolve_single(cur, r) for r in alt_rows]
    if all(res["status"] == STATUS_RESOLVED for res in alt_results):
        for row in primary_rows:
            _record(cur, stats, _excluded_result(
                row, "primary ingredient unresolved; or-alternative used instead"))
        for row, result in zip(alt_rows, alt_results):
            _record(cur, stats, result)
        return

    # Neither side fully resolves: persist both with their own diagnostics
    # (never both counted as resolved, since neither side is).
    for row, result in zip(primary_rows, primary_results):
        _record(cur, stats, result)
        _maybe_record_failure(failures, row, result)
    for row, result in zip(alt_rows, alt_results):
        _record(cur, stats, result)
        _maybe_record_failure(failures, row, result)


# ============================================================
# MAIN
# ============================================================

def _run(conn):
    _ensure_output_schema(conn)
    cur = conn.cursor()

    # Deterministic rebuild: this stage's own prior output is derived
    # data and safe to fully replace each run. Nothing else is touched.
    cur.execute("DELETE FROM recipe_ingredient_line_nutrition")

    cur.execute("""
        SELECT ri.id, ri.recipe_id, ri.recipe_section_id, ri.ingredient_id,
               ri.parsed_line_id, rilp.recipe_name, rilp.line_index, rilp.raw_text,
               rilp.quantity_value, rilp.quantity_unit,
               rilp.imperial_weight_value, rilp.imperial_weight_unit,
               rilp.imperial_volume_value, rilp.imperial_volume_unit,
               rilp.grams, rilp.ml, rilp.preparation, rilp.optional
        FROM recipe_ingredients ri
        JOIN recipe_ingredient_lines_parsed rilp ON rilp.id = ri.parsed_line_id
        ORDER BY rilp.recipe_id, rilp.recipe_section_id, rilp.line_index, ri.id
    """)
    rows = cur.fetchall()

    groups = OrderedDict()
    for row in rows:
        rd = _row_dict(row)
        key = (rd["recipe_id"], rd["recipe_section_id"], rd["line_index"])
        groups.setdefault(key, []).append(row)

    stats = Counter()
    failures = []
    for group_rows in groups.values():
        _process_group(cur, stats, failures, group_rows)

    conn.commit()
    _print_summary(stats, failures)


def resolve_ingredient_quantities(db_path=None):
    """Public entry point. Opens the project database, performs the
    deterministic rebuild of recipe_ingredient_line_nutrition, commits,
    closes the connection, and prints a summary."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    try:
        _run(conn)
    finally:
        conn.close()


def main():
    try:
        resolve_ingredient_quantities()
    except Exception:
        print("ingredient quantity resolution failed")
        raise


if __name__ == "__main__":
    main()