# ============================================================
# INGREDIENT IDENTITY — matching + write helpers for the normalize stage.
#
# Schema ownership: gastrometric/db/init_db.py is the ONLY place that
# creates or alters tables. Everything in this module assumes the
# schema it needs (ingredients, ingredient_aliases, attribute_type,
# attribute_value, identity_attribute_rule,
# identity_attribute_allowed_value, instance_attribute_value,
# usda_source_map, usda_mapping_condition) already exists.
#
# Identity model recap: identity is the SHORT list — two mentions are
# the same identity if a cook could freely substitute one for the other
# without changing the recipe's method or result. Everything else that
# differs is an attribute: decorative (safe to ignore for fridge-
# matching) or required (must match — e.g. brand for kosher salt),
# asserted per (ingredient, attribute) pair via identity_attribute_rule,
# not globally per attribute.
# ============================================================


def resolve_alias(conn, name_text):
    """Look up a parsed ingredient_name_raw string against ingredient_aliases,
    falling back to an exact ingredients.ingredient_name match. Returns
    ingredient_id or None (caller falls back to attribute-stripped
    re-match, then to 'unmatched')."""
    normalized = name_text.strip().lower()
    row = conn.execute(
        "SELECT ingredient_id FROM ingredient_aliases WHERE lower(alias) = ?",
        (normalized,),
    ).fetchone()
    if row:
        return row[0]
    row = conn.execute(
        "SELECT id FROM ingredients WHERE lower(ingredient_name) = ?",
        (normalized,),
    ).fetchone()
    return row[0] if row else None


def get_or_create_ingredient(conn, ingredient_name, notes=None):
    row = conn.execute(
        "SELECT id FROM ingredients WHERE lower(ingredient_name) = ?",
        (ingredient_name.strip().lower(),),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO ingredients (ingredient_name, notes) VALUES (?, ?)",
        (ingredient_name.strip(), notes),
    )
    return cur.lastrowid


def add_alias(conn, ingredient_id, alias_text, confidence=None, source=None):
    """Adds an alias if the exact text isn't already claimed by ANY
    identity. Returns True if inserted, False if the alias already
    existed (whether for this identity or, more importantly, a
    different one — caller should check which, since a collision
    across different identities is a curation bug, not something to
    silently ignore)."""
    existing = conn.execute(
        "SELECT ingredient_id FROM ingredient_aliases WHERE alias = ?",
        (alias_text.strip().lower(),),
    ).fetchone()
    if existing:
        return existing[0] == ingredient_id
    conn.execute(
        "INSERT INTO ingredient_aliases (alias, ingredient_id, confidence, source) "
        "VALUES (?, ?, ?, ?)",
        (alias_text.strip().lower(), ingredient_id, confidence, source),
    )
    return True


def get_or_create_attribute_type(conn, name, value_kind='enum', description=None):
    row = conn.execute(
        "SELECT id FROM attribute_type WHERE name = ?", (name,)
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO attribute_type (name, value_kind, description) VALUES (?, ?, ?)",
        (name, value_kind, description),
    )
    return cur.lastrowid


def get_or_create_attribute_value(conn, attribute_type_id, value):
    row = conn.execute(
        "SELECT id FROM attribute_value WHERE attribute_type_id = ? AND value = ?",
        (attribute_type_id, value),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO attribute_value (attribute_type_id, value) VALUES (?, ?)",
        (attribute_type_id, value),
    )
    return cur.lastrowid


def add_attribute_rule(conn, ingredient_id, attribute_type_id,
                        required_for_match=False, default_value_id=None):
    """Upsert-by-hand (not INSERT OR REPLACE) so the row keeps its id
    across re-runs — identity_attribute_allowed_value rows reference
    this id, and REPLACE would delete+reinsert, silently orphaning
    them. Returns the rule id."""
    row = conn.execute(
        "SELECT id FROM identity_attribute_rule "
        "WHERE ingredient_id = ? AND attribute_type_id = ?",
        (ingredient_id, attribute_type_id),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE identity_attribute_rule "
            "SET required_for_match = ?, default_value_id = ? WHERE id = ?",
            (int(required_for_match), default_value_id, row[0]),
        )
        return row[0]
    cur = conn.execute(
        "INSERT INTO identity_attribute_rule "
        "(ingredient_id, attribute_type_id, required_for_match, default_value_id) "
        "VALUES (?, ?, ?, ?)",
        (ingredient_id, attribute_type_id, int(required_for_match), default_value_id),
    )
    return cur.lastrowid


def set_allowed_values(conn, identity_attribute_rule_id, value_ids):
    """Restricts an enum attribute's otherwise-global value list for one
    identity (e.g. chicken breast's `state` is only ever raw/cooked).
    No call = no restriction = the full global value list is allowed."""
    for value_id in value_ids:
        conn.execute(
            "INSERT OR IGNORE INTO identity_attribute_allowed_value "
            "(identity_attribute_rule_id, value_id) VALUES (?, ?)",
            (identity_attribute_rule_id, value_id),
        )