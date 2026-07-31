# ============================================================
# BUILD INGREDIENTS
#
# Loads data/seed/ingredients.json (the curated seed of ingredient identities)
# into ingredients / ingredient_aliases / attribute_type / attribute_value
# / identity_attribute_rule / identity_attribute_allowed_value.
#
# This is what "the ingredients table should be prepopulated with
# gastrometric ingredients" (per the architecture doc) actually means in
# code: this stage runs BEFORE any recipe-facing normalization, and
# normalize_ingredient_lines matches parsed ingredient names against
# what this stage already loaded — it does not create new identities.
#
# Schema ownership: gastrometric/db/init_db.py creates every table this
# module writes to. This module only INSERTs/UPDATEs.
# ============================================================

import json

from gastrometric.config.paths import SEED_DIR, DB_PATH
from gastrometric.pipeline.normalize.ingredient_identity import (
    get_or_create_ingredient,
    add_alias,
    get_or_create_attribute_type,
    get_or_create_attribute_value,
    add_attribute_rule,
    set_allowed_values,
)

INGREDIENTS_JSON_PATH = SEED_DIR / "ingredients.json"


def build_ingredients(conn=None):
    import sqlite3
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(DB_PATH)

    with open(INGREDIENTS_JSON_PATH) as f:
        data = json.load(f)

    # # --- 1. attribute vocabulary first: identities reference these by name ---
    # attr_type_ids = {}       # type_name -> attribute_type.id
    # attr_value_ids = {}      # (type_name, value) -> attribute_value.id

    # for type_name, spec in data["attribute_types"].items():
    #     type_id = get_or_create_attribute_type(
    #         conn, type_name, value_kind=spec["value_kind"]
    #     )
    #     attr_type_ids[type_name] = type_id
    #     if spec["value_kind"] == "enum":
    #         for v in spec.get("values", []):
    #             attr_value_ids[(type_name, v)] = get_or_create_attribute_value(
    #                 conn, type_id, v
    #             )
    #     # free_text types (e.g. "brand") have no fixed value list —
    #     # any "known_values" given per-identity below are illustrative
    #     # only (e.g. "Diamond Crystal"/"Morton" for kosher salt's
    #     # brand), not a closed set to validate against, so they are
    #     # NOT loaded into attribute_value. They stay visible in
    #     # ingredients.json itself as curation-time reference.

    # --- 2. identities, aliases, attribute rules ---
    identities_loaded = 0
    aliases_loaded = 0
    alias_collisions = []
    unknown_attr_refs = []

    for ident in data["identities"]:
        name = ident["name"].strip()
        ingredient_id = get_or_create_ingredient(conn, name)

        for alias in ident.get("aliases", []):
            ok = add_alias(conn, ingredient_id, alias, source="ingredients.json")
            if ok:
                aliases_loaded += 1
            else:
                alias_collisions.append((alias, name))

        # for attr_name, spec in ident.get("attributes", {}).items():
        #     if attr_name not in attr_type_ids:
        #         unknown_attr_refs.append((name, attr_name))
        #         continue
        #     type_id = attr_type_ids[attr_name]
        #     required = bool(spec.get("required", False))

        #     default_value_id = None
        #     default_val = spec.get("default")
        #     if default_val:
        #         default_value_id = attr_value_ids.get((attr_name, default_val))

        #     rule_id = add_attribute_rule(
        #         conn, ingredient_id, type_id,
        #         required_for_match=required,
        #         default_value_id=default_value_id,
        #     )

            # # "allowed" (enum types) restricts the otherwise-global value
            # # list for this identity specifically (e.g. chicken breast's
            # # `state` is only ever raw/cooked, not the full global list
            # # that also includes partially_cooked/undercooked/etc. for
            # # identities where those distinctions matter).
            # allowed = spec.get("allowed")
            # if allowed:
            #     value_ids = [
            #         attr_value_ids[(attr_name, v)]
            #         for v in allowed
            #         if (attr_name, v) in attr_value_ids
            #     ]
            #     set_allowed_values(conn, rule_id, value_ids)

        identities_loaded += 1

    conn.commit()

    print(
        "build_ingredients: %d identities, %d aliases loaded"
        % (identities_loaded, aliases_loaded)
    )
    if alias_collisions:
        print(
            "  WARNING: %d alias(es) already claimed by a different "
            "identity, skipped:" % len(alias_collisions)
        )
        for alias, attempted_by in alias_collisions:
            print("    %r attempted by %r but already claimed elsewhere" % (alias, attempted_by))
    if unknown_attr_refs:
        print("  WARNING: attribute type refs not found in attribute_types:")
        for name, attr_name in unknown_attr_refs:
            print("    %r references unknown attribute type %r" % (name, attr_name))

    if own_conn:
        conn.close()


if __name__ == "__main__":
    build_ingredients()