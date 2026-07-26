"""
Schema for USDA-derived tables in gastrometric.db.

This is the single source of truth for these tables. init_db.py and
ingest_usda_legacy.py both call create_usda_tables() rather than each
defining their own CREATE TABLE statements, so the schema can't drift
out of sync between the two scripts again (that's what happened with
the old usda_source_map definitions).
"""


def create_usda_tables(conn):
    cur = conn.cursor()

    # Raw USDA foods, one row per fdc_id. This is exactly what USDA gave
    # us -- not canonicalized, not matched to `ingredients` in any way.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usda_foods (
            fdc_id       INTEGER PRIMARY KEY,
            description  TEXT,
            data_type    TEXT,
            category     TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS usda_nutrients (
            id            INTEGER PRIMARY KEY,
            fdc_id        INTEGER NOT NULL,
            nutrient_id   INTEGER,
            nutrient_name TEXT,
            unit          TEXT,
            amount        REAL,
            UNIQUE (fdc_id, nutrient_id),
            FOREIGN KEY (fdc_id) REFERENCES usda_foods(fdc_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS usda_food_portions (
            usda_portion_id      INTEGER PRIMARY KEY,
            fdc_id               INTEGER NOT NULL,
            amount               REAL,
            gram_weight          REAL,
            modifier             TEXT,
            portion_description  TEXT,
            measure_unit_id      INTEGER,
            measure_unit_name    TEXT,
            measure_unit_abbr    TEXT,
            FOREIGN KEY (fdc_id) REFERENCES usda_foods(fdc_id)
        )
    """)

    # Links an `ingredients` row to the fdc_id(s) that represent it.
    # Populated later by whatever matches ingredients -> USDA entries
    # (manual review, fuzzy matching, an LLM pass, etc) -- NOT by the
    # ingest script. fdc_description is a denormalized convenience copy
    # for humans reviewing mappings, not a join key.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usda_source_map (
            fdc_id              INTEGER,
            ingredient_id       INTEGER,
            fdc_description     TEXT,
            FOREIGN KEY(ingredient_id) REFERENCES ingredients(id)
        )
    """)

    # Which attribute values select a given usda_source_map row, for
    # identities that fan out to more than one FDC entry (e.g. raw vs
    # cooked chicken breast -> two different fdc_ids, same identity).
    # A mapping row with no conditions here applies unconditionally.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usda_mapping_condition (
            id                  INTEGER PRIMARY KEY,
            fdc_id              INTEGER NOT NULL,
            attribute_type_id   INTEGER NOT NULL,
            value_id            INTEGER NOT NULL,
            UNIQUE (fdc_id, attribute_type_id),
            FOREIGN KEY(attribute_type_id) REFERENCES attribute_type(id),
            FOREIGN KEY(value_id)          REFERENCES attribute_value(id)
        )
    """)

    conn.commit()