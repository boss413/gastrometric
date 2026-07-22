import sqlite3
from gastrometric.config.paths import DB_PATH, DATA_DIR


def init_db():
    print(f"Building gastrometric.db at: {DB_PATH}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()

        # ----------------------------------------------------------------
        # Core recipe tables
        # Creation order matters: parent tables before child tables.
        # ----------------------------------------------------------------

        c.execute("""
            CREATE TABLE IF NOT EXISTS recipes (
                id                      INTEGER PRIMARY KEY,
                recipe_name             TEXT NOT NULL,
                recipe_author           TEXT,
                recipe_attribution      TEXT,
                recipe_source           TEXT,
                recipe_url              TEXT,
                recipe_video            TEXT,
                recipe_notes            TEXT,
                recipe_yield            TEXT,
                recipe_state            TEXT,       -- 'raw' | 'parsed' | 'enriched'
                recipe_ingestion_method TEXT        -- 'manual'
            )
        """)

        # One row per named section (e.g. "Cook the aromatics").
        # ingredient_block and instruction_block stored as raw blobs — no splitting here.
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_sections (
                id                  INTEGER PRIMARY KEY,
                recipe_id           INTEGER NOT NULL,
                recipe_name         TEXT NOT NULL,
                section_name        TEXT,
                source_section_ref  TEXT,           -- "{recipe_name}::{section_name}"
                ingredient_block    TEXT,
                instruction_block   TEXT,
                FOREIGN KEY(recipe_id) REFERENCES recipes(id)
            )
        """)

        # One row per section — raw ingredient blob, unsplit.
        # Sole input to parse_ingredient_lines.
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_ingredient_blocks (
                id                  INTEGER PRIMARY KEY,
                recipe_id           INTEGER NOT NULL,
                recipe_section_id   INTEGER NOT NULL,
                recipe_name         TEXT NOT NULL,
                section_name        TEXT,
                raw_text            TEXT,
                FOREIGN KEY(recipe_id)         REFERENCES recipes(id),
                FOREIGN KEY(recipe_section_id) REFERENCES recipe_sections(id)
            )
        """)

        # One row per section — raw instruction blob, unsplit.
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_instruction_blocks (
                id                  INTEGER PRIMARY KEY,
                recipe_id           INTEGER NOT NULL,
                recipe_section_id   INTEGER NOT NULL,
                recipe_name         TEXT NOT NULL,
                section_name        TEXT,
                raw_text            TEXT,
                FOREIGN KEY(recipe_id)         REFERENCES recipes(id),
                FOREIGN KEY(recipe_section_id) REFERENCES recipe_sections(id)
            )
        """)

        # ----------------------------------------------------------------
        # Ingredient parse pipeline
        # recipe_ingredient_blocks
        #   → recipe_ingredient_lines_parsed  (parse_ingredient_lines)
        #   → ingredient_normalizations       (normalize_ingredient_lines)
        # ----------------------------------------------------------------

        # One row per ingredient line split from a block.
        # All enrichment columns start NULL; filled by downstream stages.
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_ingredient_lines_parsed (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                -- source
                ingredient_block_id     INTEGER NOT NULL
                                            REFERENCES recipe_ingredient_blocks(id),
                recipe_id               INTEGER NOT NULL,
                recipe_section_id       INTEGER NOT NULL,
                recipe_name             TEXT,
                section_name            TEXT,
                -- position within the block
                line_index              INTEGER NOT NULL,
                -- original text (never modified)
                raw_text                TEXT NOT NULL,
                -- parsed dimensions
                quantity_value          TEXT,
                quantity_unit           TEXT,
                imperial_weight_value   TEXT,
                imperial_weight_unit    TEXT,
                imperial_volume_value   TEXT,
                imperial_volume_unit    TEXT,
                grams                   REAL,
                ml                      REAL,
                scaling                 TEXT,
                preparation             TEXT,
                -- name as it appears after measurement / prep extraction
                ingredient_name_raw     TEXT,
                -- flags
                optional                INTEGER DEFAULT 0,
                -- audit
                parsed_at               TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(recipe_id)           REFERENCES recipes(id),
                FOREIGN KEY(recipe_section_id)   REFERENCES recipe_sections(id),
                FOREIGN KEY(ingredient_block_id) REFERENCES recipe_ingredient_blocks(id)
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_rilp_ingredient_block_id
                ON recipe_ingredient_lines_parsed (ingredient_block_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_rilp_recipe_id
                ON recipe_ingredient_lines_parsed (recipe_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_rilp_recipe_section_id
                ON recipe_ingredient_lines_parsed (recipe_section_id)
        """)

        # Normalization log — name transformation only.
        # Join to recipe_ingredient_lines_parsed on parsed_line_id for all
        # other dimensions.
        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredient_normalizations (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                parsed_line_id      INTEGER NOT NULL UNIQUE
                                        REFERENCES recipe_ingredient_lines_parsed(id),
                recipe_id           INTEGER NOT NULL,
                recipe_name         TEXT,
                raw_text            TEXT,
                ingredient_name_raw TEXT,   -- as arrived from parse stage
                ingredient_name     TEXT,   -- core ingredient after both passes
                status              TEXT NOT NULL,  -- 'ok' | 'empty' | 'reduced_to_nothing'
                normalized_at       TEXT DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingn_parsed_line_id
                ON ingredient_normalizations (parsed_line_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingn_recipe_id
                ON ingredient_normalizations (recipe_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingn_ingredient_name
                ON ingredient_normalizations (ingredient_name)
        """)

        # ----------------------------------------------------------------
        # Ingredient identity
        #
        # Ingredient identity is the primary domain model: recipes
        # reference ingredients, nutrition maps to ingredients,
        # relationships (see below) connect ingredients. Identity is
        # deliberately a SHORT list — two mentions are the same identity
        # if a cook could freely substitute one for the other without
        # changing the recipe's method or result (raw/cooked chicken
        # breast: one identity, differ by attribute below. Bread flour
        # vs. cake flour: different identities, protein content changes
        # what the recipe does).
        #
        # Canonicalization (canonical_ingredients / canonical_lookup /
        # canonical_id / canonical_group / entity_id) has been removed
        # here — it was planned but never wired into a working stage
        # (0 rows), and per the architecture decision it's being
        # eliminated outright: subsumed by this identity model plus the
        # separate ingredient relationships knowledge graph, rather than
        # kept as a third, weaker grouping concept alongside them.
        # ----------------------------------------------------------------

        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredients (
                id              INTEGER PRIMARY KEY,
                ingredient_name TEXT UNIQUE NOT NULL,
                notes           TEXT,
                created_at      TEXT DEFAULT (datetime('now'))
            )
        """)

        # Variant spellings/names that resolve to one identity above.
        # alias is globally UNIQUE by design: if two identities both
        # claim the same alias, that's a curation bug to catch at
        # load time, not something to resolve ambiguously at match time.
        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredient_aliases (
                id              INTEGER PRIMARY KEY,
                ingredient_id   INTEGER NOT NULL,
                alias           TEXT UNIQUE NOT NULL,
                confidence      REAL,
                source          TEXT,   -- e.g. 'ingredients.json', 'manual'
                FOREIGN KEY(ingredient_id) REFERENCES ingredients(id)
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingredient_aliases_ingredient_id
                ON ingredient_aliases (ingredient_id)
        """)

        # ----------------------------------------------------------------
        # Ingredient attributes
        #
        # An attribute is anything that varies about an identity WITHOUT
        # changing what identity it is (raw/cooked, bone-in/boneless,
        # salted/unsalted, brand). Whether an attribute is safe to ignore
        # during fridge-matching ("decorative") or must match exactly
        # ("required", e.g. brand for kosher salt — Diamond Crystal and
        # Morton differ ~2x by volume) is asserted per (ingredient,
        # attribute) pair in identity_attribute_rule, not globally per
        # attribute — brand is decorative for canned tomatoes but
        # required for kosher salt.
        # ----------------------------------------------------------------

        c.execute("""
            CREATE TABLE IF NOT EXISTS attribute_type (
                id              INTEGER PRIMARY KEY,
                name            TEXT UNIQUE NOT NULL,
                value_kind      TEXT NOT NULL DEFAULT 'enum'
                                    CHECK (value_kind IN ('enum', 'free_text')),
                description     TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS attribute_value (
                id                  INTEGER PRIMARY KEY,
                attribute_type_id   INTEGER NOT NULL,
                value               TEXT NOT NULL,
                UNIQUE (attribute_type_id, value),
                FOREIGN KEY(attribute_type_id) REFERENCES attribute_type(id)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS identity_attribute_rule (
                id                      INTEGER PRIMARY KEY,
                ingredient_id           INTEGER NOT NULL,
                attribute_type_id       INTEGER NOT NULL,
                required_for_match      INTEGER NOT NULL DEFAULT 0,
                default_value_id        INTEGER,
                UNIQUE (ingredient_id, attribute_type_id),
                FOREIGN KEY(ingredient_id)      REFERENCES ingredients(id),
                FOREIGN KEY(attribute_type_id)  REFERENCES attribute_type(id),
                FOREIGN KEY(default_value_id)   REFERENCES attribute_value(id)
            )
        """)

        # Per-identity restriction of an enum attribute's otherwise-global
        # value list (e.g. chicken breast's `state` is only ever
        # raw/cooked, even though the global `state` vocabulary also
        # includes partially_cooked/undercooked/overcooked for identities
        # where those distinctions matter). No rows for a given rule
        # means the full global value list is allowed.
        c.execute("""
            CREATE TABLE IF NOT EXISTS identity_attribute_allowed_value (
                id                          INTEGER PRIMARY KEY,
                identity_attribute_rule_id  INTEGER NOT NULL,
                value_id                    INTEGER NOT NULL,
                UNIQUE (identity_attribute_rule_id, value_id),
                FOREIGN KEY(identity_attribute_rule_id) REFERENCES identity_attribute_rule(id),
                FOREIGN KEY(value_id)                   REFERENCES attribute_value(id)
            )
        """)

        # Attribute values actually observed on one resolved recipe
        # ingredient mention (recipe_ingredients row below).
        c.execute("""
            CREATE TABLE IF NOT EXISTS instance_attribute_value (
                id                      INTEGER PRIMARY KEY,
                recipe_ingredient_id    INTEGER NOT NULL,
                attribute_type_id       INTEGER NOT NULL,
                value_id                INTEGER,
                value_text              TEXT,
                source                  TEXT DEFAULT 'parsed'
                                    CHECK (source IN ('parsed', 'defaulted', 'inferred_from_step')),
                UNIQUE (recipe_ingredient_id, attribute_type_id),
                FOREIGN KEY(recipe_ingredient_id) REFERENCES recipe_ingredients(id),
                FOREIGN KEY(attribute_type_id)    REFERENCES attribute_type(id),
                FOREIGN KEY(value_id)             REFERENCES attribute_value(id)
            )
        """)

        c.execute("""
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
        c.execute("""
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

        # ----------------------------------------------------------------
        # Resolved recipe ingredients
        # Populated after canonical resolution — one row per ingredient
        # occurrence in a recipe, with identity resolved.
        # ----------------------------------------------------------------
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_ingredients (
                id                  INTEGER PRIMARY KEY,
                ingredient_name     TEXT,
                preparation         TEXT,
                recipe_id           INTEGER,
                recipe_section_id   INTEGER,
                line_index          INTEGER,
                raw_text            TEXT,
                section_name        TEXT,
                ingredient_id       INTEGER,
                parsed_line_id      INTEGER    REFERENCES recipe_ingredient_lines_parsed(id),
                FOREIGN KEY(recipe_id)         REFERENCES recipes(id),
                FOREIGN KEY(recipe_section_id) REFERENCES recipe_sections(id),
                FOREIGN KEY(ingredient_id)     REFERENCES ingredients(id)
            )
        """)

        # ----------------------------------------------------------------
        # Flavor relationships
        # ----------------------------------------------------------------

        c.execute("""
            CREATE TABLE IF NOT EXISTS relationships (
                id          INTEGER PRIMARY KEY,
                source_id   INTEGER,
                target_id   INTEGER,
                score       INTEGER,
                source      TEXT,   -- 'flavor_bible'
                FOREIGN KEY(source_id) REFERENCES ingredients(id),
                FOREIGN KEY(target_id) REFERENCES ingredients(id)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS flavor_bible_raw (
                id          INTEGER PRIMARY KEY,
                source_text TEXT,
                target_text TEXT,
                score       INTEGER
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS flavor_bible_curated (
                id              INTEGER PRIMARY KEY,
                source          TEXT,
                target_cleaned  TEXT,
                score           INTEGER,
                key_ingredient	TEXT,
                seasonality	    TEXT,
                ingredient	    TEXT,
                accompaniment   TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS flavor_bible_normalized (
                id              INTEGER PRIMARY KEY,
                ingredient      TEXT,
                pairing         TEXT,
                score           INTEGER,
                key_ingredient  TEXT,
                seasonality     TEXT,
                accompaniment   TEXT,
                preparation     TEXT
            )
        """)

        # ----------------------------------------------------------------
        # Pantry / fridge
        # ----------------------------------------------------------------

        c.execute("""
            CREATE TABLE IF NOT EXISTS pantry_items (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_id   INTEGER,
                ingredient_name TEXT,
                quantity        TEXT,
                unit            TEXT,
                FOREIGN KEY(ingredient_id) REFERENCES ingredients(id)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS fridge_items (
                id              INTEGER PRIMARY KEY,
                ingredient_id   INTEGER,
                ingredient_name TEXT,
                FOREIGN KEY(ingredient_id) REFERENCES ingredients(id)
            )
        """)

        conn.commit()


def main():
    try:
        init_db()

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("""
                SELECT count(*)
                FROM sqlite_master
                WHERE type = 'table'
            """)
            table_count = c.fetchone()[0]

        print(f"{DB_PATH.name} initialised with {table_count} tables")

    except Exception:
        print("database failed to initialise")
        raise


if __name__ == "__main__":
    main()