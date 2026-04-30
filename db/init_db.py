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
        # Ingredient identity + canonical resolution
        # ----------------------------------------------------------------

        # All unique ingredient names and their recipe appearance count.
        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredients (
                id              INTEGER PRIMARY KEY,
                ingredient_name TEXT UNIQUE,
                canonical_group TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS canonical_ingredients (
                id          TEXT PRIMARY KEY,
                name        TEXT,
                base_food   TEXT,
                state       TEXT,
                form        TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS canonical_lookup (
                normalized_alias    TEXT PRIMARY KEY,
                canonical_id        TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredient_aliases (
                id              INTEGER PRIMARY KEY,
                raw_text        TEXT UNIQUE,
                canonical_group TEXT,
                alias           TEXT,
                entity_id       INTEGER,
                canonical_id    TEXT,
                confidence      INTEGER,
                source          TEXT    -- 'rule' | 'manual' | 'auto'
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS usda_source_map (
                fdc_id          INTEGER,
                canonical_id    TEXT
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
                canonical_id        TEXT,
                FOREIGN KEY(recipe_id)         REFERENCES recipes(id),
                FOREIGN KEY(recipe_section_id) REFERENCES recipe_sections(id)
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