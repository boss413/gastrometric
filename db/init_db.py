import sqlite3
from gastrometric.config.paths import DB_PATH, DATA_DIR
from pathlib import Path
DEFAULT_DB_PATH = Path("gastrometric.db")
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
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_ingredient_lines_raw (
                id                  INTEGER PRIMARY KEY,
                ingredient_block_id INTEGER NOT NULL,
                recipe_id           INTEGER NOT NULL,
                recipe_section_id   INTEGER NOT NULL,
                recipe_name         TEXT NOT NULL,
                section_name        TEXT,
                line_index          INTEGER NOT NULL,
                raw_text            TEXT NOT NULL,

                FOREIGN KEY (ingredient_block_id) REFERENCES recipe_ingredient_blocks(id),
                FOREIGN KEY (recipe_id)           REFERENCES recipes(id),
                FOREIGN KEY (recipe_section_id)   REFERENCES recipe_sections(id))
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
        # Knowledge Tables
        # ----------------------------------------------------------------
        c.execute("""
            CREATE TABLE IF NOT EXISTS culinary_sources (
                source_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name                TEXT NOT NULL UNIQUE,
                version             TEXT,
                description         TEXT,
                authority_level     INTEGER NOT NULL DEFAULT 100,
                created_at          TEXT DEFAULT (datetime('now'))
            )  
        """)
        c.execute("""
            CREATE TABLE vocabulary_terms (
                term_id       TEXT PRIMARY KEY,
                term          TEXT NOT NULL UNIQUE,
                created_at    TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        c.execute("""
            CREATE TABLE vocabulary_classes (
                class_id      TEXT PRIMARY KEY,
                class_name    TEXT NOT NULL UNIQUE
            )
        """)

        c.execute("""
            CREATE TABLE vocabulary_term_classes (
                term_id       TEXT NOT NULL,
                class_id      TEXT NOT NULL,
                PRIMARY KEY (term_id, class_id),
                FOREIGN KEY (term_id) REFERENCES vocabulary_terms(term_id),
                FOREIGN KEY (class_id) REFERENCES vocabulary_classes(class_id)     
            )
        """)

        c.execute("""
            CREATE INDEX idx_vtc_class 
            ON vocabulary_term_classes(class_id)      
        """)

        c.execute("""
            CREATE INDEX idx_vtc_term 
            ON vocabulary_term_classes(term_id)
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
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            recipe_id                 INTEGER NOT NULL,
            recipe_section_id         INTEGER NOT NULL,
            ingredient_block_id       INTEGER NOT NULL,
            recipe_ingredient_line_id INTEGER NOT NULL,

            ingredient_id TEXT,
            ingredient_phrase TEXT,
            ingredient_name_original TEXT,

            grams REAL,
            ml REAL,

            imperial_weight_value REAL,
            imperial_weight_unit TEXT,

            imperial_volume_value REAL,
            imperial_volume_unit TEXT,

            natural_portion_value REAL,
            natural_portion_min REAL,
            natural_portion_max REAL,
            natural_portion TEXT,

            packaging_count REAL,
            packaging_size_value REAL,
            packaging_size_unit TEXT,
            packaging TEXT,

            preparation TEXT,  
            notes TEXT,

            optional INTEGER NOT NULL DEFAULT 0,
            alt_group_id TEXT,
            alt_kind TEXT,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY(recipe_ingredient_line_id) REFERENCES recipe_ingredient_lines_raw(id),
            FOREIGN KEY(ingredient_block_id)       REFERENCES recipe_ingredient_blocks(id),
            FOREIGN KEY(recipe_id)                 REFERENCES recipes(id),
            FOREIGN KEY(recipe_section_id)         REFERENCES recipe_sections(id)
            )
        """)

        c.execute("""
           CREATE TABLE IF NOT EXISTS lexical_spans (
            span_id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_ingredient_line_id   INTEGER NOT NULL
                REFERENCES recipe_ingredient_lines_raw(id),
            span_order                  INTEGER NOT NULL,
            start_offset                INTEGER NOT NULL,
            end_offset                  INTEGER NOT NULL,
            text                        TEXT NOT NULL,
            normalized_value            TEXT,
            span_type                   TEXT NOT NULL,
            knowledge_id                TEXT,
            source_vocabulary           TEXT)
        """)

        c.execute("""
            CREATE INDEX idx_lexical_spans_line
                ON lexical_spans(recipe_ingredient_line_id, span_order);
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

# Parser refactor to perform no semantic understanding, only grammatical, outputs to these tables

        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredient_parse_trees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_ingredient_line_id INTEGER NOT NULL,
                parse_tree_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(recipe_ingredient_line_id) REFERENCES recipe_ingredient_lines_raw(id))
        """)

        c.execute("""
            CREATE INDEX idx_ingredient_parse_trees_line
                ON ingredient_parse_trees(recipe_ingredient_line_id);
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS observation_roles (
            role_code             TEXT PRIMARY KEY,
            description           TEXT NOT NULL)
        """)
        for role_code, description in [
            ("primary_ingredient", "Span identifying the candidate ingredient itself"),
            ("quantity",           "Span supplying the numeric quantity"),
            ("measurement",        "Span supplying the unit of measurement"),
            ("package",            "Span describing packaging (e.g. \"can\", \"jar\")"),
            ("preparation",        "Span describing preparation (e.g. \"finely diced\")"),
            ("modifier",           "Span describing a descriptive attribute (e.g. \"yellow\")"),
            ("dimension",          "Span describing size/dimension (e.g. \"large\")"),
            ("temperature",        "Span describing temperature (e.g. \"room temperature\")"),
            ("brand",              "Span naming a brand"),
            ("unknown",            "Span the builder attached but could not classify further"),
        ]:
            c.execute("""
                INSERT OR IGNORE INTO observation_roles (role_code, description)
                VALUES (?, ?)
            """, (role_code, description))

        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredient_observations (
            observation_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_ingredient_id  INTEGER NOT NULL
                REFERENCES recipe_ingredient_lines_parsed(id),
            observation_index     INTEGER NOT NULL,
            UNIQUE (recipe_ingredient_id, observation_index))
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingredient_observations_line
                ON ingredient_observations(recipe_ingredient_id)
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS observation_spans (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id        INTEGER NOT NULL
                REFERENCES ingredient_observations(observation_id),
            span_id               INTEGER NOT NULL
                REFERENCES recipe_ingredient_spans(span_id),
            role_code             TEXT NOT NULL
                REFERENCES observation_roles(role_code),
            UNIQUE (observation_id, span_id, role_code))
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_observation_spans_observation
                ON observation_spans(observation_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_observation_spans_span
                ON observation_spans(span_id)
        """)

        # relationships. this section used to be ingredient_normalizations
        # When a future work order says this file needs to be cleaned up, move 
        # this section so that it fits before those tables required for the analyzer

        c.execute("""
            CREATE TABLE IF NOT EXISTS relationships (
                relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,

                subject_type    TEXT NOT NULL,
                subject_id      TEXT NOT NULL,

                predicate       TEXT NOT NULL,

                object_type     TEXT NOT NULL,
                object_id       TEXT NOT NULL,

                source          TEXT,

                confidence      REAL
                    CHECK (
                        confidence IS NULL
                        OR (confidence >= 0.0 AND confidence <= 1.0)
                    ),

                created_at      TEXT NOT NULL DEFAULT (datetime('now')),

                UNIQUE (
                    subject_type,
                    subject_id,
                    predicate,
                    object_type,
                    object_id
                )
            )
        """)

        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_relationships_subject
                ON relationships (
                    subject_type,
                    subject_id
                    )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_relationships_object
                ON relationships (
                    object_type,
                    object_id
                    )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_relationships_subject_predicate
                ON relationships (
                    subject_type,
                    subject_id,
                    predicate
                    )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_relationships_object_predicate
                ON relationships (
                    object_type,
                    object_id,
                    predicate
                    )
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
                id              STR PRIMARY KEY,
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

        # Analysis Record: one row per Analyzer execution against one persisted
        # parse tree. canonical_result_json is the complete, unmodified return
        # value of analyze_parse_result() -- the authoritative persisted
        # Canonical Semantic Result (R0-6). status/selected_interpretation_id
        # are denormalized copies of fields already inside that JSON, kept only
        # for filtering/listing without parsing JSON per row.
        c.execute("""
            CREATE TABLE IF NOT EXISTS analysis_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_ingredient_line_id INTEGER NOT NULL,
                parse_tree_id INTEGER NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('resolved', 'ambiguous', 'unresolved', 'invalid')),
                selected_interpretation_id TEXT,
                canonical_result_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(recipe_ingredient_line_id) REFERENCES recipe_ingredient_lines_raw(id),
                FOREIGN KEY(parse_tree_id) REFERENCES ingredient_parse_trees(id))
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_analysis_records_line
            ON analysis_records(recipe_ingredient_line_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_analysis_records_parse_tree
            ON analysis_records(parse_tree_id)
        """)

        # Candidate evaluation ledger: one row per interpretation the Analyzer
        # produced for an analysis record. There is no independent candidate
        # identity in the parser or analyzer -- interpretation_id is the sole
        # identity for the 1:1 candidate/evaluation, so it is required and
        # unique per analysis record. evaluation_state mirrors the analyzer
        # interpretation status values (not a separate candidate-side vocabulary).
        c.execute("""
            CREATE TABLE IF NOT EXISTS analysis_candidate_evaluations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_record_id INTEGER NOT NULL,
                interpretation_id TEXT NOT NULL,
                evaluation_state TEXT NOT NULL
                    CHECK (evaluation_state IN ('resolved', 'ambiguous', 'unresolved', 'invalid')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(analysis_record_id) REFERENCES analysis_records(id))
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_analysis_candidate_evaluations_record
            ON analysis_candidate_evaluations(analysis_record_id)
        """)
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_analysis_candidate_evaluations_record_interpretation
            ON analysis_candidate_evaluations(analysis_record_id, interpretation_id)
        """)

        # Evidence projection: normalized copy of R0-6 evidence objects, scoped
        # to the evaluation row for their interpretation. Query-only projection
        # for knowledge-provenance lookups (e.g. "which analyses used
        # relationship 17 as evidence") -- not a second source of truth; the
        # authoritative evidence lives inside canonical_result_json.
        c.execute("""
            CREATE TABLE IF NOT EXISTS analysis_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_candidate_evaluation_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                record_id TEXT NOT NULL,
                effect TEXT NOT NULL
                    CHECK (effect IN ('supporting', 'detracting')),
                FOREIGN KEY(analysis_candidate_evaluation_id)
                    REFERENCES analysis_candidate_evaluations(id))
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_analysis_evidence_evaluation
            ON analysis_evidence(analysis_candidate_evaluation_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_analysis_evidence_kind_record
            ON analysis_evidence(kind, record_id)
        """)

        # # ----------------------------------------------------------------
        # # Ingredient attributes -- note for the next review, I believe this 
        # # is no longer used and can be deleted. please verify and suggest.
        # #
        # # An attribute is anything that varies about an identity WITHOUT
        # # changing what identity it is (raw/cooked, bone-in/boneless,
        # # salted/unsalted, brand). Whether an attribute is safe to ignore
        # # during fridge-matching ("decorative") or must match exactly
        # # ("required", e.g. brand for kosher salt — Diamond Crystal and
        # # Morton differ ~2x by volume) is asserted per (ingredient,
        # # attribute) pair in identity_attribute_rule, not globally per
        # # attribute — brand is decorative for canned tomatoes but
        # # required for kosher salt.
        # # ----------------------------------------------------------------
        # c.execute("""
        #     CREATE TABLE IF NOT EXISTS attribute_type (
        #         id              INTEGER PRIMARY KEY,
        #         name            TEXT UNIQUE NOT NULL,
        #         value_kind      TEXT NOT NULL DEFAULT 'enum'
        #                             CHECK (value_kind IN ('enum', 'free_text')),
        #         description     TEXT
        #     )
        # """)
        # c.execute("""
        #     CREATE TABLE IF NOT EXISTS attribute_value (
        #         id                  INTEGER PRIMARY KEY,
        #         attribute_type_id   INTEGER NOT NULL,
        #         value               TEXT NOT NULL,
        #         UNIQUE (attribute_type_id, value),
        #         FOREIGN KEY(attribute_type_id) REFERENCES attribute_type(id)
        #     )
        # """)
        # c.execute("""
        #     CREATE TABLE IF NOT EXISTS identity_attribute_rule (
        #         id                      INTEGER PRIMARY KEY,
        #         ingredient_id           INTEGER NOT NULL,
        #         attribute_type_id       INTEGER NOT NULL,
        #         required_for_match      INTEGER NOT NULL DEFAULT 0,
        #         default_value_id        INTEGER,
        #         UNIQUE (ingredient_id, attribute_type_id),
        #         FOREIGN KEY(ingredient_id)      REFERENCES ingredients(id),
        #         FOREIGN KEY(attribute_type_id)  REFERENCES attribute_type(id),
        #         FOREIGN KEY(default_value_id)   REFERENCES attribute_value(id)
        #     )
        # """)
        # # Per-identity restriction of an enum attribute's otherwise-global
        # # value list (e.g. chicken breast's `state` is only ever
        # # raw/cooked, even though the global `state` vocabulary also
        # # includes partially_cooked/undercooked/overcooked for identities
        # # where those distinctions matter). No rows for a given rule
        # # means the full global value list is allowed.
        # c.execute("""
        #     CREATE TABLE IF NOT EXISTS identity_attribute_allowed_value (
        #         id                          INTEGER PRIMARY KEY,
        #         identity_attribute_rule_id  INTEGER NOT NULL,
        #         value_id                    INTEGER NOT NULL,
        #         UNIQUE (identity_attribute_rule_id, value_id),
        #         FOREIGN KEY(identity_attribute_rule_id) REFERENCES identity_attribute_rule(id),
        #         FOREIGN KEY(value_id)                   REFERENCES attribute_value(id)
        #     )
        # """)
        # # Attribute values actually observed on one resolved recipe
        # # ingredient mention (recipe_ingredients row below).
        # c.execute("""
        #     CREATE TABLE IF NOT EXISTS instance_attribute_value (
        #         id                      INTEGER PRIMARY KEY,
        #         recipe_ingredient_id    INTEGER NOT NULL,
        #         attribute_type_id       INTEGER NOT NULL,
        #         value_id                INTEGER,
        #         value_text              TEXT,
        #         source                  TEXT DEFAULT 'parsed'
        #                             CHECK (source IN ('parsed', 'defaulted', 'inferred_from_step')),
        #         UNIQUE (recipe_ingredient_id, attribute_type_id),
        #         FOREIGN KEY(recipe_ingredient_id) REFERENCES recipe_ingredients(id),
        #         FOREIGN KEY(attribute_type_id)    REFERENCES attribute_type(id),
        #         FOREIGN KEY(value_id)             REFERENCES attribute_value(id)
        #     )
        # """)
        from gastrometric.pipeline.enrichment.usda.schema_usda import create_usda_tables
        create_usda_tables(conn)
        # usda_food_portions now exists (created by create_usda_tables above) —
        # this index must be created after that call, not before it.
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_usda_portions_modifier
            ON usda_food_portions(modifier)
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
        # Flavor bible relationships. the name of this table was too generic
        # ----------------------------------------------------------------
        c.execute("""
            CREATE TABLE IF NOT EXISTS flavor_bible_relationships (
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
        # ----------------------------------------------------------------
        # Nutrition Calculation
        # ----------------------------------------------------------------
       # One row per approved ingredient -> USDA mapping. Only mappings
        # with status = 'approved' in data/nutrition_mappings.json are
        # ever persisted here; 'unresolved' / missing mappings are
        # skipped entirely by the ingestion step.
        c.execute("""
            CREATE TABLE IF NOT EXISTS nutrition_ingredient_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
                mapping_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'approved' CHECK (status = 'approved'),
                source TEXT NOT NULL,
                -- default_fdc_id/raw_fdc_id/cooked_fdc_id are NOT DB-level
                -- foreign keys: usda_foods is a pre-existing table built by
                -- the USDA ingestion script, and we can't assume fdc_id
                -- carries a declared PRIMARY KEY / UNIQUE constraint there
                -- (usda_food_portions.id turned out not to -- see the
                -- comment on nutrition_mapping_portions below). A REFERENCES
                -- clause against a column without such a constraint raises
                -- "foreign key mismatch" the first time the table is
                -- touched under PRAGMA foreign_keys = ON.
                default_fdc_id INTEGER,
                raw_fdc_id INTEGER,
                cooked_fdc_id INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                CHECK (default_fdc_id IS NOT NULL OR raw_fdc_id IS NOT NULL)
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_nutrition_mappings_ingredient_id
            ON nutrition_ingredient_mappings(ingredient_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_nutrition_mappings_default_fdc_id
            ON nutrition_ingredient_mappings(default_fdc_id)
        """)
        # Not UNIQUE: two distinct mapping_key entries in
        # nutrition_mappings.json could plausibly resolve to the same
        # canonical ingredient_id (e.g. near-duplicate ingredient
        # names). Ingestion enforces "at most one mapping per
        # ingredient per rebuild" deterministically in Python (see
        # ingest_mappings.py) rather than via a DB constraint, so a
        # data-quality collision surfaces as a diagnostic instead of a
        # crash.
 
        # Declares which specific usda_food_portions row satisfies which
        # nominal recipe measurement ("modifier") for a mapped USDA
        # food -- e.g. "tbsp" -> usda_food_portions row 85577. The gram
        # weight itself always lives in usda_food_portions; this table
        # only records which portion row applies. No food-specific
        # conversion factors are hard-coded here or in Python.
        #
        # state ties each portion to the specific fdc_id it was declared
        # under in nutrition_mappings.json ('default' -> default_fdc_id,
        # 'raw' -> raw_fdc_id, 'cooked' -> cooked_fdc_id). This is required
        # because a single ingredient mapping can have different USDA
        # portions per state (e.g. brussels sprouts "cup, raw" vs
        # "cup, cooked" are different usda_food_portions rows with
        # different gram weights) -- without this column there would be
        # no way to tell a raw portion from a cooked one once both are
        # persisted under the same mapping_id.
        #
        # usda_portion_id is NOT a DB-level foreign key: usda_food_portions
        # is a pre-existing table populated by the USDA ingestion script,
        # and its "id" column isn't declared PRIMARY KEY / UNIQUE there,
        # so SQLite can't validate a REFERENCES clause against it
        # ("foreign key mismatch"). Referential integrity is instead
        # enforced in Python at ingestion time (see ingest_mappings.py),
        # which checks the portion exists and belongs to the exact fdc_id
        # for its declared state before inserting.
        c.execute("""
            CREATE TABLE IF NOT EXISTS nutrition_mapping_portions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mapping_id INTEGER NOT NULL REFERENCES nutrition_ingredient_mappings(id)
                    ON DELETE CASCADE,
                state TEXT NOT NULL DEFAULT 'default'
                    CHECK (state IN ('default', 'raw', 'cooked')),
                usda_portion_id INTEGER NOT NULL,
                modifier TEXT,
                notes TEXT
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_nutrition_mapping_portions_mapping_state
            ON nutrition_mapping_portions(mapping_id, state)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_nutrition_mapping_portions_usda_portion_id
            ON nutrition_mapping_portions(usda_portion_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_nutrition_mapping_portions_mapping_id
            ON nutrition_mapping_portions(mapping_id)
        """)
 
        # -------------------------------------------------------------
        # NEW: nutrition result tables (line / section / recipe)
        # -------------------------------------------------------------
 
        # Level 1: per ingredient-line nutrition. Preserves everything
        # needed to debug a single line's resolution independent of
        # section/recipe aggregation.
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_ingredient_line_nutrition (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_ingredient_id INTEGER NOT NULL REFERENCES recipe_ingredients(id),
                recipe_id INTEGER NOT NULL,
                recipe_section_id INTEGER,
                ingredient_id INTEGER REFERENCES ingredients(id),
                mapping_id INTEGER REFERENCES nutrition_ingredient_mappings(id),
                -- Not a DB-level FK for the same reason as
                -- nutrition_ingredient_mappings.default_fdc_id above.
                resolved_fdc_id INTEGER,
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
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_line_nutrition_recipe_ingredient_id
            ON recipe_ingredient_line_nutrition(recipe_ingredient_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_line_nutrition_recipe_id
            ON recipe_ingredient_line_nutrition(recipe_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_line_nutrition_section_id
            ON recipe_ingredient_line_nutrition(recipe_section_id)
        """)
 
        # Level 2: per recipe-section nutrition, aggregated from
        # recipe_ingredient_line_nutrition. Persisted as a first-class
        # result so other Gastrometric tools can substitute/reuse a
        # section's nutrition without recomputing it.
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_section_nutrition (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id INTEGER NOT NULL,
                recipe_section_id INTEGER NOT NULL,
                section_name TEXT,
                total_grams REAL,
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
                calculated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_section_nutrition_recipe_id
            ON recipe_section_nutrition(recipe_id)
        """)
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_section_nutrition_recipe_section
            ON recipe_section_nutrition(recipe_id, recipe_section_id)
        """)
 
        # Level 3: whole-recipe nutrition, aggregated from
        # recipe_section_nutrition. No per-serving math happens here.
        c.execute("""
            CREATE TABLE IF NOT EXISTS recipe_nutrition (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recipe_id INTEGER NOT NULL,
                total_grams REAL,
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
                calculated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_recipe_nutrition_recipe_id
            ON recipe_nutrition(recipe_id)
        """)
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