import sqlite3
import os


DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")


def create_views():

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Drop views if they exist
    c.execute("DROP VIEW IF EXISTS available_ingredients")
    c.execute("DROP VIEW IF EXISTS recipe_match_scores")
    c.execute("DROP VIEW IF EXISTS ingredient_normalization_debug")

    # All ingredient identities available in either pantry or fridge
    c.execute("""
    CREATE VIEW available_ingredients AS
    SELECT DISTINCT ingredient_id
    FROM pantry_items
    WHERE ingredient_id IS NOT NULL

    UNION

    SELECT DISTINCT ingredient_id
    FROM fridge_items
    WHERE ingredient_id IS NOT NULL;
    """)

    c.execute("""
        CREATE VIEW recipe_match_scores AS
        WITH pantry_ingredients AS (
            SELECT DISTINCT ingredient_id
            FROM pantry_items
            WHERE ingredient_id IS NOT NULL
        ),

        fridge_ingredients AS (
            SELECT DISTINCT ingredient_id
            FROM fridge_items
            WHERE ingredient_id IS NOT NULL
        ),

        available_ingredients AS (
            SELECT ingredient_id
            FROM pantry_ingredients

            UNION

            SELECT ingredient_id
            FROM fridge_ingredients
        )

        SELECT
            r.id AS recipe_id,
            r.recipe_name,

            COUNT(DISTINCT ri.id) AS total_ingredients,

            COUNT(DISTINCT CASE
                WHEN ri.ingredient_id IN (
                    SELECT ingredient_id
                    FROM available_ingredients
                )
                THEN ri.id
            END) AS matched_ingredients,

            ROUND(
                1.0 * COUNT(DISTINCT CASE
                    WHEN ri.ingredient_id IN (
                        SELECT ingredient_id
                        FROM available_ingredients
                    )
                    THEN ri.id
                END)
                / NULLIF(COUNT(DISTINCT ri.id), 0),
                2
            ) AS kitchen_match_ratio,

            COUNT(DISTINCT CASE
                WHEN ri.ingredient_id IN (
                    SELECT ingredient_id
                    FROM fridge_ingredients
                )
                THEN ri.id
            END) AS fridge_ingredients_used,

            COUNT(DISTINCT CASE
                WHEN ri.ingredient_id IN (
                    SELECT ingredient_id
                    FROM pantry_ingredients
                )
                THEN ri.id
            END) AS pantry_ingredients_used,

            GROUP_CONCAT(DISTINCT CASE
                WHEN ri.ingredient_id IN (
                    SELECT ingredient_id
                    FROM fridge_ingredients
                )
                THEN i.ingredient_name
            END) AS fridge_ingredients,

            GROUP_CONCAT(DISTINCT CASE
                WHEN ri.ingredient_id IN (
                    SELECT ingredient_id
                    FROM pantry_ingredients
                )
                THEN i.ingredient_name
            END) AS pantry_ingredients,

            GROUP_CONCAT(DISTINCT CASE
                WHEN ri.ingredient_id NOT IN (
                    SELECT ingredient_id
                    FROM available_ingredients
                )
                THEN i.ingredient_name
            END) AS missing_ingredients

        FROM recipes r
        JOIN recipe_ingredients ri
            ON r.id = ri.recipe_id
        JOIN ingredients i
            ON i.id = ri.ingredient_id

        GROUP BY
            r.id,
            r.recipe_name

        ORDER BY
            fridge_ingredients_used DESC,
            pantry_ingredients_used DESC,
            kitchen_match_ratio DESC;
        """)

    c.execute("""
    CREATE VIEW ingredient_normalization_debug AS
    SELECT
        p.id                 AS parsed_line_id,
        p.recipe_name,
        p.raw_text,
        n.ingredient_name_raw,
        n.status              AS name_status,        -- 'ok' | 'empty' | 'reduced_to_nothing'
        n.ingredient_name      AS normalized_name,
        CASE WHEN ri.ingredient_id IS NULL THEN 'UNMATCHED' ELSE 'MATCHED' END AS match_status,
        ing.ingredient_name     AS matched_ingredient,
        (
            SELECT group_concat(at.name || '=' || COALESCE(av.value, iav.value_text), ', ')
            FROM instance_attribute_value iav
            JOIN attribute_type at ON at.id = iav.attribute_type_id
            LEFT JOIN attribute_value av ON av.id = iav.value_id
            WHERE iav.recipe_ingredient_id = ri.id
        ) AS attributes_found
    FROM recipe_ingredient_lines_parsed p
    JOIN ingredient_normalizations n ON n.parsed_line_id = p.id
    LEFT JOIN recipe_ingredients ri  ON ri.parsed_line_id = p.id
    LEFT JOIN ingredients ing        ON ing.id = ri.ingredient_id;
    """)

    conn.commit()
    conn.close()

    print("Views created")