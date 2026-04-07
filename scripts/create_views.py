# scripts/create_views.py

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# drop views if they exist
c.execute("DROP VIEW IF EXISTS available_groups")
c.execute("DROP VIEW IF EXISTS recipe_match_scores")

# available ingredient groups
c.execute("""
CREATE VIEW available_groups AS
SELECT DISTINCT i.canonical_group
FROM ingredients i
JOIN pantry_items p ON i.id = p.ingredient_id

UNION

SELECT DISTINCT i.canonical_group
FROM ingredients i
JOIN fridge_items f ON i.id = f.ingredient_id;
""")

c.execute("""
CREATE VIEW recipe_match_scores AS
WITH available_groups AS (
    SELECT DISTINCT i.canonical_group
    FROM ingredients i
    JOIN pantry_items p ON i.id = p.ingredient_id

    UNION

    SELECT DISTINCT i.canonical_group
    FROM ingredients i
    JOIN fridge_items f ON i.id = f.ingredient_id
),

fridge_groups AS (
    SELECT DISTINCT i.canonical_group
    FROM ingredients i
    JOIN fridge_items f ON i.id = f.ingredient_id
)

SELECT
    r.id,
    r.name,

    COUNT(DISTINCT ri.id) AS total_ingredients,

    -- pantry + fridge match
    COUNT(DISTINCT CASE
        WHEN i.canonical_group IN (SELECT canonical_group FROM available_groups)
        THEN ri.id
    END) AS matched_ingredients,

    ROUND(
        1.0 * COUNT(DISTINCT CASE
            WHEN i.canonical_group IN (SELECT canonical_group FROM available_groups)
            THEN ri.id
        END) / COUNT(DISTINCT ri.id),
        2
    ) AS kitchen_match_ratio,

    -- fridge usage (this is the key new metric)
    COUNT(DISTINCT CASE
        WHEN i.canonical_group IN (SELECT canonical_group FROM fridge_groups)
        THEN ri.id
    END) AS fridge_ingredients_used,

    GROUP_CONCAT(DISTINCT CASE
        WHEN i.canonical_group IN (SELECT canonical_group FROM fridge_groups)
        THEN i.canonical_group
    END) AS fridge_ingredients,

    GROUP_CONCAT(DISTINCT CASE
        WHEN i.canonical_group NOT IN (SELECT canonical_group FROM available_groups)
        THEN i.canonical_group
    END) AS missing_ingredients

FROM recipes r
JOIN recipe_ingredients ri ON r.id = ri.recipe_id
JOIN ingredients i ON ri.ingredient_name = i.name

GROUP BY r.id

ORDER BY
    fridge_ingredients_used DESC,
    kitchen_match_ratio DESC;
""")

conn.commit()
conn.close()

print("Views created")