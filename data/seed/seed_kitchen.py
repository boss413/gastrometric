import sqlite3

from gastrometric.config.paths import DB_PATH


def resolve_ingredient(cursor, name):
    """
    Resolve an ingredient name to an ingredient ID.

    Matching order:
    1. Exact match against ingredients.ingredient_name
    2. Exact match against ingredient_aliases.alias

    Returns:
        tuple[int, str] | None:
            (ingredient_id, match_type), or None if no match is found.
    """
    cursor.execute(
        """
        SELECT id
        FROM ingredients
        WHERE LOWER(TRIM(ingredient_name)) = LOWER(TRIM(?))
        """,
        (name,),
    )
    row = cursor.fetchone()

    if row:
        return row[0], "ingredient_name"

    cursor.execute(
        """
        SELECT ingredient_id
        FROM ingredient_aliases
        WHERE LOWER(TRIM(alias)) = LOWER(TRIM(?))
        ORDER BY confidence DESC, id ASC
        LIMIT 1
        """,
        (name,),
    )
    row = cursor.fetchone()

    if row:
        return row[0], "alias"

    return None


def seed_kitchen():
    fridge = [
        "cabbage",
        "scallions",
        "mushrooms",
        "broccoli",
        "criminis",
        "parsley",
        "zucchini",
        "yellow squash",
    ]

    pantry = [

#Dry Goods
        "all purpose flour", "bread flour", "quinoa", "millet", "brown basmati rice",
        "brown rice", "potato starch", "corn starch", "corn flour", "corn meal", "gelatin",
        "long grain rice", "basmati rice", "sushi rice", "short grain rice", "dry short grain white rice",
        "black beans", "pinto beans", "garbanzo beans", "chickpeas",
        "bread crumbs", "instant potato flakes", "sliced wheat bread", "flour tortillas",
        "macaroni", "rigatoni", "spaghetti", "fettuccini", "ramen noodles",
        "rice noodles", "bean threads", "rice sticks", "orzo",
        "honey", "molasses", "sugar", "granulated sugar", "brown sugar", "white sugar",
        "marshmallows", "cocoa", "salt", "baking powder", "baking soda", "yeast",
        "vanilla extract", "msg", "corn syrup", "maple syrup", "agave nectar", "fried onions",
        "semi-sweet chocolate", "powdered sugar", "oil", "neutral oil",
#Acid
        "white vinegar", "balsamic vinegar", "sherry vinegar", "citric acid",
        "lemon juice", "lime juice", "shaoxing wine", "water", "stock", "broth",
        "chicken broth", "beef broth", "vegetable broth", "hot sauce", "rice vinegar", "tamarind paste",
        "sriracha", "red wine vinegar",
#Salt
        "soy sauce", "dark soy sauce", "oyster sauce", "fish sauce", "Worcestershire sauce",
        "chicken base", "chicken bouillon", "beef base", "beef bouillon", "liquid aminos", 
        "diamond crystal kosher salt",
#Canned
        "crushed tomatoes", "diced tomatoes", "spam", "whole peeled tomatoes", "evaporated milk", 
        "bamboo shoots", "water chestnuts", "diced chilis", "coconut milk", "tomato paste", "crab meat",
#Aromatics
        "onions", "carrots", "celery", "garlic", "ginger", "yellow onions",
        "oregano", "thyme", "cumin", "paprika", "coriander", "rosemary", "mustard seed",
        "chili powder", "sage", "fennel seed", "mustard", "ketchup", "dijon mustard",
        "chili flakes", "cardamom", "cinnamon", "tumeric", "curry powder", "cayenne",
        "black pepper", "kosher salt", "savory salt", "garam masala", "vanilla", "chocolate chips",
        "allspice", "bay leaf", "ground ginger", "gochujang", "saffron", "cloves", "dried oregano", 
        "star anise", "coconut extract", "green cardamom", "cinnamon sticks", "tumeric"
#Fridge
        "parmesan cheese", "mozzarella cheese", "mexican cheese blend", "string cheese",
        "whole milk", "eggs", "whole eggs", "large eggs", "medium eggs", "milk",
        "egg yolks", "egg whites", "beer", "red wine", "white wine", "ale", "lager"
        "butter", "mayonnaise", "sour cream", "yogurt", "cream cheese",
        "vegetable oil", "olive oil", "canola oil", "sesame oil", "coconut oil",
        "lard", "peanut butter", "peanuts", "cashews", "sesame seeds",
#Freezer
        "frozen peas", "frozen corn", "frozen spinach", "green beans", "spinach", 
        "chicken legs", "chicken thighs", "chicken", "pork chop",
        "ground beef", "ribeye steak", "chicken stock", "shrimp",
        "frozen mixed berries", "basil", "tofu", "bacon", "tater tots"
    ]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    fridge_successes = 0
    pantry_successes = 0
    failures = []

    try:
        for item in fridge:
            resolved = resolve_ingredient(cursor, item)

            if resolved is None:
                failures.append(("fridge", item))
                continue

            ingredient_id, _ = resolved

            cursor.execute(
                """
                INSERT INTO fridge_items (ingredient_id, ingredient_name)
                VALUES (?, ?)
                """,
                (ingredient_id, item),
            )

            fridge_successes += 1

        for item in pantry:
            resolved = resolve_ingredient(cursor, item)

            if resolved is None:
                failures.append(("pantry", item))
                continue

            ingredient_id, _ = resolved

            cursor.execute(
                """
                INSERT INTO pantry_items (ingredient_id, ingredient_name)
                VALUES (?, ?)
                """,
                (ingredient_id, item),
            )

            pantry_successes += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    total_successes = fridge_successes + pantry_successes

    print(
        f"Kitchen seeded: {total_successes} successful matches "
        f"({fridge_successes} fridge, {pantry_successes} pantry)"
    )

    print(f"Failed ingredient matches: {len(failures)}")

    if failures:
        print("Failure examples:")

        for location, item in failures[:8]:
            print(f"  - {location}: {item}")

        if len(failures) > 8:
            print(f"  ... and {len(failures) - 8} more")