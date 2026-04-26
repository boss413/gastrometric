import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gastrometric.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

fridge = ['cabbage', 'scallions', 'mushrooms', 'bean sprouts',
          'cauliflower', 'crimini', 
          ]

fridge_list = ['cabbage', 'scallions', 'bean sprouts', 'cauliflower', 'mushrooms', 'crimini']

for item in fridge_list:
    # Lookup ingredient ID first
    c.execute("SELECT id FROM ingredients WHERE ingredient_name = ?", (item,))
    row = c.fetchone()
    if row:
        ingredient_id = row[0]
        c.execute(
            "INSERT INTO fridge_items (ingredient_id, ingredient_name) VALUES (?, ?)",
            (ingredient_id, item)
        )
    else:
        print(f"WARNING: {item} not found in ingredients table")

pantry = [
    "all purpose flour", "bread flour", "quinoa", "millet", "brown basmati rice",
    "brown rice", "potato starch", "corn starch", "corn flour", "corn meal", "gelatin",
    "long grain rice", "basmati rice", "sushi rice", "short grain rice",
    "black beans", "pinto beans", "garbanzo beans", "chickpeas",
    "bread crumbs", "instant potato flakes", "sliced wheat bread", "flour tortillas",
    "macaroni", "rigatoni", "spaghetti", "fettuccini", "ramen noodles",
    "rice noodles", "bean threads", "rice sticks", "orzo",
    "honey", "molasses", "sugar", "brown sugar", "white sugar",
    "marshmallows", "cocoa", "salt", "baking powder", "baking soda", "yeast", "vanilla extract",
    "msg", "corn syrup", "maple syrup", "agave nectar",
    
    "white vinegar", "balsamic vinegar", "sherry vinegar", "citric acid",
    "lemon juice", "lime juice", "shaoxing wine", "water", "stock", "broth", 
    "chicken broth", "beef broth", "vegetable broth",
    
    "soy sauce", "oyster sauce", "fish sauce", "Worcestershire sauce",
    "chicken base", "chicken bouillon", "beef base", "beef bouillon",
    
    "crushed tomatoes", "diced tomatoes", "spam", "whole peeled tomatoes",
    "bamboo shoots", "water chestnuts", "diced chilis", "coconut milk", "tomato paste",
    
    "onions", "carrots", "celery", "garlic", "ginger",
    "oregano", "thyme", "cumin", "paprika", "coriander",
    "chili powder", "sage", "fennel seed", "mustard", "ketchup",
    "chili flakes", "cardamom", "cinnamon", "tumeric", "curry powder", "cayenne",
    
    "parmesan cheese", "mozzarella cheese", "mexican cheese blend", "string cheese",
    "milk", "eggs", "whole eggs", "large eggs", "medium eggs",
    "egg yolks", "egg whites", "beer", "red wine", "white wine",
    "butter", "mayonnaise", "sour cream", "yogurt", "cream cheese",
    "vegetable oil", "olive oil", "canola oil", "sesame oil", "coconut oil", 
    "lard", "peanut butter", "peanuts", "cashews", 
    
    "frozen peas", "frozen corn", "frozen spinach", "green beans",
    "chicken legs", "chicken thighs", "chicken", "pork chop",
    "ground beef", "ribeye steak", "chicken stock", "shrimp",
    "frozen mixed berries", "basil", "tofu"
]

for name in fridge:
    c.execute("SELECT id FROM ingredients WHERE ingredient_name = ?", (name,))
    row = c.fetchone()
    if row:
        c.execute("INSERT INTO fridge_items (ingredient_id, ingredient_name) VALUES (?, ?)", (row[0], name))

for name in pantry:
    c.execute("SELECT id FROM ingredients WHERE ingredient_name = ?", (name,))
    row = c.fetchone()
    if row:
        c.execute("INSERT INTO pantry_items (ingredient_id, ingredient_name) VALUES (?, ?)", (row[0], name))

conn.commit()
conn.close()

print("Kitchen seeded")