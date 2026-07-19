install:
ijson
pandas
pyarrow
datasette

The current state of this project is en route to a Proof of Concept such that the user can:

1. list the items in their "pantry" (non-perishable, always replenished when they run out) and "fridge" (perishable, finite) and find recipes that can be cooked, returning a list that maximizes "using up the food in the fridge"
2. see the nutritional information of a serving of a presented recipe
3. view a recipe on their phone

after installing requirements.txt, edit data/seed/seed_kitchen.py, run "python -m gastrometric.orchestration.rebuild_db" to build the database of recipes from data/seed/recipes.md.