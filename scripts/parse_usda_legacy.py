import ijson
import pandas as pd

foods = []
nutrients = []
portions = []

with open("data/usda/raw/FoodData_Central_sr_legacy_food_json_2018-04.json", "rb") as f:
    parser = ijson.items(f, "SRLegacyFoods.item")

    for food in parser:
        fdc_id = food.get("fdcId")
        desc = food.get("description")
        data_type = food.get("dataType")
        category = (food.get("foodCategory") or {}).get("description")

        foods.append({
            "fdc_id": fdc_id,
            "description": desc,
            "data_type": data_type,
            "category": category
        })

        # nutrients
        for n in food.get("foodNutrients", []):
            nutrient = n.get("nutrient", {})
            nutrients.append({
                "fdc_id": fdc_id,
                "nutrient_id": nutrient.get("id"),
                "nutrient_name": nutrient.get("name"),
                "unit": nutrient.get("unitName"),
                "amount": n.get("amount")
            })

        # portions (optional)
        for p in food.get("foodPortions", []):
            unit = (p.get("measureUnit") or {}).get("abbreviation")
            portions.append({
                "fdc_id": fdc_id,
                "gram_weight": p.get("gramWeight"),
                "unit": unit
            })

foods_df = pd.DataFrame(foods)
nutrients_df = pd.DataFrame(nutrients)
portions_df = pd.DataFrame(portions)

foods_df.to_parquet("data/usda/processed/legacy_foods.parquet")
nutrients_df.to_parquet("data/usda/processed/legacy_nutrients.parquet")
portions_df.to_parquet("data/usda/processed/legacy_portions.parquet")

print(len(foods_df))              # expect a few thousand
print(nutrients_df.shape)         # expect 100k+ rows
print(foods_df.head())