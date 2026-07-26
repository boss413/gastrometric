"""
Parse the raw USDA SR Legacy JSON dump into flat parquet files
(foods, nutrients, portions) for downstream ingestion into gastrometric.db.
"""
import ijson
import pandas as pd

from gastrometric.config.paths import DATA_DIR

RAW_PATH = DATA_DIR / "usda" / "raw" / "FoodData_Central_sr_legacy_food_json_2018-04.json"
PROCESSED_DIR = DATA_DIR / "usda" / "processed"


def parse_usda_legacy():
    foods = []
    nutrients = []
    portions = []

    with open(RAW_PATH, "rb") as f:
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
                "category": category,
            })

            for n in food.get("foodNutrients", []):
                nutrient = n.get("nutrient", {})
                amount = n.get("amount")
                nutrients.append({
                    "fdc_id": fdc_id,
                    "nutrient_id": nutrient.get("id"),
                    "nutrient_name": nutrient.get("name"),
                    "unit": nutrient.get("unitName"),
                    # ijson parses JSON numbers as decimal.Decimal by
                    # default; sqlite3 can't bind that, so cast now.
                    "amount": float(amount) if amount is not None else None,
                })

            for p in food.get("foodPortions", []):
                measure_unit = p.get("measureUnit") or {}
                gram_weight = p.get("gramWeight")
                amount = p.get("amount")
                portions.append({
                    "usda_portion_id": p.get("id"),
                    "fdc_id": fdc_id,
                    "amount": float(amount) if amount is not None else None,
                    # ijson parses JSON numbers as decimal.Decimal by
                    # default; sqlite3 can't bind that, so cast now.
                    "gram_weight": float(gram_weight) if gram_weight is not None else None,
                    "modifier": p.get("modifier"),
                    "portion_description": p.get("portionDescription"),
                    "measure_unit_id": measure_unit.get("id"),
                    "measure_unit_name": measure_unit.get("name"),
                    "measure_unit_abbr": measure_unit.get("abbreviation"),
                })

    foods_df = pd.DataFrame(foods)
    nutrients_df = pd.DataFrame(nutrients)
    portions_df = pd.DataFrame(portions)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    foods_df.to_parquet(PROCESSED_DIR / "legacy_foods.parquet")
    nutrients_df.to_parquet(PROCESSED_DIR / "legacy_nutrients.parquet")
    portions_df.to_parquet(PROCESSED_DIR / "legacy_portions.parquet")

    print(len(foods_df))              # expect a few thousand
    print(nutrients_df.shape)         # expect 100k+ rows
    print(foods_df.head())

    return foods_df, nutrients_df, portions_df


if __name__ == "__main__":
    parse_usda_legacy()