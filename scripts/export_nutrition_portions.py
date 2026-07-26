import json
import sqlite3
from pathlib import Path


DB_PATH = Path("gastrometric/data/gastrometric.db")
MAPPINGS_PATH = Path("gastrometric/data/nutrition_mappings.json")
OUTPUT_PATH = Path("gastrometric/data/nutrition_portions_for_mapping.json")


def collect_fdc_ids(mappings):
    fdc_ids = set()

    for mapping in mappings.values():
        if mapping.get("status") != "approved":
            continue

        # Standard/default mapping:
        #
        # "source_id": 173468
        if "source_id" in mapping:
            fdc_ids.add(mapping["source_id"])

        # State-specific mappings:
        #
        # "state_mappings": {
        #     "raw": 171287,
        #     "cooked": 172187
        # }
        for state_mapping in mapping.get("state_mappings", {}).values():
            if isinstance(state_mapping, int):
                fdc_ids.add(state_mapping)

            # Also support the alternate object structure in case
            # the mapping format is changed later:
            #
            # "raw": {
            #     "source_id": 171287
            # }
            elif isinstance(state_mapping, dict):
                if "source_id" in state_mapping:
                    fdc_ids.add(state_mapping["source_id"])

    return sorted(fdc_ids)


def main():
    with MAPPINGS_PATH.open("r", encoding="utf-8") as file:
        data = json.load(file)

    mappings = data["mappings"]
    fdc_ids = collect_fdc_ids(mappings)

    if not fdc_ids:
        print("No approved USDA FDC IDs found in nutrition_mappings.json.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    placeholders = ",".join("?" for _ in fdc_ids)

    query = f"""
        SELECT
            usda_portion_id,
            fdc_id,
            modifier
        FROM usda_food_portions
        WHERE fdc_id IN ({placeholders})
        ORDER BY fdc_id, usda_portion_id
    """

    rows = conn.execute(query, fdc_ids).fetchall()
    conn.close()

    output = {
        "source": "usda_food_portions",
        "portions": [dict(row) for row in rows]
    }

    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        json.dump(output, file, indent=2)

    print(f"Found {len(fdc_ids)} mapped FDC IDs.")
    print(f"Found {len(rows)} USDA portions.")
    print(f"Wrote: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()