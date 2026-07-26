import json
from pathlib import Path


MAPPINGS_PATH = Path("gastrometric/data/nutrition_mappings.json")
PORTIONS_PATH = Path("gastrometric/data/nutrition_portions_for_mapping.json")
OUTPUT_PATH = Path("gastrometric/data/nutrition_mappings_merged.json")


def load_json(path):
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def build_portion_index(portion_data):
    portions_by_fdc_id = {}

    for portion in portion_data["portions"]:
        fdc_id = portion["fdc_id"]

        portions_by_fdc_id.setdefault(fdc_id, []).append(
            {
                "usda_portion_id": portion["usda_portion_id"],
                "modifier": portion["modifier"],
            }
        )

    return portions_by_fdc_id


def add_portions(mapping, portions_by_fdc_id):
    if mapping.get("status") != "approved":
        return

    # Standard mapping:
    #
    # "source_id": 173410
    if "source_id" in mapping:
        fdc_id = mapping["source_id"]

        if fdc_id in portions_by_fdc_id:
            mapping["portions"] = portions_by_fdc_id[fdc_id]

    # State-specific mappings:
    #
    # "state_mappings": {
    #     "raw": 171287,
    #     "cooked": 172187
    # }
    if "state_mappings" in mapping:
        for state, state_mapping in mapping["state_mappings"].items():
            if isinstance(state_mapping, int):
                fdc_id = state_mapping

                if fdc_id in portions_by_fdc_id:
                    mapping["state_mappings"][state] = {
                        "source_id": fdc_id,
                        "portions": portions_by_fdc_id[fdc_id],
                    }


def main():
    mappings_data = load_json(MAPPINGS_PATH)
    portions_data = load_json(PORTIONS_PATH)

    portions_by_fdc_id = build_portion_index(portions_data)

    mappings = mappings_data["mappings"]

    approved_count = 0
    mappings_with_portions = 0
    total_portions_attached = 0

    for mapping in mappings.values():
        if mapping.get("status") != "approved":
            continue

        approved_count += 1

        before = count_portions(mapping)

        add_portions(mapping, portions_by_fdc_id)

        after = count_portions(mapping)

        if after > before:
            mappings_with_portions += 1
            total_portions_attached += after - before

    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        json.dump(mappings_data, file, indent=2)
        file.write("\n")

    print(f"Approved mappings processed: {approved_count}")
    print(f"Mappings with USDA portions: {mappings_with_portions}")
    print(f"USDA portions attached: {total_portions_attached}")
    print(f"Wrote: {OUTPUT_PATH}")


def count_portions(mapping):
    count = 0

    if "portions" in mapping:
        count += len(mapping["portions"])

    for state_mapping in mapping.get("state_mappings", {}).values():
        if isinstance(state_mapping, dict):
            count += len(state_mapping.get("portions", []))

    return count


if __name__ == "__main__":
    main()