import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Any


SAFE_USDA_CATEGORIES = {
    "Vegetables and Vegetable Products",
    "Fruits and Fruit Juices",
    "Legumes and Legume Products",
    "Cereal Grains and Pasta",
    "Dairy and Egg Products",
    "Poultry Products",
    "Beef Products",
    "Pork Products",
    "Lamb, Veal, and Game Products",
    "Fats and Oils",
    "Finfish and Shellfish Products",
    "Nut and Seed Products",
    "Spices and Herbs",
}

FALLBACK_USDA_CATEGORIES = {
    "Sweets",
    "Baked Products",
    "Snacks",
    "Restaurant Foods",
    "American Indian/Alaska Native Foods",
    "Meals, Entrees, and Side Dishes",
    "Fast Foods",
    "Beverages",
    "Miscellaneous Prepared Foods",
    "Baby Foods",
}


def normalize_text(value: str) -> str:
    """Normalize text for matching."""
    value = value.lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def tokenize(value: str) -> set[str]:
    """Return normalized tokens."""
    return set(normalize_text(value).split())


def load_json(path: Path) -> Any:
    """Load JSON from disk."""
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_json(path: Path, data: Any) -> None:
    """Write JSON to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False,
        )


def load_ingredients(
    path: Path,
) -> list[dict[str, Any]]:
    """Load Gastrometric ingredient identities."""
    data = load_json(path)

    if not isinstance(data, dict):
        raise ValueError(
            "ingredients.json must contain a top-level object."
        )

    identities = data.get("identities")

    if not isinstance(identities, list):
        raise ValueError(
            "ingredients.json must contain an "
            "'identities' array."
        )

    return identities


def get_ingredient_name(
    ingredient: dict[str, Any],
) -> str:
    """Return the canonical ingredient name."""
    for key in (
        "name",
        "ingredient",
        "canonical_name",
    ):
        if ingredient.get(key):
            return str(ingredient[key])

    raise ValueError(
        f"Ingredient has no name: {ingredient}"
    )


def get_aliases(
    ingredient: dict[str, Any],
) -> list[str]:
    """Return ingredient aliases."""
    aliases = ingredient.get("aliases", [])

    if isinstance(aliases, str):
        return [aliases]

    if isinstance(aliases, list):
        return [
            str(alias)
            for alias in aliases
        ]

    return []


def load_mappings(
    path: Path,
) -> dict[str, Any]:
    """Load persistent mapping repository."""
    if not path.exists():
        return {"mappings": {}}

    data = load_json(path)

    if "mappings" not in data:
        data["mappings"] = {}

    return data


def load_usda_rows(
    connection: sqlite3.Connection,
    table: str,
    id_column: str,
    description_column: str,
    category_column: str,
) -> list[dict[str, Any]]:
    """Load USDA food records."""
    query = f"""
        SELECT
            "{id_column}",
            "{description_column}",
            "{category_column}"
        FROM "{table}"
        WHERE "{description_column}" IS NOT NULL
    """

    rows = connection.execute(query).fetchall()

    return [
        {
            "source_id": str(row[0]),
            "description": str(row[1]),
            "category": (
                str(row[2])
                if row[2] is not None
                else None
            ),
        }
        for row in rows
    ]


def score_candidate(
    name: str,
    aliases: list[str],
    food: dict[str, Any],
) -> tuple[int, list[str]]:
    """Score a USDA food as a lexical candidate."""
    normalized_name = normalize_text(name)
    normalized_description = normalize_text(
        food["description"]
    )

    name_tokens = tokenize(name)
    description_tokens = tokenize(
        food["description"]
    )

    score = 0
    reasons = []

    if (
        normalized_name
        in normalized_description
    ):
        score += 100
        reasons.append(
            "canonical_name_contained"
        )

    for alias in aliases:
        normalized_alias = normalize_text(alias)

        if (
            normalized_alias
            in normalized_description
        ):
            score += 75
            reasons.append(
                f"alias_contained:{alias}"
            )

    overlap = (
        name_tokens
        & description_tokens
    )

    score += len(overlap) * 10

    if overlap:
        reasons.append(
            "token_overlap:"
            + ",".join(sorted(overlap))
        )

    return score, reasons


def find_candidates(
    name: str,
    aliases: list[str],
    foods: list[dict[str, Any]],
    allowed_categories: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    """Find USDA candidates within a category scope."""
    candidates = []

    for food in foods:
        category = food["category"]

        if (
            category is not None
            and category not in allowed_categories
        ):
            continue

        score, reasons = score_candidate(
            name,
            aliases,
            food,
        )

        if score <= 0:
            continue

        candidates.append(
            {
                **food,
                "score": score,
                "match_reasons": reasons,
            }
        )

    candidates.sort(
        key=lambda item: (
            item["score"],
            item["description"],
        ),
        reverse=True,
    )

    return candidates[:limit]


def create_pending_record(
    name: str,
    aliases: list[str],
    safe_candidates: list[dict[str, Any]],
    fallback_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Create a persistent pending mapping record."""
    return {
        "status": "pending_review",
        "source": None,
        "source_id": None,
        "source_description": None,
        "confidence": None,
        "selection_method": None,
        "safe_candidates": safe_candidates,
        "fallback_candidates": fallback_candidates,
        "aliases_at_review": aliases,
        "notes": (
            "Review candidates and either approve "
            "a USDA mapping or mark unresolved."
        ),
    }


def process_ingredient(
    ingredient: dict[str, Any],
    mappings: dict[str, Any],
    usda_foods: list[dict[str, Any]],
    candidate_limit: int,
) -> str:
    """Resolve one ingredient or add it to the review queue."""
    name = get_ingredient_name(ingredient)
    aliases = get_aliases(ingredient)

    existing = mappings["mappings"].get(name)

    if existing:
        return "existing"

    safe_candidates = find_candidates(
        name=name,
        aliases=aliases,
        foods=usda_foods,
        allowed_categories=SAFE_USDA_CATEGORIES,
        limit=candidate_limit,
    )

    fallback_candidates = []

    if not safe_candidates:
        fallback_candidates = find_candidates(
            name=name,
            aliases=aliases,
            foods=usda_foods,
            allowed_categories=(
                SAFE_USDA_CATEGORIES
                | FALLBACK_USDA_CATEGORIES
            ),
            limit=candidate_limit,
        )

    if not safe_candidates and not fallback_candidates:
        mappings["mappings"][name] = {
            "status": "unresolved",
            "source": None,
            "source_id": None,
            "source_description": None,
            "confidence": None,
            "selection_method": "usda_exhausted",
            "notes": (
                "No suitable USDA Legacy candidate "
                "was found."
            ),
        }

        return "unresolved"

    mappings["mappings"][name] = (
        create_pending_record(
            name=name,
            aliases=aliases,
            safe_candidates=safe_candidates,
            fallback_candidates=(
                fallback_candidates
            ),
        )
    )

    return "pending_review"


def print_review_queue(
    mappings: dict[str, Any],
) -> None:
    """Print pending mappings."""
    pending = []

    for name, mapping in mappings[
        "mappings"
    ].items():
        if mapping.get("status") == (
            "pending_review"
        ):
            pending.append(
                (name, mapping)
            )

    if not pending:
        print()
        print("No mappings require review.")
        return

    print()
    print("Mappings requiring review")
    print("==========================")

    for name, mapping in pending:
        print()
        print(f"Ingredient: {name}")

        safe = mapping.get(
            "safe_candidates",
            [],
        )

        fallback = mapping.get(
            "fallback_candidates",
            [],
        )

        if safe:
            print()
            print("USDA Legacy safe-category candidates:")

            for index, candidate in enumerate(
                safe,
                start=1,
            ):
                print(
                    f"  {index}. "
                    f"{candidate['description']} "
                    f"[{candidate['source_id']}] "
                    f"score={candidate['score']}"
                )

        if fallback:
            print()
            print(
                "USDA Legacy fallback-category "
                "candidates:"
            )

            for index, candidate in enumerate(
                fallback,
                start=1,
            ):
                print(
                    f"  {index}. "
                    f"{candidate['description']} "
                    f"[{candidate['source_id']}] "
                    f"category="
                    f"{candidate['category']} "
                    f"score="
                    f"{candidate['score']}"
                )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Incrementally resolve Gastrometric "
            "ingredient nutrition mappings."
        )
    )

    parser.add_argument(
        "--db",
        required=True,
        help="Path to gastrometric.db",
    )

    parser.add_argument(
        "--ingredients",
        required=True,
        help="Path to ingredients.json",
    )

    parser.add_argument(
        "--mappings",
        default=(
            "data/"
            "nutrition_mappings.json"
        ),
        help=(
            "Persistent nutrition mapping "
            "repository."
        ),
    )

    parser.add_argument(
        "--usda-table",
        required=True,
        help="USDA Legacy table name.",
    )

    parser.add_argument(
        "--usda-id-column",
        required=True,
        help="USDA food ID column.",
    )

    parser.add_argument(
        "--usda-description-column",
        required=True,
        help="USDA food description column.",
    )

    parser.add_argument(
        "--usda-category-column",
        required=True,
        help="USDA food category column.",
    )

    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=10,
        help=(
            "Maximum candidates to retain "
            "per search scope."
        ),
    )

    args = parser.parse_args()

    ingredient_path = Path(
        args.ingredients
    )

    mapping_path = Path(
        args.mappings
    )

    ingredients = load_ingredients(
        ingredient_path
    )

    mappings = load_mappings(
        mapping_path
    )

    connection = sqlite3.connect(
        args.db
    )

    try:
        usda_foods = load_usda_rows(
            connection=connection,
            table=args.usda_table,
            id_column=args.usda_id_column,
            description_column=(
                args.usda_description_column
            ),
            category_column=(
                args.usda_category_column
            ),
        )
    finally:
        connection.close()

    counts = {
        "existing": 0,
        "pending_review": 0,
        "unresolved": 0,
    }

    for ingredient in ingredients:
        result = process_ingredient(
            ingredient=ingredient,
            mappings=mappings,
            usda_foods=usda_foods,
            candidate_limit=(
                args.candidate_limit
            ),
        )

        counts[result] += 1

    save_json(
        mapping_path,
        mappings,
    )

    print()
    print("Nutrition mapping resolver")
    print("==========================")
    print(
        f"Ingredients checked: "
        f"{len(ingredients)}"
    )
    print(
        f"Existing mappings: "
        f"{counts['existing']}"
    )
    print(
        f"New mappings requiring review: "
        f"{counts['pending_review']}"
    )
    print(
        f"New unresolved ingredients: "
        f"{counts['unresolved']}"
    )
    print()
    print(
        f"Persistent mapping file: "
        f"{mapping_path}"
    )

    print_review_queue(
        mappings
    )


if __name__ == "__main__":
    main()