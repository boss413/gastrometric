from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DOCS_DIR = BASE_DIR / "docs"
DATA_DIR = BASE_DIR / "data"
SEED_DIR = DATA_DIR / "seed"
DB_PATH = DATA_DIR / "gastrometric.db"

# Source-of-truth JSON inputs consumed by ingestion stages.
INGREDIENTS_JSON_PATH = DATA_DIR / "ingredients.json"
NUTRITION_MAPPINGS_JSON_PATH = DATA_DIR / "nutrition_mappings.json"