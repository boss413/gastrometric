from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DOCS_DIR = BASE_DIR / "docs"
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "gastrometric.db"