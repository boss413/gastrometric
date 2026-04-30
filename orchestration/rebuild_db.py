# orchestration/rebuild_db.py

# This script Is a core development tool as changes to the structure of the database are made. 
# It deletes the existing database and rebuilds it from scratch by running all the pipeline steps in order.

from pathlib import Path
import os
import subprocess
from gastrometric.config.paths import DB_PATH, BASE_DIR, DATA_DIR

def reset_db():
    db_file = Path(DB_PATH)

    # Ensure directory exists (prevents later SQLite failure)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if db_file.exists():
        try:
            db_file.unlink()
            print(f"Deleted database: {db_file}")
        except PermissionError:
            raise RuntimeError(
                f"Could not delete {db_file}. "
                "Likely an open SQLite connection is still active."
            )

def run(script):
    print(f"\n--- Running {script} ---")
    subprocess.run(["python", os.path.join(BASE_DIR, script)], check=True)

def main():
    # 1. delete db
    print(f"Using DB at: {DB_PATH}")

    if os.path.exists(DB_PATH):
        print("Deleting existing database...")
        os.remove(DB_PATH)

    # 2. rebuild pipeline
    from gastrometric.db.init_db import init_db
    from gastrometric.pipeline.ingest.ingest_markdown import ingest_markdown
#    from gastrometric.pipeline.parse.parse_ingredient_blocks import parse_ingredient_blocks
    from gastrometric.pipeline.parse.parse_ingredient_lines import parse_ingredient_lines
    from gastrometric.pipeline.normalize.normalize_ingredient_lines import normalize_ingredient_lines
    from gastrometric.pipeline.entity.build_ingredients import build_ingredients
    from gastrometric.pipeline.canonical.generate_canonical_groups import generate_canonical_groups
    from gastrometric.pipeline.enrichment.flavor_bible.load_flavor_bible_raw import load_flavor_bible_raw
#    from gastrometric.pipeline.enrichment.flavor_bible.load_flavor_bible_curated import load_flavor_bible_curated
#    from gastrometric.pipeline.enrichment.flavor_bible.map_flavor_bible import map_flavor_bible
#    from gastrometric.data.seed.seed_kitchen import seed_kitchen
    from gastrometric.db.create_views import create_views

    init_db()
    ingest_markdown()
#    parse_ingredient_blocks()
    parse_ingredient_lines()
    normalize_ingredient_lines()
#    build_ingredients()
#    generate_canonical_groups()
    load_flavor_bible_raw()
#    load_flavor_bible_curated()
#    map_flavor_bible()
#   seed_kitchen()
#    create_views()

    print("\n✅ Database rebuilt successfully")

if __name__ == "__main__":
    main()