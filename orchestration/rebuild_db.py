from pathlib import Path
import argparse
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
    #
    # NOTE: every import below is local (right before its call site) and
    # NOT hoisted to the top of this function. Several of these modules
    # transitively import gastrometric.knowledge.loader, which builds its
    # Vocabulary() singleton as a module-level side effect at import time.
    # If any of those imports run before init_db()/build_ingredients()/
    # rebuild_knowledge()/build_relationships() have populated the DB, the
    # singleton build fails with "table 'vocabulary_terms' was not found"
    # -- regardless of what order the *calls* below are in. Keep new
    # pipeline steps imported locally, in call order, not batched at top.
    from gastrometric.db.init_db import init_db
    from gastrometric.knowledge.builders.build_ingredients import build_ingredients
    from gastrometric.knowledge.builder import rebuild_knowledge
    from gastrometric.knowledge.builders.build_relationships import build_relationships

    init_db()
    build_ingredients()
    rebuild_knowledge()
    build_relationships()

    from gastrometric.pipeline.ingest.ingest_markdown import ingest_markdown
    ingest_markdown()

    from gastrometric.pipeline.parse.parse_ingredient_blocks import parse_ingredient_blocks
    parse_ingredient_blocks()

    from gastrometric.understanding.build_lexical_spans import build_lexical_spans
    build_lexical_spans()

    from gastrometric.understanding.ingredient_parser import process_recipe_lines
    process_recipe_lines()

    from gastrometric.understanding.analyzer import persist_all_lines
    persist_all_lines()


#    from gastrometric.pipeline.observations.build_ingredient_observations import build_all_observations
#    build_all_observations()
#    from gastrometric.pipeline.normalize.normalize_ingredient_lines import normalize_ingredient_lines
#    normalize_ingredient_lines()

# I had to rename a table called relationships to flavor_bible_relationships, check for that if it's broken

#    from gastrometric.pipeline.enrichment.flavor_bible.load_flavor_bible_raw import load_flavor_bible_raw
#    load_flavor_bible_raw()

    from gastrometric.pipeline.enrichment.flavor_bible.load_flavor_bible_curated import load_flavor_bible_curated
    load_flavor_bible_curated()

#    from gastrometric.pipeline.enrichment.flavor_bible.map_flavor_bible import map_flavor_bible
#    map_flavor_bible()

    from gastrometric.data.seed.seed_kitchen import seed_kitchen
    seed_kitchen()

#    from gastrometric.pipeline.enrichment.usda.parse_usda_legacy import parse_usda_legacy
#    parse_usda_legacy()
#    from gastrometric.pipeline.enrichment.usda.ingest_usda_legacy import ingest_usda_legacy
#    ingest_usda_legacy()

    # Depends on usda_food_portions being populated by ingest_usda_legacy()
    # above, so it must run after it (and before nutrition mapping, which
    # is a separate consumer of the same USDA table).

#    from gastrometric.knowledge.builders.usda_vocabulary_builder import build_usda_vocabulary
#    build_usda_vocabulary()
#    from gastrometric.db.rebuild_nutrition import rebuild_nutrition_mappings
#    rebuild_nutrition_mappings()
#    from gastrometric.pipeline.enrichment.usda.resolve_ingredient_quantities import resolve_ingredient_quantities
#    resolve_ingredient_quantities()
#    from gastrometric.pipeline.enrichment.usda.calculate_nutrition import calculate_nutrition
#    calculate_nutrition()

    from gastrometric.db.create_views import create_views
    create_views()

    print("\n✅ Database rebuilt successfully")

if __name__ == "__main__":
    main()