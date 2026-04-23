# scripts/rebuild_db.py

import os
import subprocess

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "..", "data", "gastrometric.db")

def run(script):
    print(f"\n--- Running {script} ---")
    subprocess.run(["python", os.path.join(BASE_DIR, script)], check=True)

def main():
    # 1. delete db
    if os.path.exists(DB_PATH):
        print("Deleting existing database...")
        os.remove(DB_PATH)

    # 2. rebuild pipeline
    run("init_db.py")
    run("ingest_markdown.py")
    run("parse_ingredient_blocks.py")
    run("parse_ingredient_lines.py")
    run("build_ingredients.py")
    run("generate_canonical_groups.py")
    run("seed_kitchen.py")               # fridge/pantry script
    run("create_views.py")

    print("\n✅ Database rebuilt successfully")

if __name__ == "__main__":
    main()