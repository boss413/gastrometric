"""
Refresh database content (ingredients, vocabulary, recipes, kitchen inventory,
etc.) WITHOUT deleting the database file. Tables are emptied, schema is kept,
then the content pipeline is re-run.

Usage:
    python -m gastrometric.orchestration.reset_content
    python -m gastrometric.orchestration.reset_content --dry-run
    python -m gastrometric.orchestration.reset_content --keep some_table --keep other_table

Use rebuild_db instead if the schema itself has changed.
"""
import argparse
import sqlite3

from gastrometric.config.paths import DB_PATH

# Tables with these prefixes are never emptied (their loaders are currently
# commented out in rebuild_db, so wiping them would lose data with no way to
# repopulate it from this script). Add more with --keep.
PRESERVE_PREFIXES = ("usda_",)


def empty_tables(keep=(), dry_run=False):
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")

        tables = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        ]
        views = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'view'"
            )
        ]

        to_clear = [
            t for t in tables
            if t not in keep and not t.startswith(PRESERVE_PREFIXES)
        ]
        preserved = [t for t in tables if t not in to_clear]

        print(f"Emptying {len(to_clear)} table(s):")
        for t in to_clear:
            n = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            print(f"  - {t} ({n} rows)")
        if preserved:
            print(f"Preserving: {', '.join(preserved)}")

        if dry_run:
            print("\nDry run: nothing was changed.")
            return False

        with conn:  # single transaction: all-or-nothing
            # Views are dropped here and rebuilt by create_views() at the end.
            for v in views:
                conn.execute(f'DROP VIEW IF EXISTS "{v}"')
            for t in to_clear:
                conn.execute(f'DELETE FROM "{t}"')

            has_seq = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name = 'sqlite_sequence'"
            ).fetchone()
            if has_seq and to_clear:
                marks = ",".join("?" * len(to_clear))
                conn.execute(
                    f"DELETE FROM sqlite_sequence WHERE name IN ({marks})",
                    to_clear,
                )
        return True
    finally:
        # Close before the pipeline runs so no lock is held on the DB file.
        conn.close()


def run_pipeline():
    # NOTE: every import below is local (right before its call site) and
    # NOT hoisted to the top of this function. Several of these modules
    # transitively import gastrometric.knowledge.loader, which builds its
    # Vocabulary() singleton as a module-level side effect at import time.
    # If any of those imports run before build_ingredients()/
    # rebuild_knowledge()/build_relationships() have repopulated the tables,
    # the singleton is built from empty data. Keep new pipeline steps
    # imported locally, in call order, not batched at top.
    from gastrometric.knowledge.builders.build_ingredients import build_ingredients
    from gastrometric.knowledge.builder import rebuild_knowledge
    from gastrometric.knowledge.builders.build_relationships import build_relationships

    build_ingredients()
    rebuild_knowledge()
    build_relationships()

    from gastrometric.pipeline.ingest.ingest_markdown import ingest_markdown
    ingest_markdown()

    from gastrometric.pipeline.parse.parse_ingredient_blocks import parse_ingredient_blocks
    parse_ingredient_blocks()

    from gastrometric.orchestration.recipe_ingredient_understanding import build_lexical_spans
    build_lexical_spans()

    from gastrometric.orchestration.recipe_ingredient_understanding import process_recipe_lines
    process_recipe_lines()

    from gastrometric.orchestration.recipe_ingredient_understanding import persist_all_lines
    persist_all_lines()

    # I had to rename a table called relationships to flavor_bible_relationships, check for that if it's broken

    from gastrometric.pipeline.enrichment.flavor_bible.load_flavor_bible_curated import load_flavor_bible_curated
    load_flavor_bible_curated()

    from gastrometric.data.seed.seed_kitchen import seed_kitchen
    seed_kitchen()

    from gastrometric.db.create_views import create_views
    create_views()


def main():
    parser = argparse.ArgumentParser(
        description="Empty content tables and re-run the content pipeline (DB file is kept)."
    )
    parser.add_argument(
        "--keep", action="append", default=[], metavar="TABLE",
        help="Table to leave untouched (repeatable).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List what would be emptied without changing anything.",
    )
    args = parser.parse_args()

    print(f"Using DB at: {DB_PATH}")
    if not DB_PATH.exists():
        raise SystemExit(
            f"No database at {DB_PATH}. Run gastrometric.orchestration.rebuild_db first."
        )

    if not empty_tables(keep=set(args.keep), dry_run=args.dry_run):
        return

    run_pipeline()

    print("\n✅ Content reset successfully")


if __name__ == "__main__":
    main()