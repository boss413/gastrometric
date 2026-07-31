"""Seed builder: import approved culinary parser vocabulary.

This builder imports Gastrometric's human-approved parser vocabulary from
``data/seed/culinary_vocabulary.json`` into the ``culinary_vocabulary``
table. It is a *seed* knowledge builder, not an evidence builder:

- it does not create culinary observations
- it does not infer vocabulary classes
- it does not analyze source text or perform parser logic
- it does not generate aliases
- it does not normalize terminology beyond simple duplicate handling
- it does not create or modify table schema -- ``culinary_vocabulary`` is
  created by ``init_db.py``; this builder only ever INSERTs into it and
  assumes the table already exists

Its only responsibility is importing approved ``(term, vocabulary_class)``
pairs. See the work order for full architectural intent.

Note on ambiguous terms: ``culinary_vocabulary.term`` is UNIQUE on its own
(not on ``(term, vocabulary_class)``), so a term can only ever occupy one
row/one class. A term that the seed data associates with more than one
class (e.g. "hot" as both "seasoning" and "temperature") therefore cannot
be inserted at all under the current schema. Rather than aborting the
whole import over this, such terms are skipped and reported as warnings
in the build summary so a human can resolve them (e.g. by picking one
class in the seed file, or -- separately -- by extending the schema to
support multiple classes per term). Structural problems with the seed
file itself (bad types, empty strings, unknown classes) still fail fast,
since those indicate the seed file is broken rather than merely
ambiguous.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, List

from ..builder import KnowledgeBuilder
from ..models import BuildResult

# Valid vocabulary classes, mirrored from the runtime loader's classification
# helpers (knowledge.loader.CulinaryVocabulary). Keep this in sync with that
# module -- it is intentionally duplicated here rather than imported so this
# builder has no runtime dependency on the loader.
VALID_VOCABULARY_CLASSES = {
    "measurement",
    "natural_portion",
    "preparation",
    "temperature",
    "packaging",
    "size",
    "descriptor",
    "modifier",
    "brand",
    "seasoning",
    "state",
}


def _default_seed_path() -> Path:
    from gastrometric.config.paths import SEED_DIR

    return SEED_DIR / "culinary_vocabulary.json"


class SeedDataError(ValueError):
    """Raised when the seed JSON is structurally invalid (bad types, empty
    entries, unknown classes). NOT raised for ambiguous terms -- those are
    reported as warnings instead; see module docstring.
    """


def _normalize(term: str, *, vocabulary_class: str) -> str:
    """Shallow normalization used only for duplicate detection.

    Trims surrounding whitespace and collapses repeated internal whitespace.
    Does NOT lowercase, singularize/pluralize, stem, or otherwise rewrite the
    term -- the seed file already represents approved vocabulary.
    """
    if not isinstance(term, str):
        raise SeedDataError(
            f"Invalid entry in class {vocabulary_class!r}: "
            f"expected a string, got {type(term).__name__}: {term!r}"
        )
    normalized = " ".join(term.split())
    if not normalized:
        raise SeedDataError(
            f"Invalid entry in class {vocabulary_class!r}: "
            f"entry is empty or whitespace-only"
        )
    return normalized


def _validate_and_normalize(raw: object) -> Dict[str, List[str]]:
    """Validate the raw seed JSON and return normalized, deduplicated,
    per-class term lists.

    Raises SeedDataError on structural problems (bad top-level type, an
    unknown vocabulary class, a non-list value, or a non-string/empty
    entry). Does NOT raise for terms that appear under more than one
    class -- that is reported later as a non-fatal warning, since the
    schema simply can't store both and skipping is the safe response.
    """
    if not isinstance(raw, dict):
        raise SeedDataError(
            f"Seed file must contain a JSON object mapping vocabulary "
            f"class -> list of terms, got {type(raw).__name__}"
        )

    unknown_classes = sorted(set(raw.keys()) - VALID_VOCABULARY_CLASSES)
    if unknown_classes:
        raise SeedDataError(
            "Seed file contains unknown vocabulary class(es): "
            + ", ".join(repr(c) for c in unknown_classes)
        )

    normalized_by_class: Dict[str, List[str]] = {}

    for vocabulary_class, entries in raw.items():
        if not isinstance(entries, list):
            raise SeedDataError(
                f"Value for vocabulary class {vocabulary_class!r} must be "
                f"a list, got {type(entries).__name__}"
            )

        seen_in_class: Dict[str, None] = {}
        for entry in entries:
            normalized = _normalize(entry, vocabulary_class=vocabulary_class)
            # Duplicate within the same class: import once, silently.
            if normalized not in seen_in_class:
                seen_in_class[normalized] = None

        normalized_by_class[vocabulary_class] = list(seen_in_class.keys())

    return normalized_by_class


def _find_ambiguous_terms(
    normalized_by_class: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """Return {term: sorted candidate classes} for every term that the seed
    file associates with more than one vocabulary class.
    """
    term_to_classes: Dict[str, List[str]] = {}
    for vocabulary_class, terms in normalized_by_class.items():
        for term in terms:
            term_to_classes.setdefault(term, []).append(vocabulary_class)

    return {
        term: sorted(classes)
        for term, classes in term_to_classes.items()
        if len(classes) > 1
    }


class SeedCulinaryVocabularyBuilder(KnowledgeBuilder):
    """Imports approved parser vocabulary from the seed JSON file."""

    name = "Seed Culinary Vocabulary Builder"

    def __init__(self, seed_path: Path | str | None = None):
        self.seed_path = Path(seed_path) if seed_path is not None else _default_seed_path()

    def run(self, conn: sqlite3.Connection) -> BuildResult:
        result = BuildResult(builder_name=self.name)

        with open(self.seed_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        normalized_by_class = _validate_and_normalize(raw)

        distinct_inputs = sum(len(terms) for terms in normalized_by_class.values())
        result.distinct_inputs = distinct_inputs

        # Terms the seed file itself can't settle on a single class for.
        # These can never be inserted (term is UNIQUE on its own), so they
        # are skipped up front and reported as warnings.
        ambiguous_in_seed = _find_ambiguous_terms(normalized_by_class)

        cursor = conn.cursor()
        existing_rows = cursor.execute(
            "SELECT term, vocabulary_class FROM culinary_vocabulary"
        ).fetchall()
        existing_by_term = {term: vocabulary_class for term, vocabulary_class in existing_rows}

        created_by_class: Dict[str, int] = {c: 0 for c in sorted(normalized_by_class)}
        already_present = 0
        db_conflicts: Dict[str, List[str]] = {}  # term -> [seed_class, existing_db_class]

        for vocabulary_class, terms in normalized_by_class.items():
            for term in terms:
                if term in ambiguous_in_seed:
                    continue  # already recorded as a warning; never inserted

                existing_class = existing_by_term.get(term)
                if existing_class is None:
                    cursor.execute(
                        "INSERT INTO culinary_vocabulary (term, vocabulary_class) VALUES (?, ?)",
                        (term, vocabulary_class),
                    )
                    existing_by_term[term] = vocabulary_class
                    created_by_class[vocabulary_class] += 1
                elif existing_class == vocabulary_class:
                    # Already imported by a prior run; idempotent no-op.
                    already_present += 1
                else:
                    # Seed says one class, DB already has it under another.
                    # Same "can't store both" situation as an in-seed
                    # ambiguity -- skip and warn, don't abort the run.
                    db_conflicts[term] = sorted({vocabulary_class, existing_class})

        conn.commit()

        total_created = sum(created_by_class.values())
        result.vocabulary_created = total_created
        # `unknown_concepts` doubles here as "terms we could not confidently
        # assign a single class to" -- i.e. every skipped ambiguous term.
        result.unknown_concepts = len(ambiguous_in_seed) + len(db_conflicts)

        summary_lines: List[str] = ["", "Vocabulary by class:"]
        for vocabulary_class in sorted(created_by_class):
            summary_lines.append(f"  {vocabulary_class}: {created_by_class[vocabulary_class]:,}")
        summary_lines.append("")
        summary_lines.append(f"Already present (skipped): {already_present:,}")

        summary_lines.append("")
        if ambiguous_in_seed or db_conflicts:
            summary_lines.append("Warnings:")
            for term in sorted(ambiguous_in_seed):
                classes = ", ".join(ambiguous_in_seed[term])
                summary_lines.append(
                    f'  "{term}" recognized in multiple classes ({classes}); not imported'
                )
            for term in sorted(db_conflicts):
                classes = ", ".join(db_conflicts[term])
                summary_lines.append(
                    f'  "{term}" already exists under a different class '
                    f"than the seed file specifies ({classes}); not changed"
                )
        else:
            summary_lines.append("Warnings: none")

        result.extra_lines = summary_lines
        return result


def build(conn: sqlite3.Connection) -> BuildResult:
    """Convenience entry point for callers that already have a connection."""
    return SeedCulinaryVocabularyBuilder().run(conn)


def build_seed_culinary_vocabulary() -> BuildResult:
    """Self-contained entry point -- opens/closes its own connection,
    matches the no-arg pattern the other pipeline steps in
    orchestration/rebuild_db.py use.
    """
    from gastrometric.config.paths import DB_PATH

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        result = SeedCulinaryVocabularyBuilder().run(conn)
        print(result.render())
        return result
    except SeedDataError as e:
        # Structural seed-file problems still fail fast, but print cleanly
        # instead of a raw traceback.
        print(str(e))
        raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    build_seed_culinary_vocabulary()