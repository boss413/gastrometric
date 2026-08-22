"""Seed builder: import approved parser vocabulary.

This builder imports Gastrometric's human-approved parser vocabulary from
``data/seed/vocabulary.json`` into the vocabulary tables. It is a 
*seed* knowledge builder, not an evidence builder.

It imports approved ``(term, vocabulary_class)`` pairs. See the work order 
for full architectural intent. The schema supports assigning multiple 
vocabulary classes to a single term.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

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
    "grammar",
    "tool",
    "component",
    "substance"
}


def _default_seed_path() -> Path:
    from gastrometric.config.paths import SEED_DIR

    return SEED_DIR / "vocabulary.json"


class SeedDataError(ValueError):
    """Raised when the seed JSON is structurally invalid (bad types, empty
    entries, unknown classes).
    """


def _slugify(text: str) -> str:
    """Generate a deterministic text identifier from a term."""
    return text.lower().replace(" ", "-")


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


def _pluralize(term: str) -> str:
    """Applies basic pluralization rules to singular nouns."""
    if term.endswith("s"):
        return term + "es"
    elif term.endswith("fe"):
        return term[:-2] + "ves"
    elif term.endswith("f"):
        return term[:-1] + "ves"
    else:
        return term + "s"


def _validate_and_normalize(raw: object) -> Tuple[Dict[str, List[str]], List[str]]:
    """Validate the raw seed JSON and return normalized, deduplicated,
    per-class term lists alongside any duplicate warnings.

    Raises SeedDataError on structural problems (bad top-level type, an
    unknown vocabulary class, a non-list value, or a non-string/empty
    entry). 
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
    duplicate_warnings: List[str] = []
    
    # Classes that require automatic pluralization
    PLURAL_TARGET_CLASSES = {"natural_portion", "measurement", "tool", "packaging", "component"}

    for vocabulary_class, entries in raw.items():
        if not isinstance(entries, list):
            raise SeedDataError(
                f"Value for vocabulary class {vocabulary_class!r} must be "
                f"a list, got {type(entries).__name__}"
            )

        seen_in_class: Dict[str, None] = {}
        for entry in entries:
            normalized = _normalize(entry, vocabulary_class=vocabulary_class)
            
            # Check for standard duplicates
            if normalized in seen_in_class:
                duplicate_warnings.append(
                    f'Duplicate term detected and deleted: "{normalized}" '
                    f'in class "{vocabulary_class}"'
                )
            else:
                seen_in_class[normalized] = None
                
            # Apply auto-pluralization for target noun classes
            if vocabulary_class in PLURAL_TARGET_CLASSES:
                plural = _pluralize(normalized)
                if plural in seen_in_class:
                    duplicate_warnings.append(
                        f'Duplicate auto-plural detected and deleted: "{plural}" '
                        f'in class "{vocabulary_class}"'
                    )
                else:
                    seen_in_class[plural] = None

        normalized_by_class[vocabulary_class] = list(seen_in_class.keys())

    return normalized_by_class, duplicate_warnings


class SeedVocabularyBuilder(KnowledgeBuilder):
    """Imports approved parser vocabulary from the seed JSON file."""

    name = "Seed Vocabulary Builder"

    def __init__(self, seed_path: Path | str | None = None):
        self.seed_path = Path(seed_path) if seed_path is not None else _default_seed_path()

    def run(self, conn: sqlite3.Connection) -> BuildResult:
        result = BuildResult(builder_name=self.name)

        with open(self.seed_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        normalized_by_class, duplicate_warnings = _validate_and_normalize(raw)

        distinct_inputs = sum(len(terms) for terms in normalized_by_class.values())
        result.distinct_inputs = distinct_inputs

        cursor = conn.cursor()
        
        created_by_class: Dict[str, int] = {c: 0 for c in sorted(normalized_by_class)}
        already_present = 0

        inserted_classes = set()

        for vocabulary_class, terms in normalized_by_class.items():
            class_id = _slugify(vocabulary_class)
            
            if class_id not in inserted_classes:
                cursor.execute(
                    "INSERT OR IGNORE INTO vocabulary_classes (class_id, class_name) VALUES (?, ?)",
                    (class_id, vocabulary_class)
                )
                inserted_classes.add(class_id)

            for term in terms:
                term_id = _slugify(term)
                
                # Insert the term record
                cursor.execute(
                    "INSERT OR IGNORE INTO vocabulary_terms (term_id, term) VALUES (?, ?)",
                    (term_id, term)
                )

                # Insert the classification mapping 
                try:
                    cursor.execute(
                        "INSERT INTO vocabulary_term_classes (term_id, class_id) VALUES (?, ?)",
                        (term_id, class_id)
                    )
                    created_by_class[vocabulary_class] += 1
                except sqlite3.IntegrityError:
                    # Occurs if mapping already exists
                    already_present += 1

        conn.commit()

        total_created = sum(created_by_class.values())
        result.vocabulary_created = total_created
        result.unknown_concepts = 0

        summary_lines: List[str] = ["", "Vocabulary by class:"]
        for vocabulary_class in sorted(created_by_class):
            summary_lines.append(f"  {vocabulary_class}: {created_by_class[vocabulary_class]:,}")
        summary_lines.append("")
        summary_lines.append(f"Already present (skipped): {already_present:,}")

        summary_lines.append("")
        if duplicate_warnings:
            summary_lines.append("Warnings:")
            for warning in duplicate_warnings:
                summary_lines.append(f"  {warning}")
        else:
            summary_lines.append("Warnings: none")

        result.extra_lines = summary_lines
        return result


def build(conn: sqlite3.Connection) -> BuildResult:
    """Convenience entry point for callers that already have a connection."""
    return SeedVocabularyBuilder().run(conn)


def build_seed_vocabulary() -> BuildResult:
    """Self-contained entry point."""
    from gastrometric.config.paths import DB_PATH

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        result = SeedVocabularyBuilder().run(conn)
        print(result.render())
        return result
    except SeedDataError as e:
        print(str(e))
        raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    build_seed_vocabulary()