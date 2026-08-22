from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import NoReturn

from gastrometric.config.paths import DB_PATH, SEED_DIR
from gastrometric.knowledge.builder import KnowledgeBuilder
from gastrometric.knowledge.models import BuildResult

# vocabulary_terms.term_id is the actually-canonical identity for a
# vocabulary term (deterministically derived, primary key); the `term`
# text column is not reliable to match against -- SeedVocabularyBuilder
# does `INSERT OR IGNORE INTO vocabulary_terms (term_id, term)`, so when
# the same term is seeded under multiple classes with different casing,
# whichever casing is processed first "wins" the term_id and the other
# casing is silently dropped by OR IGNORE. term_id itself doesn't have
# this problem, since it's always the same deterministic slug regardless
# of which class or casing produced it -- so that's what relationship
# endpoints resolve against, the same way ingredient endpoints resolve
# against ingredients.id rather than ingredient_name.
#
# Imported (not duplicated) because this is an algorithm, not data --
# duplicating it would risk silently drifting out of sync with the
# vocabulary builder's actual canonicalization if that ever changes.
# seed_vocabulary_builder.py has no import-time side effects (unlike
# knowledge/loader.py), so this doesn't reintroduce the singleton problem.
from .seed_vocabulary_builder import _slugify

# The set of predicates the relationship builder is willing to persist.
#
# This is deliberately just a set of strings with no attached semantics --
# component_of / natural_portion_of / variety_of do not have inverses,
# transitive behavior, or any other reasoning defined here. That belongs to
# a later knowledge/query layer, not the seed loader.
#
# This lives here, locally, rather than in its own module: three original strings
# don't warrant a dedicated predicate registry yet. Once the relationship
# knowledge schema defines a real predicate model (e.g. predicates curated
# in the seed/schema itself), this constant should be replaced by that,
# not grown into a second Python-side source of truth.
SUPPORTED_PREDICATES: frozenset[str] = frozenset({
    "component_of",
    "is_a",
    "default_for",
    "contains",
    "derived_from",
    "natural_portion_of",
    "variety_of",
})

# Endpoint types supported by the R0-1/R0-2 contract, and the field each
# one carries its identifying value in.
#
# "ingredient" and "vocabulary" have a resolver: the builder will look
# them up against current knowledge and, when found, store the resolved
# canonical identifier. "reference" has no resolver at all -- it is
# always stored exactly as supplied.
#
# Resolution is a normalization step, not a validity gate: a
# structurally well-formed "ingredient"/"vocabulary" endpoint that simply
# doesn't resolve against *today's* knowledge is not an error. It is
# stored as supplied, same as a "reference" endpoint would be. Endpoint
# existence is not a build-time invariant -- see _resolve_endpoint.
SUPPORTED_ENDPOINT_TYPES = {"ingredient", "vocabulary", "reference"}

_ENDPOINT_IDENTIFYING_FIELD: dict[str, str] = {
    "ingredient": "id",
    "vocabulary": "term",
    "reference": "term",
}

_REQUIRED_RELATIONSHIP_FIELDS = ("subject", "predicate", "object", "source", "confidence")


class RelationshipBuildError(ValueError):
    """A single relationship assertion failed validation.

    Carries the seed index and a human-readable, actionable explanation so
    callers can point curators directly at the bad entry, per the work
    order's error-reporting requirement.
    """

    def __init__(self, index: int | str, detail: str) -> None:
        self.index = index
        self.detail = detail
        super().__init__(f"Invalid relationship at relationships[{index}]:\n{detail}")


def _fail(index: int, detail: str) -> NoReturn:
    raise RelationshipBuildError(index, detail)


def _validate_endpoint(role: str, endpoint: object, index: int) -> tuple[str, str]:
    """Validate subject/object *structure* and return (type, normalized_id).

    This checks well-formedness only -- unsupported type, missing field,
    wrong field type. It does NOT check whether the endpoint resolves
    against current knowledge; that's _resolve_endpoint's job, and
    failure to resolve is not a validation error.
    """

    if not isinstance(endpoint, dict):
        _fail(index, f"{role} must be an object.")

    endpoint_type = endpoint.get("type")
    if not isinstance(endpoint_type, str) or endpoint_type not in SUPPORTED_ENDPOINT_TYPES:
        _fail(
            index,
            f'{role}.type = {endpoint_type!r}\n\nUnsupported endpoint type.',
        )

    field_name = _ENDPOINT_IDENTIFYING_FIELD[endpoint_type]

    if field_name not in endpoint:
        _fail(
            index,
            f'{role}.type = "{endpoint_type}"\n\n'
            f'Missing required field "{field_name}".',
        )

    raw_value = endpoint[field_name]

    if not isinstance(raw_value, str):
        _fail(
            index,
            f'{role}.type = "{endpoint_type}"\n{role}.{field_name} = {raw_value!r}\n\n'
            f'Field "{field_name}" must be a string, got {type(raw_value).__name__}.',
        )

    if not raw_value.strip():
        _fail(
            index,
            f'{role}.type = "{endpoint_type}"\n\n'
            f'Missing required field "{field_name}" (empty string).',
        )

    # Whitespace is stripped for the identifying value; this is the only
    # normalization applied before resolution is attempted -- no
    # case-folding here, since that's specifically what _resolve_endpoint
    # does (or doesn't do) depending on whether resolution succeeds.
    return endpoint_type, raw_value.strip()


def _validate_predicate(predicate: object, index: int) -> str:
    if not isinstance(predicate, str) or predicate not in SUPPORTED_PREDICATES:
        _fail(
            index,
            f'predicate = {predicate!r}\n\nUnsupported relationship predicate.',
        )
    return predicate


def _validate_confidence(confidence: object, index: int) -> float:
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        _fail(index, f"confidence = {confidence!r}\n\nConfidence must be numeric.")

    if not (0.0 <= confidence <= 1.0):  # type: ignore[operator]
        _fail(
            index,
            f"confidence = {confidence!r}\n\nConfidence must be between 0.0 and 1.0.",
        )

    return float(confidence)  # type: ignore[arg-type]


def _validate_source(source: object, index: int) -> str:
    if not isinstance(source, str) or not source.strip():
        _fail(index, f"source = {source!r}\n\nSource must be a non-empty string.")
    return source


def _resolve_endpoint(
    endpoint_type: str,
    value: str,
    known_ingredient_ids: set[str],
    known_vocabulary_term_ids: set[str],
) -> tuple[str, bool]:
    """Attempt to resolve a well-formed endpoint against current knowledge.

    Resolution is an optimization/normalization step, not a prerequisite
    for persisting a relationship. A structurally valid endpoint that
    doesn't currently resolve is not an error -- it's stored exactly as
    supplied, same as a "reference" endpoint. This is what lets a
    relationship assertion survive vocabulary/ingredient knowledge that
    hasn't been curated yet, or that gets refactored later, without the
    seed becoming a set of foreign keys that silently break.

    Returns (value_to_persist, resolved).
    """

    if endpoint_type == "ingredient":
        # ingredients.id is already the canonical form -- nothing to
        # normalize on success, and nothing to rewrite on failure.
        return value, value in known_ingredient_ids

    if endpoint_type == "vocabulary":
        term_id = _slugify(value)
        if term_id in known_vocabulary_term_ids:
            # Resolved: store the canonical term_id so this joins
            # cleanly against vocabulary_terms.term_id regardless of
            # whatever casing relationships.json spelled it with.
            return term_id, True
        # Unresolved: retain exactly as supplied. Slugification is part
        # of resolution, so it isn't applied when resolution fails --
        # we don't rewrite an assertion we couldn't actually confirm
        # against current knowledge.
        return value, False

    # "reference" has no resolver by definition; always stored as supplied.
    return value, False


class RelationshipBuilder(KnowledgeBuilder):
    """Load and validate relationship assertions into the relationships table.

    Scope, per the R0-3 work order: structural/predicate/endpoint
    validation, duplicate detection, and persistence only. No semantic
    inference, canonicalization, graph traversal, or analyzer behavior.

    Endpoint existence is not a build-time invariant: "ingredient" and
    "vocabulary" endpoints are resolved against current knowledge when
    possible, but a well-formed endpoint that doesn't currently resolve
    is still persisted, as supplied -- not rejected. Only structural
    problems (unsupported type, missing/wrong-type field) are errors.
    See _resolve_endpoint.
    """

    name = "relationships"

    def __init__(self, seed_path: Path | None = None) -> None:
        self.seed_path = seed_path or (SEED_DIR / "relationships.json")

    # -- knowledge lookups ------------------------------------------------

    @staticmethod
    def _load_known_ingredient_ids(cursor: sqlite3.Cursor) -> set[str]:
        cursor.execute("SELECT id FROM ingredients")
        return {row[0] for row in cursor.fetchall()}

    @staticmethod
    def _load_known_vocabulary_term_ids(cursor: sqlite3.Cursor) -> set[str]:
        # Resolve against term_id (the deterministic, canonical slug),
        # not the `term` display column -- see the module-level comment
        # on _slugify for why `term` isn't safe to match against.
        cursor.execute("SELECT term_id FROM vocabulary_terms")
        return {row[0] for row in cursor.fetchall()}

    # -- main entry point ---------------------------------------------------

    def run(self, conn: sqlite3.Connection) -> BuildResult:
        print("Building relationships to gastrometric.db")

        try:
            with self.seed_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            relationships = data.get("relationships")
            if not isinstance(relationships, list):
                raise ValueError(
                    "relationships.json must contain a 'relationships' array."
                )

            cursor = conn.cursor()

            known_ingredient_ids = self._load_known_ingredient_ids(cursor)
            known_vocabulary_term_ids = self._load_known_vocabulary_term_ids(cursor)

            # Idempotent rebuild: this builder owns the full contents of
            # the relationships table, same as IngredientBuilder does for
            # ingredients/ingredient_aliases.
            cursor.execute("DELETE FROM relationships")

            relationship_count = 0
            duplicate_count = 0
            unresolved_count = 0

            # key -> (source, confidence, first_seen_index), used to detect
            # duplicate/conflicting assertions within this seed file.
            seen: dict[tuple[str, str, str, str, str], tuple[str, float, int]] = {}

            for index, relationship in enumerate(relationships):
                if not isinstance(relationship, dict):
                    _fail(index, "Relationship entry must be an object.")

                missing = [
                    field
                    for field in _REQUIRED_RELATIONSHIP_FIELDS
                    if field not in relationship
                ]
                if missing:
                    _fail(
                        index,
                        f"Missing required field(s): {', '.join(missing)}.",
                    )

                subject_type, subject_value = _validate_endpoint(
                    "subject", relationship["subject"], index
                )
                object_type, object_value = _validate_endpoint(
                    "object", relationship["object"], index
                )

                predicate = _validate_predicate(relationship["predicate"], index)
                source = _validate_source(relationship["source"], index)
                confidence = _validate_confidence(relationship["confidence"], index)

                subject_value, subject_resolved = _resolve_endpoint(
                    subject_type,
                    subject_value,
                    known_ingredient_ids,
                    known_vocabulary_term_ids,
                )
                object_value, object_resolved = _resolve_endpoint(
                    object_type,
                    object_value,
                    known_ingredient_ids,
                    known_vocabulary_term_ids,
                )
                if not subject_resolved or not object_resolved:
                    unresolved_count += 1

                key = (subject_type, subject_value, predicate, object_type, object_value)

                if key in seen:
                    prev_source, prev_confidence, prev_index = seen[key]

                    if prev_source == source and prev_confidence == confidence:
                        # Exact duplicate assertion -- schema treats the
                        # relationship itself as unique, so this collapses
                        # to a single row rather than an error.
                        duplicate_count += 1
                        continue

                    # Same (subject, predicate, object) but different
                    # source/confidence. The relationships table has no
                    # provenance table and defines the 5-column tuple as
                    # unique, so there is nowhere to preserve a second
                    # provenance record -- this is a genuine conflict.
                    _fail(
                        index,
                        f"Conflicting duplicate of relationships[{prev_index}]:\n"
                        f"subject.type = \"{subject_type}\"\n"
                        f"predicate = \"{predicate}\"\n"
                        f"object.type = \"{object_type}\"\n\n"
                        f'relationships[{prev_index}] has source="{prev_source}", '
                        f"confidence={prev_confidence}, but relationships[{index}] "
                        f'has source="{source}", confidence={confidence}. '
                        "The relationship schema treats (subject, predicate, "
                        "object) as unique, so these cannot both be stored.",
                    )

                seen[key] = (source, confidence, index)

                cursor.execute(
                    """
                    INSERT INTO relationships (
                        subject_type,
                        subject_id,
                        predicate,
                        object_type,
                        object_id,
                        source,
                        confidence
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        subject_type,
                        subject_value,
                        predicate,
                        object_type,
                        object_value,
                        source,
                        confidence,
                    ),
                )
                relationship_count += 1

            conn.commit()

            print(f"{relationship_count:,} relationships added to relationships")
            if duplicate_count:
                print(f"{duplicate_count:,} duplicate relationship(s) skipped")
            if unresolved_count:
                print(
                    f"{unresolved_count:,} relationship(s) persisted with an "
                    "unresolved endpoint (valid, but no matching ingredient/"
                    "vocabulary entry exists yet)"
                )

            return BuildResult(
                builder_name=self.name,
                distinct_inputs=len(relationships),
                relationships_created=relationship_count,
                duplicates_skipped=duplicate_count,
            )

        except FileNotFoundError:
            conn.rollback()
            print(f"ERROR: Could not find {self.seed_path}")
            raise

        except json.JSONDecodeError as e:
            conn.rollback()
            print(f"ERROR: Failed to parse {self.seed_path.name}: {e}")
            raise

        except RelationshipBuildError as e:
            conn.rollback()
            print(f"ERROR: {e}")
            raise

        except sqlite3.Error as e:
            conn.rollback()
            print(f"ERROR: Failed to write to database: {e}")
            raise

        except Exception as e:
            conn.rollback()
            print(f"ERROR: {e}")
            raise


def build_relationships(seed_path: Path | None = None) -> BuildResult:
    """Build relationship assertions into gastrometric.db."""

    conn = sqlite3.connect(DB_PATH)
    try:
        return RelationshipBuilder(seed_path).run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    build_relationships()