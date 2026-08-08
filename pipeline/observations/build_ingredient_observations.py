"""
gastrometric/pipeline/observation/build_ingredient_observations.py

Observation Builder stage.

    Recipe -> Parser -> [OBSERVATION BUILDER] -> Identity Resolution -> ...

Responsibility (and ONLY responsibility): group immutable parser spans into
candidate ingredient observations, and record that grouping as rows in
`ingredient_observations` / `observation_spans`.

This module never:
  - CREATEs/ALTERs schema (schema lives only in db/init_db.py)
  - modifies recipe_ingredient_spans
  - resolves identity, aliases, or nutrition (vocabulary_id / ingredient_id
    on recipe_ingredient_spans are reserved for Identity Resolution -- this
    stage never reads or writes them)
  - introduces culinary vocabulary into Python
  - uses randomness, confidence scores, or probabilistic matching

Everything here is a fixed, explicit rule applied deterministically. Same
parser output in -> same observations out, every time.

--------------------------------------------------------------------------
UPSTREAM SCHEMA (actual, as populated by the parser)

    recipe_ingredient_spans(
        span_id               INTEGER PRIMARY KEY,
        recipe_ingredient_id  INTEGER NOT NULL REFERENCES recipe_ingredient_lines_parsed(id),
        start_offset          INTEGER,
        end_offset            INTEGER,
        raw_text              TEXT NOT NULL,
        normalized_text       TEXT NOT NULL,
        span_type             TEXT NOT NULL,
        vocabulary_id         TEXT,   -- not read/written by this stage
        ingredient_id         TEXT,   -- not read/written by this stage
        metadata_json         TEXT,
        parser_order          INTEGER NOT NULL
    )

`recipe_ingredient_id` is the clause: one row in recipe_ingredient_lines_parsed
is one ingredient line (or, for coordinated ingredients like "squash or
zucchini", one candidate ingredient -- that splitting already happens
upstream of this stage, so a clause here almost always carries at most one
Ingredient span).

Observed span_type vocabulary (from the type-inventory debug report) and
how this stage treats each one:

    Ingredient                  -> seeds an observation (attach_primary_ingredients)
    VolumeExpression             \
    WeightExpression              > role: quantity (attach_quantities)
    NaturalPortionExpression     /
    Measurement                 /
    PackageExpression            -> role: package (attach_packages)
    Preparation                  -> role: preparation (attach_preparation)
    Modifier                     \
    State                         > role: modifier (attach_modifiers)
    Dimension (not yet observed) -> role: dimension (attach_modifiers)
    Temperature                  -> role: temperature (attach_temperature)
    Brand                        -> role: brand (attach_brand)
    Unknown, Noise, GrammarMarker -> intentionally never attached (evidence
                                      preserved for the analyzer/curator to
                                      interpret later, e.g. as vocabulary gaps)

There is no separate span type for "the unit" once a quantity has already
been attached -- VolumeExpression/WeightExpression/NaturalPortionExpression
already bundle amount and unit together (e.g. "2 tbsp" is a single span),
so quantity-bearing spans attach directly to the nearest ingredient in one
step rather than through an intermediate "attach the unit to the quantity"
step.
--------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional

# Same convention as the other pipeline stages (see scripts/link_ingredients.py):
# path is computed relative to this file, not the current working directory.
# This file lives at gastrometric/pipeline/observations/, two levels below
# the repo root (gastrometric/) that contains data/gastrometric.db.
DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "gastrometric.db"
)


# ---------------------------------------------------------------------------
# Data model (in-memory only -- nothing here is a schema definition)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ParserSpan:
    span_id: int
    recipe_ingredient_id: int
    span_type: str
    raw_text: str
    normalized_text: str
    start_offset: Optional[int]
    end_offset: Optional[int]
    parser_order: int


@dataclass
class PendingObservation:
    """An observation under construction, before it is written to the DB."""
    recipe_ingredient_id: int
    observation_index: int
    primary_span: ParserSpan
    # role_code -> list of span ids attached under that role
    attachments: dict = field(default_factory=lambda: defaultdict(list))

    def attach(self, role_code: str, span: ParserSpan) -> None:
        self.attachments[role_code].append(span.span_id)

    def attached_span_ids(self) -> set:
        out = set()
        for ids in self.attachments.values():
            out.update(ids)
        return out


# ---------------------------------------------------------------------------
# SQL: read parser evidence, write observation evidence
# ---------------------------------------------------------------------------

CLAUSE_QUERY = "SELECT id FROM recipe_ingredient_lines_parsed ORDER BY id"

SPAN_QUERY = """
    SELECT span_id, recipe_ingredient_id, span_type, raw_text, normalized_text,
           start_offset, end_offset, parser_order
    FROM recipe_ingredient_spans
    WHERE recipe_ingredient_id = ?
    ORDER BY parser_order
"""

INSERT_OBSERVATION = """
    INSERT INTO ingredient_observations (recipe_ingredient_id, observation_index)
    VALUES (?, ?)
"""

INSERT_OBSERVATION_SPAN = """
    INSERT INTO observation_spans (observation_id, span_id, role_code)
    VALUES (?, ?, ?)
"""


def fetch_spans_for_clause(conn: sqlite3.Connection, recipe_ingredient_id: int) -> list[ParserSpan]:
    rows = conn.execute(SPAN_QUERY, (recipe_ingredient_id,)).fetchall()
    return [
        ParserSpan(
            span_id=r[0], recipe_ingredient_id=r[1], span_type=r[2],
            raw_text=r[3], normalized_text=r[4],
            start_offset=r[5], end_offset=r[6], parser_order=r[7],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Attachment rules
#
# Each rule is a small, explicit, deterministic function. Rules never
# overwrite a previous rule's attachments; they only add new ones. Future
# attachment behavior should be added as a new rule appended to RULES,
# not by editing an existing rule's logic.
# ---------------------------------------------------------------------------

def _nearest_by_parser_order(target: ParserSpan, candidates: list[PendingObservation]):
    """
    Deterministic nearest-neighbor selection used by several rules below.

    Distance is |target.parser_order - candidate.primary_span.parser_order|.
    Ties are broken in favor of the candidate whose primary_span has the
    smaller parser_order (i.e. the earlier / leftmost ingredient in the
    clause). This tie-break is fixed and does not depend on input order,
    so results are reproducible.

    Returns None if `candidates` is empty -- callers must treat that as
    "leave unattached", never as "pick anything."
    """
    best: Optional[PendingObservation] = None
    best_distance: float = float("inf")
    for obs in candidates:
        distance = abs(target.parser_order - obs.primary_span.parser_order)
        if distance < best_distance or (
            distance == best_distance
            and best is not None
            and obs.primary_span.parser_order < best.primary_span.parser_order
        ):
            best = obs
            best_distance = distance
    return best


def attach_primary_ingredients(clause_spans: list[ParserSpan]) -> list[PendingObservation]:
    """
    Seed one observation per Ingredient-type span in clause order.

    This is the only rule permitted to *create* observations. All later
    rules only attach additional spans to observations that already exist.
    """
    observations: list[PendingObservation] = []
    index = 0
    for span in clause_spans:
        if span.span_type == "Ingredient":
            obs = PendingObservation(
                recipe_ingredient_id=span.recipe_ingredient_id,
                observation_index=index,
                primary_span=span,
            )
            obs.attach("primary_ingredient", span)
            observations.append(obs)
            index += 1
    return observations


def split_coordinated_ingredients(
    observations: list[PendingObservation], clause_spans: list[ParserSpan]
) -> list[PendingObservation]:
    """
    Explicit extension point for coordinated-ingredient handling
    (e.g. "salt and pepper", "carrots, celery, and onion").

    Under the current parser, each coordinated ingredient is already
    recognized as its own Ingredient span, so attach_primary_ingredients
    above already produces one observation per ingredient and there is
    nothing further to split here. This rule exists as a named, dedicated
    step -- per the "add a rule, don't rewrite a rule" architecture -- so
    that if a future parser change ever emits a single combined Ingredient
    span for a coordinated list, the fix belongs here.

    It deliberately does not attempt any text-splitting: this stage never
    re-tokenizes or rereads raw text, and doing so here would violate
    that contract.
    """
    return observations


def _attach_nearest_to_ingredient(
    role_code: str, span_type: str,
    observations: list[PendingObservation], clause_spans: list[ParserSpan],
) -> None:
    """Generic nearest-ingredient attachment, used for several span types."""
    if not observations:
        return
    for span in clause_spans:
        if span.span_type != span_type:
            continue
        target = _nearest_by_parser_order(span, observations)
        if target is not None:
            target.attach(role_code, span)


def attach_quantities(observations, clause_spans) -> None:
    """
    DEBUGGING NOTE (found via the span-type inventory report): the real
    parser has no separate "Quantity" span type. The amount is carried by
    one of several self-contained spans -- VolumeExpression ("2 tbsp"),
    WeightExpression ("14 oz" / bare "1"), NaturalPortionExpression
    ("a pinch"-style counts), or occasionally a bare "Measurement" span
    that already includes both a number and, sometimes, a lone unit
    fragment ("1", " pinch"). None of these carry a separate unit
    sub-span to attach afterward, so there is nothing left for a second
    "attach the unit to the quantity" step to do -- that step (formerly
    attach_measurements) was a no-op by construction, because the
    "Quantity" type it depended on never existed. It has been removed;
    every amount-bearing span type now attaches directly to the nearest
    ingredient under the single "quantity" role.
    """
    for span_type in ("VolumeExpression", "WeightExpression", "NaturalPortionExpression", "Measurement"):
        _attach_nearest_to_ingredient("quantity", span_type, observations, clause_spans)


def attach_packages(observations, clause_spans) -> None:
    _attach_nearest_to_ingredient("package", "PackageExpression", observations, clause_spans)


def attach_preparation(observations, clause_spans) -> None:
    _attach_nearest_to_ingredient("preparation", "Preparation", observations, clause_spans)


def attach_modifiers(observations, clause_spans) -> None:
    """
    DEBUGGING NOTE: the real span types are "Modifier" and "State"
    (e.g. "whole", "smoked", "fermented") -- "Attribute"/"Dimension" do
    not occur in the data at all, so this rule never fired.

    The original rule also required strict span-order adjacency
    (distance == 1) to the ingredient, on the theory that a modifier
    "immediately" describes it. Real data breaks that: e.g. in
    "1 tsp. smoked Spanish paprika", the clause is
        [0] Ingredient 'Spanish paprika'
        [1] VolumeExpression '1'
        [2] State 'smoked'
    -- "smoked" sits at distance 2 because the amount span comes between
    it and the ingredient in parser_order, not because it's ambiguous.
    Since clauses in this dataset carry at most one ingredient candidate
    (coordinated ingredients like "squash or zucchini" already arrive as
    separate recipe_ingredient_id rows upstream), nearest-ingredient
    attachment is just as safe here as strict adjacency and doesn't drop
    real modifiers. Dimension is kept in the type list in case it's used
    in the future; it does not currently appear in the data.
    """
    for span_type, role_code in (
        ("Modifier", "modifier"),
        ("State", "modifier"),
        ("Dimension", "dimension"),
    ):
        _attach_nearest_to_ingredient(role_code, span_type, observations, clause_spans)


def attach_temperature(observations, clause_spans) -> None:
    _attach_nearest_to_ingredient("temperature", "Temperature", observations, clause_spans)


def attach_brand(observations, clause_spans) -> None:
    _attach_nearest_to_ingredient("brand", "Brand", observations, clause_spans)


RULES = [
    split_coordinated_ingredients,
    attach_quantities,
    attach_packages,
    attach_preparation,
    attach_modifiers,
    attach_temperature,
    attach_brand,
]


# ---------------------------------------------------------------------------
# Per-clause orchestration
# ---------------------------------------------------------------------------

def build_observations_for_clause(clause_spans: list[ParserSpan]) -> list[PendingObservation]:
    observations = attach_primary_ingredients(clause_spans)
    for rule in RULES:
        if rule is split_coordinated_ingredients:
            observations = rule(observations, clause_spans)
        else:
            rule(observations, clause_spans)
    return observations


def persist_observations(
    conn: sqlite3.Connection, observations: list[PendingObservation]
) -> list[int]:
    observation_ids = []
    for obs in observations:
        cur = conn.execute(
            INSERT_OBSERVATION, (obs.recipe_ingredient_id, obs.observation_index)
        )
        observation_id = cur.lastrowid
        observation_ids.append(observation_id)
        for role_code, span_ids in obs.attachments.items():
            for span_id in span_ids:
                conn.execute(INSERT_OBSERVATION_SPAN, (observation_id, span_id, role_code))
    return observation_ids


# ---------------------------------------------------------------------------
# Run summary
# ---------------------------------------------------------------------------

@dataclass
class RunStats:
    clauses_processed: int = 0
    clauses_with_multiple_observations: int = 0
    observations_created: int = 0
    spans_attached: int = 0
    spans_seen: int = 0
    unattached_span_types: Counter = field(default_factory=Counter)

    @property
    def unattached_spans(self) -> int:
        return self.spans_seen - self.spans_attached

    def report(self) -> str:
        avg_obs_per_clause = (
            self.observations_created / self.clauses_processed if self.clauses_processed else 0.0
        )
        avg_spans_per_obs = (
            self.spans_attached / self.observations_created if self.observations_created else 0.0
        )
        lines = [
            f"Ingredient clauses processed: {self.clauses_processed}",
            f"Observations created: {self.observations_created}",
            f"Average observations per clause: {avg_obs_per_clause:.1f}",
            f"Average spans per observation: {avg_spans_per_obs:.1f}",
            f"Unattached spans: {self.unattached_spans}",
            f"Clauses producing multiple observations: {self.clauses_with_multiple_observations}",
        ]
        if self.unattached_span_types:
            top = self.unattached_span_types.most_common(5)
            formatted = ", ".join(f"{t}: {n}" for t, n in top)
            lines.append(f"Most common unattached span types: {formatted}")
        return "\n".join(lines)


def build_all_observations(conn: Optional[sqlite3.Connection] = None) -> RunStats:
    """
    Entry point used by the pipeline. Matches the other stages' convention
    of managing its own connection (see scripts/link_ingredients.py) AND
    of printing its own progress/summary (see parse_ingredient_lines,
    the vocabulary builder, etc. -- every stage reports on itself; the
    orchestrator doesn't print on a stage's behalf). Called with no
    arguments, it opens DB_PATH itself, closes it when done, and prints
    its own run summary before returning.

    `conn` stays available as an optional override so this function remains
    unit-testable against an in-memory database, and so a future caller that
    wants to share one connection across several stages still can. The
    summary still prints in that case too, for consistency.
    """
    owns_connection = conn is None
    if conn is None:
        conn = sqlite3.connect(DB_PATH)

    try:
        print("Building ingredient observations")
        stats = RunStats()
        clause_ids = [r[0] for r in conn.execute(CLAUSE_QUERY).fetchall()]

        for recipe_ingredient_id in clause_ids:
            clause_spans = fetch_spans_for_clause(conn, recipe_ingredient_id)
            if not clause_spans:
                continue

            stats.clauses_processed += 1
            stats.spans_seen += len(clause_spans)

            observations = build_observations_for_clause(clause_spans)
            persist_observations(conn, observations)

            stats.observations_created += len(observations)
            if len(observations) > 1:
                stats.clauses_with_multiple_observations += 1

            attached_ids = set()
            for obs in observations:
                attached_ids |= obs.attached_span_ids()
            stats.spans_attached += len(attached_ids)
            for span in clause_spans:
                if span.span_id not in attached_ids:
                    stats.unattached_span_types[span.span_type] += 1

        conn.commit()
        print(stats.report())
        return stats
    finally:
        if owns_connection:
            conn.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build ingredient observations from parser spans.")
    parser.add_argument(
        "db_path", nargs="?", default=None,
        help="Path to the gastrometric sqlite database (defaults to DB_PATH)",
    )
    args = parser.parse_args()

    if args.db_path:
        conn = sqlite3.connect(args.db_path)
        try:
            build_all_observations(conn)
        finally:
            conn.close()
    else:
        build_all_observations()


if __name__ == "__main__":
    main()