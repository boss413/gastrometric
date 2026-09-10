"""
Orchestration for the recipe-ingredient-understanding pipeline.

This module is the source-aware counterpart to `gastrometric.understanding
.ingredient_parser`: it knows about SQLite, table names, and how a
`recipe_ingredient_line_id` threads `lexical_spans` through to
`ingredient_parse_trees`. `ingredient_parser.py` itself knows none of
this -- `IngredientParser.parse()` takes a plain `List[LexicalToken]` and
returns a `ParseResult`, with no awareness of where those tokens came from
or where the result is going. That separation is intentional: parsing
stays testable and reusable independent of storage, and every
source-specific concern (reading, writing, schema checks, composing with
the lex and analyzer stages) lives here instead.

Per the plan discussed with the maintainer, this file is where the
lex-stage, parser-stage, and analyzer-stage orchestration all live, so
the full pipeline -- lex -> parse -> analyze -- can be composed and run
from one place, or any stage run/debugged in isolation. Each stage's
public entry point (`build_lexical_spans`, `process_recipe_lines`,
`persist_all_lines`) opens and closes its own connection and commits its
own transaction, so a caller (e.g. `rebuild_db.py`, or a future
orchestrator processing lines from a different source) can invoke them
individually, in series, or compose new orchestration around the
underlying pure functions (`lex`, `IngredientParser.parse`,
`analyze_line`/`analyze_parse_result`) the same way this file does,
without needing to go through the whole-database convenience functions.
"""

import dataclasses
import itertools
import json
import sqlite3
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from gastrometric.config.paths import DB_PATH
from gastrometric.understanding.analyzer import analyze_parse_result, _project_selected_references
from gastrometric.understanding.ingredient_parser import IngredientParser, LexicalToken
from gastrometric.understanding.lex import LexicalSpan, lex


# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------


def _connect() -> sqlite3.Connection:
    """Connect to the Gastrometric SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _fetch_raw_lines(conn: sqlite3.Connection) -> List[Tuple[int, str]]:
    """Read every (id, raw_text) row from recipe_ingredient_lines_raw.

    Ordered by id for deterministic processing order.
    """
    cursor = conn.execute(
        "SELECT id, raw_text FROM recipe_ingredient_lines_raw ORDER BY id"
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def _clear_lexical_spans(conn: sqlite3.Connection) -> None:
    """Truncate lexical_spans so the rebuild is deterministic and
    rerunnable. No incremental updates -- full regeneration only.
    """
    conn.execute("DELETE FROM lexical_spans;")


def _insert_span(
    conn: sqlite3.Connection, recipe_ingredient_line_id: int, span: LexicalSpan
) -> None:
    """Persist a single LexicalSpan exactly as returned by lex().

    One row per classification in ``span.span_types`` -- every row
    shares the same ``span_order``/offsets/text, and each row's
    ``span_type`` is treated as an opaque string -- never inspected or
    transformed -- so future lexer versions may emit new span types with
    no code changes here.

    ``normalized_value``, ``knowledge_id`` and ``source_vocabulary`` are
    each aligned positionally with ``span_types`` for vocabulary-matched
    spans (both tuples are built in lock-step by lex()'s
    ``_merge_vocabulary_spans``) -- row i gets classification i's OWN
    normalized value, never another classification's. For span types
    that carry no vocabulary provenance (Symbol/Quantity/Unknown --
    identifiable by ``source_vocabulary`` being None),
    ``normalized_value``/``knowledge_id`` are plain scalars and
    ``source_vocabulary`` is None, same as before.
    """
    fields = dataclasses.asdict(span)
    span_types: Tuple[str, ...] = fields["span_types"]
    source_vocabulary: Optional[Tuple[str, ...]] = fields["source_vocabulary"]

    if source_vocabulary is not None:
        sources: Iterable[Optional[str]] = source_vocabulary
        normalized_values: Iterable[Optional[Any]] = fields["normalized_value"]
        knowledge_ids: Iterable[Optional[Any]] = fields["knowledge_id"]
    else:
        sources = itertools.repeat(None)
        normalized_values = itertools.repeat(fields["normalized_value"])
        knowledge_ids = itertools.repeat(fields["knowledge_id"])

    for span_type, source, normalized_value, knowledge_id in zip(
        span_types, sources, normalized_values, knowledge_ids
    ):
        conn.execute(
            """
            INSERT INTO lexical_spans (
                recipe_ingredient_line_id,
                span_order,
                start_offset,
                end_offset,
                text,
                normalized_value,
                span_type,
                knowledge_id,
                source_vocabulary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recipe_ingredient_line_id,
                fields["span_order"],
                fields["start_offset"],
                fields["end_offset"],
                fields["text"],
                normalized_value,
                span_type,
                knowledge_id,
                source,
            ),
        )


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def _print_statistics(lines_processed: int, all_spans: Sequence[LexicalSpan]) -> None:
    """Print rebuild statistics. Span-type counts are derived dynamically
    from whatever span_type values were actually produced -- no hardcoded
    list of known span types.

    A span with multiple classifications contributes to every
    classification's count (matching how it's persisted: one row per
    classification -- see _insert_span). "Lexical spans created" still
    counts distinct LexicalSpan occurrences, not rows.
    """
    total_spans = len(all_spans)
    average = (total_spans / lines_processed) if lines_processed else 0.0

    type_counts: Counter = Counter(
        span_type for span in all_spans for span_type in span.span_types
    )

    print(f"Ingredient lines processed: {lines_processed}")
    print()
    print(f"Lexical spans created: {total_spans}")
    print()
    print(f"Average spans per line: {average:.2f}")
    print()
    print("Span types")
    print()
    for span_type in sorted(type_counts):
        print(f"{span_type}: {type_counts[span_type]}")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def build_lexical_spans() -> None:
    """Read every recipe_ingredient_lines_raw row, lex it exactly once,
    and persist the resulting spans into lexical_spans.

    Deterministic and rerunnable: lexical_spans is fully truncated and
    regenerated on every call.
    """
    conn = _connect()
    try:
        raw_lines = _fetch_raw_lines(conn)
        _clear_lexical_spans(conn)

        all_spans: List[LexicalSpan] = []

        for line_id, raw_text in raw_lines:
            try:
                spans = lex(raw_text)
            except Exception:
                print(f"lex() failed on ingredient line id={line_id!r}")
                print(f"raw_text={raw_text!r}")
                raise

            for span in spans:
                _insert_span(conn, line_id, span)
            all_spans.extend(spans)

        conn.commit()
    finally:
        conn.close()

    _print_statistics(len(raw_lines), all_spans)


# ---------------------------------------------------------------------------
# PARSER STAGE
# ---------------------------------------------------------------------------
#
# Required schema (created by init_db.py -- NOT by this module):
#
#   recipe_ingredient_lines_raw(
#       id, ingredient_block_id, recipe_id, recipe_section_id,
#       recipe_name, section_name, line_index, raw_text
#   )
#   lexical_spans(
#       span_id, recipe_ingredient_line_id, span_order, start_offset,
#       end_offset, text, normalized_value, span_type, knowledge_id,
#       source_vocabulary
#   )
#   ingredient_parse_trees(
#       id INTEGER PRIMARY KEY, recipe_ingredient_line_id INTEGER,
#       parse_tree_json TEXT
#   )
#
# `parse_tree_json` holds a serialized `ParseResult` (node_type
# "ParseResult", with a `candidates` list of one or more `Candidate`
# objects, each `{tree: <complete IngredientLine>, unresolved: [...]}`)
# -- not a single tree. An unambiguous line still produces exactly one
# candidate, so a consumer that only needs "the" tree can read
# `candidates[0].tree`, but the column always carries the full candidate
# set rather than a pre-chosen interpretation.
#
# `recipe_ingredient_line_id` on both `lexical_spans` and
# `ingredient_parse_trees` is expected to reference
# `recipe_ingredient_lines_raw.id`. This function only reads `lexical_spans`
# and writes `ingredient_parse_trees`; it does not create, alter, or assume
# write access to any table.

def process_recipe_lines(db_path: Any = DB_PATH):
    db_path_str = str(db_path)
    conn = sqlite3.connect(db_path_str)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing_tables = {row['name'] for row in cursor.fetchall()}
    required_tables = {'lexical_spans', 'recipe_ingredient_lines_raw', 'ingredient_parse_trees'}

    missing_tables = required_tables - existing_tables
    if missing_tables:
        conn.close()
        raise RuntimeError(
            f"Expected tables {missing_tables} are missing in the database at {db_path_str}. "
            "Please run init_db.py first to create the necessary tables."
        )

    cursor.execute('''
        SELECT DISTINCT recipe_ingredient_line_id
        FROM lexical_spans
        ORDER BY recipe_ingredient_line_id
    ''')
    line_ids = [row['recipe_ingredient_line_id'] for row in cursor.fetchall()]

    parser = IngredientParser()

    for line_id in line_ids:
        cursor.execute('''
            SELECT * FROM lexical_spans
            WHERE recipe_ingredient_line_id = ?
            ORDER BY span_order ASC
        ''', (line_id,))

        spans = []
        for row in cursor.fetchall():
            row_keys = row.keys()
            token = LexicalToken(
                span_id=row['span_id'],
                span_order=row['span_order'],
                start_offset=row['start_offset'],
                end_offset=row['end_offset'],
                text=row['text'],
                normalized_value=row['normalized_value'],
                span_type=row['span_type'],
                knowledge_id=row['knowledge_id'] if 'knowledge_id' in row_keys else None,
                source_vocabulary=row['source_vocabulary'] if 'source_vocabulary' in row_keys else None,
            )
            spans.append(token)

        parse_result = parser.parse(spans)
        tree_json = json.dumps(parse_result.to_dict())

        cursor.execute('''
            INSERT INTO ingredient_parse_trees
            (recipe_ingredient_line_id, parse_tree_json)
            VALUES (?, ?)
        ''', (line_id, tree_json))

    conn.commit()
    conn.close()
    print(f"Successfully built ASTs for {len(line_ids)} ingredient lines.")
# ---------------------------------------------------------------------------
# ANALYZER STAGE
# ---------------------------------------------------------------------------
#
# Moved verbatim from `gastrometric.understanding.analyzer`, same as the
# parser stage above: `analyze_line`/`analyze_parse_result` (pure -- JSON/
# dict in, dict out, no SQLite) stay in `analyzer.py`, exactly like
# `IngredientParser.parse()` stayed in `ingredient_parser.py`. Everything
# below that opens a `sqlite3.Connection` -- reading `ingredient_parse_trees`,
# writing `recipe_ingredient_lines_parsed`/`analysis_records`/
# `analysis_candidate_evaluations`/`analysis_evidence`, and the execution
# report built on top of that -- moves here, mirroring `process_recipe_lines`
# above exactly.

# ---------------------------------------------------------------------------
# DB reader + orchestration
#
# `ingredient_parse_trees` had no existing reader prior to this module. This
# reads it and evaluates each row; per the RO-10 work order boundary, it
# does NOT write anything -- RO-8 (not yet implemented) owns the analysis
# persistence artifact, and if RO-8 assigns write responsibility to the
# Analyzer, a thin caller can iterate this generator and persist each
# result without that write path living inside evaluation logic here.
# ---------------------------------------------------------------------------


def _read_parse_trees(
    conn: sqlite3.Connection, line_ids: Optional[Iterable[int]] = None
) -> Iterable[sqlite3.Row]:
    cursor = conn.cursor()
    if line_ids is not None:
        line_ids = list(line_ids)
        placeholders = ",".join("?" for _ in line_ids)
        cursor.execute(
            f"""
            SELECT id, recipe_ingredient_line_id, parse_tree_json
            FROM ingredient_parse_trees
            WHERE recipe_ingredient_line_id IN ({placeholders})
            ORDER BY recipe_ingredient_line_id, id
            """,
            line_ids,
        )
    else:
        cursor.execute(
            """
            SELECT id, recipe_ingredient_line_id, parse_tree_json
            FROM ingredient_parse_trees
            ORDER BY recipe_ingredient_line_id, id
            """
        )
    return cursor.fetchall()


def analyze_all_lines(
    db_path: Optional[Any] = None, line_ids: Optional[Iterable[int]] = None
) -> Iterable[Tuple[int, int, dict]]:
    """Reads persisted parse trees from `ingredient_parse_trees` and yields
    `(recipe_ingredient_line_id, parse_tree_id, canonical_semantic_result)`
    triples. `parse_tree_id` is the id of the exact `ingredient_parse_trees`
    row the result was produced from, so a persistence layer can populate
    `analysis_records.parse_tree_id` without a second lookup. Read-only:
    see module docstring above.

    `line_ids=None` (default) reads every persisted line -- the current
    whole-database debugging convenience (RO-10 SS15). Passing an explicit
    iterable of `recipe_ingredient_line_id` values restricts the read to
    just those lines, without changing anything else about this function's
    behavior -- this is the "clean invocation boundary" SS15 asks for, so
    a future targeted-pipeline caller can reuse this unchanged.
    """
    from gastrometric.config.paths import DB_PATH
    from gastrometric.knowledge.loader import knowledge as runtime_knowledge

    path = str(db_path or DB_PATH)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        for row in _read_parse_trees(conn, line_ids):
            parse_result = json.loads(row["parse_tree_json"])
            result = analyze_parse_result(parse_result, runtime_knowledge)
            yield row["recipe_ingredient_line_id"], row["id"], result
    finally:
        conn.close()

_STATUS_ORDER = ["resolved", "ambiguous", "unresolved", "invalid"]

# Every `unresolved[].reason` string this Analyzer can currently emit,
# verified by direct inspection of the source (grep '"reason":'), not
# guessed or restated from an external description. Kept here purely as
# documentation -- the report below tallies whatever actually appears via
# a Counter, so this list is a reading aid, not a hardcoded bucket set,
# and a reason absent from this list would still show up correctly if the
# analyzer ever grew one.
#
#   unrecognized_span                    -- _resolve_ingredient,
#                                            _build_preparation_modifier,
#                                            _resolve_component,
#                                            _carry_through_unresolved
#   unknown_ingredient                   -- _resolve_ingredient
#   unrecognized_measurement_structure   -- _resolve_dangling_ranges
#   range_quantity_not_representable     -- _resolve_dangling_ranges (mismatch #1)
#   dangling_range                       -- _build_scalar_quantity (mismatch #1)
#   no_quantity_value                    -- _build_scalar_quantity
#   unparseable_quantity_value           -- _build_scalar_quantity
#   missing_unit                         -- _build_scalar_quantity
#   unparseable_quantity                 -- _assign_quantities' fallback;
#                                            currently unreachable, since
#                                            _build_scalar_quantity always
#                                            returns one of the four
#                                            specific reasons above rather
#                                            than None -- kept as a
#                                            defensive catch-all, not dead
#                                            code to be relied on staying
#                                            empty if that contract changes.
#   additional_measurement_unsupported   -- _assign_quantities
#   package_count_or_term_not_determined -- _build_package
#   no_references_produced               -- _evaluate_candidate (empty candidate)
#   no_parser_candidates                 -- analyze_parse_result (empty ParseResult)

# ---------------------------------------------------------------------------
# Persistence -- both RO-10 outputs, one transaction per run
# ---------------------------------------------------------------------------


def _fetch_line_lineage(conn: sqlite3.Connection) -> Dict[int, Tuple[int, int, int]]:
    """Bulk-fetches (ingredient_block_id, recipe_id, recipe_section_id) for
    every `recipe_ingredient_lines_raw` row, keyed by its id -- one query
    for the whole run rather than one per line. Reproduces the lineage
    dimensions already denormalized directly onto that table (see the
    target-DDL comment above), rather than inventing a new lineage model
    (work order SS1)."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, ingredient_block_id, recipe_id, recipe_section_id
        FROM recipe_ingredient_lines_raw
        """
    )
    return {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}


def _persist_analysis_record(
    conn: sqlite3.Connection,
    recipe_ingredient_line_id: int,
    parse_tree_id: int,
    result: dict,
) -> int:
    """Writes the diagnostic artifact (RO-8): one canonical semantic result
    plus its evaluation/evidence projections. `result` is inserted into
    `canonical_result_json` unmodified -- the complete, schema-conforming
    return value of `analyze_parse_result()`, never narrowed. The rows
    below are query projections derived from it, not a second source of
    truth. Caller owns the transaction. Returns the new
    `analysis_records.id`.

    Assumes init_db.py's DDL for `analysis_candidate_evaluations` has:
      - `interpretation_id TEXT NOT NULL` (NOT `candidate_id` -- there is
        no candidate identity independent of `interpretation.id`).
      - `evaluation_state`'s CHECK allowing exactly
        {'resolved','ambiguous','unresolved','invalid'}, identical to
        `interpretation.status` -- inserted verbatim below, no
        translation.
    Both confirmed in prior review; this will raise a sqlite3
    IntegrityError/OperationalError if that DDL hasn't actually been
    applied yet.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO analysis_records
            (recipe_ingredient_line_id, parse_tree_id, status,
             selected_interpretation_id, canonical_result_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            recipe_ingredient_line_id,
            parse_tree_id,
            result["status"],
            result.get("selected_interpretation"),
            json.dumps(result),
        ),
    )
    analysis_record_id = cursor.lastrowid
    # cursor.lastrowid is typed Optional[int] (None only when the last
    # statement wasn't an INSERT, or no row was inserted); immediately
    # after the INSERT above on an AUTOINCREMENT table, it is always a
    # real int.
    assert analysis_record_id is not None
    for interpretation in result["interpretations"]:
        cursor.execute(
            """
            INSERT INTO analysis_candidate_evaluations
                (analysis_record_id, interpretation_id, evaluation_state)
            VALUES (?, ?, ?)
            """,
            (analysis_record_id, interpretation["id"], interpretation["status"]),
        )
        evaluation_id = cursor.lastrowid
        for evidence in interpretation.get("evidence", []):
            cursor.execute(
                """
                INSERT INTO analysis_evidence
                    (analysis_candidate_evaluation_id, kind, record_id, effect)
                VALUES (?, ?, ?, ?)
                """,
                (evaluation_id, evidence["kind"], evidence["record_id"], evidence["effect"]),
            )
    return analysis_record_id


def _persist_parsed_rows(
    conn: sqlite3.Connection,
    recipe_ingredient_line_id: int,
    ingredient_block_id: int,
    recipe_id: int,
    recipe_section_id: int,
    rows: List[Dict[str, Any]],
) -> int:
    """Writes the primary downstream output (`recipe_ingredient_lines_parsed`)
    for one line. Lineage FKs mirror `recipe_ingredient_lines_raw`'s own
    convention exactly. Caller owns the transaction. Returns the number of
    rows written."""
    cursor = conn.cursor()
    for row in rows:
        cursor.execute(
            """
            INSERT INTO recipe_ingredient_lines_parsed (
                recipe_ingredient_line_id, ingredient_block_id, recipe_id, recipe_section_id,
                ingredient_id, ingredient_phrase, ingredient_name_original,
                grams, ml,
                imperial_weight_value, imperial_weight_unit,
                imperial_volume_value, imperial_volume_unit,
                natural_portion_value, natural_portion_min, natural_portion_max, natural_portion,
                packaging_count, packaging_size_value, packaging_size_unit, packaging,
                preparation, size, notes,
                optional, alt_group_id, alt_kind
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recipe_ingredient_line_id, ingredient_block_id, recipe_id, recipe_section_id,
                row["ingredient_id"], row["ingredient_phrase"], row["ingredient_name_original"],
                row["grams"], row["ml"],
                row["imperial_weight_value"], row["imperial_weight_unit"],
                row["imperial_volume_value"], row["imperial_volume_unit"],
                row["natural_portion_value"], row["natural_portion_min"], row["natural_portion_max"], row["natural_portion"],
                row["packaging_count"], row["packaging_size_value"], row["packaging_size_unit"], row["packaging"],
                row["preparation"], row["size"], row["notes"],
                row["optional"], row["alt_group_id"], row["alt_kind"],
            ),
        )
    return len(rows)


def _classify_ambiguous_reason(result: dict) -> str:
    """Distinguishes SS I.4's compound quantity/package-scope ambiguity (a
    single viable interpretation whose own status is "ambiguous") from
    genuine multi-candidate ambiguity (more than one viable
    interpretation), using only `analyze_parse_result()`'s public output
    -- the same viable-count logic `_derive_result` already uses, not a
    new heuristic.

    RO-9 SS M revision: multi-candidate ambiguity now splits further.
    When `_derive_result` still populated a `selected_interpretation`
    despite the top-level status being "ambiguous", that means evidence
    weighting found a genuine, unresolvable tie (case 3 in
    `_project_selected_references`'s docstring) rather than the old
    catch-all -- worth its own bucket so a curator scanning the report
    can tell "these still have a row, just need a tiebreak" apart from
    "these produced nothing at all"."""
    viable = [i for i in result["interpretations"] if i["status"] != "invalid"]
    if len(viable) == 1 and viable[0]["status"] == "ambiguous":
        return "compound_quantity_or_package_scope"
    if len(viable) > 1 and result.get("selected_interpretation"):
        return "tied_evidence_score"
    return "other"


class ReportStats:
    """Aggregate counters for the execution report (item 17). Built
    incrementally from `analyze_all_lines()`'s output -- no per-line state
    is retained, so this never prints a line-by-line dump regardless of
    database size."""

    def __init__(self) -> None:
        self.total_lines = 0
        self.status_counts: "Counter[str]" = Counter()
        self.ambiguous_reason_counts: "Counter[str]" = Counter()
        self.unresolved_reason_counts: "Counter[str]" = Counter()
        self.lines_zero_row = 0
        self.lines_one_row = 0
        self.lines_multi_row = 0
        self.unmapped_units: "Counter[str]" = Counter()
        self.unmapped_package_size_count = 0
        self.missing_lineage_line_ids: List[int] = []

    def record_result(self, result: dict) -> None:
        self.total_lines += 1
        self.status_counts[result["status"]] += 1
        if result["status"] == "ambiguous":
            self.ambiguous_reason_counts[_classify_ambiguous_reason(result)] += 1
        # Tally every unresolved[].reason occurrence, across every
        # reference, across every interpretation -- not one reason per
        # line. A line can fail for more than one reason at once, and a
        # line that decomposes into multiple references (SS I) can
        # independently accumulate several unresolved reasons across
        # those references. These counts are NOT expected to sum to
        # status_counts["unresolved"].
        for interpretation in result["interpretations"]:
            for reference in interpretation.get("references", []):
                for entry in reference.get("unresolved", []):
                    self.unresolved_reason_counts[entry["reason"]] += 1

    def record_projection(
        self, rows: List[dict], unmapped_units: List[str], unmapped_package_size: List[str]
    ) -> None:
        if len(rows) == 0:
            self.lines_zero_row += 1
        elif len(rows) == 1:
            self.lines_one_row += 1
        else:
            self.lines_multi_row += 1
        for unit in unmapped_units:
            self.unmapped_units[unit] += 1
        self.unmapped_package_size_count += len(unmapped_package_size)

    def record_missing_lineage(self, recipe_ingredient_line_id: int) -> None:
        self.missing_lineage_line_ids.append(recipe_ingredient_line_id)


def _print_report(stats: "ReportStats") -> None:
    rule = "\u2500" * 64
    total = stats.total_lines

    print("Analyzer Execution Report")
    print(rule)
    print(f"{'Lines analyzed:':<40}{total:>10,}")
    print(f"{'Lines producing zero downstream rows:':<40}{stats.lines_zero_row:>10,}")
    print(f"{'Lines producing one downstream row:':<40}{stats.lines_one_row:>10,}")
    print(f"{'Lines producing multiple downstream rows:':<40}{stats.lines_multi_row:>10,}")

    print()
    for status in _STATUS_ORDER:
        count = stats.status_counts.get(status, 0)
        pct = (count / total * 100) if total else 0.0
        print(f"{status.capitalize() + ':':<40}{count:>10,} ({pct:5.1f}%)")

    print()
    print("Unresolved reasons (occurrences, not lines -- a line can carry")
    print("more than one; see ReportStats.record_result):")
    if stats.unresolved_reason_counts:
        for reason, count in sorted(
            stats.unresolved_reason_counts.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            print(f"  {reason:<42}{count:>8,}")
    else:
        print("  none")

    print()
    print("Ambiguous reasons (one per ambiguous line):")
    if stats.ambiguous_reason_counts:
        for reason, count in sorted(
            stats.ambiguous_reason_counts.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            print(f"  {reason:<42}{count:>8,}")
    else:
        print("  none")

    critical_issues: List[str] = []
    if stats.unmapped_units:
        total_unmapped = sum(stats.unmapped_units.values())
        units_list = ", ".join(sorted(stats.unmapped_units))
        critical_issues.append(
            f"{total_unmapped} measurement-quantity occurrence(s) used a unit "
            f"this projection cannot classify as metric/imperial weight/"
            f"volume: {units_list}. No grams/ml/imperial_* value was "
            f"populated for these -- see _route_measurement_quantity's "
            f"docstring (FLAGGED GAP #1)."
        )
    if stats.unmapped_package_size_count:
        critical_issues.append(
            f"{stats.unmapped_package_size_count} package.size occurrence(s) "
            f"have no column in the current downstream schema -- preserved "
            f"only in canonical_result_json (FLAGGED GAP #5)."
        )
    if stats.missing_lineage_line_ids:
        sample = stats.missing_lineage_line_ids[:10]
        critical_issues.append(
            f"{len(stats.missing_lineage_line_ids)} line(s) produced downstream "
            f"rows but had no matching recipe_ingredient_lines_raw lineage row "
            f"-- those rows were NOT written. Sample "
            f"recipe_ingredient_line_id values: {sample}."
        )

    if critical_issues:
        print()
        print("CRITICAL ISSUES:")
        for issue in critical_issues:
            print(f"  - {issue}")

    print(rule)


def persist_all_lines(
    db_path: Optional[Any] = None, line_ids: Optional[Iterable[int]] = None
) -> ReportStats:
    """Runs `analyze_all_lines()` and writes BOTH RO-10 outputs for every
    line: the primary downstream rows (`recipe_ingredient_lines_parsed`)
    and the diagnostic artifact (`analysis_records`/
    `analysis_candidate_evaluations`/`analysis_evidence`).

    `line_ids=None` (default) analyzes the whole database -- a debugging
    convenience (item 15), not a permanent design assumption. Pass an
    explicit iterable of `recipe_ingredient_line_id` values for a future
    targeted-pipeline invocation; nothing else here needs to change.

    Single transaction for the whole run (matching
    `process_recipe_lines()`'s existing commit-once pattern); rolls back
    entirely on any error so no line is left with diagnostic rows but no
    primary rows, or vice versa. Returns a `ReportStats` (see `main()` for
    the printed report built from it).
    """
    from gastrometric.config.paths import DB_PATH

    path = str(db_path or DB_PATH)
    write_conn = sqlite3.connect(path)
    stats = ReportStats()
    try:
        lineage = _fetch_line_lineage(write_conn)
        for recipe_ingredient_line_id, parse_tree_id, result in analyze_all_lines(path, line_ids):
            stats.record_result(result)

            _persist_analysis_record(write_conn, recipe_ingredient_line_id, parse_tree_id, result)

            rows, unmapped_units, unmapped_package_size = _project_selected_references(result)
            stats.record_projection(rows, unmapped_units, unmapped_package_size)

            if rows:
                line_lineage = lineage.get(recipe_ingredient_line_id)
                if line_lineage is None:
                    stats.record_missing_lineage(recipe_ingredient_line_id)
                else:
                    ingredient_block_id, recipe_id, recipe_section_id = line_lineage
                    _persist_parsed_rows(
                        write_conn,
                        recipe_ingredient_line_id,
                        ingredient_block_id,
                        recipe_id,
                        recipe_section_id,
                        rows,
                    )
        write_conn.commit()
    except Exception:
        write_conn.rollback()
        raise
    finally:
        write_conn.close()
    return stats


def main() -> None:
    stats = persist_all_lines()
    _print_report(stats)


if __name__ == "__main__":
    main()