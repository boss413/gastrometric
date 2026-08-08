"""
gastrometric/pipeline/observations/debug_observations.py

QA suite for the parser and Observation Builder. Read-only -- every report
here only SELECTs, so it's safe to run against the real gastrometric.db at
any time.

    python -m gastrometric.pipeline.observations.debug_observations --report completeness
    python -m gastrometric.pipeline.observations.debug_observations --report histogram
    python -m gastrometric.pipeline.observations.debug_observations --report samples --limit 20
    python -m gastrometric.pipeline.observations.debug_observations --report missing-ingredient --limit 20
    python -m gastrometric.pipeline.observations.debug_observations --report missing-quantity --limit 20
    python -m gastrometric.pipeline.observations.debug_observations --report missing-package --limit 20
    python -m gastrometric.pipeline.observations.debug_observations --report missing-preparation --limit 20
    python -m gastrometric.pipeline.observations.debug_observations --report missing-measurement
    python -m gastrometric.pipeline.observations.debug_observations --report unattached
    python -m gastrometric.pipeline.observations.debug_observations --report unattached --type Measurement --limit 20
    python -m gastrometric.pipeline.observations.debug_observations --report clause --id 817
    python -m gastrometric.pipeline.observations.debug_observations --report suspicious --limit 5
    python -m gastrometric.pipeline.observations.debug_observations --report regression
    python -m gastrometric.pipeline.observations.debug_observations --report all

Reports (see each function's docstring for detail):

  completeness         Structural completeness -- clause/observation counts,
                        role coverage, most common role combinations.
  histogram             Distribution of spans-per-observation.
  samples               A spread of individual observations, human-readable.
  missing-ingredient     Clauses with zero Ingredient span at all (parser gap).
  missing-quantity        Observations with an ingredient but no quantity.
  missing-package          Observations with an ingredient but no package.
  missing-preparation      Observations with an ingredient but no preparation.
  missing-measurement      Explains why this no longer applies (see note below).
  unattached            Span-type-and-text frequency breakdown of everything
                        the builder left unattached, with in-context examples.
  clause                Full reconstruction of one clause: raw line, parser
                        spans, and the observation(s) built from them.
  suspicious            Worst offenders: largest clauses, clauses with the
                        most unattached spans.
  regression            Runs a directory of hand-picked "golden" ingredient
                        lines through the pipeline and prints PASS/FAIL.

NOTE on "missing-measurement": after the last debugging pass, "measurement"
is no longer a separate role -- VolumeExpression / WeightExpression /
NaturalPortionExpression / bare Measurement spans all attach under the
single "quantity" role, because the real parser never emits a standalone
unit-only span to pair with a separate "Quantity" span. --report
missing-measurement is kept as a CLI option (so it doesn't error out) but
just prints this explanation and points at --report missing-quantity,
which is the accurate equivalent under the current role vocabulary.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from collections import Counter, defaultdict
from typing import Optional

DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "gastrometric.db"
)

# Golden-example test files live here (see report_regression below).
EXAMPLES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "tests", "parser_examples"
)

ROLE_DISPLAY_ORDER = [
    ("primary_ingredient", "ingredient"),
    ("quantity", "quantity"),
    ("measurement", "measurement"),  # legacy role, kept for display safety only
    ("package", "package"),
    ("preparation", "preparation"),
    ("modifier", "modifier"),
    ("dimension", "dimension"),
    ("temperature", "temperature"),
    ("brand", "brand"),
    ("unknown", "unknown"),
]
ROLE_DISPLAY_NAME = dict(ROLE_DISPLAY_ORDER)

# Span types that are *intentionally* never attached by any rule -- their
# presence in "unattached" reports is expected, not a bug.
ALWAYS_UNATTACHED_TYPES = {"Unknown", "Noise", "GrammarMarker"}


# ---------------------------------------------------------------------------
# Schema-agnostic helpers
# ---------------------------------------------------------------------------

_schema_cache: dict = {}


def _line_text_column(conn: sqlite3.Connection) -> Optional[str]:
    """
    Best-effort discovery of a human-readable line-text column on
    recipe_ingredient_lines_parsed. Falls back to None, in which case
    callers reconstruct clause text from span raw_text instead.
    """
    if "line_col" in _schema_cache:
        return _schema_cache["line_col"]
    columns = [row[1] for row in conn.execute(
        "PRAGMA table_info(recipe_ingredient_lines_parsed)"
    ).fetchall()]
    candidates = [
        "raw_line", "line_text", "original_text", "original_line",
        "text", "ingredient_line", "content", "raw_text",
    ]
    found = next((c for c in candidates if c in columns), None)
    _schema_cache["line_col"] = found
    return found


def _recipe_label_columns(conn: sqlite3.Connection) -> tuple:
    """Best-effort discovery of recipe id / title columns, for display only."""
    if "recipe_cols" in _schema_cache:
        return _schema_cache["recipe_cols"]
    columns = [row[1] for row in conn.execute(
        "PRAGMA table_info(recipe_ingredient_lines_parsed)"
    ).fetchall()]
    recipe_id_col = next((c for c in ("recipe_id",) if c in columns), None)
    title_col = None
    if recipe_id_col:
        try:
            recipe_columns = [row[1] for row in conn.execute(
                "PRAGMA table_info(recipes)"
            ).fetchall()]
            title_col = next(
                (c for c in ("title", "name", "recipe_name") if c in recipe_columns), None
            )
        except sqlite3.OperationalError:
            pass
    _schema_cache["recipe_cols"] = (recipe_id_col, title_col)
    return recipe_id_col, title_col


def _clause_spans(conn: sqlite3.Connection, recipe_ingredient_id: int) -> list:
    return conn.execute(
        """
        SELECT span_id, span_type, raw_text, start_offset, end_offset, parser_order
        FROM recipe_ingredient_spans
        WHERE recipe_ingredient_id = ?
        ORDER BY parser_order
        """,
        (recipe_ingredient_id,),
    ).fetchall()


def _line_raw_text(conn: sqlite3.Connection, recipe_ingredient_id: int) -> Optional[str]:
    col = _line_text_column(conn)
    if not col:
        return None
    row = conn.execute(
        f"SELECT {col} FROM recipe_ingredient_lines_parsed WHERE id = ?",
        (recipe_ingredient_id,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _clause_display_text(conn: sqlite3.Connection, recipe_ingredient_id: int, spans: Optional[list] = None) -> str:
    line_text = _line_raw_text(conn, recipe_ingredient_id)
    if line_text:
        return line_text
    if spans is None:
        spans = _clause_spans(conn, recipe_ingredient_id)
    reconstructed = " ".join(s[2] for s in spans)
    return f"{reconstructed}  (reconstructed from spans -- no line-text column found)"


def _recipe_label(conn: sqlite3.Connection, recipe_ingredient_id: int, recipe_id_col, title_col) -> str:
    if not recipe_id_col:
        return f"recipe_ingredient_id={recipe_ingredient_id}"
    row = conn.execute(
        f"SELECT {recipe_id_col} FROM recipe_ingredient_lines_parsed WHERE id = ?",
        (recipe_ingredient_id,),
    ).fetchone()
    if not row:
        return f"recipe_ingredient_id={recipe_ingredient_id}"
    recipe_id = row[0]
    if title_col:
        trow = conn.execute(
            f"SELECT {title_col} FROM recipes WHERE id = ?", (recipe_id,)
        ).fetchone()
        if trow and trow[0]:
            return f"{trow[0]} (recipe_id={recipe_id})"
    return f"recipe_id={recipe_id}"


def _caret_position(line_text: str, start_offset, end_offset, raw_text: str) -> Optional[tuple]:
    """
    Best-effort (position, length) of a span within line_text, for drawing
    a "^^^^" marker underneath it. Tries the stored offsets first (they may
    be stale, e.g. unicode fractions like "½" get normalized to "0.5" by
    the time they reach raw_text, which shifts length); falls back to a
    plain substring search. Returns None if neither works, so callers can
    degrade gracefully instead of drawing a misleading caret.
    """
    needle = raw_text.strip()
    if start_offset is not None and end_offset is not None:
        if 0 <= start_offset < end_offset <= len(line_text):
            candidate = line_text[start_offset:end_offset]
            if candidate.strip().lower() == needle.lower():
                return (start_offset, end_offset - start_offset)

    idx = line_text.lower().find(needle.lower())
    if idx != -1:
        return (idx, len(needle))
    return None


def _print_with_caret(line_text: str, start_offset, end_offset, raw_text: str) -> None:
    print(line_text)
    pos = _caret_position(line_text, start_offset, end_offset, raw_text)
    if pos:
        offset, length = pos
        print(" " * offset + "^" * max(length, 1))
    else:
        print(f"    (unable to align {raw_text!r} to the line text)")


# ---------------------------------------------------------------------------
# Report: span type inventory
# ---------------------------------------------------------------------------

def report_span_type_inventory(conn: sqlite3.Connection) -> None:
    """High-level pass/fail per span_type: does anything attach it at all?"""
    print("=" * 60)
    print("SPAN TYPE INVENTORY")
    print("=" * 60)
    print(f"{'span_type':<26}{'total':>8}{'attached':>10}{'unattached':>12}")
    print("-" * 56)

    rows = conn.execute(
        """
        SELECT
            s.span_type,
            COUNT(*) AS total,
            SUM(CASE WHEN os.span_id IS NOT NULL THEN 1 ELSE 0 END) AS attached
        FROM recipe_ingredient_spans s
        LEFT JOIN observation_spans os ON os.span_id = s.span_id
        GROUP BY s.span_type
        ORDER BY total DESC
        """
    ).fetchall()

    for span_type, total, attached in rows:
        attached = attached or 0
        unattached = total - attached
        flag = ""
        if attached == 0 and span_type not in ALWAYS_UNATTACHED_TYPES and span_type != "Ingredient":
            flag = "  <-- never attached"
        print(f"{span_type:<26}{total:>8}{attached:>10}{unattached:>12}{flag}")
    print()


# ---------------------------------------------------------------------------
# Report: structural completeness  (item 1 -- highest priority)
# ---------------------------------------------------------------------------

def report_completeness(conn: sqlite3.Connection) -> None:
    """
    The single highest-value report: are clauses turning into complete
    observations, and if not, which role is most often missing?

    Uses the *actual* role vocabulary (see observation_roles), not the
    hypothetical Quantity/Measurement split from the original design doc.
    Quantity and Measurement were merged into one "quantity" role during
    the last debugging pass, since the real parser has no standalone
    "Quantity" span type to pair a unit with. If you're comparing this
    output against an older sketch that shows them as separate rows,
    that's why they're combined here.
    """
    print("=" * 60)
    print("STRUCTURAL COMPLETENESS")
    print("=" * 60)

    clause_count = conn.execute(
        "SELECT COUNT(DISTINCT recipe_ingredient_id) FROM recipe_ingredient_spans"
    ).fetchone()[0]
    observation_count = conn.execute(
        "SELECT COUNT(*) FROM ingredient_observations"
    ).fetchone()[0]
    clauses_with_observation = conn.execute(
        "SELECT COUNT(DISTINCT recipe_ingredient_id) FROM ingredient_observations"
    ).fetchone()[0]

    print(f"Clauses:                {clause_count:>6}")
    print(f"  with >=1 observation: {clauses_with_observation:>6}")
    print(f"  with 0 observations:  {clause_count - clauses_with_observation:>6}"
          "   (see --report missing-ingredient)")
    print(f"Observations:            {observation_count:>6}")
    print()

    # Role coverage: how many observations have each role attached at all.
    role_counts = dict(conn.execute(
        """
        SELECT role_code, COUNT(DISTINCT observation_id)
        FROM observation_spans
        GROUP BY role_code
        """
    ).fetchall())

    print("Role coverage (observations containing each role):")
    denom = observation_count or 1
    for role_code, display in ROLE_DISPLAY_ORDER:
        n = role_counts.get(role_code, 0)
        if n == 0 and role_code not in ("primary_ingredient", "quantity"):
            continue  # skip roles that never occur, to keep this readable
        pct = 100.0 * n / denom
        print(f"  {display:<14}{n:>8}  ({pct:5.1f}%)")
    print()

    # Most common role combinations -- generalizes the old
    # "ingredient only / ingredient+quantity / ..." idea without hardcoding
    # a specific role list, so it stays accurate as roles evolve.
    combo_counts: Counter = Counter()
    for (observation_id,) in conn.execute("SELECT observation_id FROM ingredient_observations"):
        roles: set = {str(r[0]) for r in conn.execute(
            "SELECT DISTINCT role_code FROM observation_spans WHERE observation_id = ?",
            (observation_id,),
        ).fetchall()}
        display_role_list: list = [ROLE_DISPLAY_NAME.get(role, role) for role in roles]
        display_roles = tuple(sorted(display_role_list))
        combo_counts[display_roles] += 1

    print("Most common role combinations:")
    for combo, count in combo_counts.most_common(10):
        label = " + ".join(combo) if combo else "(no roles -- shouldn't happen)"
        print(f"  {count:>6}   {label}")
    print()


# ---------------------------------------------------------------------------
# Report: spans-per-observation histogram  (item 5)
# ---------------------------------------------------------------------------

def report_histogram(conn: sqlite3.Connection) -> None:
    print("=" * 60)
    print("SPANS PER OBSERVATION")
    print("=" * 60)

    rows = conn.execute(
        """
        SELECT observation_id, COUNT(*) AS span_count
        FROM observation_spans
        GROUP BY observation_id
        """
    ).fetchall()

    zero_span_observations = conn.execute(
        """
        SELECT COUNT(*) FROM ingredient_observations o
        WHERE NOT EXISTS (SELECT 1 FROM observation_spans os WHERE os.observation_id = o.observation_id)
        """
    ).fetchone()[0]

    counts = Counter(span_count for _, span_count in rows)
    if zero_span_observations:
        counts[0] += zero_span_observations

    if not counts:
        print("(no observations found)\n")
        return

    max_bucket = max(counts)
    for n in range(0, max_bucket + 1):
        c = counts.get(n, 0)
        if c == 0:
            continue
        label = f"{n} span" if n == 1 else f"{n} spans"
        print(f"  {label:<12}{c:>6}")
    print()


# ---------------------------------------------------------------------------
# Report: sample observations
# ---------------------------------------------------------------------------

def report_sample_observations(conn: sqlite3.Connection, limit: int) -> None:
    print("=" * 60)
    print("SAMPLE OBSERVATIONS")
    print("=" * 60)

    total = conn.execute("SELECT COUNT(*) FROM ingredient_observations").fetchone()[0]
    if total == 0:
        print("(no observations found)\n")
        return

    stride = max(total // limit, 1)
    obs_ids = [row[0] for row in conn.execute(
        "SELECT observation_id FROM ingredient_observations ORDER BY observation_id"
    ).fetchall()][::stride][:limit]

    for obs_id in obs_ids:
        recipe_ingredient_id = conn.execute(
            "SELECT recipe_ingredient_id FROM ingredient_observations WHERE observation_id = ?",
            (obs_id,),
        ).fetchone()[0]

        present_roles = {r[0] for r in conn.execute(
            "SELECT DISTINCT role_code FROM observation_spans WHERE observation_id = ?",
            (obs_id,),
        ).fetchall()}

        print()
        print("Observation")
        print()
        for role_code, display_name in ROLE_DISPLAY_ORDER:
            if role_code in present_roles:
                print(display_name)
        print()
        print("-" * 28)
        print()
        print(_clause_display_text(conn, recipe_ingredient_id))
    print()


# ---------------------------------------------------------------------------
# Report: clauses with no ingredient observation (parser gap, not builder)
# ---------------------------------------------------------------------------

def report_clauses_missing_ingredient(conn: sqlite3.Connection, limit: int) -> None:
    print("=" * 60)
    print("CLAUSES WITH NO INGREDIENT OBSERVATION")
    print("=" * 60)

    recipe_id_col, title_col = _recipe_label_columns(conn)

    rows = conn.execute(
        """
        SELECT l.id
        FROM recipe_ingredient_lines_parsed l
        WHERE NOT EXISTS (
            SELECT 1 FROM ingredient_observations o
            WHERE o.recipe_ingredient_id = l.id
        )
        AND EXISTS (
            SELECT 1 FROM recipe_ingredient_spans s
            WHERE s.recipe_ingredient_id = l.id
        )
        ORDER BY l.id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        print("(none found)\n")
        return

    for (recipe_ingredient_id,) in rows:
        spans = _clause_spans(conn, recipe_ingredient_id)
        print()
        print(f"recipe: {_recipe_label(conn, recipe_ingredient_id, recipe_id_col, title_col)}")
        print(f"line:   {_clause_display_text(conn, recipe_ingredient_id, spans)}")
        print("parser spans:")
        for span_id, span_type, raw_text, start_offset, end_offset, parser_order in spans:
            print(f"    [{parser_order}] {span_type:<18} {raw_text!r}")
    print()


# ---------------------------------------------------------------------------
# Report: observations missing a given role  (generalizes the old
# missing-quantity report to cover quantity / package / preparation)
# ---------------------------------------------------------------------------

def report_observations_missing_role(conn: sqlite3.Connection, role_code: str, label: str, limit: int) -> None:
    print("=" * 60)
    print(f"OBSERVATIONS MISSING {label.upper()}")
    print("=" * 60)

    recipe_id_col, title_col = _recipe_label_columns(conn)

    rows = conn.execute(
        """
        SELECT o.observation_id, o.recipe_ingredient_id
        FROM ingredient_observations o
        WHERE NOT EXISTS (
            SELECT 1 FROM observation_spans os
            WHERE os.observation_id = o.observation_id
              AND os.role_code = ?
        )
        ORDER BY o.observation_id
        LIMIT ?
        """,
        (role_code, limit),
    ).fetchall()

    if not rows:
        print("(none found)\n")
        return

    for observation_id, recipe_ingredient_id in rows:
        ingredient_row = conn.execute(
            """
            SELECT span_id FROM observation_spans
            WHERE observation_id = ? AND role_code = 'primary_ingredient'
            """,
            (observation_id,),
        ).fetchone()
        ingredient_text = None
        if ingredient_row:
            r = conn.execute(
                "SELECT raw_text FROM recipe_ingredient_spans WHERE span_id = ?",
                (ingredient_row[0],),
            ).fetchone()
            ingredient_text = r[0] if r else None

        spans = _clause_spans(conn, recipe_ingredient_id)
        print()
        print(f"recipe:      {_recipe_label(conn, recipe_ingredient_id, recipe_id_col, title_col)}")
        print(f"line:        {_clause_display_text(conn, recipe_ingredient_id, spans)}")
        print(f"observation: {ingredient_text!r} (observation_id={observation_id})")
        print("parser spans in clause:")
        for span_id, span_type, raw_text, start_offset, end_offset, parser_order in spans:
            print(f"    [{parser_order}] {span_type:<18} {raw_text!r}")
    print()


def report_missing_measurement_note() -> None:
    print("=" * 60)
    print("MISSING MEASUREMENT")
    print("=" * 60)
    print(
        "\"measurement\" is no longer a separate role. VolumeExpression,\n"
        "WeightExpression, NaturalPortionExpression, and bare Measurement\n"
        "spans all attach under the single \"quantity\" role now, since the\n"
        "real parser never emits a standalone unit-only span to pair with a\n"
        "separate \"Quantity\" span. Use --report missing-quantity instead --\n"
        "it covers the same ground under the current role vocabulary.\n"
    )


# ---------------------------------------------------------------------------
# Report: unattached spans, by type and text, with in-context examples
# ---------------------------------------------------------------------------

def report_unattached_detail(
    conn: sqlite3.Connection,
    span_type_filter: Optional[str],
    text_limit: int,
    example_limit: int,
) -> None:
    """
    For each span_type with unattached spans, show which specific texts are
    most commonly left unattached, then a handful of in-context examples
    with a caret under the unattached span. This is the report that tells
    you *why* a span went unattached -- e.g. "cups" unattached 145 times
    might mean the builder's rule for that type is fine but this specific
    clause shape (no ingredient found) is the real cause, whereas a type
    that's unattached 100% of the time and never appears in the "types"
    report's attached column at all is a builder rule gap.
    """
    print("=" * 60)
    print("UNATTACHED SPANS -- DETAIL")
    print("=" * 60)

    if span_type_filter:
        types_to_show = [span_type_filter]
    else:
        rows = conn.execute(
            """
            SELECT s.span_type, COUNT(*) AS unattached
            FROM recipe_ingredient_spans s
            WHERE NOT EXISTS (
                SELECT 1 FROM observation_spans os WHERE os.span_id = s.span_id
            )
            GROUP BY s.span_type
            ORDER BY unattached DESC
            """
        ).fetchall()
        types_to_show = [r[0] for r in rows]

    for span_type in types_to_show:
        unattached_rows = conn.execute(
            """
            SELECT s.span_id, s.recipe_ingredient_id, s.raw_text, s.normalized_text,
                   s.start_offset, s.end_offset
            FROM recipe_ingredient_spans s
            WHERE s.span_type = ?
              AND NOT EXISTS (
                  SELECT 1 FROM observation_spans os WHERE os.span_id = s.span_id
              )
            """,
            (span_type,),
        ).fetchall()

        if not unattached_rows:
            continue

        note = "  (expected -- never attached by design)" if span_type in ALWAYS_UNATTACHED_TYPES else ""
        print()
        print(f"{span_type}  ({len(unattached_rows)} unattached){note}")
        print("-" * 40)

        text_counts = Counter((r[3] or r[2]).strip().lower() for r in unattached_rows)
        for text, count in text_counts.most_common(text_limit):
            dots = "." * max(28 - len(text), 3)
            print(f"{text}{dots}{count}")

        print()
        print("Examples")
        for span_id, recipe_ingredient_id, raw_text, normalized_text, start_offset, end_offset in unattached_rows[:example_limit]:
            line_text = _line_raw_text(conn, recipe_ingredient_id) or _clause_display_text(conn, recipe_ingredient_id)
            print()
            _print_with_caret(line_text, start_offset, end_offset, raw_text)
    print()


# ---------------------------------------------------------------------------
# Report: single clause reconstruction  (item 4 -- "show me clause 817")
# ---------------------------------------------------------------------------

def report_clause(conn: sqlite3.Connection, recipe_ingredient_id: int) -> None:
    exists = conn.execute(
        "SELECT 1 FROM recipe_ingredient_lines_parsed WHERE id = ?", (recipe_ingredient_id,)
    ).fetchone()
    if not exists:
        print(f"No clause with recipe_ingredient_id={recipe_ingredient_id} found.\n")
        return

    spans = _clause_spans(conn, recipe_ingredient_id)

    print("=" * 60)
    print(f"CLAUSE {recipe_ingredient_id}")
    print("=" * 60)
    print()
    print("RAW")
    print()
    print(_clause_display_text(conn, recipe_ingredient_id, spans))
    print()
    print("-" * 34)
    print()
    print("Parser spans")
    print()
    for span_id, span_type, raw_text, start_offset, end_offset, parser_order in spans:
        print(f"{span_type:<18} {raw_text}")
    print()
    print("-" * 34)

    observations = conn.execute(
        "SELECT observation_id, observation_index FROM ingredient_observations "
        "WHERE recipe_ingredient_id = ? ORDER BY observation_index",
        (recipe_ingredient_id,),
    ).fetchall()

    if not observations:
        print()
        print("No observations were produced for this clause.")
        print()
        return

    attached_span_ids: set = set()
    for observation_id, observation_index in observations:
        print()
        print(f"Observation {observation_index}")
        print()
        role_rows = conn.execute(
            """
            SELECT os.role_code, s.raw_text, s.span_id
            FROM observation_spans os
            JOIN recipe_ingredient_spans s ON s.span_id = os.span_id
            WHERE os.observation_id = ?
            """,
            (observation_id,),
        ).fetchall()
        role_order = {code: i for i, (code, _) in enumerate(ROLE_DISPLAY_ORDER)}
        role_rows.sort(key=lambda r: role_order.get(r[0], 99))
        for role_code, raw_text, span_id in role_rows:
            attached_span_ids.add(span_id)
            print(f"{ROLE_DISPLAY_NAME.get(role_code, role_code):<18} {raw_text}")

    unattached = [s for s in spans if s[0] not in attached_span_ids]
    if unattached:
        print()
        print("Unattached in this clause:")
        for span_id, span_type, raw_text, start_offset, end_offset, parser_order in unattached:
            print(f"    {span_type:<18} {raw_text!r}")
    print()


# ---------------------------------------------------------------------------
# Report: suspicious / worst offenders  (item 6)
# ---------------------------------------------------------------------------

def report_suspicious(conn: sqlite3.Connection, limit: int) -> None:
    print("=" * 60)
    print("SUSPICIOUS CLAUSES")
    print("=" * 60)

    clause_ids = [r[0] for r in conn.execute(
        "SELECT DISTINCT recipe_ingredient_id FROM recipe_ingredient_spans"
    ).fetchall()]

    stats = []
    for cid in clause_ids:
        spans = _clause_spans(conn, cid)
        total_spans = len(spans)
        observation_ids = [r[0] for r in conn.execute(
            "SELECT observation_id FROM ingredient_observations WHERE recipe_ingredient_id = ?", (cid,)
        ).fetchall()]
        attached_ids = set()
        for oid in observation_ids:
            for r in conn.execute(
                "SELECT span_id FROM observation_spans WHERE observation_id = ?", (oid,)
            ).fetchall():
                attached_ids.add(r[0])
        stats.append({
            "clause_id": cid,
            "total_spans": total_spans,
            "observation_count": len(observation_ids),
            "unattached": total_spans - len(attached_ids),
        })

    print()
    print(f"Largest clauses (most parser spans) -- top {limit}")
    print("-" * 40)
    for s in sorted(stats, key=lambda x: x["total_spans"], reverse=True)[:limit]:
        text = _clause_display_text(conn, s["clause_id"])
        print()
        print(f'"{text}"')
        print(f'Parser: {s["total_spans"]} spans')
        print(f'Observations: {s["observation_count"]}')

    print()
    print("-" * 40)
    print(f"Most unattached spans in one clause -- top {limit}")
    print("-" * 40)
    for s in sorted(stats, key=lambda x: x["unattached"], reverse=True)[:limit]:
        if s["unattached"] == 0:
            continue
        text = _clause_display_text(conn, s["clause_id"])
        attached = s["total_spans"] - s["unattached"]
        print()
        print(f'"{text}"')
        print(f'Parser: {s["total_spans"]} spans')
        print(f'Attached: {attached}')
        print(f'Unattached: {s["unattached"]}')
    print()


# ---------------------------------------------------------------------------
# Report: golden-example regression suite  (item 7)
# ---------------------------------------------------------------------------

def _invoke_parser(raw_text: str):
    """
    NOT WIRED YET.

    This is the integration point for standalone regression testing: given
    one raw ingredient line, it should return the list of spans the real
    parser would produce for it (independent of any recipe already in the
    database), in the same shape as recipe_ingredient_spans rows.

    I don't have the parser's actual module path or function signature --
    that was never part of any work order so far, only its output tables.
    Point this at the real entry point (e.g.
    `from gastrometric.pipeline.parser.parse_line import parse_ingredient_line`)
    to make --report regression fully standalone. Until then, report_regression
    falls back to matching each example line against clauses that already
    exist verbatim in the current database.
    """
    raise NotImplementedError(
        "_invoke_parser is not wired to the real parser yet -- see this "
        "function's docstring in debug_observations.py."
    )


def _load_golden_examples(path: str) -> list:
    """
    Reads every .txt file in `path`. One ingredient line per row. Blank
    lines and lines starting with '#' are ignored. An optional
    '|required_role,required_role' suffix overrides the default pass
    criterion for that line, e.g.:

        salt to taste|ingredient

    (without an override, a line only needs "ingredient" -- no quantity --
    to pass; the default full bar is ingredient + quantity, since that's
    what most lines should have).
    """
    examples = []
    if not os.path.isdir(path):
        return examples

    for filename in sorted(os.listdir(path)):
        if not filename.endswith(".txt"):
            continue
        with open(os.path.join(path, filename), "r", encoding="utf-8") as f:
            for lineno, raw_line in enumerate(f, start=1):
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "|" in line:
                    text, required = line.split("|", 1)
                    required_roles = tuple(r.strip() for r in required.split(",") if r.strip())
                else:
                    text = line
                    required_roles = ("ingredient", "quantity")
                examples.append({
                    "text": text.strip(),
                    "required_roles": required_roles,
                    "source": f"{filename}:{lineno}",
                })
    return examples


def _evaluate_example(conn: sqlite3.Connection, text: str, required_roles: tuple) -> tuple:
    """
    Returns (status, detail) where status is one of PASS / FAIL / SKIP.

    Tries the real parser first (via _invoke_parser); if that's not wired
    yet, falls back to looking up a clause in the current database whose
    line text matches `text` exactly (case-insensitive), and evaluating
    against its already-built observation. SKIP means neither worked.
    """
    try:
        spans = _invoke_parser(text)
        # If/when wired: run spans through build_ingredient_observations'
        # in-memory rule pipeline directly, without touching the DB.
        from gastrometric.pipeline.observations.build_ingredient_observations import (
            build_observations_for_clause,
        )
        pending = build_observations_for_clause(spans)
        if not pending:
            return "FAIL", "no observation produced"
        roles_present = set()
        for obs in pending:
            roles_present |= set(obs.attachments.keys())
        missing = [r for r in required_roles if ROLE_DISPLAY_NAME.get(r, r) not in
                   {ROLE_DISPLAY_NAME.get(x, x) for x in roles_present}]
        if missing:
            return "FAIL", f"missing: {', '.join(missing)}"
        return "PASS", ""
    except NotImplementedError:
        pass

    col = _line_text_column(conn)
    if not col:
        return "SKIP", "no line-text column found and parser not wired"

    row = conn.execute(
        f"SELECT id FROM recipe_ingredient_lines_parsed WHERE lower({col}) = lower(?)",
        (text,),
    ).fetchone()
    if not row:
        return "SKIP", "not found in corpus and parser not wired"

    recipe_ingredient_id = row[0]
    observation_ids = [r[0] for r in conn.execute(
        "SELECT observation_id FROM ingredient_observations WHERE recipe_ingredient_id = ?",
        (recipe_ingredient_id,),
    ).fetchall()]
    if not observation_ids:
        return "FAIL", "no observation produced"

    for observation_id in observation_ids:
        roles_present = {r[0] for r in conn.execute(
            "SELECT DISTINCT role_code FROM observation_spans WHERE observation_id = ?",
            (observation_id,),
        ).fetchall()}
        display_roles = {ROLE_DISPLAY_NAME.get(r, r) for r in roles_present}
        if all(req in display_roles for req in required_roles):
            return "PASS", ""

    return "FAIL", f"no observation had all of: {', '.join(required_roles)}"


def report_regression(conn: sqlite3.Connection, examples_dir: str, verbose: bool) -> None:
    print("=" * 60)
    print("REGRESSION")
    print("=" * 60)

    examples = _load_golden_examples(examples_dir)
    if not examples:
        print(f"(no example files found under {examples_dir})")
        print(
            "Create .txt files there, one ingredient line per row, e.g.:\n"
            "    2 cups flour\n"
            "    salt to taste|ingredient\n"
        )
        return

    counts = Counter()
    for ex in examples:
        status, detail = _evaluate_example(conn, ex["text"], ex["required_roles"])
        counts[status] += 1
        if verbose or status != "PASS":
            suffix = f"  ({detail})" if detail else ""
            print(f"{status:<5} {ex['text']}{suffix}")
        else:
            print(status)

    print()
    print(f"{counts['PASS']} passed, {counts['FAIL']} failed, {counts['SKIP']} skipped "
          f"({len(examples)} total)")
    if counts["SKIP"]:
        print(
            "SKIP means the parser isn't wired for standalone testing yet and the\n"
            "line wasn't found verbatim in the current database -- see "
            "_invoke_parser's docstring."
        )
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

ALL_REPORT_CHOICES = [
    "types", "completeness", "histogram", "samples",
    "missing-ingredient", "missing-quantity", "missing-package",
    "missing-preparation", "missing-measurement",
    "unattached", "clause", "suspicious", "regression", "all",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="QA suite for the parser and Observation Builder.")
    parser.add_argument("--report", choices=ALL_REPORT_CHOICES, default="all")
    parser.add_argument("--limit", type=int, default=15)
    parser.add_argument("--id", type=int, default=None, help="recipe_ingredient_id for --report clause")
    parser.add_argument("--type", type=str, default=None, help="span_type filter for --report unattached")
    parser.add_argument("--examples-dir", type=str, default=EXAMPLES_DIR)
    parser.add_argument("--verbose", action="store_true", help="show every line for --report regression, not just failures")
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path or DB_PATH)
    try:
        if args.report == "types":
            report_span_type_inventory(conn)
        elif args.report == "completeness":
            report_completeness(conn)
        elif args.report == "histogram":
            report_histogram(conn)
        elif args.report == "samples":
            report_sample_observations(conn, args.limit)
        elif args.report == "missing-ingredient":
            report_clauses_missing_ingredient(conn, args.limit)
        elif args.report == "missing-quantity":
            report_observations_missing_role(conn, "quantity", "quantity", args.limit)
        elif args.report == "missing-package":
            report_observations_missing_role(conn, "package", "package", args.limit)
        elif args.report == "missing-preparation":
            report_observations_missing_role(conn, "preparation", "preparation", args.limit)
        elif args.report == "missing-measurement":
            report_missing_measurement_note()
        elif args.report == "unattached":
            report_unattached_detail(conn, args.type, text_limit=args.limit, example_limit=3)
        elif args.report == "clause":
            if args.id is None:
                print("--report clause requires --id <recipe_ingredient_id>")
            else:
                report_clause(conn, args.id)
        elif args.report == "suspicious":
            report_suspicious(conn, args.limit)
        elif args.report == "regression":
            report_regression(conn, args.examples_dir, args.verbose)
        elif args.report == "all":
            report_span_type_inventory(conn)
            report_completeness(conn)
            report_histogram(conn)
            report_clauses_missing_ingredient(conn, args.limit)
            report_observations_missing_role(conn, "quantity", "quantity", args.limit)
            report_observations_missing_role(conn, "package", "package", args.limit)
            report_observations_missing_role(conn, "preparation", "preparation", args.limit)
            report_unattached_detail(conn, None, text_limit=10, example_limit=2)
            report_suspicious(conn, min(args.limit, 5))
            report_sample_observations(conn, args.limit)
            if os.path.isdir(args.examples_dir):
                report_regression(conn, args.examples_dir, args.verbose)
    finally:
        conn.close()


if __name__ == "__main__":
    main()