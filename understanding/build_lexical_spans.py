"""
gastrometric.understanding.build_lexical_spans
================================================

Stage: lexical span persistence.

This module is a thin pipeline adapter. It has exactly one job: read every
row of ``recipe_ingredient_lines_raw``, call
``gastrometric.understanding.lex.lex`` exactly once per row, and persist
whatever ``LexicalSpan`` objects come back into ``lexical_spans``.

It performs NO interpretation of the spans it persists:

    * no clause splitting on "or" / "preferably" / "plus" / etc.
    * no observation creation
    * no ambiguity resolution
    * no selection between overlapping spans
    * no normalization beyond what lex() already returned
    * no ingredient inference
    * no mutation of lexer output (span_type, values, etc.)
    * no quantity attachment
    * no nutrition logic

If ``lex()`` returns a span, this module writes it to the database exactly
as returned -- nothing more, nothing less.

The rebuild is deterministic and rerunnable: every run truncates
``lexical_spans`` and regenerates it from scratch. There is no incremental
update path.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from collections import Counter
from typing import List, Sequence, Tuple

from gastrometric.config.paths import DB_PATH
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

    Mirrors the dataclass fields directly onto the lexical_spans schema.
    span_type is treated as an opaque string -- never inspected or
    transformed -- so future lexer versions may emit new span types with
    no code changes here.
    """
    fields = dataclasses.asdict(span)
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
            fields["normalized_value"],
            fields["span_type"],
            fields["knowledge_id"],
            fields["source_vocabulary"],
        ),
    )


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def _print_statistics(lines_processed: int, all_spans: Sequence[LexicalSpan]) -> None:
    """Print rebuild statistics. Span-type counts are derived dynamically
    from whatever span_type values were actually produced -- no hardcoded
    list of known span types.
    """
    total_spans = len(all_spans)
    average = (total_spans / lines_processed) if lines_processed else 0.0

    type_counts: Counter = Counter(span.span_type for span in all_spans)

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


if __name__ == "__main__":
    build_lexical_spans()