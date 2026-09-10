"""
Orchestration for the inventory-ingredient-understanding pipeline.

This is the inventory counterpart to
`gastrometric.orchestration.recipe_ingredient_understanding`: it drives the
same generic lex -> parse -> analyze pipeline, but for a single piece of
arbitrary inventory ingredient text, with no recipe context and no
persistence of the pipeline's intermediate artifacts.

Unlike the recipe orchestration, inventory ingredient text has no
`recipe_ingredient_line_id` to thread through -- there is no raw-line row
to read, no lineage to look up, and nothing to persist here at all. The
lex stage's ordinary output (`List[LexicalSpan]`) is adapted in-memory
into the parser's expected `List[LexicalToken]` input using the exact
same one-row-per-classification / positionally-aligned-vocabulary-fields
logic `recipe_ingredient_understanding._insert_span` uses when writing to
`lexical_spans` -- this module just builds `LexicalToken` objects instead
of SQL rows, since there is no table to write to. See `_spans_to_tokens`
below. One deliberate addition beyond a pure field-for-field mirror:
`_as_span_text` reproduces the implicit float/int -> text coercion that
`lexical_spans.normalized_value`'s TEXT column affinity applies for free
on the recipe path (see that function's docstring) -- without it, a bare
Quantity span's numeric `normalized_value` reaches `analyzer._span_norm()`
as a raw `float` instead of the string it gets on the recipe path, and
`_span_norm()`'s `.strip()` call raises `AttributeError`.

The returned value is the complete, unmodified dict produced by
`analyze_parse_result()` -- the same `status` / `interpretations` /
`selected_interpretation` contract the recipe pipeline produces (compare
`recipe_ingredient_understanding.analyze_all_lines`), so any consumer that
already knows how to read one recipe analysis result can read an
inventory analysis result without a separate schema.

This module intentionally does not:
  * open a database connection,
  * write `lexical_spans`, `ingredient_parse_trees`, `analysis_records`,
    `analysis_candidate_evaluations`, or `analysis_evidence`,
  * call `inventory_ingredient_orchestrator.py` or `inventory_editor.py`.

Persisting the returned result into `inventory_items.analysis_result_json`,
and deriving `ingredient_id` / `resolution_status` from it, is the job of
the inventory application/orchestration layer that calls this module.
"""

import dataclasses
import itertools
from typing import Any, Dict, Iterable, List, Optional, Tuple

from gastrometric.understanding.analyzer import analyze_parse_result
from gastrometric.understanding.ingredient_parser import IngredientParser, LexicalToken
from gastrometric.understanding.lex import LexicalSpan, lex
from gastrometric.knowledge.loader import knowledge as runtime_knowledge


# ---------------------------------------------------------------------------
# Lex-span -> parser-token adapter
# ---------------------------------------------------------------------------
#
# Mirrors `recipe_ingredient_understanding._insert_span` field for field,
# except it builds `LexicalToken` objects in memory instead of emitting
# `lexical_spans` INSERT statements -- there is no `lexical_spans` row (and
# therefore no real, DB-assigned `span_id`) for inventory text, so
# `span_id` here is a synthetic, process-local counter that only needs to
# be unique *within* this one call's token list. Everything else --
# one token per classification in `span_types`, with `normalized_value` /
# `knowledge_id` / `source_vocabulary` aligned positionally to that same
# classification for vocabulary-matched spans, or repeated as a plain
# scalar when `source_vocabulary is None` -- replicates that function's
# documented contract exactly.
#
# One thing `_insert_span` gets for free that this in-memory adapter must
# reproduce explicitly: `lexical_spans.normalized_value` is a TEXT-affinity
# SQLite column. `lex()` puts a plain Python `float` in `normalized_value`
# for a bare Quantity span (`"2"` -> `2.0`, `"1/2"` -> `0.5`), and
# `analyzer._span_norm()` calls `.strip()` on whatever it finds there. On
# the recipe path that float is bound as an INSERT parameter into a TEXT
# column, so SQLite's column-affinity conversion silently stringifies it
# (`2.0` -> `"2.0"`) before `process_recipe_lines()` ever reads it back --
# nobody wrote that coercion, SQLite performs it implicitly on every
# recipe line with a numeric quantity. This adapter never touches a
# database, so nothing performs that coercion for it; `_as_span_text`
# below reproduces it by hand so `_span_norm()` sees the same string-like
# value here that it would after a real `lexical_spans` round trip.


def _as_span_text(value: Any) -> Any:
    """Reproduces SQLite's TEXT-column-affinity stringification for a
    `normalized_value` scalar that never passes through an actual TEXT
    column here.

    `None` and `str` pass through unchanged (already what `_span_norm()`
    expects). Anything else -- concretely, the `float` `lex()` produces
    for a bare Quantity span -- is stringified with plain `str()`, which
    matches SQLite's INTEGER/REAL -> TEXT affinity conversion for the
    digit and simple-fraction quantities `lex()` actually emits (`2` ->
    `"2"`, `0.5` -> `"0.5"`). This is not guaranteed to match SQLite's own
    `sqlite3_snprintf`-based formatting byte-for-byte in every numeric
    edge case (e.g. very large exponents), only for the ordinary quantity
    values this pipeline sees in practice.
    """
    if value is None or isinstance(value, str):
        return value
    return str(value)


def _spans_to_tokens(spans: Iterable[LexicalSpan]) -> List[LexicalToken]:
    """Adapts `lex()`'s `List[LexicalSpan]` output into the
    `List[LexicalToken]` shape `IngredientParser.parse()` expects.
    """
    tokens: List[LexicalToken] = []
    synthetic_span_id = 0

    for span in spans:
        fields = dataclasses.asdict(span)
        span_types: Tuple[str, ...] = fields["span_types"]
        source_vocabulary: Optional[Tuple[str, ...]] = fields["source_vocabulary"]

        if source_vocabulary is not None:
            sources: Iterable[Optional[str]] = source_vocabulary
            # `Any`, not `Optional[Any]`: these values are genuinely
            # nullable at runtime (same as the recipe module's DB-sourced
            # `normalized_value`/`knowledge_id`), but `Optional[Any]`
            # makes strict type checkers treat the `None` branch as
            # distinct from `Any` and flag it against LexicalToken's
            # declared `str` parameter below. Plain `Any` is assignable
            # unconditionally and doesn't change anything at runtime.
            normalized_values: Iterable[Any] = fields["normalized_value"]
            knowledge_ids: Iterable[Any] = fields["knowledge_id"]
        else:
            sources = itertools.repeat(None)
            normalized_values = itertools.repeat(fields["normalized_value"])
            knowledge_ids = itertools.repeat(fields["knowledge_id"])

        for span_type, source, normalized_value, knowledge_id in zip(
            span_types, sources, normalized_values, knowledge_ids
        ):
            synthetic_span_id += 1
            tokens.append(
                LexicalToken(
                    span_id=synthetic_span_id,
                    span_order=fields["span_order"],
                    start_offset=fields["start_offset"],
                    end_offset=fields["end_offset"],
                    text=fields["text"],
                    normalized_value=_as_span_text(normalized_value),
                    span_type=span_type,
                    knowledge_id=knowledge_id,
                    source_vocabulary=source,
                )
            )

    return tokens


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def understand_inventory_ingredient(text: str) -> Dict[str, Any]:
    """Runs the full lex -> parse -> analyze pipeline on one piece of
    arbitrary inventory ingredient text and returns the complete analyzer
    result, unmodified.

    Takes plain text with no recipe context: no recipe ID, recipe
    ingredient line ID, parse-tree database ID, or existing recipe
    database record is required or accepted. Persists nothing -- this
    function never opens a database connection, and creates no rows in
    `lexical_spans`, `ingredient_parse_trees`, `analysis_records`,
    `analysis_candidate_evaluations`, or `analysis_evidence`. It reuses
    `lex()`, `IngredientParser.parse()`, and `analyze_parse_result()`
    exactly as the recipe pipeline does (see
    `recipe_ingredient_understanding.py`); none of that logic is
    reimplemented here.

    The caller (`inventory_ingredient_orchestrator.py`) is responsible for
    persisting the returned result into
    `inventory_items.analysis_result_json` and for deriving
    `ingredient_id` / `resolution_status` from it.

    Args:
        text: Arbitrary human-authored inventory ingredient text, e.g.
            "cabbage", "2 heads cabbage", "purple cabbage thing".

    Returns:
        The complete `AnalysisResult` dict produced by
        `analyze_parse_result()` -- `status`, `interpretations`,
        `selected_interpretation`, and every other interpretation/
        reference/evidence field the analyzer currently produces.
    """
    spans = lex(text)
    tokens = _spans_to_tokens(spans)

    parser = IngredientParser()
    parse_result = parser.parse(tokens)

    return analyze_parse_result(parse_result.to_dict(), runtime_knowledge)