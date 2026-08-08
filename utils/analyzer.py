"""
analyzer.py

STUB — NOT IMPLEMENTED.

Reads PARSER SPANS directly (parse_ingredient_lines.parse_ingredient_line
output — not normalized/resolved output) and asks the diagnostic
questions a human curator needs answered:

    - Did the parser find zero ingredients?
    - Did the parser find multiple ingredients?
    - Did the parser leave unknown spans?
    - Did modifiers remain unattached?
    - Did package expressions associate cleanly?
    - Did quantity associate cleanly?
    - Can USDA resolution proceed?

This is deliberately NOT the same thing as normalization or identity
resolution succeeding or failing — the analyzer's job is to make parser
output legible to a person: what was recognized, what was inferred nowhere
(this stage doesn't infer anything), and what remains unresolved, so
missing vocabulary, missing ingredient identities, malformed recipes, and
parser logic failures are distinguishable from each other instead of all
looking like "normalization produced something wrong".

There was no prior code doing this job — the old parser had no concept
of leaving ambiguity visible; it always produced exactly one
ingredient_name_raw per line, correct or not, with no signal for which.
Nothing to port in here.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class CuratorReport:
    """Sketch of the shape this stage should produce — not wired up to
    anything. See the work order example: raw text, recognized
    ingredient spans, recognized vocabulary spans, unknown spans,
    unresolved ambiguities, suggested ingredient, suggested USDA
    entity."""
    raw_text: str
    ingredient_spans: List[dict] = field(default_factory=list)
    vocabulary_spans: List[dict] = field(default_factory=list)
    unknown_spans: List[dict] = field(default_factory=list)
    unresolved_ambiguities: List[str] = field(default_factory=list)
    suggested_ingredient: Optional[str] = None
    suggested_usda_entity: Optional[str] = None


def analyze(recognized_spans, raw_text):
    """Takes one line's recognized_spans (straight from the parser) and
    produces a CuratorReport.

    NOT IMPLEMENTED — out of scope for the parser refactor that produced
    this stub. See module docstring for the diagnostic questions this
    stage needs to answer.
    """
    raise NotImplementedError(
        "analyzer.analyze is a stub — see module docstring."
    )