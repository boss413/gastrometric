"""
Understanding seam for the inventory application (BE-01 / BE-02C boundary).

BE-01 defines the *shape* of the seam that
``gastrometric.application.inventory_editor`` calls to turn raw human
inventory input into a semantic result. BE-01 deliberately does not
implement ingredient resolution, alias matching, fuzzy matching, or
ambiguity scoring — that is BE-02A/BE-02C's job.

Until BE-02C supplies the real implementation, every input is treated
as unresolved and no ingredient is invented. This is intentional: an
inventory application that guessed a canonical ingredient here would
be "implementing a second semantic engine to fill the seam," which
BE-01 explicitly must not do.

BE-02C is expected to either:
  * replace ``understand_inventory_input`` in this module with a real
    implementation that has the same signature/return type, or
  * be wired in by passing a different callable via the ``understand``
    keyword argument that ``inventory_editor`` functions already
    accept.
Either integration path works without changing inventory_editor.py.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class UnderstandingResult:
    """
    The result of running raw inventory input through understanding.

    status: "resolved" | "ambiguous" | "unresolved"
    ingredient_id: canonical ingredient ID, required when status is
        "resolved" and MUST be None otherwise.
    raw_result: the complete, unmodified analyzer-shaped result. This
        is stored verbatim in analysis_result_json — never reduced to
        just status + ingredient_id.
    """

    status: str
    ingredient_id: Optional[str]
    raw_result: Dict[str, Any] = field(default_factory=dict)


def understand_inventory_input(original_input: str) -> UnderstandingResult:
    """
    Placeholder understanding operation (BE-01).

    Always returns "unresolved" with no ingredient_id. Replace this
    function's implementation, or supply an alternate callable via the
    `understand` parameter on inventory_editor's functions, once
    BE-02C's understanding pipeline exists.
    """
    raw_result = {
        "status": "unresolved",
        "input": original_input,
        "interpretations": [],
        "selected_interpretation": None,
        "note": (
            "Placeholder result from BE-01. The real understanding "
            "pipeline (BE-02A/BE-02C) has not been connected yet."
        ),
    }
    return UnderstandingResult(status="unresolved", ingredient_id=None, raw_result=raw_result)