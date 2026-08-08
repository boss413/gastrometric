"""
identity_resolution.py

STUB — NOT IMPLEMENTED.

Consumes normalized ingredient observations (see
normalize_ingredient_observations.assemble_ingredient_observation) and
is responsible for:

    - ingredient identity (mapping an observation to a canonical
      ingredient entity)
    - aliases (which alias string maps to which canonical entity)
    - ingredient relationships (substitutions, parent/child forms, e.g.
      "kosher salt" vs "salt")
    - canonical entities (the single authoritative record for a given
      real-world ingredient)
    - ambiguity resolution (an observation that could plausibly match
      more than one canonical entity)

Nothing from the old (pre-refactor) parser belonged here — the old
parser's "protect known ingredient phrases" logic is a PARSING
responsibility under the new architecture (Stage 2 span recognition;
see parse_ingredient_lines.py), not identity resolution. Matching a
string against a known name list is a dictionary lookup; deciding what
that name canonically IS, and resolving it when the observation is
ambiguous, is what this stage exists for. There was no prior code doing
that job, so there is nothing to port in here.
"""


def resolve_identity(normalized_observation):
    """Takes one normalized ingredient observation and returns a
    resolved ingredient identity (canonical entity + alias record +
    confidence/ambiguity info for the analyzer to surface).

    NOT IMPLEMENTED — out of scope for the parser refactor that produced
    this stub. See module docstring for this stage's full
    responsibilities.
    """
    raise NotImplementedError(
        "identity_resolution.resolve_identity is a stub — see module docstring."
    )