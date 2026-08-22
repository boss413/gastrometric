# R0-7 — Candidate Evaluation and Evidence
### Design Specification for the Analyzer's candidate-evaluation stage
### Revision 2 — incorporates review feedback (state naming, invalid-interpretation counting, ingredient-resolution independence, explicit range forms)

**Status:** design spec, no implementation
**Target file (later work order):** `gastrometric/understanding/analyzer.py` (internal stages/functions — not a separate module; that split is not yet justified)
**Depends on:** `gastrometric/understanding/semantic_result_schema.json` (output contract), `gastrometric/understanding/ingredient_parser.py` (input contract), `gastrometric/knowledge/loader.py` (evidence source)

---

## 0. Purpose

This document defines **how** `analyzer.py` turns a `ParseResult` (a set of structurally
valid `Candidate`s from the parser) into the `interpretations[]` of a Canonical Semantic
Result, and **what counts as evidence** while doing so.

It does **not** define:
- the output schema (already defined — `semantic_result_schema.json`)
- a numerical confidence/scoring formula
- new relationship predicates or ontology/entity types
- persistence of analysis records
- the implementation of `analyzer.py` itself

This spec is the behavioral contract those things will later be built against.

---

## 1. Candidate vs. Interpretation

Two terms are used precisely and are not interchangeable.

**Parser Candidate** (`ingredient_parser.Candidate`)
The parser's complete, structurally valid syntactic reading of the entire ingredient
line. It exists because a lexical position may carry more than one `span_type`
classification (lexical ambiguity), and the parser expands the cross-product of those
classifications into one candidate per structurally valid combination. The parser does
not rank, score, or select among candidates — see `ingredient_parser.py`'s LEXICAL
AMBIGUITY CONTRACT.

**Semantic Interpretation** (`interpretation` in the output schema)
The Analyzer's semantic evaluation of exactly *one* parser Candidate. An interpretation
answers:
- What ingredient entity (if any) does each reference resolve to?
- Does a component have a valid relationship to its ingredient?
- What do quantities/packages/per-item quantities apply to?
- Are modifiers attached to a plausible semantic target?
- Are alternatives/conjunctions semantically coherent?
- What, if anything, is materially unresolved?
- What evidence supports these conclusions?

**Lineage:** every parser Candidate produces exactly one Analyzer Interpretation —
**including** candidates that evaluate as `invalid` (§2, §13). The Analyzer does not
drop candidates from `interpretations[]`; it evaluates each one and reports the
outcome, even a negative one. This is what keeps the result auditable: a caller can
see *why* a line only produced, say, one resolved interpretation out of three parser
candidates, rather than silently losing the other two.

---

## 2. Candidate Evaluation States

**Revision note:** the first draft of this spec used an internal `valid` state that
does not exist in `semantic_result_schema.json`'s `interpretation.status` enum
(`resolved | ambiguous | unresolved | invalid`). That was a naming bug — it implied a
value that could never actually be written into the output contract. This revision
removes `valid` entirely and evaluates every candidate directly into one of the
schema's own status values:

| Interpretation status | Meaning |
|---|---|
| `resolved` | The candidate produces a coherent interpretation and every semantically required slot is resolved. |
| `unresolved` | The candidate produces a coherent interpretation, but required semantic material could not be resolved. |
| `invalid` | The candidate cannot represent a semantically coherent reading at all — internal contradiction (§12). |

**`viable`** is used in this spec as a non-schema umbrella term meaning
*`resolved` or `unresolved`* — i.e. "produced a coherent interpretation, whether or
not everything in it resolved." It is never itself written to `status`; it exists
only so §13 can talk about "interpretations that aren't `invalid`" concisely.

> **Assumption (flagged):** the schema's shared `status` enum technically permits an
> individual `interpretation.status` to be `"ambiguous"`, since `interpretation` and
> the top-level result reuse the same enum definition. This spec treats `ambiguous`
> as a **result-level** concept only (§13) and never assigns it to a single
> interpretation — a single candidate is always resolved, unresolved, or invalid on
> its own terms; ambiguity is a property of *how many* viable interpretations survive
> across candidates, not a property of one. The shared enum is read as schema reuse
> for convenience, not as licensing per-interpretation ambiguity.

Example — `2 tbsp xyz powder`, single candidate:
```
quantity   = 2 tbsp        (understood)
ingredient = unresolved    (xyz powder unrecognized — see §7 for why this is an
                             ingredient-resolution outcome, not a relationship one)
```
→ interpretation status `unresolved`. Everything the Analyzer could resolve is kept
(§10); the interpretation is not discarded and is not `invalid`.

---

## 3. Evaluation Is Not Selection

The Analyzer evaluates **every** candidate produced by the parser. Candidate order
carries no semantic meaning (per the parser contract), so the Analyzer must not:
- treat `candidates[0]` as preferred,
- stop once it finds one candidate that resolves cleanly,
- discard a viable candidate solely because another viable candidate has higher
  confidence.

Example:
```
Candidate A: confidence 0.94, status resolved
Candidate B: confidence 0.61, status resolved
```
Both survive into the result:
```
status = ambiguous
interpretations = [A, B]
```
Confidence expresses *relative support*, not a license to prune. This is why the
schema's `selected_interpretation` field can be `null` — R0-7 does not reintroduce an
implicit selection mechanism to work around that; a genuinely ambiguous result stays
ambiguous until whatever later selection policy consumes `selected_interpretation`
decides otherwise. That policy is out of scope here.

---

## 4. Evaluation Dimensions

Candidate evaluation is a set of **independent, evidence-bearing considerations**. Not
every dimension applies to every candidate (e.g. `2 carrots` has no component to
evaluate).

| Dimension | Question |
|---|---|
| Ingredient resolution | Can the ingredient expression resolve to an ingredient entity (§7)? |
| Component resolution | Does a component have a valid relationship to the relevant ingredient? |
| Quantity attachment | Does the quantity have a coherent target (§11 for range forms)? |
| Package interpretation | Do package count/term/size form a coherent package structure? |
| Modifier attachment | Can a vocabulary modifier attach to an appropriate semantic target? |
| Relation coherence | Are conjunctions/alternatives semantically coherent? |
| Unresolved material | Is lexical material left without a semantic interpretation? |
| Knowledge support | Does the knowledge base provide evidence for the interpretation? |
| Structural consistency | Does the resulting semantic structure stay internally coherent? |

Each dimension that applies to a given candidate produces zero or more `evidence`
entries (§5) and contributes to that candidate's evaluation state (§2) and eventual
confidence (§8). Dimensions are evaluated independently of one another; one dimension
failing does not short-circuit evaluation of the others — all applicable evidence
should still be collected so the interpretation is fully explained (§10).

---

## 5. Evidence

Evidence explains *why* the Analyzer assigned support (or detraction) to an
interpretation. It uses the schema's existing shape as-is — R0-7 introduces no new
fields:

```json
{ "kind": "...", "record_id": "...", "effect": "supporting" }
```

`record_id` is interpreted according to `kind` (per the schema's own docstring); it is
not a universal foreign key. Not every observation needs a persisted evidence entry —
e.g. an exact ingredient match may be useful internally to evaluation without
necessarily requiring a database record behind `record_id`, exactly as the schema
already allows.

### Initial evidence kinds

| kind | Meaning | Precise resolution path |
|---|---|---|
| `exact_ingredient_match` | The normalized ingredient expression directly identifies an `ingredients.id` | `garlic` → `garlic` |
| `alias_match` | An `ingredient_aliases` entry resolves the expression to an `ingredients.id` | `tomatoes` → `tomato` |
| `relationship_match` | A knowledge-graph relationship supports a component/natural-portion/variety reading | `rib --component_of--> celery` |
| `vocabulary_match` | A recognized vocabulary term supports a semantic role | `diced` → preparation |
| `structural_match` | Parser structure supports the semantic construction | `quantity + ingredient` |
| `unresolved_material` | Something semantically required remains unresolved | `xyz powder` |

`exact_ingredient_match` and `alias_match` are **mutually exclusive per reference**: a
given ingredient reference resolves via exactly one of the two paths (whichever
actually matched), never both. Neither is a stand-in for the other, and neither
implies anything about `relationship_match` — see §7 for why ingredient resolution
and relationship evidence are independent.

`ingredient_aliases` is the *only* alias mechanism in this system. There is no generic
vocabulary-alias abstraction; `alias_match` must not be read or implemented as if one
existed.

### `effect` is a schema requirement, not an R0-7 policy

Every evidence entry must carry `effect: "supporting" | "detracting"` because the
schema requires it — but **R0-7 does not define the mapping from evidence `kind` to
`effect`**. In particular, this spec does not assert that `unresolved_material` is
always `detracting`, or that any other kind always carries a fixed `effect`. Deciding
that mapping is confidence-calibration work, and belongs with §8's deferred numerical
formula, not with this document. (An earlier draft of this spec floated
`unresolved_material → detracting` as a likely default; that floated mapping is
withdrawn here so this section doesn't quietly become the scoring policy it's
supposed to avoid defining.)

### A note on `component.term`

The schema's `reference.component` field is named `term`, and in the current system a
vocabulary term's normalized surface form happens to coincide with its `term_id`
(the relationship table's `subject_id`/`object_id` for `subject_type: "vocabulary"`
rows). R0-7 does not rename this field or otherwise treat it as anything other than
what the schema already says — a vocabulary term, not a canonical entity (§6). This
is flagged only so that if vocabulary representation ever diverges from its
normalized surface form (e.g. a future `term_id` that isn't just a slug of the
surface text), that divergence is a deliberate schema/loader decision, not something
this spec quietly assumed away.

---

## 6. Relationship Evidence

Relationship knowledge bridges vocabulary terms and ingredient entities **without**
promoting the vocabulary term into an entity.

`2 ribs celery`:
```
component = rib          (vocabulary term)
ingredient = celery      (ingredient entity, resolved independently — see §7)
```
looked up as:
```
(vocabulary: rib) --component_of--> (ingredient: celery)
```
producing:
```json
{ "kind": "relationship_match", "record_id": "1", "effect": "supporting" }
```
where `"1"` is the `relationships.relationship_id` row. This is evidence *for* the
existing interpretation — it is not a new semantic entity, and it does not cause
`component: "rib"` to become anything resembling `components.id`.

The same pattern applies to:
- `1 clove garlic` via `clove --natural_portion_of--> garlic`
- `grape tomatoes` via `grape --variety_of--> tomato`

Relationship lookups should go through the loader's existing query surface
(`knowledge.relationships_for_subject`, `relationships_for_object`,
`find_relationships`) rather than the Analyzer querying SQLite directly — the loader
already established that boundary and R0-7 does not revisit it.

**Relationship evidence presupposes, rather than establishes, ingredient
identity.** A relationship match supports a *component/natural-portion/variety*
reading once `ingredient = celery` (etc.) is already on the table; it is not how
`celery` got resolved as an ingredient in the first place. See §7.

---

## 7. Ingredient Resolution Is Independent of Relationship Evidence

**Revised rule (this section replaces the original, looser wording).**

Ingredient resolution is a specific, separate act: matching a (possibly modified)
ingredient expression against the canonical `ingredients` set, either directly
(`exact_ingredient_match`) or via `ingredient_aliases` (`alias_match`). It does not
consult the relationship graph, and the relationship graph is not a gate that has to
pass before ingredient resolution can be considered.

**Rule:** failure to resolve an ingredient expression to a canonical ingredient
entity is unresolved semantic material (§10). Missing relationship knowledge, by
itself, **does not constitute such a failure** — the two are independent failure
surfaces:

- **Ingredient-resolution failure:** the expression doesn't match `ingredients`
  directly or via alias → the ingredient is `unresolved`, regardless of what
  relationship data does or doesn't exist.
- **Relationship-evidence absence:** the graph has no assertion connecting a
  vocabulary term to an ingredient → no `relationship_match` evidence entry is
  produced for whatever structural role (component / natural-portion / variety) the
  candidate proposes. This does not, by itself, change the ingredient-resolution
  outcome, and does not invalidate the candidate (§12).

This distinction matters concretely: `chili paste`, `chicken juice`, `chicken jus`,
and similar expressions remain **eligible to resolve directly against
`ingredients`/`ingredient_aliases`** — now, or as the ingredient set grows — entirely
independent of whether any `component_of` / `natural_portion_of` / `variety_of`
relationship happens to exist for them. The relationship graph must never become an
accidental prerequisite for recognizing something as an ingredient at all.

The knowledge graph as a whole remains **open-world**: absence of an assertion means
"no supporting relationship is known," never "this relationship is false," and never
"this ingredient doesn't exist." This matters operationally, since the relationship
table and vocabulary/ingredient sets will keep growing and being curated; a design
that required exhaustive relationship coverage before a line could be interpreted
would regress correctness every time the ontology is (as it always will be, to some
degree) incomplete.

---

## 8. Confidence (conceptual only)

Confidence represents **the strength of evidence supporting an interpretation
relative to the Analyzer's rules** — an Analyzer support score, not a calibrated
probability (matching the schema's own description of `interpretation.score`).

It is conceptually influenced by things such as:
- exact ingredient identification
- alias identification
- relationship evidence
- coherent modifier attachment
- coherent quantity attachment
- unresolved required material (reduces support)
- semantic conflicts (reduces support)

**Explicitly out of scope for R0-7:** any numeric weighting (`exact match = +0.4`,
`alias = +0.2`, `relationship = +0.15`, ...), any calibration procedure, any
statistical prior, and — per §5 — the evidence-kind-to-`effect` mapping. Those are
implementation/calibration decisions for a later work order, once evidence collection
itself (§5–§7) is in place to calibrate against.

---

## 9. Evidence ≠ Confidence

These remain two separate concepts in both the schema and this spec:

```
2 ribs celery

evidence:
    exact_ingredient_match(celery)
    relationship_match(rib → celery)
    structural_match(quantity + component + ingredient)

confidence:
    0.98
```

- **Evidence** explains the basis for an interpretation — a list of discrete,
  attributable observations.
- **Confidence** summarizes the Analyzer's overall assessment of that interpretation.

The evidence list must not carry per-item numerical weights; if it did, `evidence`
would silently become the calibration mechanism this spec explicitly defers (§8).

---

## 10. Unresolved Material

Unresolved material is semantically meaningful in its own right, not a failure to be
discarded.

`2 tbsp xyz powder`:
```
quantity:   2 tbsp                      (understood — kept)
ingredient: unresolved                   (ingredient-resolution failure, §7)
unresolved: [{ text: "xyz powder", reason: "unknown_vocabulary", source_spans: [...] }]
```

This reduces the interpretation's confidence and yields an `unresolved` interpretation
status, but the Analyzer must **preserve everything it did understand** — it must not
collapse a partially-understood candidate down to bare `invalid` because *some* part of
it didn't resolve. `unresolved` and `invalid` are not the same failure mode (§2), and
conflating them would throw away legitimately useful partial understanding (e.g. the
quantity, here) that a later stage — or a human reviewing the result — could still use.

Reasons should be attributable in the same spirit as evidence kinds (`unknown_vocabulary`
is the one worked example given); this spec does not enumerate an exhaustive reason
taxonomy — that's calibration/implementation detail, same as §8.

---

## 11. Quantity Forms: Ordinary Range vs. Per-Item Range

Two distinct semantic shapes both involve a numeric range, and R0-7 keeps them
explicitly separate because they answer different questions.

### Ordinary range
`3-5 medium peppers` and `3 to 5 medium peppers` produce the **same** semantic
quantity — the `-` and `to` are syntactic variants of one range concept, distinguished
only by `source_spans`, never by semantics:
```json
{ "form": "range", "lower": 3, "upper": 5, "unit_type": "natural_portion", "unit_term": "pepper" }
```
This is **one quantity**, answering "how many peppers total."

### Per-item range
`4 chicken breasts (5-6 ounces each)` is a **different relationship**: a primary
quantity (`4 chicken-breast`) plus a second quantity that scopes to *each instance* of
the first, carried in `per_item_quantity`:
```json
{
  "quantity": { "form": "scalar", "value": 4, "unit_type": "natural_portion", "unit_term": "chicken-breast" },
  "per_item_quantity": { "form": "range", "lower": 5, "upper": 6, "unit_type": "measurement", "unit_term": "ounce" }
}
```
This answers two questions at once — "how many" and "how much does each one weigh" —
and the presence of `per_item_quantity` is itself what establishes that scoping (no
separate flag is needed, per the schema).

**Rule for candidate evaluation:** a candidate's quantity-attachment evaluation (§4)
must determine which of these two shapes a parsed range represents *before* evaluating
coherence — an ordinary range is evaluated as a single `quantity`; a per-item range is
evaluated as a `quantity` + `per_item_quantity` pair, and the two fields must be
checked for mutual coherence (e.g. the per-item unit should make sense as "per unit of"
the primary quantity's unit) rather than treated as two independent, unrelated
quantities. Which shape the parser handed the Analyzer is a structural fact from the
Candidate tree (per-item ranges arrive as an already-distinguished
`ParentheticalExpression`/sibling structure, per the parser contract) — the Analyzer
evaluates the shape it's given; it does not infer per-item scoping from a bare
adjacent range on its own initiative.

---

## 12. Candidate Rejection (what makes a candidate `invalid`)

The Analyzer marks a candidate `invalid` **only** when semantic evaluation establishes
that it cannot represent a coherent reading at all. This is deliberately narrow,
phrased as what it is *not*, because the failure mode this spec exists to prevent is
invalidity-by-culinary-intuition:

**Not sufficient grounds for rejection, individually or combined:**
- an optional relationship is missing (§7)
- an ingredient isn't in some particular ontology (§7)
- a vocabulary term has no known relationship (§6, §7)
- a phrase is unusual or uncommon
- the system has "insufficient statistical evidence" (there is no statistical model —
  see §8)
- another candidate has higher confidence (§3)

**What can make a candidate `invalid`** — evaluation surfacing a genuine internal
contradiction in the candidate itself, e.g.:
- a modifier's `applies_to` target does not exist anywhere in this candidate's
  structure (nothing for it to attach to),
- a relation (`conjunction`/`alternative`/`preference`) references members that are
  not actually present as references in this candidate,
- two evaluation dimensions produce results for the same semantic slot that cannot
  both be true simultaneously (a structural contradiction, not "the Analyzer prefers
  a different reading").

Example — `1 cup chili paste`:
- If `chili paste` resolves to an ingredient (`chili-paste`), directly or via alias
  (§5, §7) → `resolved`.
- If it does not (yet) correspond to any ingredient entity → **ingredient-resolution
  failure** (§7), i.e. `ingredient: unresolved` with matching `unresolved[]` material.
  This is `unresolved`, **not** `invalid` — and it is `unresolved` regardless of
  whether `chili paste`/`chili`/`paste` have any relationship assertions at all. The
  absence of a relationship is irrelevant to this outcome; it is not the cause of it
  (§7 corrects an earlier draft that implied otherwise).
- The Analyzer must not invent `chili` and `paste` as separate entities to force a
  resolution.

> **Assumption (flagged, unchanged from draft 1):** the affirmative "what can make a
> candidate invalid" list above is scoped to structural/internal-consistency failures
> only. I expect this to be a starting point that the implementation work order will
> need to test against real candidates and extend carefully — but any extension should
> stay in this same category (structural incoherence), not drift into "this
> combination seems culinarily implausible."

---

## 13. Result Status Derivation

**Revised per review feedback** — the original version said status was derived from
"surviving (valid/unresolved) interpretations" while also saying invalid candidates
remain in `interpretations[]` for auditability. Those two statements were in tension.
This section resolves it explicitly.

**Every parser Candidate becomes exactly one entry in `interpretations[]`, including
candidates evaluated as `invalid`.** Nothing is removed from the array for auditing
reasons (§1). The top-level `status`, however, is derived only from the **viable**
subset — `resolved` or `unresolved` interpretations (§2) — not from the raw size of
`interpretations[]`:

| Viable interpretations (resolved + unresolved) | Result status |
|---|---|
| 0 | `invalid` |
| 1, fully resolved | `resolved` |
| 1, but required material unresolved | `unresolved` |
| >1 | `ambiguous` |

**Explicit rule for mixed viable/invalid sets:** an `invalid` interpretation never
counts toward the "viable interpretations" total above, and its mere presence in
`interpretations[]` does not by itself make the result `ambiguous`. Concretely:

```
2 parser candidates
  A = resolved
  B = invalid

viable count = 1 (A only; B does not count)
top-level status = resolved
interpretations = [A, B]      (B stays, for audit — see §1)
```

```
3 parser candidates
  A = resolved
  B = invalid
  C = unresolved

viable count = 2 (A and C)
top-level status = ambiguous   (>1 viable, per table — see note below)
interpretations = [A, B, C]
```

```
2 parser candidates, both invalid

viable count = 0
top-level status = invalid
interpretations = [A, B]       (both present; result communicates "no coherent
                                 reading survived," while still exposing what was
                                 attempted)
```

> **Assumption (flagged, carried from draft 1):** the table does not separately
> address a viable set that mixes `resolved` and `unresolved` members (the second
> example above). Per the table as written, `>1 viable → ambiguous` is
> unconditional — it does not matter whether the surviving members are individually
> resolved or unresolved. This spec does not invent a fourth, finer-grained status to
> capture "ambiguous, and also something in it didn't fully resolve"; if that
> distinction turns out to matter in practice, it would need a schema change
> (e.g. inspecting `interpretations[].status` directly, which remains available to
> any consumer even when the top-level `status` is `ambiguous`), not a new top-level
> enum value invented here.

---

## 14. Evidence Does Not Create New Interpretations

Evidence supports the interpretation that already exists from candidate structure — it
never becomes a mechanism for inventing a new one.

`1 clove garlic`: the relationship `clove --natural_portion_of--> garlic` is evidence
*for* the existing `quantity: 1 clove, ingredient: garlic` interpretation. It must not
cause the Analyzer to also emit a second interpretation such as `ingredient: clove`.

**Candidate generation remains exclusively the parser's responsibility.** The Analyzer
evaluates the candidates it is given; relationships, aliases, and every other evidence
source are read-only inputs to evaluation, never candidate factories.

---

## 15. Summary of the Behavioral Contract

**Candidate evaluation**
- One parser Candidate → one Analyzer Interpretation, always — including `invalid`
  ones (§1, §13).
- All parser candidates are evaluated; none are skipped (§3).
- Candidate order has no semantic significance (§3).
- Evaluation assigns support/confidence conceptually (§8), not numerically.
- A viable lower-confidence candidate is not silently discarded solely because
  another candidate scores higher (§3).
- Each candidate resolves to `resolved`, `unresolved`, or `invalid` — the schema's own
  status values, with no separate internal "valid" state (§2).
- The result's top-level `status` is derived from the count of *viable*
  (`resolved`/`unresolved`) interpretations only; `invalid` interpretations remain in
  `interpretations[]` but do not count toward that total (§13).
- The Analyzer does not generate new candidates (§14).

**Evidence**
- Evidence explains support for an interpretation (§5, §9).
- Evidence references concrete knowledge records where applicable, interpreted
  per-`kind` (§5).
- `exact_ingredient_match` and `alias_match` are distinct, mutually exclusive
  ingredient-resolution paths; there is no generic vocabulary-alias mechanism (§5).
- Relationship matches are evidence, never entities, and never the mechanism by which
  ingredient identity itself is established (§6, §7).
- Missing relationship knowledge is not negative evidence and does not by itself
  invalidate a candidate or mark an ingredient unresolved — ingredient resolution and
  relationship evidence are independent (§7).
- Evidence and confidence remain separate concepts (§9).
- The evidence-kind-to-`effect` mapping is explicitly deferred, not assumed (§5, §8).
- Evidence does not itself create additional interpretations (§14).
- Evidence provenance is compatible with the existing schema, unmodified (§5).
- Unresolved material is represented explicitly, never silently discarded (§10).
- Ordinary ranges and per-item ranges are distinct semantic shapes and must be
  evaluated as such (§11).

### Explicit non-goals (unchanged from the work order)

R0-7 does **not** define:
- the numerical confidence algorithm
- confidence calibration (including the evidence-kind-to-`effect` mapping)
- statistical priors
- ranking weights
- a selected-interpretation policy
- new relationship predicates
- new ontology/entity types
- persistence of analysis records
- the implementation of `analyzer.py`

---

## Appendix: Worked Examples

**`2 ribs celery`** — single candidate, fully resolved:
```
component: rib
ingredient: celery
evidence: [relationship_match(rib component_of celery), exact_ingredient_match(celery),
           structural_match(quantity+component+ingredient)]
interpretation.status: resolved
result.status: resolved
```

**`2 tbsp xyz powder`** — single candidate, partially resolved:
```
quantity: 2 tbsp
ingredient: unresolved
unresolved: [{text: "xyz powder", reason: "unknown_vocabulary"}]
evidence: [structural_match(quantity+ingredient-slot), unresolved_material(xyz powder)]
interpretation.status: unresolved
result.status: unresolved
```

**Ambiguous span, e.g. "ribs" as component vs. natural portion** — two structurally
valid candidates, both viable:
```
result.status: ambiguous
interpretations:
  - id: A, confidence: 0.94, status: resolved   (component reading)
  - id: B, confidence: 0.61, status: resolved   (natural-portion reading)
```
Neither is dropped; `selected_interpretation` stays `null` unless/until a later
selection policy (out of scope here) resolves it.

**`1 cup chili paste`, no relationship in graph, and no ingredient/alias match either**
— single candidate:
```
ingredient: unresolved
unresolved: [{text: "chili paste", reason: "unknown_vocabulary"}]
interpretation.status: unresolved
result.status: unresolved
```
This is an **ingredient-resolution** outcome (§7) — it would be exactly the same
result whether or not any `component_of`/`variety_of`/etc. relationship existed for
`chili` or `paste`, because relationship absence was never the cause. If `chili-paste`
is later added to `ingredients` (directly or via alias), the same line resolves
cleanly without any relationship-graph change being required.

**Two candidates, one invalid** — e.g. a structurally-proposed relation whose member
reference doesn't actually exist in that candidate's tree:
```
interpretations:
  - id: A, status: resolved,  confidence: 0.90
  - id: B, status: invalid    (dangling relation member — structural contradiction)

viable count: 1 (A only)
result.status: resolved
interpretations: [A, B]   (B retained for audit, per §13)
```

**Ordinary range vs. per-item range** (§11):
```
"3-5 medium peppers"
  quantity: { form: range, lower: 3, upper: 5, unit_type: natural_portion, unit_term: pepper }
  (single quantity — "how many peppers")

"4 chicken breasts (5-6 ounces each)"
  quantity:          { form: scalar, value: 4, unit_type: natural_portion, unit_term: chicken-breast }
  per_item_quantity: { form: range,  lower: 5, upper: 6, unit_type: measurement, unit_term: ounce }
  (two related quantities — "how many" and "how much does each weigh")
```