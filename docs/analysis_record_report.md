# R0-8: Analysis Record / Report — Design Specification

Status: Design specification for review. No implementation code included, per work order scope.

---

## 0. Scope note

This document is a persistence *design*, not `analyzer.py`, not a change to R0-6
(`semantic_result_schema.json`), and not a change to R0-7 (candidate evaluation /
evidence design). It assumes, without re-deriving, the existing patterns visible in
`init_db.py` and `ingredient_parser.py`:

- `ingredient_parse_trees` is keyed by `recipe_ingredient_line_id` (FK to
  `recipe_ingredient_lines_raw.id`) and stores the whole parser output as
  `parse_tree_json`, with `id INTEGER PRIMARY KEY AUTOINCREMENT` and
  `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`.
- The existing raw-ingredient-line identity is `recipe_ingredient_lines_raw.id`,
  already reused as a foreign key by `ingredient_parse_trees`. R0-8 reuses this same
  identity rather than inventing a new one.
- SQLite is the persistence layer; index-per-foreign-key is the established
  convention (see `idx_ingredient_parse_trees_line`,
  `idx_ingredient_observations_line`, etc.).

Everything below follows that precedent unless a deviation is explicitly justified.

---

## 1. Analysis Record conceptual model

One **Analysis Record** = one Analyzer execution against one persisted
`ingredient_parse_trees` row, for one `recipe_ingredient_lines_raw` line.

Conceptually it is a container for three things:

1. **Identity/lineage** — which line, which parse tree, when.
2. **Candidate-evaluation ledger** — every candidate the Analyzer looked at, what
   state it reached, and which interpretation (if any) it produced.
3. **The canonical semantic result** — the R0-6 object, retained verbatim, not
   re-derived or re-modeled.

A failed, unresolved, or ambiguous analysis is a first-class, equally-persisted
outcome — there is no "success path" table and "failure path" table. This falls out
naturally from persisting the evaluation ledger unconditionally (see §4) and the
canonical result unconditionally (its `status` enum already includes `invalid`).

---

## 2. Identity and lineage

```
recipe_ingredient_lines_raw.id  ──►  ingredient_parse_trees.id  ──►  analysis_records.id
        (source line)                  (parse result)                 (this work order)
```

`analysis_records` carries two foreign keys, both required:

- `recipe_ingredient_line_id` → `recipe_ingredient_lines_raw(id)` — same identity
  `ingredient_parse_trees` already uses. This is redundant with the line ID
  reachable via `parse_tree_id → ingredient_parse_trees.recipe_ingredient_line_id`,
  but the redundancy is intentional: it lets a curator query analyses by source
  line without joining through the parse-tree table, and it protects the record's
  meaning if a parse tree is ever re-run and superseded (see §14 for what
  "superseded" implies here).
- `parse_tree_id` → `ingredient_parse_trees(id)` — identifies the *exact* parse
  result that produced the candidates this analysis evaluated. This is the
  authoritative answer to "which parser output generated these candidates?"
  (work order §14).

Both are `NOT NULL`. An analysis without a source line or without a parse result is
not a representable state — the Analyzer's precondition is always a persisted
parse tree.

---

## 3. Candidate lineage (dual identity, not collapsed)

Per work order §3, candidate identity and interpretation identity are preserved as
two distinct fields on the same evaluation row, joined 1:1 (per R0-7), rather than
assumed interchangeable:

- `candidate_id` — the parser's candidate identifier, copied from
  `parse_tree_json` (whatever scalar/string form the parser already assigns; not
  reinvented here).
- `interpretation_id` — the R0-6 `interpretation.id` this candidate produced, if
  any. Nullable (see §4 for when it's null).

Keeping these as separate columns — rather than a single merged ID — is what
lets a future Analyzer version stop assuming 1:1 candidate→interpretation without
a migration of the audit schema; the join is explicit data, not a naming
convention.

---

## 4. Candidate-evaluation persistence

Table: `analysis_candidate_evaluations`, one row per candidate presented to the
Analyzer, **unconditionally** — including candidates evaluated as `invalid`.
Nothing is discarded for failing evaluation (work order §4).

Columns:

- `id` — surrogate PK.
- `analysis_record_id` → `analysis_records(id)`.
- `candidate_id` — see §3. Copied from the parse tree, not re-derived.
- `evaluation_state` — `valid | unresolved | invalid`, the R0-7 candidate-level
  state. This is **not** the same value space as canonical `status`
  (`resolved | ambiguous | unresolved | invalid`) even though two labels overlap
  textually — work order §5 is explicit that these are distinct axes and must not
  be substituted for one another. Storing them in differently-named columns on
  different tables (`analysis_candidate_evaluations.evaluation_state` vs.
  `analysis_records.status`, §10) makes the distinction structural, not just
  documented.
- `interpretation_id` — nullable. Null exactly when the candidate produced no
  interpretation object in the canonical result (this can happen for an `invalid`
  candidate). Not null when an interpretation exists, whatever that
  interpretation's own `status` is — an `unresolved` candidate can still produce an
  interpretation with unresolved material (work order §5), so "has an
  interpretation_id" and "evaluation_state = valid" are independent facts.
- `created_at` — `DEFAULT CURRENT_TIMESTAMP`, matching existing convention.

What is deliberately **not** copied onto this row: the candidate's underlying
spans/observations, lexical classification, or any other parser-internal detail.
Those remain reachable through `analysis_records.parse_tree_id →
ingredient_parse_trees.parse_tree_json`. Only `candidate_id` — the minimum needed
to look the candidate back up in the parse tree — is copied, per work order §4's
instruction not to duplicate the parse tree "for convenience."

Index: `idx_analysis_candidate_evaluations_record` on `analysis_record_id`
(existing-convention pattern).

---

## 5. Interpretation / canonical-result persistence

The canonical semantic result (R0-6) is stored **verbatim, as a single JSON blob**,
on `analysis_records.canonical_result_json`. It is not decomposed into relational
tables for `references`, `quantities`, `packages`, `modifiers`, `relations`, etc.

Rationale, directly from the work order's own constraints:

- §6 says the record "does not create a competing semantic representation" and
  that the canonical-result schema is not R0-8's to redefine. Relationally
  decomposing `references`/`quantities`/`modifiers`/`relations` into new tables
  *is* defining a second representation of that schema — every future R0-6 change
  would then require a matching migration of R0-8 tables just to stay
  representationally equivalent, which is exactly the coupling §6 warns against.
- The whole-JSON approach is also the established precedent:
  `ingredient_parse_trees.parse_tree_json` already treats the upstream stage's
  authoritative structured output as an opaque, versioned-by-reference blob rather
  than something the persistence layer re-models.

`interpretation_id` values referenced from `analysis_candidate_evaluations` (§4)
and `analysis_records.selected_interpretation_id` (§8) are therefore *logical*
references into `canonical_result_json.interpretations[].id` — not enforceable
SQL foreign keys, since SQLite cannot constrain into a JSON column. This is a
real limitation (see §15, open questions) but matches how `ingredient_parse_trees`
already relies on convention rather than DB constraints for its own JSON payload.

---

## 6. Evidence persistence

Table: `analysis_evidence`, normalized, one row per R0-6 evidence object,
**in addition to** evidence already present inside `canonical_result_json`
(intentional redundancy, justified below — this is the "combination" option
work order §7 leaves open, and it is the one recommended choice in this design).

Columns:

- `id` — surrogate PK.
- `analysis_candidate_evaluation_id` → `analysis_candidate_evaluations(id)` —
  evidence is scoped to the interpretation an evaluation produced; since
  candidate↔interpretation is 1:1 (§3), joining through the evaluation row avoids
  needing a separate `analysis_interpretations` table just to hang evidence off of.
- `kind` — copied verbatim from R0-6 evidence (`relationship_match`,
  `alias_match`, `exact_ingredient_match`, `vocabulary_match`, `structural_match`,
  `unresolved_material`, or others R0-6/R0-7 define — this table does not
  constrain the enum beyond what R0-6 already does, to avoid a second place that
  needs updating when R0-6's evidence kinds change).
- `record_id` — copied verbatim; scoped by `kind` per R0-6's own definition, not
  reinterpreted here.
- `effect` — `supporting | detracting`, copied verbatim.

**Why normalize evidence but not the rest of the canonical result:** work order §8
calls out the knowledge-provenance requirement specifically — a curator needs to
ask "which analyses used relationship 17 as evidence?" (e.g. when deciding
whether a knowledge relationship is being over/under-used, or when a bad
relationship is discovered and its blast radius needs auditing). That query is
impractical against a JSON blob at scale without SQLite's JSON1 extension, and
even with it, an indexed relational table is the more direct fit for a query
pattern the work order explicitly names as a curation requirement (§17). No
weighting, scoring, or calibration is introduced — `analysis_evidence` is a
straight copy of the three R0-6 evidence fields, nothing computed.

This is the one deliberate duplication of canonical-result content in this
design. It is bounded (three scalar fields per evidence item) and is not
comparable to duplicating the parse tree, which the work order separately warns
against (§4) because of its size and its already-durable location.

Index: `idx_analysis_evidence_evaluation` on `analysis_candidate_evaluation_id`;
`idx_analysis_evidence_kind_record` on `(kind, record_id)` to support the
provenance query above.

---

## 7. Unresolved-material persistence

No separate unresolved-material table or model. R0-6's `reference.unresolved[]`
array (`text`, `reason`, `source_spans`) is already the structured representation
work order §9 asks to preserve, and it survives automatically because
`canonical_result_json` is stored whole (§5). Introducing a parallel
`analysis_unresolved_material` table would be exactly the "second unresolved-data
model" §9 says not to invent absent a concrete persistence requirement — no such
requirement surfaces here, since nothing in §17's curation/debugging use cases
needs unresolved material queried outside the context of its owning
interpretation.

---

## 8. Status, confidence, and selected-interpretation persistence

- `analysis_records.status` — a **denormalized copy** of
  `canonical_result_json.status`. Defined meaning: enables filtering/listing
  analyses by outcome (e.g. "show all `invalid` analyses for regression review")
  without parsing JSON per row. It is a copy, not a reinterpretation — no
  aggregation logic produces it; it is written at persist-time straight from the
  canonical result the Analyzer just emitted. This is distinct in both name and
  table from `analysis_candidate_evaluations.evaluation_state` (§4), preserving
  the candidate-level/result-level separation work order §10 requires.
- **Confidence**: no additional column. Per-interpretation `score` already lives
  in `canonical_result_json.interpretations[].score` and is not duplicated
  elsewhere, per work order §11's instruction not to duplicate confidence without
  a defined meaning. No aggregate/top-level confidence is introduced by this
  design — see §15 as an open question if a concrete need for one surfaces later.
- **Selected interpretation**: `analysis_records.selected_interpretation_id`,
  nullable, a denormalized copy of `canonical_result_json.selected_interpretation`.
  Null is a valid, expected state (e.g. `ambiguous` results) and is not treated as
  missing data or an error — it is written as NULL exactly when the canonical
  result's own field is null. Because it is copied at persist-time rather than
  computed, it cannot itself represent "no policy has run yet" versus "policy ran
  and produced no selection" any more precisely than R0-6 already does; if a later
  stage performs selection *after* the Analyzer runs, work order §12 requires that
  event to be distinguishable from "Analyzer evaluated candidates" — this design
  treats that as an **update** to the analysis record (new `canonical_result_json`
  reflecting the selection, new `selected_interpretation_id`) rather than a new
  record, but flags the update-vs-append question as unresolved (§15) pending
  confirmation of how/whether post-hoc selection is expected to work.

---

## 9. Relationship to `ingredient_parse_trees`

`analysis_records.parse_tree_id` is a required FK to `ingredient_parse_trees(id)`.
`ingredient_parse_trees` remains the sole authoritative store of parser output;
`analysis_records` never copies `parse_tree_json` content beyond the minimal
`candidate_id` values needed on evaluation rows (§4). The parse tree is looked up,
not duplicated, whenever a curator needs to see the spans/observations behind a
candidate.

## 10. Relationship to the existing ingredient-line identity

`analysis_records.recipe_ingredient_line_id` reuses
`recipe_ingredient_lines_raw.id` directly — the same identity
`ingredient_parse_trees.recipe_ingredient_line_id` already uses. No new line
identifier is introduced. See §2 for why it is stored redundantly with
`parse_tree_id` rather than only reachable transitively.

---

## 11. Persistence representation

**Recommendation: Option C (hybrid)**, matching the shape work order §16 sketches:

```
analysis_records
    id, recipe_ingredient_line_id, parse_tree_id,
    status, selected_interpretation_id,
    canonical_result_json,
    created_at

analysis_candidate_evaluations
    id, analysis_record_id, candidate_id,
    evaluation_state, interpretation_id,
    created_at

analysis_evidence
    id, analysis_candidate_evaluation_id,
    kind, record_id, effect
```

Justification against the three options:

- **Pure relational (A)** would require decomposing the R0-6 schema into tables,
  which §6 rules out as creating a competing semantic representation, and would
  also force a table (or table family) per candidate/evaluation/evidence anyway
  — Option A doesn't actually avoid the hybrid shape, it just also decomposes the
  canonical result on top of it.
- **Pure serialized (B)** — everything as JSON blobs on `analysis_records` — would
  satisfy §6 and §9 cleanly but fails the curation query pattern in §17 (e.g.
  "which analyses evaluated a given candidate as invalid," "which analyses used
  relationship 17") without ad hoc JSON scanning, and it also erases the
  candidate/interpretation dual-identity distinction §3 requires be explicit.
- **Hybrid (C)** keeps the canonical result exactly as R0-6 defines it (no
  competing schema), keeps the parse tree exactly as `ingredient_parse_trees`
  already stores it (no duplication), and adds exactly two small relational
  tables sized to the two concrete cross-cutting query needs the work order names:
  candidate-level auditing (§4/§17) and knowledge-evidence provenance (§8/§17).

This directly extends the existing `ingredient_parse_trees` precedent (opaque JSON
column for the upstream stage's authoritative output) rather than replacing it
with a different pattern.

---

## 12. Required metadata / provenance

Minimum retained, per work order §15: `analysis_records.id`,
`recipe_ingredient_line_id`, `parse_tree_id`, `created_at`. These are sufficient to
answer "what was analyzed and from what parse result, and when" but — per §15's
explicit caution — **do not by themselves guarantee reproducibility**. Nothing in
the supplied schema/code shows an existing Analyzer-version, vocabulary-snapshot,
or relationship-snapshot mechanism, so none is invented here (§15's instruction).
This is called out as an explicit open question in §15 below rather than silently
assumed either way.

## 13. Audit / debug / curation requirements

Satisfied by the design above:

- "What text was analyzed" → `recipe_ingredient_line_id`.
- "What parser result produced the candidates" → `parse_tree_id`.
- "Which candidate produced each interpretation" →
  `analysis_candidate_evaluations.(candidate_id, interpretation_id)`.
- "How was that candidate evaluated" → `evaluation_state`.
- "What evidence supported/detracted" → `analysis_evidence`, queryable directly by
  `kind`/`record_id` for knowledge-curation feedback loops.
- "What canonical result was emitted" → `canonical_result_json`, unmodified R0-6
  shape.
- Failed/ambiguous/unresolved analyses remain fully queryable (`status`,
  `evaluation_state`) rather than being dropped or flattened.

---

## 14. Explicit non-goals (restated from work order §18, unchanged)

This design does not implement `analyzer.py`, candidate-generation rules, parser
grammar, semantic interpretation rules, a confidence formula or calibration,
candidate ranking, a selected-interpretation policy, new vocabulary classes, new
ingredient entities, new relationship predicates, ontology expansion, nutrition
resolution, or recipe-level aggregation. It persists the analysis event and its
lineage only.

---

## 15. Unresolved design decisions (must be settled before implementation)

1. **Update vs. append semantics.** If a future stage performs interpretation
   selection after the Analyzer has already persisted a record (work order §12),
   should that update the existing `analysis_records` row (mutating
   `canonical_result_json`/`selected_interpretation_id`) or append a new record
   that supersedes the prior one? This design assumes update-in-place but the
   work order does not settle it, and it affects whether `analysis_records` needs
   a supersession/versioning column.
2. **Re-parse/re-analysis identity.** If a line is re-parsed (new
   `ingredient_parse_trees` row) and re-analyzed, do old `analysis_records` rows
   referencing the superseded parse tree remain as historical audit trail
   (this design's assumption), or should they be marked/archived somehow? No
   existing mechanism for this was visible in the supplied schema.
3. **Reproducibility/versioning metadata.** Whether Analyzer implementation
   version, vocabulary knowledge version, or relationship knowledge version need
   to be captured is flagged but not decided — no such mechanism exists elsewhere
   in the supplied code, and inventing one is out of scope per §15's own
   instruction. Needs input from whoever owns the knowledge rebuild/seed process.
4. **Logical vs. enforced FK for `interpretation_id`.** Because
   `canonical_result_json` is opaque to SQLite, `interpretation_id` values on
   `analysis_candidate_evaluations` and `selected_interpretation_id` on
   `analysis_records` cannot be enforced by a real foreign key. Whether this is
   acceptable (matching the existing `parse_tree_json` precedent) or whether the
   project wants an application-level consistency check at write time is an open
   question for the implementer, not this design.
5. **Evidence enum ownership.** `analysis_evidence.kind` intentionally does not
   duplicate an enum constraint from R0-6 in the DB schema (to avoid a second
   place that drifts from R0-6). Confirm this is acceptable versus adding a
   `CHECK` constraint mirrored from R0-6's evidence kinds.