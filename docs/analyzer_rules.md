# RO-9: Deterministic v1 Analyzer Rules — Design Specification

**Status:** design spec, no implementation
**Target implementation (later work order):** `gastrometric/understanding/analyzer.py`
**Depends on:**
`gastrometric/understanding/semantic_result_schema.json` (RO-6, output contract),
`RO-7` (candidate evaluation states, evidence model, confidence boundary),
`RO-8` (persistence — referenced only for vocabulary, not revisited here),
`ingredient_parser.py` (`Candidate`/`ParseResult`/`IngredientReference` contract,
LEXICAL AMBIGUITY CONTRACT),
`gastrometric.knowledge.loader.RuntimeKnowledge` (`knowledge`, the only
evidence source — no other query surface exists or is invented here)

**Does not implement `analyzer.py`.**

---

## 0. Purpose and boundary

RO-6 defined what the output *means*. RO-7 defined how candidates are evaluated
and what evidence *is*, deliberately leaving numeric weighting undefined. RO-8
defined what gets persisted. RO-9 is the last purely-design step before
implementation: **given one parser `Candidate` and `knowledge`, exactly what
deterministic steps produce its `interpretation`.**

RO-9 is intentionally narrow. It does not re-derive RO-6's schema, re-litigate
RO-7's evaluation-state or evidence model, invent new relationship predicates,
invent a numeric confidence formula, or describe everything the Analyzer might
ever need to know. Every rule below exists to answer one question for one of
the letters A–M in the work order: *what does v1 do, mechanically, right now,*
given the loader's actual query surface (`knowledge.ingredients`,
`knowledge.ingredient_aliases`, `knowledge.resolve_ingredient_alias`,
`knowledge.classes_for`, the `is_*` predicates, `knowledge.relationships_for_subject`,
`relationships_for_object`, `find_relationships`) and the parser's actual
structural contract (fragmented-phrase merging, parenthetical grouping,
conjunction/alternative wrapping, package sibling adjacency, `UnknownSequence`,
`NotesExpression`). Where the work order's boundary note applies, this spec
says so explicitly rather than silently expanding scope.

---

## 1. Terminology (recap, not redefinition)

Unchanged from RO-7 §1–§2: **Candidate** (parser) vs. **Interpretation**
(Analyzer); one Candidate → exactly one Interpretation, including `invalid`
ones; interpretation status ∈ `{resolved, unresolved, invalid}`; **viable**
(RO-7's non-schema umbrella term) = `resolved` or `unresolved`. RO-9 does not
introduce new states — it only makes the *triggers* for reaching each state
mechanical.

---

## 2. Master decision table

This expands the work order's skeleton table into the full deterministic rule
set. Each row's **Rule** is unpacked fully in the lettered section named in
the last column.

| Stage | Input | Rule | Output | Evidence | § |
|---|---|---|---|---|---|
| Ingredient resolution | ingredient expression | normalized form ∈ `knowledge.ingredients` | `ingredient` = that name | `exact_ingredient_match` | A |
| Ingredient resolution | ingredient expression | not exact, but `knowledge.resolve_ingredient_alias` returns a different name | `ingredient` = canonical name | `alias_match` | A |
| Ingredient resolution | ingredient expression | neither exact nor alias | `ingredient` = null, entry in `unresolved[]` | `unresolved_material` | A |
| Component resolution | component term + resolved ingredient | `find_relationships(subject_type="vocabulary", subject_id=term, predicate="component_of", object_type="ingredient", object_id=ingredient)` non-empty | `component` = term; `quantity.unit_term` = term | `relationship_match` (one per row) | B |
| Component resolution | component term + resolved ingredient | relationship absent, but candidate tree already structures term as component | `component` = term (structural only) | `vocabulary_match` + `structural_match` | B |
| Natural portion vs. component | portion term + resolved ingredient | `natural_portion_of` relationship found | `quantity.unit_type=natural_portion`, `unit_term=term`; `component` left unset | `relationship_match` | C |
| Natural portion vs. component | portion term + resolved ingredient | no relationship either way | follow candidate tree's own structural placement (component-shaped vs. bare-portion-shaped) | `vocabulary_match` + `structural_match` | C |
| Quantity | scalar MeasurementExpression | construct scalar quantity | `quantity` (form=scalar) | `structural_match` | D, H |
| Quantity | range MeasurementExpression (`-` or `to`) | construct range quantity from parser's `form=range`, never from literal connective text | `quantity` (form=range) | `structural_match` | D |
| Quantity | parenthetical quantity sibling to a primary quantity | attach as per-item | `per_item_quantity` | `structural_match` | D, H |
| Package | PackageExpression + adjacent sibling Measurement/Parenthetical | construct package with independent `count`/`package_term`/`size` | `package` | `structural_match` (+ `vocabulary_match` for `package_term`) | E |
| Protected phrase | overlapping lexical spans | never re-interpreted inside one candidate; alternate readings live in sibling candidates only | reference fields set exactly once, from this tree | `structural_match` | F |
| Modifier | vocabulary modifier + tree attachment position | resolve `applies_to` from the closed class table | `modifier` | `vocabulary_match` | G |
| Compound ingredient | `IngredientReference.ingredient` = `ConjunctionExpression`/`AlternativeExpression` | decompose into one `Reference` per ingredient; inherit `preparation`/`modifiers`/`notes` on each; if a real `quantity`/`package` is present, do not distribute it — flag `ambiguous` instead | multiple `Reference`s + `relation` | `structural_match` (+ per-ingredient evidence) | I |
| Relation | Conjunction/Alternative/Preference node (post-decomposition) | validate all members/`base`/`preferred` resolve to references present in this candidate | `relation` | `structural_match` | I |
| Unresolved | `UnknownSequence`, unsupported modifier class, unrecognized relation shape | preserve as `unresolved[]` entry, do not fail the candidate | `unresolved[]` entry | `unresolved_material` | J |
| Notes | `IngredientReference.notes` (from `NotesExpression`) | carry through 1:1 to the containing `Reference.notes` as `{text, source_spans}` — **not** `unresolved`; construction rule fixed, but blocked on the RO-6 amendment landing and on parse-tree confirmation for conjoined-reference attachment | `Reference.notes` *(proposed schema addition, §J)* | `vocabulary_match` | J |
| Candidate | dangling relation member / modifier target absent / per-item with no primary / conflicting slot assignment | reject | interpretation `status=invalid` | evidence for whichever check fired | K |
| Evidence | every check above | emit exactly one entry per matching fact (relationship rows are not deduplicated) | `evidence[]` | — | L |
| Confidence | interpretation status | fixed placeholder lookup, not a weighted formula | `score` | — | M |
| Result | all interpretations for a line | derive top-level `status` from viable count | `status`, `interpretations[]` | — | RO-7 §13 (unchanged) |

---

## A. Ingredient resolution precedence

Given a reference's ingredient expression (already merged across fragmented
same-typed phrases per parser contract item 7a — the Analyzer never re-splits
it):

1. **Exact.** Normalize the expression. If it is a member of `knowledge.ingredients`
   → `ingredient` = that name. Evidence: `exact_ingredient_match`.
2. **Alias.** Else, call `knowledge.resolve_ingredient_alias(expression)`. If
   the return value differs from the input → `ingredient` = the returned
   canonical name. Evidence: `alias_match`.
3. **Both.** Exact and alias are evaluated in that fixed order (1 then 2), and
   step 1 short-circuits step 2 — a reference resolves via exactly one path,
   never both, matching RO-7 §5's "mutually exclusive per reference." This is
   a lookup order, not a competing interpretation: it produces one answer for
   the one candidate already on the table (RO-7 §14).
4. **Neither.** `ingredient` = null; add `{ text: expression, reason:
   "unknown_ingredient", source_spans }` to `unresolved[]`. Evidence:
   `unresolved_material`. Per RO-7 §7 this is independent of any relationship
   data — no relationship lookup is attempted or relevant here at all.
5. **Ambiguous between multiple ingredient identities, within one candidate.**
   Given the loader's actual shape — `knowledge.ingredients` is a set and
   `resolve_ingredient_alias` is a total function returning exactly one name —
   a single ingredient expression cannot resolve to two different canonical
   ingredients inside one evaluation. **This case is definitionally
   unreachable at the single-candidate level.** What looks like "ingredient
   ambiguity" (e.g. "cloves" as Ingredient vs. NaturalPortion) is a
   *between-candidate* phenomenon per the parser's LEXICAL AMBIGUITY CONTRACT:
   it already exists as two separate `Candidate`s before the Analyzer runs,
   each resolved independently by steps 1–4 above, with the top-level result
   status (`ambiguous`) emerging from RO-7 §13, not from a new rule here. If
   the loader ever exposed one alias string mapped to two canonical
   ingredients, that would be a knowledge-authoring defect, not a case for
   this Analyzer to adjudicate.

**Worked:** `tomato` → exact, `ingredient=tomato`. `tomatoes` → not exact
(plural), `resolve_ingredient_alias("tomatoes") = "tomato"` → alias match.
`diced tomatoes` → the ingredient expression is `tomatoes` alone (`diced` is a
separate, post-nominal `PreparationExpression` per parser contract item 7, not
part of the ingredient phrase) → alias match on `tomatoes`, plus a
`preparation` modifier (§G) for `diced`.

---

## B. Component resolution — `2 ribs celery`

1. Recognize `rib` as component vocabulary: `knowledge.classes_for("rib")`
   contains `"component"`. (There is no dedicated `is_component()` convenience
   predicate in the loader — unlike `is_measurement`/`is_preparation`/etc. — so
   this is the one vocabulary check in this spec that must go through the
   generic `classes_for` lookup rather than a named `is_*` helper.)
2. Resolve `celery` as ingredient via §A.
3. Look up `knowledge.find_relationships(subject_type="vocabulary",
   subject_id="rib", predicate="component_of", object_type="ingredient",
   object_id="celery")` — scoped to *this candidate's own already-resolved
   ingredient*, not an unscoped `relationships_for_subject("vocabulary",
   "rib")` call, so irrelevant assertions about `rib` and unrelated
   ingredients never enter this candidate's evaluation at all.
4. **Relationship exists (exactly one row).** `component = "rib"`;
   `quantity = { form: scalar, value: 2, unit_type: natural_portion,
   unit_term: "rib" }` (component and unit_term share the term, per the
   schema's own note). Evidence: `relationship_match`, `exact_ingredient_match`,
   `structural_match`.
5. **Relationship does not exist.** Per RO-7 §7/§12, a missing relationship is
   *never* by itself grounds for `invalid` or `unresolved`. The Analyzer
   still builds the component reading **if and only if this candidate's own
   tree already structures `rib` as a component node** (Candidate variant C
   in the parser's own worked example — the Analyzer follows the tree it was
   given, it does not invent structure). In that case: same `component`/
   `quantity` construction as step 4, but evidence is `vocabulary_match` +
   `structural_match` only (no `relationship_match`). This candidate is not
   penalized to `invalid` or `unresolved` for the missing relationship; it
   simply carries less supporting evidence than a sibling candidate that does
   have it — a distinction §M's confidence table can express, §K cannot.
6. **Multiple relationships exist** for the exact scoped triple
   (`vocabulary:rib`, `component_of`, `ingredient:celery`) — e.g. duplicate
   assertions from different sources. Rule: **emit one `relationship_match`
   evidence entry per matching `Relationship` row**, not deduplicated to one.
   Each is a distinct persisted assertion with its own `record_id`/`source`/
   `confidence`; collapsing them would discard provenance the evidence model
   exists to preserve (RO-7 §5). This never produces more than one
   interpretation (RO-7 §14) — only more evidence under the one interpretation
   already on the table.

---

## C. Natural portions vs. component — `1 clove garlic`, `3 sprigs thyme`

Same term (`clove`), same vocabulary classification ambiguity the work order
flags, resolved deterministically:

1. If `find_relationships(subject_type="vocabulary", subject_id=term,
   predicate="component_of", object_type="ingredient", object_id=ingredient)`
   is non-empty → component reading (§B).
2. Else if `find_relationships(..., predicate="natural_portion_of", ...)` is
   non-empty → natural-portion reading: `quantity = { unit_type:
   natural_portion, unit_term: term, ... }`; **`component` is left unset** —
   a natural portion counts the ingredient itself, it does not name a
   structural part of it, so it occupies only the quantity slot.
3. Else (term carries `"natural_portion"` and/or `"component"` in
   `classes_for(term)`, but no relationship supports either reading against
   *this* resolved ingredient): follow whichever structural shape this
   candidate's own tree already committed to (the between-candidate ambiguity
   is the parser's concern, per §A.5 and the LEXICAL AMBIGUITY CONTRACT —
   the Analyzer here is only choosing the *construction*, given the shape,
   never choosing the shape itself). Evidence is `vocabulary_match` +
   `structural_match` only.
4. If `classes_for(term)` contains neither class at this position, the
   parser contract guarantees this cannot happen (lexical candidates only
   ever come from the lexer's own classification) — out of scope.

`1 clove garlic`: `clove --natural_portion_of--> garlic` found →
`quantity = { form: scalar, value: 1, unit_type: natural_portion, unit_term:
clove }`, `ingredient = garlic` (exact match). `3 sprigs thyme`: same shape
with `sprig --natural_portion_of--> thyme`, assuming that relationship is
seeded; if not, falls to step 3.

---

## D. Range recognition

The parser has already decided `form: scalar | range` structurally — that
decision does not depend on whether the source text used `-` or `to`; only
`source_spans` differs (parser output contract, RO-6 rule 4). The Analyzer's
rule is correspondingly trivial and singular: **branch construction on the
parser's own `form` field, never on the literal connective text.** There is
exactly one construction path for range quantities:

```json
{ "form": "range", "lower": <parser lower>, "upper": <parser upper>,
  "unit_type": ..., "unit_term": ..., "source_spans": <parser's spans, verbatim> }
```

`3-5 medium peppers` and `3 to 5 medium peppers` therefore produce byte-for-byte
identical quantity objects except `source_spans`. There is no second code path
that could accidentally diverge, because there is no branch on the connective
at all — asking "was it `-` or `to`" is not a question this rule ever asks.

**Per-item detection.** A range or scalar quantity is `per_item_quantity`
rather than an ordinary second quantity iff:
1. the reference already has a primary (non-parenthetical) `MeasurementExpression`
   in `quantity`, **and**
2. this quantity is structurally a `ParentheticalExpression` sibling of that
   primary (parser output contract rule 3 + rule 6 analog for measurements) —
   never inferred from bare adjacency of two un-grouped ranges.

If a parenthetical quantity exists with **no** primary quantity elsewhere on
the reference, there is nothing for "each" to scope against — this is a
structural-contradiction case, see §K.3.

`4 chicken breasts (5-6 ounces each)`: primary `quantity = { form: scalar,
value: 4, unit_type: natural_portion, unit_term: chicken-breast }` (§H);
parenthetical range → `per_item_quantity = { form: range, lower: 5, upper: 6,
unit_type: measurement, unit_term: ounce }`.

---

## E. Package semantics — `2 cans (14-ounce) diced tomatoes`

Three independent constructions on the **same** reference, assigned in any
order (they don't depend on each other):

- `package`: `PackageExpression` → `package.count` (its associated bare
  number) + `package.package_term` (normalized package vocabulary, e.g.
  `can`). If a `ParentheticalExpression`/`MeasurementExpression` is a sibling
  of the `PackageExpression` in original source order (parser output contract
  rule 6 — package and its associated measurement remain siblings, never
  merged) → that becomes `package.size` (a nested quantity). If no such
  sibling exists, `package.size` is simply omitted (it's optional in the
  schema).
- `ingredient`: resolved from the `IngredientExpression` (`tomatoes`) per §A,
  entirely independent of the package structure.
- `modifiers`: `diced` resolved per §G, `applies_to: ingredient`, entirely
  independent of the package structure.

The Analyzer must **not** flatten these: `package.count=2`,
`package.package_term=can`, `package.size={14, ounce}`, `ingredient=tomato`,
and the `diced` modifier are five separate assignments on one reference, never
collapsed into a single `quantity`. If the reference additionally carried an
ordinary top-level measurement *not* grouped with the `PackageExpression`,
that becomes an independent `quantity` per §H — package and quantity may
coexist, per the schema's own note that "package count and package size are
independent quantities."

---

## F. Protected phrase / longest semantic expression behavior

This is not a new lexer mechanism (the work order explicitly rules that out)
— it is RO-7 §1/§3's per-candidate independence, restated at the
constituent-span level. Per the LEXICAL AMBIGUITY CONTRACT, if `"diced
tomatoes"` as one compound ingredient phrase and `"diced" + "tomato"` as
preparation-plus-ingredient are both structurally possible, that is **two
separate `Candidate`s in `ParseResult.candidates`**, not one tree with two
competing readings glued together. The Analyzer's rule is therefore simply:
**populate a reference's fields strictly from what this candidate's own tree
assigned to those slots, and never reach back into raw spans to construct an
alternative reading of a span this tree already resolved.** "Protection" is
not a property the Analyzer computes — it falls out automatically from
evaluating one candidate's already-fixed tree in isolation, exactly as every
other rule in this document already assumes. Any genuinely alternative
reading is, by construction, a different candidate, evaluated independently
and appearing (if viable) alongside this one in `interpretations[]`.

---

## G. Modifier attachment

`modifier_class` (what kind of vocabulary is this term?) and `applies_to`
(what does it attach to *in this candidate's tree*?) are answered by two
different, independent lookups: `modifier_class` comes from
`knowledge.classes_for(term)` / the matching `is_*` predicate; `applies_to`
comes from the term's structural position in the tree. The closed table below
covers the currently supported classes (per the loader's `is_*` surface,
excluding `ingredient`/`grammar`, which are not modifier classes, and
`packaging`, which is handled entirely by §E):

| `modifier_class` | Default `applies_to` | Override condition |
|---|---|---|
| `preparation` | `ingredient` | if this preparation term is itself the structural target of a `size` modifier (e.g. "large **dice**"), no override to the preparation's own attachment — it still attaches to `ingredient` |
| `size` | `ingredient` (bare noun/reference) | `preparation`, if this size term is structurally attached to a `PreparationExpression` rather than the ingredient noun (e.g. "large **dice**") |
| `state` | `ingredient` | — |
| `descriptor` | `ingredient` | — |
| `temperature` | `ingredient` | — |
| `seasoning` / `brand` | `ingredient` | rare as a modifier position at all (usually their own ingredient/component); if the tree does place one as a modifier, default applies |

`applies_to` is read off the tree's actual parent/sibling relationship, never
guessed from the term's meaning — this table is a closed lookup from
`(modifier_class, structural attachment position)` to `applies_to`. A
vocabulary class not in this table is unresolved material (§J), not an
invented default, per the work order's boundary note (RO-9 documents
currently-supported classes, not every hypothetical future one).

**`large pepper, diced`**: `large` → `modifier_class=size`, structurally
attached to the ingredient noun `pepper` → `applies_to=ingredient`. `diced` →
`modifier_class=preparation` → `applies_to=ingredient`. Two independent
modifiers, both targeting the ingredient.

**`large dice`**: `dice` is the `PreparationExpression` itself →
`modifier_class=preparation`, `applies_to=ingredient`. `large` is structurally
pre-nominal to `dice` specifically (not to any ingredient noun in this
phrase) → `modifier_class=size`, `applies_to=preparation`.

---

## H. Quantity attachment

| Structural input | Target | `unit_type` / `unit_term` |
|---|---|---|
| number + measurement term (`2 cups carrots`) | `reference.quantity` | `measurement`, term as given |
| number + bare ingredient noun, no separate counting-unit term (`2 carrots`) | `reference.quantity` | `natural_portion`, `unit_term` = the ingredient's own singular vocabulary form (`carrot`) |
| number + distinct natural-portion vocabulary term (`1 clove garlic`) | `reference.quantity` | per §C |
| number + component term (`2 ribs celery`) | `reference.quantity` (component doubles as unit_term) | per §B |
| primary + parenthetical sibling | `quantity` + `per_item_quantity` | per §D |
| package count/size | `package.count` / `package.size` | never `reference.quantity` — see §E |
| range shape (either connective) | as above, `form=range` | per §D |

The `2 carrots` row resolves the parser contract's own explicitly-deferred
question ("whether '2 carrots' means two natural-portion units of carrot") —
RO-9's answer: **yes, deterministically**, whenever a bare quantity attaches
directly to a reference whose ingredient is fully resolved and no distinct
counting-unit vocabulary term is present in the structure at all.

---

## I. Relation handling — compound ingredient decomposition

**Correction to an earlier draft.** §I previously assumed a
`ConjunctionExpression`/`AlternativeExpression` over two `IngredientExpression`s
produces two separate `IngredientReference`s at the parser level, joined
afterward by a relation. The confirmed parse tree for `salt and pepper to
taste` shows this is wrong: parser output contract rule 5 ("the TYPE of the
operands determines where the group attaches **on** `IngredientReference`")
means exactly what it says — the compound `ConjunctionExpression` attaches
to the **`ingredient` field of one single `IngredientReference`**. There is
one parser reference for `"salt and pepper to taste"`, not two. Everything
below replaces the earlier §I.

**Why the Analyzer, not the parser, must split it.** RO-6 types
`reference.ingredient` as `["string", "null"]` — a single canonical id. One
canonical `Reference` cannot represent a compound ingredient. The parser is
not obligated to solve this (its job is structure, not schema conformance);
the Analyzer is, because it is the layer responsible for producing something
RO-6-valid.

**Decision — decomposition rule (v1):**

1. **Decompose.** A parser `IngredientReference` whose `ingredient` field is
   a `ConjunctionExpression`/`AlternativeExpression` over `IngredientExpression`s
   becomes **one canonical `Reference` per operand ingredient**, each
   resolved independently per §A (exact/alias/unresolved) exactly as if it
   were the sole ingredient on its own reference.
2. **Relate.** A `relation` is constructed connecting the resulting
   references: `{ relation_type: "conjunction"|"alternative", members:
   [ref_a.id, ref_b.id], source_spans: [<"and"/"or" span>] }` (or
   `relation_type: "preference"` with `base`/`preferred`, for a
   comma-plus-`"preferably"` shape — construction branches on the parser's
   relation-node shape, never inferred).
3. **Inherit modifiers and notes, do not duplicate everything.** Fields that
   semantically describe the compound expression as a whole — `preparation`,
   `modifiers`, `notes` — are **inherited by every resulting canonical
   reference**, unmodified, one copy each. These are not per-ingredient
   measurements; there is nothing to distribute, so inheritance is exact
   and requires no scope judgment: `large salt and pepper`, `salt and
   pepper, minced`, and `salt and pepper to taste` all simply copy the
   shared modifier/note onto both `r1` and `r2`.
4. **Quantity and package are not modifiers — they require scope
   evaluation, not inheritance.** A `quantity` or `package` attached to a
   compound-ingredient reference has materially different semantics: unlike
   a modifier, it makes a factual (often nutritional) claim about *how
   much*, and that claim does not obviously distribute across multiple
   ingredients. If a real (non-empty) `quantity`/`package` is attached to a
   reference whose `ingredient` is compound, and the deterministic rule set
   here has no basis for establishing how it distributes across the
   decomposed references, **do not guess**: construct both decomposed
   references carrying the *same* quantity/package value (preserving both
   possible readings rather than picking one), and set this candidate's
   `interpretation.status = "ambiguous"`. This is not `invalid` (nothing is
   structurally contradictory) and not `unresolved` (the material isn't
   missing, its scope is indeterminate) — it is a genuine `ambiguous`
   reading in RO-7's sense, and it is not the Analyzer's job to resolve it
   by culinary intuition (RO-7 §12's boundary, extended here to quantity
   scope): the correct remediation is a corrected source recipe, not a
   heuristic in the Analyzer.
5. **No sophisticated scope inference.** Step 4 does not attempt partial or
   weighted distribution (e.g. splitting a quantity in half, or preferring
   whichever ingredient is "usually" measured that way). The rule is binary:
   compound ingredient + empty/absent quantity-package → §3 applies cleanly,
   no ambiguity. Compound ingredient + populated quantity/package → §4
   applies, flagged `ambiguous`, full stop.
6. **Coherence.** A relation is coherent iff every id in `members` (or
   `base`/`preferred`) is a `reference.id` actually present in this
   candidate's `references[]` — automatically true here since the Analyzer
   itself just constructed those references from the same decomposition, but
   stated for completeness and because it is still the general dangling-id
   check for any relation shape the parser might emit (§K.1).
7. **N-ary compounds.** `"salt, pepper, and paprika"`, if the parser nests
   two binary `ConjunctionExpression`s, decomposes into **three** references
   with **two** `relation` entries (one per binary node) — v1 does not flatten
   nested binary wrapping into a single three-member relation (parser output
   contract rule 4 only defines binary wrapping; the schema's `members` array
   has no stated n-ary flattening rule).

**Scope of this decision.** This is stated for `ingredient`-field compounds
specifically, since that is the case that violates a hard schema constraint
(single canonical `ingredient` id) and the case actually observed in the
confirmed parse tree. Whether the same decompose-and-inherit treatment
should generalize to compound `preparation`/`component` (parser output
contract rule 5 lists those as symmetric cases) is not decided here — those
fields don't carry the same single-value schema constraint in the same
forcing way, and no parse tree exercising that case has been reviewed yet.
Flagged for RO-10/future RO-9 revision, not assumed.

---

## J. Unresolved material

Concrete, closed set of triggers for a `reference.unresolved[]` entry
(candidate stays viable; nothing else on the reference is discarded, per RO-7
§10):

| Trigger | `reason` |
|---|---|
| Ingredient expression matches neither exact nor alias (§A.4) | `unknown_ingredient` |
| Parser-emitted `UnknownSequence` (output contract rule 8), carried through 1:1 | `unrecognized_span` |
| Modifier whose `modifier_class` is not in §G's table | `unsupported_modifier_class` |
| Relation node whose shape (predicate/type) the Analyzer doesn't recognize, but whose members **are** all structurally present (not dangling — see §K.1 for the dangling case) | `unrecognized_relation_shape` |

**`NotesExpression` is explicitly *not* a §J trigger.** An earlier draft of
this spec routed `NotesExpression` content into `unresolved[]`. That was
wrong, and is retracted: per the confirmed `ASTNode` definitions,
`NotesExpression` is created *only* when the vocabulary has positively
recognized a span as a grammar/annotation phrase, and `IngredientReference`
already carries a dedicated `notes: List[ASTNode]` field, distinct from and
sibling to `unresolved: List[ASTNode]`. Writing it into `unresolved[]` would
have asserted the opposite of what the parser established (confident
recognition, not failure-to-resolve), and would have discarded a distinction
(`notes` vs. `unresolved`) the parser has already made — which RO-9 must not
do, per the general preservation principle closing this section.

**Decision: `notes` is a required RO-6 schema addition, not an Analyzer
workaround.** `IngredientReference.notes` is first-class parser output,
already scoped to the correct `IngredientReference` (no per-line ambiguity —
this corrects an earlier draft's unnecessary "last reference in source
order" fallback, which solved a scoping problem that does not exist), and
semantically meaningful recipe-line content. RO-9's deterministic rule is
therefore:

> A parser `NotesExpression` on `IngredientReference.notes` produces a
> corresponding entry on the containing canonical `Reference.notes`,
> preserving its lexical provenance. Notes are not unresolved material and
> do not affect ingredient resolution merely by existing. Evidence:
> `vocabulary_match` (the grammar/annotation classification itself), never
> `unresolved_material`.

**Field shape — resolved.** The confirmed `ASTNode` hierarchy shows
`NotesExpression(ContainerNode)` carries no fields of its own beyond
`children: List[ASTNode]` — it is structurally identical to
`IngredientExpression`, `PreparationExpression`, `ComponentExpression`,
`PackageExpression`, and `MeasurementExpression`, every one of which is
likewise a bare `ContainerNode` subclass. Whatever mechanism the Analyzer
already needs for turning any of *those* into a canonical `term`/text plus
`source_spans` (required for `modifier.term`, `component`, preparation
modifiers, etc. throughout §A–§I) applies to `NotesExpression` without
modification — there is no notes-specific extraction problem here, only the
general child-flattening the Analyzer already performs everywhere else.

**Proposed RO-6 amendment (recommendation, not an edit RO-9 makes):**
```json
"notes": {
  "type": "array",
  "items": {
    "type": "object",
    "additionalProperties": false,
    "required": ["text", "source_spans"],
    "properties": {
      "text": { "type": "string" },
      "source_spans": { "type": "array", "minItems": 1, "items": { "type": "string" } }
    }
  }
}
```
Modeled directly on the existing `unresolved` object shape (`text` +
`source_spans`), minus `reason` — a note isn't a failure to resolve
something, so it needs no failure explanation; it's the freestanding-content
counterpart to `unresolved`, not a variant of it. This mirrors
`IngredientReference.notes`'s own `List[ASTNode]` cardinality: one entry per
`NotesExpression` node, not a single merged string. This is a proposal for
RO-6's owner to accept, reject, or reshape — RO-9 does not add it to
`semantic_result_schema.json` itself.

**Still blocked — attachment for conjoined references.** *Resolved.* The
confirmed parse tree for `salt and pepper to taste` shows `notes` lives on
the single parser `IngredientReference` that itself holds the compound
`ConjunctionExpression` ingredient — it is not per-branch, it describes the
reference as a whole (as do `preparation`/`modifiers`, had they been
present). Per §I's decomposition rule, a note attached to a compound parser
reference is **inherited by each canonical `Reference` produced from that
compound** — one copy per decomposed reference, not split, not attached to
only one. `to taste` is not `unresolved`.

This is worth stating as a general rule, not a one-off:

> The Analyzer preserves a distinction the parser has already made
> (`unresolved` vs. `notes` vs. `preparation` vs. `component`, ...) unless
> the canonical schema explicitly declares that distinction out of scope.
> It does not collapse distinctions into `unresolved` just because the
> schema hasn't caught up yet.

**`unresolved` vs. `invalid` (restated from RO-7 §2, §10, §12):** unresolved
= material is present and structurally sound, but this rule set has no place
to put it (candidate stays viable, everything else resolved is kept). Invalid
= the candidate's own structure contradicts itself (§K) — the whole candidate
is rejected.

---

## K. Invalidity — closed list

1. A relation's `members`/`base`/`preferred` references an id not present in
   this candidate's `references[]` (§I).
2. A modifier's `applies_to` target node does not exist anywhere in this
   candidate's structure (§G assigns `applies_to=preparation` but this
   candidate's tree has no preparation constituent for this reference —
   nothing to attach to).
3. A `per_item_quantity`-shaped parenthetical exists with no primary
   `quantity` on the same reference to scope against (§D). (If that same
   parenthetical content could validly stand alone as an ordinary quantity,
   that reading belongs to a *different* candidate, never smuggled in as a
   fallback inside this one — §F.)
4. A constructed `package.size` that is not actually the `PackageExpression`'s
   sibling per the tree (would require violating parser output contract rule
   6 to build) — listed for completeness; current grammar should not produce
   this.
5. Two evaluation dimensions assign incompatible values to the same schema
   slot (RO-7 §12's residual catch-all — e.g. a quantity's `form` somehow
   determined as both `scalar` and `range` from the same candidate; §D's
   single deterministic construction path should make this unreachable in
   practice).

This list stays inside RO-7 §12's boundary: **structural/internal
contradiction only.** Missing relationship knowledge, an unusual phrase, or
another candidate scoring higher are never grounds for `invalid` (RO-7 §12,
carried forward unchanged).

---

## L. Evidence generation — exact triggers

| kind | Emitted exactly when |
|---|---|
| `exact_ingredient_match` | §A.1 fires |
| `alias_match` | §A.2 fires |
| `relationship_match` | Any `find_relationships`/`relationships_for_subject`/`relationships_for_object` call in §B/§C returns ≥1 row supporting this candidate's proposed structural role — **one entry per matching row**, not deduplicated (§B.6) |
| `vocabulary_match` | A `classes_for`/`is_*` check succeeds and is used to justify a role (§B, §C, §G) with no `relationship_match` backing it |
| `structural_match` | A parser structural pattern is successfully mapped to its schema construction (quantity, range, per-item pairing, package, relation) — once per successfully-constructed compound object, not once per leaf field |
| `unresolved_material` | Once per `unresolved[]` entry added (§J) |

**`effect` mapping (new, minimal, non-numeric).** RO-7 §5 explicitly left
`kind → effect` undecided pending calibration. The schema nonetheless requires
`effect` on every evidence entry, and RO-9 — unlike RO-7 — must leave RO-10 an
implementable rule, not another open question. RO-9's resolution is the
smallest possible one: a **fixed categorical label**, not a weight or score —
`unresolved_material → "detracting"`; every other kind above →
`"supporting"`. This is called out explicitly as RO-9's one deliberate
extension of RO-7 §5, and it does not touch, determine, or substitute for
confidence (§M) — it is a binary label required by the schema, nothing more.

---

## M. Confidence

The work order requires RO-9 to pick one of three options rather than leave
this to the implementer.

- **Option 1 (heuristic weights)** — rejected. Any `exact match = +0.4`-style
  formula is exactly the calibration mechanism RO-7 §8 reserved for a later
  work order; inventing it here would silently do that work under RO-9's
  name.
- **Option 3 (defer confidence generation)** — rejected. `score` is `required`
  on every `interpretation` in the RO-6 schema; there is no way to emit a
  valid result without it.
- **Option 2 (deterministic placeholder) — selected.**

```
resolved   -> score = 1.0
ambiguous  -> score = 0.5
unresolved -> score = 0.5
invalid    -> score = 0.0
```

**Correction:** an earlier version of this table omitted `ambiguous`
entirely, even though the schema's `status` enum permits it at the
per-`interpretation` level (not just the top-level `ParseResult` level) —
surfaced by §I.4's compound-quantity-scope case. `ambiguous` is given the
same placeholder as `unresolved` (0.5): both represent "the interpretation
is present and structurally sound, but something about it — missing
material vs. indeterminate scope — keeps it short of full confidence." This
is a placeholder-table completeness fix, not a new weighting decision; it
does not compare relative severity between the two states, consistent with
§M's overall stance that this table draws no fine distinctions pending real
calibration.

Fixed, status-derived, entirely deterministic, and requires zero per-evidence
weighting decisions. It is trivially replaceable by a real calibration
formula later (RO-7 §8) without touching any construction/evaluation logic
above — only this one three-row table changes when calibration work begins.
This placeholder does **not** attempt to distinguish, e.g., the two `resolved`
candidates in RO-7 §3's worked example (0.94 vs. 0.61) — under this table both
would score `1.0`. That loss of relative ranking is an accepted, explicit
consequence of Option 2 over Option 1, not an oversight; re-introducing
relative ranking is calibration work, out of scope here by the same
reasoning that ruled out Option 1.

---

## Worked examples

**`2 carrots`** — single candidate:
```
ingredient: carrot                         (exact_ingredient_match)
quantity: { form: scalar, value: 2, unit_type: natural_portion, unit_term: carrot }
evidence: [exact_ingredient_match, structural_match]
status: resolved   score: 1.0
```

**`1 clove garlic`**:
```
ingredient: garlic                         (exact_ingredient_match)
quantity: { form: scalar, value: 1, unit_type: natural_portion, unit_term: clove }
evidence: [exact_ingredient_match, relationship_match(clove natural_portion_of garlic),
           structural_match]
status: resolved   score: 1.0
```

**`2 ribs celery`**:
```
ingredient: celery                         (exact_ingredient_match)
component: rib
quantity: { form: scalar, value: 2, unit_type: natural_portion, unit_term: rib }
evidence: [exact_ingredient_match, relationship_match(rib component_of celery),
           structural_match]
status: resolved   score: 1.0
```

**`3-5 medium peppers`** and **`3 to 5 medium peppers`** — identical semantic
result, differing only in `source_spans`:
```
ingredient: pepper                         (exact_ingredient_match)
quantity: { form: range, lower: 3, upper: 5, unit_type: natural_portion, unit_term: pepper }
modifiers: [{ modifier_class: size, term: medium, applies_to: ingredient }]
evidence: [exact_ingredient_match, structural_match, vocabulary_match(medium)]
status: resolved   score: 1.0
```

**`4 chicken breasts (5-6 ounces each)`**:
```
ingredient: chicken-breast                 (exact_ingredient_match, or alias if seeded as alias)
quantity:          { form: scalar, value: 4, unit_type: natural_portion, unit_term: chicken-breast }
per_item_quantity: { form: range, lower: 5, upper: 6, unit_type: measurement, unit_term: ounce }
evidence: [exact_ingredient_match, structural_match]
status: resolved   score: 1.0
```

**`2 cans (14-ounce) diced tomatoes`**:
```
ingredient: tomato                         (alias_match: tomatoes -> tomato)
package: { count: 2, package_term: can, size: { form: scalar, value: 14, unit_type: measurement, unit_term: ounce } }
modifiers: [{ modifier_class: preparation, term: diced, applies_to: ingredient }]
evidence: [alias_match, structural_match, vocabulary_match(can), vocabulary_match(diced)]
status: resolved   score: 1.0
```

**`1 medium garlic clove, minced`**:
```
ingredient: garlic                         (exact_ingredient_match)
quantity: { form: scalar, value: 1, unit_type: natural_portion, unit_term: clove }
modifiers: [
  { modifier_class: size, term: medium, applies_to: reference },
  { modifier_class: preparation, term: minced, applies_to: ingredient }
]
evidence: [exact_ingredient_match, relationship_match(clove natural_portion_of garlic),
           structural_match, vocabulary_match(medium), vocabulary_match(minced)]
status: resolved   score: 1.0
```
`medium` describes the counted unit (`clove`), which has no schema-level
attachment slot of its own (`applies_to` only permits `ingredient | component
| reference | preparation`) — the deterministic fallback is `reference`, the
closest available target, since `clove` here is a quantity `unit_term`, not a
`component` or `preparation` node.

**`salt and pepper to taste`** — resolved.
```
references: [
  { id: r1, ingredient: salt,   notes: [{ text: "to taste", source_spans: ["to taste"] }] },
  { id: r2, ingredient: pepper, notes: [{ text: "to taste", source_spans: ["to taste"] }] }
]
relations: [{ relation_type: conjunction, members: [r1, r2], source_spans: ["and"] }]
evidence: [exact_ingredient_match(salt), alias_match(pepper) or exact_ingredient_match(pepper),
           structural_match(compound decomposition), structural_match(relation), vocabulary_match(to taste)]
status: resolved   score: 1.0
```
Single parser `IngredientReference` with a compound (`ConjunctionExpression`)
`ingredient` and a `notes` entry describing the reference as a whole →
decomposed per §I into two canonical references, `notes` inherited by both
(§I.3, §J). No `quantity`/`package` is present, so §I.4's ambiguity trigger
does not fire — this is a clean `resolved` interpretation.

**`2 cans tomatoes and beans`** — ambiguous, by §I.4/§I.5.
```
references: [
  { id: r1, ingredient: tomato, package: { count: 2, package_term: can, ... } },
  { id: r2, ingredient: bean,   package: { count: 2, package_term: can, ... } }
]
relations: [{ relation_type: conjunction, members: [r1, r2], source_spans: ["and"] }]
evidence: [exact_ingredient_match(tomato), exact_ingredient_match(bean),
           structural_match(compound decomposition), structural_match(relation)]
status: ambiguous   score: 0.5   (per §M's corrected placeholder table)
```
A compound-ingredient reference carries a real, non-empty `package` (`2
cans`). §I.4 forbids guessing whether both ingredients each get 2 cans, the
2 cans are split between them, or some other reading — the Analyzer
constructs both decomposed references carrying the *same* package value
(preserving, not resolving, the possible readings) and marks the
interpretation `ambiguous`. Per RO-7 §13, `ambiguous` is a top-level
`ParseResult`-level status concept; whether it is expressed here as one
`interpretation` with `status: ambiguous` or as multiple sibling
interpretations (one per possible distribution) each individually
`unresolved`/`resolved` is a construction-mechanics question this document
leaves to RO-10, since RO-7 did not specify which of those two shapes
`ambiguous` takes at the interpretation-array level for a *single*-candidate
source of ambiguity (as opposed to the ordinary case of ambiguity between
multiple `Candidate`s). Flagged below as an open item — not fabricated here.
The remediation this spec recommends, per your decision, is a corrected
source recipe (e.g. `"2 cans tomatoes and 1 can beans"`), not a scoring
heuristic.

**Unknown ingredient expression** (e.g. `1 cup xyz powder`, no exact/alias
match, no relationship data relevant since ingredient resolution never
reaches the relationship graph per §A.4/§7):
```
quantity: { form: scalar, value: 1, unit_type: measurement, unit_term: cup }
ingredient: null
unresolved: [{ text: "xyz powder", reason: "unknown_ingredient", source_spans: [...] }]
evidence: [structural_match(quantity), unresolved_material]
status: unresolved   score: 0.5
```

---

## Summary of the behavioral contract

- Ingredient resolution: exact → alias → unresolved, in that fixed order;
  never blocked or altered by relationship data (§A, RO-7 §7).
- Component vs. natural-portion is a relationship-first, tree-fallback
  decision — never guessed from the term alone (§B, §C).
- Range form (`-` vs. `to`) never re-enters the Analyzer's decision — the
  parser's `form` field is the only thing branched on (§D).
- Package layers (count/term/size) are always independent of ingredient and
  modifier resolution on the same reference (§E).
- Overlapping-span "protection" is a consequence of per-candidate
  independence, not a new mechanism (§F).
- `modifier_class` and `applies_to` are two independent lookups, resolved
  against a closed table of currently-supported classes (§G).
- A quantity's target is derived from a fixed structural-input table,
  including an explicit resolution of the parser's own deferred "`2 carrots`"
  question (§H).
- Relations are always binary per node, validated for dangling members, never
  semantically second-guessed (§I). A compound `ingredient` on a single
  parser reference is decomposed by the Analyzer into one reference per
  ingredient plus a relation — modifiers/notes inherited exactly, quantity/
  package scope never guessed, flagged `ambiguous` instead when indeterminate
  (§I).
- Unresolved material has a closed, named set of triggers, including one
  explicitly flagged RO-6/parser schema gap (`NotesExpression`) (§J).
- Invalidity stays within RO-7 §12's structural-contradiction boundary, with
  five concrete, closed triggers (§K).
- Evidence emission is fully triggered (§L), including one minimal, explicitly
  flagged new decision (`effect` mapping) beyond what RO-7 left open.
- Confidence is a fixed three-value placeholder table tied to interpretation
  status, chosen explicitly over heuristic weighting (§M).

### Explicit non-goals (unchanged from the work order)

RO-9 does not define: new relationship predicates, new vocabulary classes,
numerical calibration/weighting beyond the one flagged placeholder table
(§M), a selected-interpretation policy, persistence, `analyzer.py` itself, or
**the RO-6 schema amendment §J proposes** — RO-9 specifies the deterministic
rule and a recommended shape for that amendment, but does not add the field
to `semantic_result_schema.json` itself; that remains RO-6's to accept.

**Status of this document:** functionally complete for v1. §J's `notes`
question is fully settled (construction rule, proposed schema shape, and
scope). §I was corrected based on a confirmed parse tree — the earlier
"parser splits conjunctions into two references" assumption was wrong; the
Analyzer now performs that decomposition, with an explicit inherit-vs.-flag
rule for shared modifiers/notes versus quantity/package scope. Two
mechanics-level questions remain genuinely open (items 5 and 6 below) and
should be resolved before or during RO-10, but neither blocks the rest of
this document.

### Flagged assumptions / open items (carried into RO-10)

1. §A.5 — within-candidate ingredient-identity ambiguity is treated as
   unreachable given the loader's current one-to-one alias mapping; RO-10
   should assert this rather than silently rely on it.
2. §B.6 — duplicate relationship rows for the same triple each get their own
   evidence entry; if the knowledge base is expected to de-duplicate
   assertions upstream, this rule becomes moot but is harmless either way.
3. §J's `notes` handling is now fully resolved: construction rule, proposed
   RO-6 schema shape (`{text, source_spans}`), and scope (inherited by every
   canonical reference produced from a compound-ingredient decomposition,
   §I.3) are all settled from the confirmed `ASTNode` hierarchy and parse
   tree. What remains open is only RO-6 actually accepting/landing the
   schema amendment — a process step, not a design question.
4. §I was substantively corrected, not just extended: the earlier draft's
   assumption that the parser splits a conjunction of `IngredientExpression`s
   into two `IngredientReference`s at parse time was wrong — the confirmed
   tree shows one reference with a compound `ingredient` field, and RO-9 now
   specifies that the Analyzer performs the decompose-and-relate step. This
   is the correction of an incorrect assumption in an earlier draft, called
   out explicitly rather than silently amended.
5. §I.4's quantity/package-scope-ambiguity case (`"2 cans tomatoes and
   beans"`) exposed a construction-mechanics question RO-7 didn't specify:
   whether a single-candidate source of ambiguity is expressed as one
   `interpretation` with `status: ambiguous`, or as multiple sibling
   interpretations each individually resolved/unresolved. This spec assumes
   the former (one `ambiguous` interpretation carrying both possible
   readings) but flags the choice explicitly for RO-10 rather than treating
   it as settled by implication.
6. §I's decomposition rule is scoped to compound `ingredient` fields only
   (the case a hard schema constraint forces and the case actually observed
   in the parse tree). Whether compound `preparation`/`component` (parser
   output contract rule 5's symmetric cases) should decompose the same way
   is explicitly left open — not decided, not assumed.
7. §L's `effect` mapping (`unresolved_material` → detracting, everything else
   → supporting) is RO-9's own minimal addition beyond RO-7 §5's deferral;
   confirm this is acceptable before RO-10 hard-codes it.
8. §M's placeholder table intentionally collapses relative confidence between
   multiple `resolved` candidates (e.g. RO-7 §3's 0.94-vs-0.61 example both
   become 1.0 here), and now also gives `ambiguous` the same 0.5 as
   `unresolved` without distinguishing them — both flagged as accepted,
   explicit trade-offs, not oversights, pending a real calibration work
   order.