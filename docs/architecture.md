# Gastrometric Architecture

## Product Philosophy

Gastrometric is **not** a recipe manager.

Gastrometric is a culinary reasoning system. Recipes, nutrition databases, flavor references, and user inventories are evidence used to construct, enrich, and query a persistent culinary knowledge graph.

The long-term intellectual property of the project is not the recipe corpus itself, but the knowledge graph built from that corpus.

---

# Product Goals

## Proof of Concept (Current Milestone)

Given:

* a kitchen inventory
* a recipe corpus
* USDA nutrition data

Gastrometric can:

* Recommend recipes that maximize use of perishable ingredients.
* Display recipes for cooking.
* Calculate nutritional information from ingredient mappings.

Features outside this workflow should remain modular but may be deferred until after the proof of concept.

---

# Core Design Principles

## Gastrometric Owns the Ontology

Gastrometric owns its ingredient identities and culinary ontology.

External systems enrich Gastrometric but never define it.

Examples include:

* USDA FoodData Central
* FoodOn
* Flavor Bible
* Wikipedia
* FoodSubs
* Future knowledge sources

Gastrometric identifiers remain stable regardless of changes to any external dataset.

---

## Recipes Are Evidence

Recipes are not the knowledge model.

Recipes are evidence from which Gastrometric derives:

* ingredient identities
* culinary relationships
* preparation techniques
* transformations
* recipe methods
* statistical patterns

---

## Deterministic Pipeline

Every pipeline stage must be:

* independently executable
* idempotent
* debuggable

Intermediate tables remain persistent by design.

This is an intentional architectural decision that supports deterministic processing, debugging, and future LLM guardrails.

---

# Knowledge Layers

Gastrometric is composed of five interacting knowledge layers.

## 1. Vocabulary

Represents the language used by humans.

Examples:

* scallions
* EVOO
* boneless skinless chicken thighs

Responsibilities:

* aliases
* spelling corrections
* parser vocabulary
* normalization dictionaries

---

## 2. Ingredient Identity Graph

Represents persistent ingredient entities.

Identity answers:

> "What ingredient is this?"

Examples:

* chicken breast
* chicken thigh
* smoked paprika
* Hungarian paprika
* Spanish paprika

Ingredient identities contain relationships such as:

* parent/child
* substitutions
* external mappings
* possible states
* properties

Ingredient identities are culinary concepts, not USDA concepts.

---

## 3. Recipe Knowledge

Recipes are stored as structured evidence.

Contains:

* parsed ingredients
* parsed instructions
* metadata
* provenance
* ratings
* execution plans

Recipes are not considered authoritative representations of culinary knowledge.

---

## 4. Cooking Knowledge

Represents how food behaves.

Examples:

* techniques
* transformations
* pairings
* substitutions
* semantic recipe components
* reusable recipe methods

This layer is the long-term intellectual property of Gastrometric.

---

## 5. Kitchen State

Represents the user's current inventory.

Kitchen state is dynamic.

It includes:

* purchased ingredients
* leftover ingredients
* transformed ingredients

Kitchen state is produced by recipe execution.

---

# Recipe Pipeline

```
Recipe
        ↓
Recipe Sections
        ↓
Ingredient Blocks
Instruction Blocks
        ↓
Ingredient Lines
        ↓
Ingredient Parsing
        ↓
Ingredient Normalization
        ↓
Ingredient Identity Resolution
        ↓
Knowledge Graph Enrichment
```

---

# Recipe Ingestion

All ingestion sources produce the same internal Recipe JSON schema before entering the pipeline.

Current source:

* data/seed/recipes.md

Future sources include:

* recipe URLs
* Food Network
* Serious Eats
* Alton Brown
* New York Times
* Bon Appétit
* America's Test Kitchen
* OCR
* user uploads
* APIs

Each recipe maintains its original provenance.

Recipe lifecycle:

* Requested
* Ingested
* Normalized
* Modified

Modified recipes become child recipe entities while preserving the original source.

---

# Parsing

Parsing extracts structure only.

Input:

```
2 pounds boneless skinless chicken breast,
cut into 1 inch cubes
```

Produces:

* quantity
* unit
* ingredient text
* preparation
* parentheticals

Parsing performs no semantic interpretation.

---

# Normalization

Normalization performs deterministic vocabulary cleanup.

Examples:

```
scallions
↓

green onion
```

```
extra virgin olive oil
↓

olive oil
```

```
boneless skinless chicken breast
↓

chicken breast
```

Normalization prepares text for identity lookup.

Normalization **does not**:

* assign ingredient identities
* collapse culinary distinctions
* group ingredients nutritionally

Vocabulary should eventually reside in configurable dictionaries rather than embedded regular expressions.

---

# Ingredient Identity

Ingredient Identity is the central domain model.

Identity answers:

> "What ingredient is this?"

Examples:

* chicken breast
* chicken thigh
* smoked paprika
* Hungarian paprika
* Spanish paprika

These remain distinct because cooks distinguish them.

Ingredient identities may represent:

* agricultural products
* processed foods
* recipe-derived ingredients

Examples:

* pizza dough
* simple syrup
* mirepoix
* pan sauce

Each ingredient may reference external identifiers but is never defined by them.

---

# External Enrichment

Ingredient identities may be enriched by external systems.

Examples:

* USDA
* FoodOn
* Wikipedia
* Flavor Bible

Example model:

```
Ingredient
    ingredient_id

External IDs

    USDA
    FoodOn
    Wikipedia
```

External identifiers are replaceable.

Gastrometric identifiers are permanent.

---

# USDA Mapping

USDA mapping is nutritional enrichment.

It is not canonicalization.

Pipeline:

```
Ingredient Identity
        ↓
USDA Entity
        ↓
Nutrition
```

USDA also provides examples of meaningful culinary states but does not define Gastrometric's ontology.

---

# Ingredient States

Ingredients possess meaningful culinary states.

Examples:

* raw broccoli
* roasted broccoli
* steamed broccoli
* riced cauliflower
* orange zest
* orange juice
* shredded cooked chicken

Statefulness is a first-class concept.

Meaningful states are determined by the culinary knowledge graph rather than arbitrary adjective+noun combinations.

---

# Transformations

Transformations connect meaningful ingredient states.

Example:

```
Raw Chicken Breast
        ↓
Roast
        ↓
Roasted Chicken Breast
```

Recipes provide evidence that transformations occurred.

Recipes do not define transformations.

---

# Recipes as Execution Plans

Recipes describe transformations between ingredient states.

Conceptually:

```
Known State 0

↓

Known State 1

↓

Known State 2

↓

Finished Dish
```

Eventually recipes become executable state graphs rather than ordered text instructions.

---

# Recipe Sections

Recipe sections are preserved exactly as authored.

Section names are presentation artifacts, not semantic truth.

Gastrometric should eventually infer reusable semantic components from recipe content.

Example:

A recipe may never contain the heading:

> Make the Pan Sauce

Gastrometric may infer that component from:

Inputs:

* shallots
* wine
* stock
* butter

Technique evidence:

* deglaze
* scrape fond
* reduce
* whisk in butter

The inferred semantic component becomes knowledge independent of the original section heading.

---

# Recipe Methods

Recipe methods are reusable culinary patterns.

Examples include:

* stir fry
* taco
* casserole
* soup
* stew
* curry
* pasta
* pilaf
* sandwich
* grain bowl
* quiche
* frittata

These are not recipe sections.

Recipes may instantiate one or more methods.

Recipe methods support:

* substitution
* modular cooking
* recipe composition
* recipes-as-ingredients

---

# Knowledge Graphs

## Ingredient Identity Graph

Contains:

* identities
* aliases
* parent relationships
* child relationships
* sibling relationships
* properties
* ingredient states
* external identifiers

---

## Culinary Relationship Graph

Contains:

* substitutions
* pairings
* cuisines
* techniques
* recipe methods
* functional roles
* preparation knowledge
* semantic recipe components

Sources include:

* Flavor Bible
* FoodSubs
* Serious Eats
* Modernist Cuisine
* Harold McGee
* Kenji López-Alt
* Wikipedia
* FoodOn
* future chef agents

---

## Recipe Knowledge Graph

Derived statistically from the recipe corpus.

Contains:

* ingredient frequencies
* ingredient communities
* co-occurrence statistics
* inferred substitutions
* inferred methods
* recipe structures

Recipes provide evidence rather than authority.

---

# Culinary Agents

| Agent                     | Responsibility                            | Inputs                                                      | Outputs                                                              |
| ------------------------- | ----------------------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------------- |
| Vocabulary Agent          | Maintain culinary language                | Recipes, user edits, scraped text                           | aliases, parser vocabulary, normalization dictionaries               |
| Ingredient Identity Agent | Maintain ingredient entities              | Vocabulary, FoodOn, USDA, Wikipedia                         | ingredient identities, relationships, metadata                       |
| Nutrition Agent           | Maintain nutrition mappings               | USDA                                                        | ingredient ↔ USDA mappings, confidence                               |
| Chef Agent                | Maintain culinary knowledge               | Flavor Bible, McGee, Modernist Cuisine, Serious Eats, Kenji | substitutions, pairings, techniques, transformations, recipe methods |
| Corpus Agent              | Learn statistically                       | Recipe corpus                                               | frequencies, ingredient communities, inferred cooking patterns       |
| Recipe Agent              | Convert recipes into structured knowledge | Markdown, scraped recipes, user recipes                     | parsed recipes, execution plans, inferred components                 |
| Review Agent              | Quality assurance                         | proposals from other agents                                 | approved, rejected, confidence-adjusted graph changes                |

---

# Current Architectural Decisions

* Ingredient Identity replaces canonicalization as the central domain model.
* Canonicalization is no longer a standalone pipeline stage.
* USDA mapping is enrichment, not ontology.
* Recipes are evidence rather than the primary knowledge representation.
* Ingredient states are first-class concepts.
* Recipes are modeled as transformations between ingredient states.
* Recipe sections are presentation artifacts.
* Semantic recipe components should be inferred.
* Recipe methods represent reusable cooking structures independent of recipe sections.

---

# Open Questions

The following remain intentionally unresolved.

## Ingredient States

What qualifies as a meaningful culinary state?

Potential evidence:

* USDA
* recipes
* grocery catalogs
* culinary literature

---

## State Representation

Should culinary states be represented as:

* graph entities
* attributes of ingredient identities
* dedicated state models

---

## Semantic Components

How should reusable recipe components be inferred?

Possible approaches:

* curated taxonomies
* statistical analysis
* LLM-assisted inference
* hybrid systems

---

## Knowledge Acquisition

How should new culinary knowledge enter the graph?

Possible sources include:

* curated references
* deterministic imports
* agent-assisted research
* human review

---

## Confidence and Provenance

Every inferred relationship, state, normalization, mapping, or transformation should eventually record:

* source
* confidence
* responsible agent
* review status

---

# Long-Term Vision

Gastrometric is evolving from a recipe database into a culinary knowledge system.

Recipes are evidence.

Knowledge graphs are the product.

The purpose of the system is to understand ingredients, cooking, and kitchen state well enough to generate useful plans for real cooks rather than simply retrieve stored recipes.