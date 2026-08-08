# ingredient_vocabulary.py
#
# Vocabulary for NORMALIZE-TIME surface-form cleanup, plus a layer of
# rules (flagged explicitly below) that are NOT normalization at all —
# they are identity decisions that were incorrectly encoded as text
# rules and have not yet been migrated to the identity-resolution stage.
#
# Pipeline position:
#
#   RAW RECIPE -> INGEST -> PARSE -> NORMALIZE -> IDENTITY RESOLUTION -> ...
#                                     ^^^^^^^^^     ^^^^^^^^^^^^^^^^^^
#                              this file, mostly     this file, partly
#                                                     (flagged, to be moved)
#
# THIS FILE IS NOT IMPORTED BY THE PARSER. Since the parser boundary
# refactor, the PARSE stage (parse_ingredient_lines.py) gets its
# vocabulary from two places, neither of which is this file:
#
#   parser_vocabulary.py              Permanent, parser-owned grammar:
#                                       noise phrases, closed-class
#                                       measurement units, truncation
#                                       fixes. Not culinary knowledge,
#                                       never sourced from the database.
#
#   culinary_vocabulary_bootstrap.py   Temporary static fallback for
#                                       PARSE-time culinary knowledge
#                                       (protected ingredient-name
#                                       phrases, preparation/state
#                                       vocabulary, portion terms),
#                                       consumed only through
#                                       vocabulary_provider.py, which is
#                                       what actually lets that data come
#                                       from `ingredients`/
#                                       `ingredient_aliases`/
#                                       `attribute_type`/`attribute_value`
#                                       instead once seeded.
#
# THIS file remains the source for the NORMALIZE stage
# (normalize_ingredient_lines.py) and is unaffected by that refactor —
# TYPO_FIXES, QUALIFIER_STRIP_PATTERNS, DRIED_DISTINCT,
# PROTECTED_PREP_PHRASES, and the plural/inflection rules below all still
# live here, because normalize-time ownership wasn't in scope for the
# parser boundary work. The re-exports below exist only so any existing
# `from ingredient_vocabulary import NOISE_PHRASES` (etc.) style import
# in the normalize stage keeps working; the canonical source for those
# specific names is parser_vocabulary.py.
#
# ---------------------------------------------------------------------
# IMPORTANT: identity decisions living in this file (not yet migrated)
# ---------------------------------------------------------------------
# A review found that several entries below don't just clean up surface
# form — they decide that two DIFFERENT culinary ingredients are the
# same thing, or replace one ingredient's name with another's. That is
# identity resolution, and it belongs in a future identity-resolution
# stage that consults the ingredient identity resource (ingredients.json)
# as its source of truth, not in a hardcoded text-substitution list here.
#
# Each such entry is marked inline with:
#     # IDENTITY DECISION — see file header
#
# Known examples (not exhaustive — this list is illustrative of the
# category, not a complete audit):
#   - TYPO_FIXES: "mahi mahi" -> "tuna"            (biologically wrong —
#         a direct symptom of doing identity work by regex instead of by
#         a real lookup against an identity resource)
#   - TYPO_FIXES: "pancetta" -> "bacon"
#   - TYPO_FIXES: "haddock|tilapia|sole" -> "whitefish"
#   - TYPO_FIXES: "schmaltz|drippings" -> "rendered fat"
#   - TYPO_FIXES: "v8" -> "tomato juice"
#   - TYPO_FIXES: "guacamole" -> "avocado"
#   - DRIED_DISTINCT (whole constant): "is dried-X a different ingredient
#         than X" is an identity/substitutability judgment, not a surface-
#         form rule.
#   - PROTECTED_PREP_PHRASES (whole constant): the mirror-image judgment
#         ("is ground-ginger a different ingredient than ginger").
#
# These are NOT being removed or fixed in this refactor — this pass is
# scoped to the PARSE stage only. They are flagged so:
#   (a) nothing new gets added to this category by accident, and
#   (b) the eventual normalize/identity-resolution refactor has a map of
#       what to migrate and where it should end up (identity resolution,
#       consulting ingredients.json's identities + aliases, NOT a static
#       list here).
#
# What normalization *should* be doing instead, for the genuinely-surface
# entries in TYPO_FIXES (spelling, punctuation, regional spelling of the
# SAME word — e.g. "chilli" -> "chili", "worchestershire" -> "worcestershire
# sauce"): that part is fine to keep as-is; it's real normalization.
#
# What "core ingredient" means for QUALIFIER_STRIP_PATTERNS
# -----------------------------------------------------------
#   "freshly ground black pepper"  ->  "black pepper"
#   "boneless skinless chicken breast"  ->  "chicken breast"
#   "extra-virgin olive oil"  ->  "olive oil"
#   "slivered almonds"  ->  "almonds"      <- cut descriptor stripped
#   "low-sodium chicken broth"  ->  "chicken broth"
#
# What normalization must NOT do:
#   "olive oil"  ->  "oil"          <- that is identity resolution (downstream)
#   "chicken breast"  ->  "chicken" <- same
#
# HOW TO EXTEND
# -------------
#   TYPO_FIXES          List of (compiled_re, replacement) pairs, for
#                       GENUINE spelling/punctuation variants only. If
#                       the rule would make two different ingredients
#                       resolve to the same name, it does not belong
#                       here — flag it and route it to identity
#                       resolution instead.
#
#   QUALIFIER_STRIP_PATTERNS
#                       List of compiled regexes stripped from the name
#                       to expose the core ingredient. Applied in order;
#                       whitespace is collapsed after each. Only for
#                       descriptors that are identity-neutral in all
#                       cases (fresh/frozen/organic/boneless/...).

import re

from gastrometric.config.parser_vocabulary import (  # noqa: F401  (re-exported for compatibility)
    NOISE_PHRASES,
    GRAM_UNITS,
    ML_UNITS,
    IMPERIAL_WEIGHT_UNITS,
    IMPERIAL_VOLUME_UNITS,

)
# from gastrometric.config.culinary_vocabulary_bootstrap import (  # noqa: F401
#     PROTECTED_PHRASES,
#     PREP_PATTERNS,
#     TEMPERATURE_STATE_PATTERNS,
#     PORTION_TERMS,
# )

# ============================================================
# NORMALIZE-TIME VOCABULARY
# ============================================================

# -----------------------------------------------------------
# PASS 1 — TYPO FIXES
#
# Purpose: unify spelling variants, regional synonyms, and brand
# names to a standard English form BEFORE qualifier stripping.
# These preserve the ingredient's identity — they only fix the surface form.
#
# Applied to lowercased ingredient_name_raw in order.
# Each rule is (compiled_pattern, replacement_string).
#
# HOW TO ADD: append (re.compile(r'...', re.I), "standard form") tuples.
# Keep replacements as the most common English name for the ingredient.
# -----------------------------------------------------------

TYPO_FIXES = [
    # punctuation / spelling
    (re.compile(r'\bworchestershire\b|\bworcester sauce\b', re.I), "worcestershire sauce"),
    (re.compile(r'\bscalion\b',           re.I), "scallion"),
    (re.compile(r'\bchilli\b',            re.I), "chili"),
    # regional synonyms → standard EN
    (re.compile(r'\bcoriander leaves?\b', re.I), "cilantro"),
    (re.compile(r'\bspring onions?\b',    re.I), "green onion"),
    (re.compile(r'\bscallions?\b',        re.I), "green onion"),
    (re.compile(r'\byams?\b',             re.I), "sweet potato"),
    (re.compile(r'\bprawns?\b',           re.I), "shrimp"),
    (re.compile(r'\bcalamari\b',          re.I), "squid"),
    (re.compile(r'\bhaddock\b|\btilapia\b|\bsole\b', re.I), "whitefish"),  # IDENTITY DECISION — see file header
    (re.compile(r'\bschmaltz?\b|\bdrippings\b', re.I), "rendered fat"),  # IDENTITY DECISION — see file header
    # brand / trade names → generic
    (re.compile(r'\btabasco sauce\b',           re.I), "hot sauce"),
    (re.compile(r'\btabasco\b',           re.I), "hot sauce"),
    (re.compile(r'\bgrey poupon\b',       re.I), "dijon mustard"),
    (re.compile(r'\bv8\b',               re.I), "tomato juice"),  # IDENTITY DECISION — see file header
    (re.compile(r'\bpanko\b',            re.I), "bread crumbs"),
    # alternate spellings of flour types
    (re.compile(r'\bap flour\b',          re.I), "all-purpose flour"),
    (re.compile(r'\b00 flour\b',          re.I), "bread flour"),
    (re.compile(r'\bpizza flour\b',       re.I), "bread flour"),
    # sugar synonyms
    (re.compile(r'\bicing sugar\b',       re.I), "powdered sugar"),
    (re.compile(r'\bconfectioners\'?\s+sugar\b', re.I), "powdered sugar"),
    (re.compile(r'\bgranulated sugar\b',  re.I), "sugar"),
    (re.compile(r'\bwhite sugar\b',       re.I), "sugar"),
    # cheese
    (re.compile(r'\bparmigiano[- ]reggiano\b|\bparmigiano\b|\bpecorino\b', re.I), "parmesan"),
    # wine
    (re.compile(r'\bxiaoxing\b',          re.I), "shaoxing"),
    # chocolate
    (re.compile(r'\bchocolate morsels?\b|\bchocolate pieces?\b'
                r'|\bchocolate chunks?\b|\bchocolate drops?\b', re.I), "chocolate chips"),
    (re.compile(r'\bsemi-?sweet chocolate\b|\bbittersweet chocolate\b', re.I), "chocolate"),
    # misc
    (re.compile(r'\bguacamole\b',         re.I), "avocado"),  # IDENTITY DECISION — see file header
    (re.compile(r'\bbrusselss?\b(?!\s+sprouts?)', re.I), "brussels sprouts"),
    # (?:s)? on "mushroom" absorbs an already-present "mushroom"/"mushrooms"
    # right after the variety name, so "porcini mushrooms" doesn't become
    # "porcini mushroom mushrooms".
    (re.compile(r'\bporcini\b(?:\s+mushrooms?)?',    re.I), "porcini mushroom"),
    (re.compile(r'\bportobello\b(?:\s+mushrooms?)?', re.I), "portobello mushroom"),
    (re.compile(r'\bshiitake\b(?:\s+mushrooms?)?',   re.I), "shiitake mushroom"),
    (re.compile(r'\bcrimini\b(?:\s+mushrooms?)?',    re.I), "crimini mushroom"),
    (re.compile(r'\bmahi[- ]?mahi\b|\bmahi\b', re.I), "tuna"),  # IDENTITY DECISION — see file header
    (re.compile(r'\bpancetta\b',          re.I), "bacon"),  # IDENTITY DECISION — see file header
    (re.compile(r'\bbasil pesto\b',       re.I), "pesto"),
    (re.compile(r'\bporkchop\b|\bloin chop\b', re.I), "pork chop"),
    (re.compile(r'\bleg quarter\b|\bdrumstick\b', re.I), "chicken leg"),
    (re.compile(r'\bchuck roast\b|\brib roast\b', re.I), "beef roast"),
    (re.compile(r'\bkettle[- ]style chips\b|\bkettle chips\b', re.I), "potato chips"),
    (re.compile(r'\bgingersnap\b|\bgrahams?\b(?!\s+cracker)', re.I), "cookies"),
    (re.compile(r'\brice krispies?\b|\brice crisp(?:ies)?\b', re.I), "rice cereal"),
]


# -----------------------------------------------------------
# PASS 2 — QUALIFIER STRIP PATTERNS
#
# Purpose: strip qualifying words that surround the core ingredient
# so the name is clean enough for ingredient_id matching.
#
# These remove descriptors of HOW the ingredient is prepared, its
# freshness, size, cut style, or diet classification — NOT what it is.
#
# Applied in ORDER to the lowercased, typo-fixed name.
# Each pattern is stripped globally; whitespace is collapsed after each.
#
# Rules are narrow by design.  When in doubt, do NOT strip — it is
# better to leave a qualifier in than to destroy a meaningful name.
# Canonicalization (the downstream step) handles semantic collapsing.
#
# HOW TO ADD: append re.compile(r'\bword\b', re.I) entries.
# -----------------------------------------------------------

QUALIFIER_STRIP_PATTERNS = [

    # --- freshness / temperature state (already in prep col, strip from name) ---
    re.compile(r'\bfreshly\b',              re.I),
    re.compile(r'\bfresh\b',                re.I),
    re.compile(r'\bdried\b',                re.I),
    re.compile(r'\bfrozen\b',               re.I),
    re.compile(r'\bchilled\b',              re.I),
    re.compile(r'\bcooked\b',              re.I),
    re.compile(r'\bdry\b',              re.I),
    re.compile(r'\bthawed\b',               re.I),
    re.compile(r'\broom[- ]temperature\b',  re.I),

    # --- cut / form descriptors (already captured in prep col) ---
    re.compile(r'\bwhole\b',                re.I),
    re.compile(r'\bslivered\b',             re.I),
    re.compile(r'\bsliced\b',               re.I),
    re.compile(r'\bchopped\b',              re.I),
    re.compile(r'\bminced\b',               re.I),
    re.compile(r'\bdiced\b',                re.I),
    re.compile(r'\bgrated\b',               re.I),
    re.compile(r'\bshredded\b',             re.I),
    re.compile(r'\bground\b(?!\s+(?:beef|pork|turkey|chicken|lamb|'
               r'pepper|coriander|cumin|ginger|cinnamon|nutmeg|'
               r'allspice|cloves|cardamom|turmeric|paprika|mustard|fennel))',
               re.I),    # strip "ground" only when NOT part of a protected compound
    re.compile(r'\bcubed\b',                re.I),
    re.compile(r'\bhalved\b',               re.I),
    re.compile(r'\bquartered\b',            re.I),
    re.compile(r'\bcrumbled\b',             re.I),
    re.compile(r'\bpowdered\b(?!\s+sugar)', re.I),   # "powdered sugar" is the ingredient
    re.compile(r'\bflaked\b',               re.I),

    # --- size / grade (standalone only — not inside compound names) ---
    re.compile(r'\bextra[- ]large\b',       re.I),
    re.compile(r'\blarge\b',                re.I),
    re.compile(r'\bmedium\b',               re.I),
    re.compile(r'\bsmall\b',                re.I),
    re.compile(r'\bjumbo\b',                re.I),
    re.compile(r'\bbaby\b',                 re.I),

    # --- cut / animal prep ---
    re.compile(r'\bboneless\b',             re.I),
    re.compile(r'\bskinless\b',             re.I),
    re.compile(r'\bbone[- ]in\b',           re.I),
    re.compile(r'\bskin[- ]on\b',           re.I),

    # --- fat / sodium / diet qualifiers ---
    re.compile(r'\blow[- ]fat\b',           re.I),
    re.compile(r'\breduced[- ]fat\b',       re.I),
    re.compile(r'\bfull[- ]fat\b',          re.I),
    re.compile(r'\blow[- ]sodium\b',        re.I),
    re.compile(r'\breduced[- ]sodium\b',    re.I),
    re.compile(r'\bunsalted\b',             re.I),
    re.compile(r'\blightly salted\b',       re.I),
    re.compile(r'\bsalted\b',              re.I),

    # --- origin / provenance ---
    re.compile(r'\bhomemade\b',             re.I),
    re.compile(r'\bstore[- ]bought\b',      re.I),
    re.compile(r'\bcommercial\b',           re.I),
    re.compile(r'\borganic\b',              re.I),
    re.compile(r'\bfarm[- ]fresh\b',        re.I),

    # --- purity / refinement ---
    re.compile(r'\bextra[- ]virgin\b',      re.I),
    re.compile(r'\bvirgin\b',              re.I),
    re.compile(r'\bunrefined\b',            re.I),
    re.compile(r'\brefined\b',              re.I),
    re.compile(r'\bpure\b',                re.I),
    re.compile(r'\braw\b',                 re.I),

    # --- trailing source phrases ("juice of 2 lemons" → "lemon juice") ---
    # handled in normalize_ingredient_lines.py as a special case
]

# ---------------------------------------------------------------------------
# ADDITIONS TO ingredient_vocabulary.py
# ---------------------------------------------------------------------------
# Add these constants to the NORMALIZE-TIME section.
# Imported by normalize_flavor_bible.py.
# ---------------------------------------------------------------------------


# -----------------------------------------------------------
# NON_INGREDIENT_HINTS
# If any of these substrings appear in a flavor bible target entry,
# the row is a cuisine/technique/dish description — not an ingredient.
# -----------------------------------------------------------

NON_INGREDIENT_HINTS = [
    "cuisine", "dishes", "foods", "course", "menu",
    "technique", "cooking", "preparation",
    "flavor", "taste", "temperature", "season",
]


# -----------------------------------------------------------
# PROTECTED_PREP_PHRASES
#
# Atomic ingredient names that must survive normalization unchanged.
# Checked AFTER comma resolution (since the flavor bible stores
# "ginger, ground" not "ground ginger") but BEFORE any word stripping.
#
# Covers two kinds of protection:
#   1. Modifier is identity-bearing for this specific ingredient
#      ("ground ginger" is a different product from fresh ginger root)
#   2. First word looks like a texture/state word but is part of the name
#      ("sour cream", "sweet potato", "sweet pepper")
#
# Sorted longest-first at import time so more-specific phrases shadow
# shorter ones sharing a keyword.
# -----------------------------------------------------------

# IDENTITY DECISION — see file header. This entire constant is a
# same-vs-different-ingredient judgment (e.g. "is ground ginger a
# different ingredient than ginger?") and belongs in identity
# resolution, consulting ingredients.json's identities, not here.
PROTECTED_PREP_PHRASES = [
    # dairy — "sour" and "sweet" are part of the name, not descriptors
    "sour cream",
    "sweet butter",
    # vegetables where the qualifier is the variety, not a descriptor
    "sweet potato",
    "sweet pepper",
    "sweet onion",
    "sweet corn",
    "sweet pea",
    "wild mushroom",
    "wild rice",
    "wild boar",
    "sour cherry",                 # distinct from sweet/dark cherries
    # ginger forms — completely different products
    "ground ginger",
    "fresh ginger",
    "candied ginger",
    # pepper — ground forms are distinct from whole
    "ground black pepper",
    "ground white pepper",
    "ground pepper",
    # smoked — distinct products, not "ingredient + technique note"
    "smoked paprika",
    "smoked salmon",
    "smoked trout",
    "smoked mackerel",
    "smoked haddock",
    "smoked ham",
    "smoked bacon",
    "smoked sausage",
    # preserved — the preservation IS the ingredient
    "preserved lemon",
    "preserved lime",
    # candied
    "candied lemon peel",
    "candied orange peel",
    # dried chiles — distinct from fresh
    "dried chiles",
    "dried chile",
    # ground spices — "ground" is identity-bearing (dried + milled ≠ whole)
    "ground coriander",
    "ground cumin",
    "ground cinnamon",
    "ground nutmeg",
    "ground allspice",
    "ground cloves",
    "ground cardamom",
    "ground turmeric",
    "ground paprika",
    "ground mustard",
    "ground fennel",
    # ground meats
    "ground beef",
    "ground pork",
    "ground turkey",
    "ground chicken",
    "ground lamb",
]


# -----------------------------------------------------------
# DRIED_DISTINCT
#
# Base ingredient names (singular) for which "dried X" is a
# genuinely separate ingredient from the fresh form.
#
# Rule of thumb: would a recipe that calls for the fresh form work
# if you substituted the dried form without adjustment? If no, it
# belongs here.
#
# Herbs and spices are deliberately absent — "dried thyme" and
# "thyme" are the same ingredient with a prep note.
# Smoked/candied/preserved forms use PROTECTED_PREP_PHRASES instead
# because those words are technique-bearing, not just state descriptors.
#
# Stored as singular forms; the normalization step singularizes
# before checking, so plurals are covered automatically.
# -----------------------------------------------------------

# IDENTITY DECISION — see file header. Whether "dried X" is a distinct
# ingredient from "X" is a substitutability/identity judgment, not a
# surface-form rule. Belongs in identity resolution, consulting
# ingredients.json's identities, not here.
DRIED_DISTINCT = {
    # stone fruits and berries — dried form is a categorically different product
    "apricot",
    "fig",
    "plum",          # dried plum = prune; flavor bible may use either
    "cherry",
    "cranberry",
    "blueberry",
    "mango",
    "pineapple",
    "date",
    "raisin",        # always dried, but may appear as "dried raisin"
    "currant",
    # ginger — "dried ginger" (not ground) is still distinct from fresh root
    "ginger",
    # mushrooms — dried shiitake / porcini behave differently from fresh
    "mushroom",
}


# -----------------------------------------------------------
# LEADING_CATEGORY_REVERSALS
# Left side of a "category, specific" comma phrase where the specific
# is appended AFTER the category to form natural English.
# "vinegar, balsamic" → "balsamic vinegar"
#
# Note: "onions" is intentionally NOT here. "onions, sweet" should
# become "sweet onions" (a taxonomy variety), not be reversed to
# "sweet onions" via reversal logic — it gets there via COLOR_QUALIFIERS
# being extended to cover variety qualifiers. See VARIETY_QUALIFIERS.
# -----------------------------------------------------------

LEADING_CATEGORY_REVERSALS = {
    "bell pepper",
    "basil",
    "vinegar",
    "wine",
    "cheese",
    "oil",
    "chile peppers",
    "tomato",
    "sausage",
    "liqueur",
    "sauce",
    "pepper",
    "mushroom",
    "mustard",
    "stock",
    "broth",
}


# -----------------------------------------------------------
# PLURAL_CATEGORIES
# Left side of a "category, specific" comma phrase where the right
# side IS the ingredient and the left is only the grouping label.
# "berries, strawberry" → "strawberry"
# -----------------------------------------------------------

PLURAL_CATEGORIES = {
    "berries",
    "nuts",
    "seeds",
    "fruits",
    "spices",
    "greens",
    "herbs",
    "fish",
    "meat",
    "lettuce",
}


# -----------------------------------------------------------
# VARIETY_QUALIFIERS
# When these appear as the RIGHT side of a comma phrase, they are
# prepended to form the full variety name.
# Extends the same mechanic as COLOR_QUALIFIERS.
#
# "pepper, black"      → "black pepper"      (color)
# "onions, sweet"      → "sweet onions"      (variety)
# "mushrooms, wild"    → "wild mushrooms"    (variety/taxonomy)
# "onions, caramelized"→ "caramelized onions" (preparation — kept, not stripped,
#                         because this is a prepared ingredient with distinct use)
# -----------------------------------------------------------

COLOR_QUALIFIERS = {
    "black", "red", "green", "yellow", "white",
    "brown", "light", "heavy", "dark", "pink", "purple",
}

VARIETY_QUALIFIERS = {
    # taste-based variety names that are part of the ingredient name
    "sweet", "sour", "bitter", "hot", "mild",
    # origin/character varieties
    "wild", "cultivated",
    # preparation-as-variety (these are distinct culinary ingredients)
    "caramelized", "roasted", "fried",
    # size/age varieties
    "baby", "young",
}

# Combined set used by _resolve_comma_phrase
COMMA_RIGHT_QUALIFIERS = COLOR_QUALIFIERS | VARIETY_QUALIFIERS


# -----------------------------------------------------------
# STATE_ONLY_PREP_WORDS
#
# Stripped from the ingredient name AND recorded in `preparation`.
# Only words where stripping NEVER changes the identity of the ingredient.
#
# Deliberately narrow. When in doubt, leave the word in the name —
# canonicalization has more context to decide.
#
# NOT included:
#   "dried"     — context-dependent; see DRIED_DISTINCT
#   "preserved" — usually identity-bearing; see PROTECTED_PREP_PHRASES
#   "pickled"   — pickled X ≠ X in most culinary contexts
#   "smoked"    — graph edge; see PROTECTED_PREP_PHRASES
#   "sweet"     — variety qualifier or part of name; see VARIETY_QUALIFIERS
#   "sour"      — part of name (sour cream); see PROTECTED_PREP_PHRASES
#   "wild"      — taxonomy category; see VARIETY_QUALIFIERS
#   "aged"      — often identity-bearing (aged cheddar, aged balsamic)
# -----------------------------------------------------------

STATE_ONLY_PREP_WORDS = {
    "fresh",    # "fresh thyme" → "thyme"; "fresh ginger" is protected above
    "raw",      # "raw honey" → "honey"
    "grilled",  # "grilled chicken" → "chicken"
    "frozen",   # "frozen peas" → "peas"
    "canned",   # "canned tomatoes" → "tomatoes"
    "bottled",
}


# -----------------------------------------------------------
# PLURAL_IRREGULAR
# Irregular food-specific plural → singular mappings applied
# before the suffix-rule singularizer.
# Only covers cases the suffix rules would get wrong.
# -----------------------------------------------------------

PLURAL_IRREGULAR = {
    "leaves":   "leaf",        # but "brussels sprouts leaves" → keep? flag for review
    "halves":   "half",
    "knives":   "knife",       # unlikely but present in some entries
    "loaves":   "loaf",
}


# -----------------------------------------------------------
# PLURAL_SUFFIX_RULES
# Ordered list of (suffix_to_strip, replacement) tuples.
# Applied only when no PLURAL_IRREGULAR match exists.
# More-specific rules (longer suffixes) must come first.
#
# Each rule is applied only if the resulting stem is >= 3 chars
# to avoid over-stripping short words.
# -----------------------------------------------------------

PLURAL_SUFFIX_RULES = [
    ("ches",  "ch"),    # "peaches" → "peach"  (before -es rule)
    ("shes",  "sh"),    # "radishes" → "radish"
    ("xes",   "x"),     # "boxes" → "box"  (unlikely in food)
    ("ies",   "y"),     # "berries" → "berry", "cherries" → "cherry"
    ("ves",   "f"),     # "loaves" → "loaf"  (backup; irregular covers most)
    ("es",    ""),      # "tomatoes" → "tomato", "peaches" already handled
    ("s",     ""),      # "apricots" → "apricot", "onions" → "onion"
]

# Words that look plural but are not — never singularize these.
PLURAL_EXCEPTIONS = {
    "asparagus", "hummus", "couscous", "quinoa", "falafel",
    "molasses", "oats", "grits", "greens", "bitters",
    "lemongrass", "watercress", "endives", "vegetable", "apples",
    # these end in -s but are already singular
    "anise", "chives", "cloves", "dates", "limes", "olives",
    "grapes", "capers", "truffles", "noodles", "sprouts",
}


# -----------------------------------------------------------
# PREP_INFLECTIONS
# Inflected forms of words KEPT in the name → base form.
# Applied as a final token-level pass.
#
# "lime, juiced" → comma resolve → "lime juiced"
#               → inflection    → "lime juice"
# -----------------------------------------------------------

PREP_INFLECTIONS = {
    "juiced":   "juice",
    "zested":   "zest",
    "powdered": "powder",
    "pureed":   "puree",
}