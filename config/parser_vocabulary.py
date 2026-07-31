# parser_vocabulary.py
#
# Vocabulary OWNED BY THE PARSING STAGE, PERMANENTLY. Everything here is
# grammar/tokenization, not culinary knowledge, and is never sourced from
# the database.
#
# Pipeline position:
#
#   RAW RECIPE -> INGEST -> PARSE -> NORMALIZE -> IDENTITY RESOLUTION -> ...
#                            ^^^^^
#                       this file lives here
#
# As of the CulinaryVocabulary migration, ALL culinary word lists
# (measurement, packaging, natural_portion, preparation, ingredient_form,
# size, descriptor, shape, state, temperature, modifier, seasoning,
# brand) have been removed from this file and from
# parse_ingredient_lines.py entirely. They come exclusively from
# gastrometric.knowledge.loader.CulinaryVocabulary at runtime, via its
# public per-category accessor methods (measurements(), packaging(),
# state(), ...), reading the
# `culinary_vocabulary`/`culinary_aliases` tables. Nothing in this file
# duplicates that data, even temporarily.
#
# WHAT REMAINS HERE
# -------------------
#   NOISE_PHRASES              Hedge phrases erased outright ("as
#                               needed", "to taste"). Not ingredient-
#                               specific, not culinary data - sentence
#                               filler.
#
#   CUT_TEMPLATE_PATTERNS      Regex TEMPLATES for cut/slice/dice
#                               phrasing ("cut into 1/2-inch {shapes}",
#                               "chopped into large {shapes}", ...). These
#                               are sentence structure - verb, direction,
#                               dimension, preposition - not vocabulary.
#                               The {shapes} placeholder is filled in at
#                               CompiledVocabulary build time from
#                               self.by_class["shape"] (parse_ingredient_lines.py's
#                               CompiledVocabulary, bridged from
#                               CulinaryVocabulary's accessors via
#                               _fetch_vocab_class). The
#                               template itself never changes based on
#                               what's in the database.
#
#   GRAMMAR_ONLY_PREP_PATTERNS  Cut-related regex patterns that don't
#                               involve any vocabulary word list at all -
#                               open captures ("cut crosswise into
#                               [^,;]+") and numeric patterns ("cut in
#                               \d+ pieces"). Pure grammar, no
#                               {shapes}/{sizes} substitution needed or
#                               possible.
#
#   GRAM_UNITS / ML_UNITS /
#   IMPERIAL_WEIGHT_UNITS /
#   IMPERIAL_VOLUME_UNITS       Unit ROUTING classification, not
#                               vocabulary recognition. See the comment
#                               above these sets, below.
#
# WHAT USED TO BE HERE AND WHY IT'S GONE
# ----------------------------------------
#   TRUNCATION_FIXES has been deleted. It was a hardcoded
#   pattern -> replacement dictionary for OCR/copy-paste corruption
#   repair ("oi" -> "oil", "chiv" -> "chive", ...), and several of its
#   entries were plain culinary nouns, not grammar - exactly the kind of
#   thing this migration exists to get out of Python. It has been
#   replaced by a runtime PREFIX-MATCHING ALGORITHM in
#   parse_ingredient_lines.py: an unrecognized token is compared against
#   the full set of runtime vocabulary words (every CulinaryVocabulary
#   class plus the ingredient-identity vocabulary from
#   VocabularyProvider); if exactly one canonical word begins with that
#   token, the token is expanded to it, otherwise it is left alone. The
#   algorithm itself is grammar (it's a general string-repair procedure,
#   not a list of culinary facts) and stays in Python; the word list it
#   searches against is 100% runtime-sourced. See
#   `_expand_truncated_tokens` / `_expand_truncated_word` in
#   parse_ingredient_lines.py.

import re


# ============================================================
# PARSE-TIME VOCABULARY (permanent)
# ============================================================

# -----------------------------------------------------------
# NOISE PHRASES
# Removed entirely from ingredient text (not moved to prep).
# -----------------------------------------------------------

NOISE_PHRASES = [
    "plus more", "as needed", "to taste", "if desired",
    "as desired", "optional", "if needed", "add more"
]


# -----------------------------------------------------------
# UNIT ROUTING CLASSIFICATION
#
# NOT vocabulary recognition - that question ("is this string a
# measurement word at all?") is answered by CulinaryVocabulary's
# measurements()/packaging()/natural_portions() accessors now, and can grow via
# synonyms/aliases without a code change.
#
# This is a different, narrower question: for a unit ALREADY recognized
# as a measurement, which physical system/quantity-kind does it belong
# to (metric weight vs metric volume vs imperial weight vs imperial
# volume), and what's its fixed conversion factor (a kilogram is always
# 1000 grams)? That's a closed, permanent fact about the metric/imperial
# systems themselves - it doesn't vary with culinary knowledge, doesn't
# grow as new ingredients or synonyms are added, and every value here
# will always be true regardless of what's seeded in
# culinary_vocabulary. Kept in Python for that reason; see _route_unit.
# -----------------------------------------------------------

GRAM_UNITS = frozenset({
    'g', 'kg', 'gram', 'grams', 'kilogram', 'kilograms',
})
ML_UNITS = frozenset({
    'ml', 'mls', 'milliliter', 'milliliters', 'millilitre', 'millilitres',
    'liter', 'liters', 'litre', 'litres', 'l',
})
IMPERIAL_WEIGHT_UNITS = frozenset({
    'oz', 'ounce', 'ounces', 'lb', 'pound', 'pounds',
})
IMPERIAL_VOLUME_UNITS = frozenset({
    'cup', 'cups',
    'tbsp', 'tablespoon', 'tablespoons',
    'tsp', 'teaspoon', 'teaspoons',
    'pint', 'pints',
    'quart', 'quarts', 'qt',
    'gallon', 'gallons',
    'fl oz', 'fluid ounce', 'fluid ounces',
})


# -----------------------------------------------------------
# CUT-PATTERN GRAMMAR
# Structural templates for cutting/slicing phrases. The {shapes}
# placeholder is filled in by CompiledVocabulary from
# self.by_class["shape"] - see parse_ingredient_lines.py.
# Order matters: dimensioned templates before the bare "cut into
# {shapes}" template, since the latter is a strict subset of the
# former's phrasing and must not shadow it in the combined alternation.
# -----------------------------------------------------------

CUT_TEMPLATE_PATTERNS = [
    # "cut/sliced/chopped/torn (crosswise|lengthwise)? in/into <dimension>
    # -inch (or ") <shapes>" - e.g. "cut into 1/2-inch dice",
    # "torn into 1/2" pieces", with an optional "N by " cross-dimension
    # prefix (e.g. "1 by 1/8-inch strips").
    r'(?:cut|sliced|chopped|torn)\s+(?:crosswise\s+|lengthwise\s+)?'
    r'(?:in|into)\s+(?:\d[\d./]*\s*(?:x|by)\s*)?\d[\d./]*(?:-inch|")[a-z-]*\s+'
    r'(?:{shapes})',
    # Same, without an inch marker at all - "cut into 6 wedges".
    r'(?:cut|sliced|chopped|torn)\s+(?:crosswise\s+|lengthwise\s+)?'
    r'(?:in|into)\s+\d[\d./]*\s+'
    r'(?:{shapes})',
    # No dimension, optional "bite-sized"/"large"/"small" modifier -
    # "cut into pieces", "chopped into large chunks".
    r'(?:cut|sliced|chopped|torn)\s+into\s+'
    r'(?:bite[- ]sized\s+|large\s+|small\s+)?(?:{shapes})',
]


# -----------------------------------------------------------
# CUT-PATTERN GRAMMAR (no vocabulary substitution)
# Open-capture / numeric cut phrasing that has no vocabulary word list to
# substitute - kept verbatim as parser grammar.
# -----------------------------------------------------------

GRAMMAR_ONLY_PREP_PATTERNS = [
    r'cut in half',
    r'cut in \d+ pieces',
    r'cut in \d+',
    r'cut crosswise into [^,;]+',
    r'sliced crosswise into [^,;]+',
    r'crosswise into [^,;]+',
    r'cut lengthwise into [^,;]+',
    r'sliced lengthwise into [^,;]+',
    r'lengthwise into [^,;]+',
]