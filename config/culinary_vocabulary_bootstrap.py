# culinary_vocabulary_bootstrap.py
#
# TEMPORARY COMPATIBILITY DATA ONLY. This module is not parser vocabulary.
#
# It holds the same culinary knowledge that used to be hardcoded directly
# in the parser (ingredient names/aliases, and preparation/state
# vocabulary) as a STATIC FALLBACK, so parsing keeps working exactly as
# it does today while db/init_db.py, the ingredients/ingredient_aliases
# tables, and the attribute_type/attribute_value tables are the actual
# source of truth going forward.
#
# Ownership:
#   - Ingredient names + aliases  -> `ingredients`, `ingredient_aliases`
#   - Preparation / state / size / bone / skin / salt / sodium-level
#     vocabulary                  -> `attribute_type` + `attribute_value`
#   - Ingredient-specific portion/count terms (clove, floweret, spear,
#     medallion, ...) -> a future USDA-ingestion-populated table (does
#     not exist yet at the time of writing)
#
# This module is consumed ONLY by StaticVocabularyProvider (see
# vocabulary_provider.py), which DatabaseVocabularyProvider falls back to
# per-category when its query returns nothing (e.g. the relevant table is
# still unseeded). The parsing ALGORITHM never imports this module or
# ingredients.json-shaped data directly — it only ever sees whatever
# VocabularyProvider.protected_phrases()/prep_patterns()/etc. return.
#
# When ingredients/ingredient_aliases and attribute_type/attribute_value
# are fully seeded, this module's content becomes redundant and can be
# deleted outright — nothing structural depends on it existing, by
# design.

import re


# ============================================================
# BOOTSTRAP: ingredient names/aliases
# Mirrors what `ingredients.ingredient_name` + `ingredient_aliases.alias`
# will eventually provide. Only multi-word entries matter here — the
# purpose is preventing one of the words inside a known ingredient name
# from being mis-extracted as a prep verb or unit (e.g. the "ground" in
# "ground beef"), not identifying ingredients.
# ============================================================

PROTECTED_PHRASES = [
    # dairy
    "half-and-half", "half and half", "half & half",
    "heavy cream", "whipping cream",
    "cream of tartar", "cream of wheat", "cream of mushroom soup",
    # flour
    "all purpose flour", "all-purpose flour", "bread flour", "cake flour",
    "whole wheat flour", "self rising flour", "self-rising flour",
    # pepper compounds — longest first so "freshly ground black pepper" beats "black pepper"
    "freshly ground black pepper", "freshly ground pepper",
    "ground black pepper", "ground white pepper",
    "crushed red pepper flakes", "crushed red pepper", "red pepper flakes",
    # ground meats
    "ground beef", "ground pork", "ground turkey",
    "ground chicken", "ground lamb", "pork ribs", "beef ribs", "baby back ribs", 
    "spare ribs", "short ribs", "style ribs",
    # ground spices
    "ground coriander", "ground cumin", "ground ginger", "ground cinnamon",
    "ground nutmeg", "ground allspice", "ground cloves", "ground cardamom",
    "ground turmeric", "ground paprika", "ground mustard", "ground fennel",
    # dried herbs
    "dried thyme", "dried oregano", "dried basil", "dried rosemary",
    "dried sage", "dried parsley", "dried dill", "dried mint", "dried chili",
    "dried rubbed sage",
    # oils (protect "extra virgin" from being parsed as size + adj)
    "extra virgin olive oil", "extra-virgin olive oil",
    "olive oil", "vegetable oil", "canola oil",
    "sesame oil", "coconut oil",
    # sugars
    "brown sugar", "white sugar", "granulated sugar",
    "powdered sugar", "confectioners sugar", "confectioners' sugar",
    # sauces
    "soy sauce", "fish sauce", "hot sauce", "worcestershire sauce",
    # cheeses
    "parmesan cheese", "cheddar cheese", "mozzarella cheese",
    # onions
    "green onions", "spring onions",
    "red onion", "yellow onion", "white onion",
    # tomatoes
    "crushed tomatoes", "diced tomatoes", "tomato paste", "tomato sauce",
    # water compounds (protect "boiling" from being extracted as a state adj)
    "boiling water", "cold water", "ice water", "warm water",
    # other
    "spanish chorizo",
]


# ============================================================
# BOOTSTRAP: preparation / state vocabulary
# Mirrors what `attribute_value.value` (joined through `attribute_type`
# for the preparation/state/temperature attribute types) will eventually
# provide. Stored here as ready-to-use regex fragments (spaces, not the
# underscore-separated form attribute_value.value actually uses in the
# DB) purely because that's the fastest-to-write bootstrap format; the
# DB-backed path builds equivalent patterns from `value` at runtime — see
# vocabulary_provider.attribute_value_to_pattern().
# ============================================================

PREP_PATTERNS = [
    # --- multi-word cut patterns ---
    r'chopped into large chunks',
    r'chopped into small chunks',
    r'chopped into bite[- ]sized chunks',
    r'chopped into bite[- ]sized pieces',
    r'chopped into chunks',
    r'chopped into pieces',
    # --- generalized "<verb> in/into <size>-inch <shape>" cut patterns ---
    # Covers cut/sliced/chopped/torn, "in" or "into", an optional "N by "
    # cross-dimension prefix (e.g. "1 by 1/8-inch"), and any of the common
    # trailing shape nouns (singular or plural). One flexible family here
    # replaces what used to be a dozen near-duplicate, easy-to-miss entries
    # (e.g. no "dice"/"cubes"/"rings"/"wedges" variant existed before,
    # which left the "-inch" token stranded in ingredient_name_raw).
    r'(?:cut|sliced|chopped|torn)\s+(?:crosswise\s+|lengthwise\s+)?'
    r'(?:in|into)\s+(?:\d[\d./]*\s*(?:x|by)\s*)?\d[\d./]*-inch[a-z-]*\s+'
    r'(?:rounds?|pieces?|chunks?|strips?|cubes?|dice|wedges?|'
    r'matchsticks?|batons?|slices?|rings?)',
    r'(?:cut|sliced|chopped|torn)\s+(?:crosswise\s+|lengthwise\s+)?'
    r'(?:in|into)\s+(?:\d[\d./]*\s*(?:x|by)\s*)?\d[\d./]*"[a-z-]*\s+'
    r'(?:rounds?|pieces?|chunks?|strips?|cubes?|dice|wedges?|'
    r'matchsticks?|batons?|slices?|rings?)',
    r'very thinly sliced',
    r'thinly sliced',
    r'roughly chopped',
    r'finely chopped',
    r'coarsely chopped',
    r'chopped fine',
    r'sliced thinly',
    r'sliced thin',
    r'finely sliced',
    r'finely minced',
    r'diced fine',
    r'(?:cut|sliced|chopped|torn)\s+(?:crosswise\s+|lengthwise\s+)?'
    r'(?:in|into)\s+\d[\d./]*\s+'
    r'(?:rounds?|pieces?|chunks?|strips?|cubes?|dice|wedges?|'
    r'matchsticks?|batons?|slices?|rings?)',
    r'cut into pieces',
    r'cut into chunks',
    r'cut into strips',
    r'cut into rounds',
    r'cut into bite[- ]sized chunks',
    r'cut into bite[- ]sized pieces',
    r'cut in half',
    r'cut in \d+ pieces',
    r'cut in \d+',
    r'sliced into bite[- ]sized chunks',
    r'sliced into bite[- ]sized pieces',
    r'cut crosswise into [^,;]+',
    r'sliced crosswise into [^,;]+',
    r'crosswise into [^,;]+',
    r'cut lengthwise into [^,;]+',
    r'sliced lengthwise into [^,;]+',
    r'lengthwise into [^,;]+',
    r'quartered lengthwise',
    r'halved lengthwise',
    r'sliced lengthwise',
    r'cut lengthwise',
    r'stems removed',
    r'crust removed',
    # --- purpose phrases ---
    # NOTE: "for dusting" / "for sprinkling" / "for greasing" etc. are
    # intentionally NOT here. They describe what the ingredient is used
    # for, not a technique applied to it, so they're captured whole (e.g.
    # "for greasing the pan") as a note by _clean_name's trailing
    # "for <purpose>" handling instead of being partially eaten here.
    # --- packing ---
    r'loosely packed',
    # --- state / cut adjectives (single-word, must come after multi-word) ---
    r'\bde-stemmed\b',
    r'\bde-veined\b',
    r'\bboneless\b',
    r'\bskinless\b',
    r'\bdiced?\b',
    r'\bfinely\b',
    r'\bcoarsely\b',
    r'\broughly\b',
    r'\bchopped\b',
    r'\bminced\b',
    r'\bsliced\b',
    r'\bpeeled\b',
    r'\bgrated\b',
    r'\bcrushed\b',
    r'\bseeded\b',
    r'\bbeaten\b',
    r'\bwashed\b',
    r'\bmashed\b',
    r'\bscrubbed\b',
    r'\btrimmed\b',
    r'\bseparated\b',
    r'\bdivided\b',
    r'\bmelted\b',
    r'\bcubed\b',
    r'\bsifted\b',
    r'\bpacked\b',
    r'\bsoftened\b',
    r'\bshredded\b',
    r'\bcut\b',
    r'\bhalved\b',
    r'\bquartered\b',
    r'\bcracked\b',
    r'\bcored\b',
    r'\bdeveined\b',
    r'\bdebearded\b',
    r'\bpatted dry\b',
    r'\blengthwise\b',
    r'\bcrosswise\b',
]


# -----------------------------------------------------------
# TEMPERATURE / STATE PATTERNS  (moved to prep, not name)
# -----------------------------------------------------------

TEMPERATURE_STATE_PATTERNS = [
    r'at\s+room[\s-]temperature',
    r'room[\s-]temperature',
    r'very\s+cold',
    r'very\s+hot',
    r'\bboiling\b(?!\s+water)',   # "boiling water" is protected above
    r'\bchilled\b',
    r'\bcold\b',
    r'\bwarmed?\b',
    r'\bhot\b',
    r'\bfrozen\b',
    r'\bthawed\b',
    r'\biced\b',
    r'\brefrigerated\b',
    r'\bfresh(?:ly)?\b',
]


# ============================================================
# BOOTSTRAP: ingredient-specific portion/count terms
# These are NOT closed-class measurement units (cup, tbsp, oz, ...) —
# they only make sense for particular ingredients (a "clove" is garlic,
# a "sprig" is an herb, a "head" is lettuce/garlic/cauliflower, a "rib"
# is celery). Future USDA ingestion is expected to populate a dedicated
# table for this (not yet created); recipe ingestion will keep enriching
# it over time. Until that table exists, this bootstrap set is the only
# source. Genuine closed-class units (cup/tbsp/oz/lb/g/ml/...) stay in
# parser_vocabulary.py — they are grammar, not culinary knowledge, and
# are not expected to ever come from the database.
# ============================================================

PORTION_TERMS = frozenset({
    'sprig', 'sprigs', 'head', 'bunch', 'stalk', 'stalks',
    'leaf', 'clove', 'cloves',
    'floweret', 'florets', 'spear', 'spears', 'medallion', 'medallions',
    'wedge', 'wedges', 'fillet', 'fillets',
})
# NOTE: "rib"/"ribs" and "leaves" (plural) were deliberately tried here and
# reverted after corpus regression testing (283-line test corpus) showed
# they're too often part of the ingredient's own name/form rather than a
# counting unit to strip blindly — "celery ribs" (the ingredient's own
# form), "seeds and ribs removed" (a pepper's membrane, not a unit),
# "rib roast"/"prime rib" (a cut name), "basil leaves"/"bay leaves" (part
# of the ingredient name far more often than "1 leaf of X" is a real
# measurement). This is exactly the kind of judgment call that benefits
# from being data-driven and disambiguated with real usage frequency once
# a real portion-terms table exists, rather than a blanket include/exclude
# here — flagging as a known limitation of the bootstrap set, not a
# permanent design decision.