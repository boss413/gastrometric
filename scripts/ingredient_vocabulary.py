# ingredient_vocabulary.py
#
# Shared vocabulary for the ingredient parsing pipeline.
# Imported by parse_ingredient_lines.py and normalize_ingredient_lines.py.
#
# Two clearly separate sections:
#
#   PARSE-TIME  (used by parse_ingredient_lines.py)
#     Controls how raw text is split into qty / unit / prep / name.
#
#   NORMALIZE-TIME  (used by normalize_ingredient_lines.py)
#     Pass 1 — TYPO_FIXES: correct spelling variants before any stripping.
#     Pass 2 — QUALIFIER_STRIP_PATTERNS: remove words that wrap the core
#               ingredient without being part of it.
#
# What "core ingredient" means here
# ----------------------------------
# The goal of normalization is a name clean enough for a human to recognize
# and for a future ingredient_id lookup to match against.  Examples:
#
#   "freshly ground black pepper"  →  "black pepper"
#   "boneless skinless chicken breast"  →  "chicken breast"
#   "extra-virgin olive oil"  →  "olive oil"
#   "slivered almonds"  →  "almonds"      ← cut descriptor stripped
#   "low-sodium chicken broth"  →  "chicken broth"
#
# What normalization must NOT do:
#   "olive oil"  →  "oil"      ← that is canonicalization (downstream)
#   "chicken breast"  →  "chicken"  ← same
#
# HOW TO EXTEND
# -------------
#   PROTECTED_PHRASES   Append phrases whose internal words must survive
#                       prep/unit extraction unchanged.  Longer phrases
#                       should come first (sorted at runtime anyway).
#
#   NOISE_PHRASES       Strings erased entirely from the name during parsing.
#
#   PREP_PATTERNS       Ordered list of regex strings.  Multi-word / more-
#                       specific patterns MUST precede single-word patterns
#                       that share a keyword with them.
#
#   TYPO_FIXES          List of (compiled_re, replacement) pairs.
#                       Matched against lowercased ingredient_name_raw.
#
#   QUALIFIER_STRIP_PATTERNS
#                       List of compiled regexes stripped from the name to
#                       expose the core ingredient.  Applied in order;
#                       whitespace is collapsed after each.

import re


# ============================================================
# PARSE-TIME VOCABULARY
# ============================================================

# -----------------------------------------------------------
# PROTECTED PHRASES
# Temporarily tokenized so their internal words survive
# prep/unit extraction (e.g. "ground" in "ground beef").
# -----------------------------------------------------------

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
    "ground chicken", "ground lamb",
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


# -----------------------------------------------------------
# NOISE PHRASES
# Removed entirely from ingredient text (not moved to prep).
# -----------------------------------------------------------

NOISE_PHRASES = [
    "plus more", "as needed", "to taste", "if desired",
    "as desired", "optional",
]


# -----------------------------------------------------------
# PREP PATTERNS
# Multi-word / more-specific patterns MUST precede the single-word
# patterns that share a keyword with them.
# -----------------------------------------------------------

PREP_PATTERNS = [
    # --- multi-word cut patterns ---
    r'chopped into large chunks',
    r'chopped into small chunks',
    r'chopped into bite[- ]sized chunks',
    r'chopped into bite[- ]sized pieces',
    r'chopped into chunks',
    r'chopped into pieces',
    r'sliced into \d[\d./]*-inch[a-z-]* rounds',
    r'sliced into \d[\d./]*-inch[a-z-]* pieces',
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
    r'cut into \d[\d./]*-inch[a-z-]* rounds',
    r'cut into \d[\d./]*-inch[a-z-]* pieces',
    r'cut into \d[\d./]*-inch[a-z-]* chunks',
    r'cut into \d[\d./]*-inch[a-z-]* strips',
    r'cut into \d[\d./]* pieces',
    r'cut into \d[\d./]* chunks',
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
    r'sliced crosswise into [^,;]+',
    r'crosswise into [^,;]+',
    # --- purpose phrases ---
    r'for dusting',
    r'for sprinkling',
    r'for sprinklng',   # OCR variant
    r'for greasing',
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
]


# -----------------------------------------------------------
# TEMPERATURE / STATE PATTERNS  (moved to prep, not name)
# -----------------------------------------------------------

TEMPERATURE_STATE_PATTERNS = [
    r'at\s+room[\s-]temperature',
    r'room[\s-]temperature',
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


# -----------------------------------------------------------
# UNIT SETS
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

# Full recognized unit token set (used in or-alternative splitting)
UNIT_VOCAB = (
    GRAM_UNITS | ML_UNITS | IMPERIAL_WEIGHT_UNITS | IMPERIAL_VOLUME_UNITS | {
        'part', 'pinch', 'pinches', 'handful', 'recipe',
        'sprig', 'sprigs', 'head', 'bunch', 'stalks',
        'leaf', 'clove', 'cloves', 'stick', 'sticks', 'riibs', 'rib', 'slice', 'slices', 'strip',
        'strips', 'slices', 'box', 'can', 'cans', 'jar', 'bottle',
    }
)


# -----------------------------------------------------------
# TRUNCATION FIXES  (OCR / copy-paste truncation repair)
# Applied as a final pass during parsing, before writing ingredient_name_raw.
# -----------------------------------------------------------

TRUNCATION_FIXES = {
    r'\boi\b':           "oil",
    r'\bsausag\b':       "sausage",
    r'\bleav\b':         "leaves",
    r'\bnoodl\b':        "noodle",
    r'\bchiv\b':         "chive",
    r'\bparsley leav\b': "parsley leaves",
}


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
    (re.compile(r'\bhaddock\b|\btilapia\b|\bsole\b', re.I), "whitefish"),
    (re.compile(r'\bschmaltz?\b|\bdrippings\b', re.I), "rendered fat"),
    # brand / trade names → generic
    (re.compile(r'\btabasco\b',           re.I), "hot sauce"),
    (re.compile(r'\bgrey poupon\b',       re.I), "dijon mustard"),
    (re.compile(r'\bv8\b',               re.I), "tomato juice"),
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
    (re.compile(r'\bguacamole\b',         re.I), "avocado"),
    (re.compile(r'\bbrusselss?\b(?!\s+sprouts?)', re.I), "brussels sprouts"),
    (re.compile(r'\bporcini\b',           re.I), "porcini mushroom"),
    (re.compile(r'\bportobello\b',        re.I), "portobello mushroom"),
    (re.compile(r'\bshiitake\b',          re.I), "shiitake mushroom"),
    (re.compile(r'\bcrimini\b',           re.I), "crimini mushroom"),
    (re.compile(r'\bmahi[- ]?mahi\b|\bmahi\b', re.I), "tuna"),
    (re.compile(r'\bpancetta\b',          re.I), "bacon"),
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