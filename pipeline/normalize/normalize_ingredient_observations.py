"""
normalize_ingredient_observations.py

STUB — NOT WIRED UP. This is a starting point, not a working stage.

Consumes parser evidence (parse_ingredient_lines.py's `recognized_spans`
per line) and is responsible for:

    - assembling vocabulary around ingredient candidates
    - attaching modifiers (which Modifier/Preparation/State/Temperature
      spans belong to which IngredientSpan)
    - deciding whether adjacent vocabulary should become part of an
      ingredient identity
    - deciding whether "ham and bean soup mix" is one ingredient or
      several (the parser only marks "and" as a GrammarMarker span; it
      does not split)
    - expanding truncated/corrupted tokens (lexical inference, not
      grammar)
    - producing normalized ingredient observations for Identity
      Resolution to consume

It must NOT decide USDA mappings, canonical ingredient relationships,
substitutions, or nutrition — that's Identity Resolution and Nutrition
Resolution's job, further downstream.

WHAT'S BELOW: functions moved here verbatim (or near-verbatim) from
parse_ingredient_lines.py because the work order identified them as
belonging to this stage, not parsing. None of them are wired into a
pipeline yet — that's this stage's own implementation work, out of scope
for the parser refactor that produced this stub.
"""

import re


# ============================================================
# MOVED FROM parse_ingredient_lines.py: TRUNCATION / CORRUPTION REPAIR
#
# "Requires lexical inference. Not grammar." (work order, Stage
# "Move to Normalization"). Unchanged in logic from the parser version —
# an unrecognized word is compared against every canonical word
# available at runtime; if EXACTLY ONE canonical word begins with it,
# it's expanded to that word. Needs its own canonical-word set built
# from CulinaryVocabulary + VocabularyProvider, since it no longer has
# access to the parser's CompiledVocabulary singleton.
# ============================================================

_MIN_TRUNCATION_PREFIX_LEN = 2


def build_canonical_words(culinary, provider):
    """Flat, lowercase set of every whole word available at runtime, from
    every CulinaryVocabulary class plus every ingredient identity/alias
    (VocabularyProvider.ingredient_identities()). Multi-word canonical
    phrases are exploded into their individual words, since prefix
    expansion below operates one token at a time.

    `culinary` / `provider` are CulinaryVocabulary / VocabularyProvider
    instances — see parse_ingredient_lines.py for how those get
    constructed and which accessor names CulinaryVocabulary actually
    exposes (this stage should reuse parse_ingredient_lines._VOCAB_CLASSES
    / _fetch_vocab_class rather than re-deriving that bridging logic, if
    imported into the same pipeline)."""
    words = set()
    for cls in ("measurement", "packaging", "natural_portion", "preparation",
                "ingredient_form", "size", "descriptor", "shape", "state",
                "temperature", "modifier", "seasoning", "brand"):
        accessor = getattr(culinary, cls + "s", None) or getattr(culinary, cls, None)
        if accessor is None:
            continue
        try:
            terms = accessor()
        except TypeError:
            continue
        for term in terms:
            words.update(term.lower().split())
    for phrase in provider.ingredient_identities():
        words.update(phrase.lower().split())
    return words


def _expand_truncated_word(word, canonical_words):
    lw = word.lower()
    if len(lw) < _MIN_TRUNCATION_PREFIX_LEN or lw in canonical_words:
        return word
    matches = {w for w in canonical_words if w != lw and w.startswith(lw)}
    if len(matches) == 1:
        return matches.pop()
    return word


def expand_truncated_tokens(text, canonical_words):
    return re.sub(
        r"[a-zA-Z']+",
        lambda m: _expand_truncated_word(m.group(0), canonical_words),
        text
    )


# ============================================================
# MOVED FROM parse_ingredient_lines.py: "A and B" INGREDIENT-COUNT
# DECISION
#
# Work order, "Reevaluate _split_multi_ingredients": "Normalization
# decides whether 'ham and bean soup mix' is one ingredient or multiple
# because this requires ingredient knowledge." The parser now only
# marks "and" as a GrammarMarker span (see
# parse_ingredient_lines._leftover_spans) instead of calling this.
#
# This heuristic (short, digit-free, not a bare size word on either
# side) is the OLD parser's entire "ingredient knowledge" for this
# decision — which is to say, none: it's a text-shape heuristic, not
# real ingredient knowledge. Kept here as a plausible starting point,
# not a finished answer; a real implementation should almost certainly
# consult ingredient identity data (e.g. does "ham and bean soup mix"
# match a single seeded ingredient/alias verbatim?) before falling back
# to a text-shape guess like this one.
# ============================================================

_BARE_SIZE_WORDS = {
    "large", "extra-large", "extra large", "medium", "small", "jumbo",
    "very", "quite", "rather", "fairly", "slightly",
}


def split_multi_ingredients_heuristic(text):
    """Split "salt and pepper" → ["salt", "pepper"] only for short,
    digit-free candidate names. NOT WIRED UP — see module docstring."""
    if " and " in text:
        parts = [p.strip() for p in text.split(" and ", 1)]
        if (all(not re.search(r'\d', p) for p in parts) and len(text.split()) <= 6
                and all(p.lower() not in _BARE_SIZE_WORDS and p for p in parts)):
            return parts
    return [text]


# ============================================================
# MOVED FROM parse_ingredient_lines.py: ACTION-WORD / SIZE-ADJECTIVE
# REMOVAL
#
# Work order: both flagged WRONG for the parser ("parser vocabulary
# should remain available as evidence" / "size words are ingredient
# evidence, they should remain"). The parser no longer calls either of
# these — the words they used to delete now survive as Modifier spans
# (or, for _ACTION_WORDS' tiny ad hoc set, as Unknown spans if not
# seeded under any CulinaryVocabulary class). Kept here only as a
# reference starting point for "attaching modifiers" work — NOT because
# removal is the right operation at this stage either. Normalization's
# job is to decide which ingredient a modifier belongs to, not to decide
# whether to delete it.
# ============================================================

_ACTION_WORDS = {"washed", "separated", "into"}


def remove_action_words_reference(text):
    """Reference only — see caveat above. Do not wire this up as-is."""
    tokens, result = text.split(), []
    for i, w in enumerate(tokens):
        if w.lower() == "and":
            prev_ok = i > 0 and tokens[i-1].lower() not in _ACTION_WORDS
            next_ok = i < len(tokens)-1 and tokens[i+1].lower() not in _ACTION_WORDS
            if not (prev_ok and next_ok):
                continue
        elif w.lower() in _ACTION_WORDS:
            continue
        result.append(w)
    return " ".join(result)


# ============================================================
# NOT YET IMPLEMENTED — this stage's actual job
# ============================================================

def assemble_ingredient_observation(recognized_spans):
    """Takes one line's `recognized_spans` (see
    parse_ingredient_lines.parse_ingredient_line) and produces a
    normalized ingredient observation: which IngredientSpan(s) are the
    real ingredient(s) for this line, which Modifier/Preparation/State/
    Temperature spans attach to which, and what the assembled
    (normalized, not truncation-corrupted) ingredient name is.

    NOT IMPLEMENTED — out of scope for the parser refactor that produced
    this stub. See module docstring for this stage's full
    responsibilities.
    """
    raise NotImplementedError(
        "normalize_ingredient_observations.assemble_ingredient_observation "
        "is a stub — see module docstring."
    )