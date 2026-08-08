"""
diagnose_phrase_index.py
=========================

Read-only diagnostic for the gastrometric.knowledge.loader <-> lex.py
contract boundary. Checks three hypotheses directly:

    1. Is phrase_index_for("ingredient") unexpectedly small?
    2. Is max_words always 1 (blocking multi-word phrase matching)?
    3. Are the keys shaped the way lex.py assumes (lowercase, exact
       alias text, one entry per alias)?

Run from your project root:

    python diagnose_phrase_index.py

Nothing here touches the database or mutates anything -- it only reads
whatever knowledge.phrase_index_for() and knowledge.vocabulary_classes
already return.
"""

from __future__ import annotations

from gastrometric.knowledge.loader import knowledge

# Known aliases to specifically probe, based on the ingredients.json
# excerpt under discussion. Add more here as needed.
PROBE_CASES = [
    ("ingredient", "chicken breast"),
    ("ingredient", "chicken breasts"),
    ("ingredient", "breasts"),
    ("ingredient", "boneless skinless chicken breast"),
    ("ingredient", "chk breast"),
    ("ingredient", "all-purpose flour"),
    ("ingredient", "all purpose flour"),
    ("ingredient", "ap flour"),
    ("ingredient", "plain flour"),
]


def _describe_index(vocabulary_class: str) -> None:
    result = knowledge.phrase_index_for(vocabulary_class)

    print(f"--- knowledge.phrase_index_for({vocabulary_class!r}) ---")
    print(f"  type(result): {type(result)!r}")

    if not isinstance(result, tuple) or len(result) != 2:
        print(f"  UNEXPECTED SHAPE: expected a 2-tuple (index, max_words), got {result!r}")
        print()
        return

    index, max_words = result
    print(f"  type(index): {type(index)!r}")
    print(f"  len(index) [distinct phrase keys]: {len(index)}")
    print(f"  max_words: {max_words!r}")

    if max_words == 1:
        print("  ^^ HYPOTHESIS 2 LIKELY CONFIRMED: max_words == 1 means lex.py will")
        print("     never even attempt a 2+ word phrase lookup for this class, no")
        print("     matter what the index contains.")

    total_matches = 0
    for key, matches in index.items():
        try:
            total_matches += len(matches)
        except TypeError:
            print(f"  UNEXPECTED VALUE SHAPE at key {key!r}: {matches!r} (not sized)")
    print(f"  total PhraseMatch objects across all keys: {total_matches}")

    sample_keys = list(index.keys())[:8]
    print(f"  sample keys: {sample_keys}")

    # Case-sensitivity check: lex.py always looks up phrase_text.lower().
    # If the index has any non-lowercase keys, exact-match lookups from
    # lex.py against those keys will silently miss forever.
    non_lowercase_keys = [k for k in index.keys() if k != k.lower()]
    if non_lowercase_keys:
        print(f"  HYPOTHESIS 3 LIKELY CONFIRMED: {len(non_lowercase_keys)} keys are not")
        print(f"     lowercase, e.g. {non_lowercase_keys[:5]!r}. lex.py looks up with")
        print("     .lower(), so these keys can never be hit.")

    # Sample a PhraseMatch to check attribute shape.
    if index:
        sample_key = next(iter(index))
        sample_match = index[sample_key][0]
        print(f"  sample match for key {sample_key!r}: {sample_match!r}")
        print(
            f"    hasattr(.knowledge_id): {hasattr(sample_match, 'knowledge_id')}, "
            f"hasattr(.normalized_value): {hasattr(sample_match, 'normalized_value')}"
        )
    print()


def _probe_specific_aliases() -> None:
    print("--- probing specific aliases lex.py would look up ---")
    for vocabulary_class, phrase in PROBE_CASES:
        index, _max_words = knowledge.phrase_index_for(vocabulary_class)
        key = phrase.strip().lower()
        hit = index.get(key)
        status = f"FOUND ({len(hit)} match(es))" if hit else "MISSING"
        print(f"  [{vocabulary_class}] {phrase!r} -> key {key!r}: {status}")
    print()


def main() -> None:
    print("=" * 78)
    print("vocabulary_classes known to the loader:")
    print(f"  {sorted(knowledge.vocabulary_classes)}")
    print("=" * 78)
    print()

    for vocabulary_class in ("ingredient", "measurement", "preparation", "brand", "grammar"):
        _describe_index(vocabulary_class)

    _probe_specific_aliases()


if __name__ == "__main__":
    main()