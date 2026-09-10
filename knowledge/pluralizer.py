"""General-purpose pluralization helper.

This is the shared internal standard for reasoning about singular/plural
word forms across gastrometric. It backs both knowledge builders (e.g. seed
vocabulary import) and, eventually, search/reference lookups. Anything that
needs to know whether a term is plural, or needs its counterpart form,
should import from here rather than reimplementing ad hoc pluralization
rules, so every subsystem agrees on the same forms for the same words.

Backed by the ``inflect`` library.
"""
from __future__ import annotations

from typing import cast

import inflect

_engine = inflect.engine()


def is_plural(word: str) -> bool:
    """Return True if `word` appears to already be in plural form."""
    return bool(_engine.singular_noun(cast(inflect.Word, word)))


def pluralize(word: str) -> str:
    """Return the plural form of `word`.

    If `word` is already plural, it is returned unchanged.
    """
    if is_plural(word):
        return word
    plural = _engine.plural_noun(cast(inflect.Word, word))
    return plural if plural else word


def singularize(word: str) -> str:
    """Return the singular form of `word`.

    If `word` is already singular, it is returned unchanged.
    """
    singular = _engine.singular_noun(cast(inflect.Word, word))
    return singular if singular else word


def both_forms(word: str) -> tuple[str, str]:
    """Return a `(singular, plural)` pair for `word`.

    Works regardless of whether `word` is given in singular or plural form:
    the given form is preserved exactly, and the counterpart is derived.
    """
    if is_plural(word):
        return singularize(word), word
    return word, pluralize(word)