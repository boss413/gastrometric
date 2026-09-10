"""
Shared FastAPI dependencies for the API (inventory: BE-03; recipe search: BE-06).

Every `inventory_editor` function already accepts an optional `db_path`
override (that's how the application/orchestration layers are tested
without touching the development database). This dependency threads that
same override through the HTTP layer without adding a `db_path` request
parameter that would leak database configuration to clients: production
requests get `None` (meaning "use the application's configured
`DB_PATH`"), and tests override this dependency via
`app.dependency_overrides[get_db_path]` to point at a temporary database.

`get_knowledge` follows the identical pattern for the runtime knowledge
singleton BE-06's ingredient search needs: production requests get the
repository's own already-loaded singleton, and tests override via
`app.dependency_overrides[get_knowledge]` the same way. This is NOT a
second knowledge-loading mechanism -- it's a thin FastAPI dependency
wrapper around the exact same module-level singleton import used
elsewhere in the repository (e.g.
`inventory_ingredient_understanding.py`'s
`from gastrometric.knowledge.loader import knowledge as runtime_knowledge`).
"""

from typing import Any, Optional

from gastrometric.knowledge.loader import knowledge as _runtime_knowledge


def get_db_path() -> Optional[str]:
    """Production default: no override, `inventory_editor` uses its own
    configured `DB_PATH`. Tests replace this via
    `app.dependency_overrides`.
    """
    return None


def get_knowledge() -> Any:
    """Production default: the module-level runtime knowledge singleton,
    loaded once by `gastrometric.knowledge.loader`. Tests replace this
    via `app.dependency_overrides` with a constructed knowledge object
    (see `test_ingredient_matcher.py`'s `_StubKnowledge` for the
    pattern), the same way `get_db_path` is overridden with a temp
    database path.

    Typed `Any` rather than a concrete class because only the singleton's
    import path is confirmed (`from gastrometric.knowledge.loader import
    knowledge`) -- its actual class name was not given to me. See
    `application/ingredient_matcher.py`'s `RuntimeKnowledgeLike` Protocol
    for the structurally-typed alternative used where the shape (not
    just the existence) of `knowledge` matters.
    """
    return _runtime_knowledge