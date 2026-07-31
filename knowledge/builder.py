"""Shared builder interface.

Every ingestion/builders/*.py module implements this so rebuild_db.py can
orchestrate them uniformly without knowing their internals.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable, List

from .models import BuildResult


class KnowledgeBuilder:
    name: str = "base"

    def run(self, conn: sqlite3.Connection) -> BuildResult:
        raise NotImplementedError


def run_builders(conn: sqlite3.Connection, builders: Iterable[KnowledgeBuilder]) -> List[BuildResult]:
    return [builder.run(conn) for builder in builders]
