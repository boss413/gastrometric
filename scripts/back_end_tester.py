#!/usr/bin/env python3
"""
gastrometric/scripts/back_end_tester.py

Backend system exerciser and chain-of-custody verification tool (BE-08).

This is NOT a unit-test suite -- existing tests remain authoritative for
individual functions and API contracts. This tool asks a different
question: "if the frontend submits this input, what happens to it as it
travels through the backend, and where does the first unexpected result
occur?"

It behaves like the frontend: its primary interaction with Gastrometric
is HTTP, against a REAL RUNNING API process, never by importing and
calling application/orchestration/analyzer/repository functions
directly. The one exception is diagnostic observation: after an HTTP
operation, this tool may read SQLite directly (or, for selecting valid
real test inputs up front, read runtime knowledge directly) to establish
persistence evidence or pick inputs known to exist -- never as the
primary way scenarios interact with the backend, and never to set up
scenario state (every piece of inventory data a scenario depends on is
created through POST /api/inventory, never through a direct INSERT).

--------------------------------------------------------------------
Running this tool
--------------------------------------------------------------------
Prerequisites (the actual real environment, not simulated by this
script):

    python -m gastrometric.orchestration.rebuild_db
    uvicorn gastrometric.api.api_app:app --host 0.0.0.0 --port 8000

Then, from another terminal:

    python -m gastrometric.scripts.back_end_tester
    python -m gastrometric.scripts.back_end_tester --base-url http://127.0.0.1:8000
    python -m gastrometric.scripts.back_end_tester --db-path /path/to/gastrometric.db
    python -m gastrometric.scripts.back_end_tester --json-out trace.json
    python -m gastrometric.scripts.back_end_tester --keep-data

--------------------------------------------------------------------
Dependencies
--------------------------------------------------------------------
Standard library only (urllib, sqlite3, json, argparse) -- deliberately
avoids adding `requests`/`httpx` as a new project dependency purely for
this diagnostic script, since the stdlib is sufficient for simple
GET/POST/PATCH/DELETE calls with JSON bodies.
"""

import argparse
import json
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# DB_PATH resolution -- use the existing configured location, never a
# hard-coded/new database (work order section 15).
# ---------------------------------------------------------------------------

try:
    from gastrometric.config.paths import DB_PATH as _CONFIGURED_DB_PATH
except ImportError:
    _CONFIGURED_DB_PATH = None


# ---------------------------------------------------------------------------
# Diagnostic vocabulary (work order section 13)
# ---------------------------------------------------------------------------

STATUS_EXPECTED_PASS = "EXPECTED PASS"
STATUS_EXPECTED_EMPTY = "EXPECTED EMPTY RESULT"
STATUS_UNEXPECTED = "UNEXPECTED RESULT"
STATUS_HTTP_ERROR = "HTTP ERROR"
STATUS_INTERNAL_EXCEPTION = "INTERNAL EXCEPTION"
STATUS_DATA_LIMITATION = "DATA LIMITATION"

SUBSYSTEM_API = "API"
SUBSYSTEM_APPLICATION = "application"
SUBSYSTEM_ORCHESTRATION = "orchestration"
SUBSYSTEM_UNDERSTANDING = "understanding"
SUBSYSTEM_KNOWLEDGE = "knowledge"
SUBSYSTEM_PERSISTENCE = "persistence"
SUBSYSTEM_RECIPE_DATA = "recipe data"
SUBSYSTEM_MATCHING = "matching"
SUBSYSTEM_DATA_LIMITATION_TAG = "test/data limitation"
SUBSYSTEM_UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# HTTP client (stdlib only)
# ---------------------------------------------------------------------------


@dataclass
class HttpResult:
    method: str
    url: str
    request_body: Optional[Dict[str, Any]]
    status: Optional[int]
    response_body: Any
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status is not None and 200 <= self.status < 300


def http_request(
    method: str, url: str, json_body: Optional[Dict[str, Any]] = None, timeout: float = 10.0
) -> HttpResult:
    """
    Issues one HTTP request and returns a structured result regardless of
    outcome -- including a non-2xx status or a connection failure -- so
    callers always have something to record in the trace rather than an
    unhandled exception. This is the ONLY way scenarios talk to the
    backend (work order section 1): no scenario in this file imports
    application/orchestration/analyzer/repository code as its primary
    mechanism.
    """
    data = None
    headers = {"Accept": "application/json"}
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            raw_body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw_body = exc.read()
    except urllib.error.URLError as exc:
        return HttpResult(
            method=method, url=url, request_body=json_body,
            status=None, response_body=None, error=str(exc),
        )

    if raw_body:
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = raw_body.decode("utf-8", errors="replace")
    else:
        body = None

    return HttpResult(method=method, url=url, request_body=json_body, status=status, response_body=body)


# ---------------------------------------------------------------------------
# Database observation (diagnostic only -- see module docstring)
# ---------------------------------------------------------------------------


def _connect_readonly(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_inventory_item(db_path: str, item_id: int) -> Optional[Dict[str, Any]]:
    conn = _connect_readonly(db_path)
    try:
        row = conn.execute("SELECT * FROM inventory_items WHERE id = ?", (item_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def table_exists(db_path: str, table_name: str) -> bool:
    conn = _connect_readonly(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table_name,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Real-data discovery (work order section 17: do not guess valid inputs)
# ---------------------------------------------------------------------------


def discover_real_recipe_name_query(db_path: str) -> Optional[str]:
    """Picks a search term guaranteed to have at least one result via
    GET /api/recipes/search?type=name, by reading an actual persisted
    recipe_name and using its first word -- never a guessed/hardcoded
    recipe title.
    """
    conn = _connect_readonly(db_path)
    try:
        row = conn.execute("SELECT recipe_name FROM recipes ORDER BY id ASC LIMIT 1").fetchone()
    finally:
        conn.close()
    if not row or not row["recipe_name"]:
        return None
    return row["recipe_name"].split()[0]


def discover_real_recipe_ingredient(db_path: str) -> Optional[str]:
    """
    Picks the most frequently-used real ingredient identity in
    `recipe_ingredient_lines_parsed` -- deliberately the MOST common one
    (not just any one), to maximize the chance that the same identity
    also appears in more than one recipe (useful for search) and that an
    inventory item created for it will produce at least one match in
    Scenario 6/7.
    """
    conn = _connect_readonly(db_path)
    try:
        row = conn.execute(
            """
            SELECT ingredient_id, COUNT(*) AS n
            FROM recipe_ingredient_lines_parsed
            WHERE ingredient_id IS NOT NULL AND TRIM(ingredient_id) != ''
            GROUP BY ingredient_id
            ORDER BY n DESC, ingredient_id ASC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    return row["ingredient_id"] if row else None


def discover_second_real_recipe_ingredient(db_path: str, exclude: Optional[str]) -> Optional[str]:
    """
    Finds a SECOND, DIFFERENT real ingredient identity (by frequency,
    excluding `exclude`) for Scenario 2's text-change test. Calling
    `discover_real_recipe_ingredient` twice would risk returning the
    identical value both times (it always picks the single most
    frequent one) -- which would make the "before" and "after" inventory
    text the same string, silently turning the update scenario into a
    no-op rather than a genuine text change. This function exists
    specifically to avoid that.
    """
    conn = _connect_readonly(db_path)
    try:
        row = conn.execute(
            """
            SELECT ingredient_id, COUNT(*) AS n
            FROM recipe_ingredient_lines_parsed
            WHERE ingredient_id IS NOT NULL AND TRIM(ingredient_id) != ''
              AND ingredient_id != ?
            GROUP BY ingredient_id
            ORDER BY n DESC, ingredient_id ASC
            LIMIT 1
            """,
            (exclude or "",),
        ).fetchone()
    finally:
        conn.close()
    return row["ingredient_id"] if row else None


def discover_real_alias() -> Optional[Tuple[str, str]]:
    """
    Attempts to find one real (alias, canonical_name) pair from the
    actual runtime knowledge loader. This reads
    `gastrometric.knowledge.loader.knowledge` directly rather than via
    HTTP -- an explicit, narrow exception to the "primary mechanism is
    HTTP" rule, justified the same way direct SQLite reads are: this is
    input SELECTION (picking a real, valid test value), not the
    scenario's actual interaction with the backend. The alias itself is
    still exercised through the real
    GET /api/recipes/search?type=ingredient HTTP endpoint once found.

    Returns None if the loader can't be imported or has no aliases --
    in which case the alias variant of Scenario 5 is reported as a data
    limitation, never faked with an invented alias.
    """
    try:
        from gastrometric.knowledge.loader import knowledge
    except ImportError:
        return None
    aliases = getattr(knowledge, "ingredient_aliases", None)
    if not aliases:
        return None
    try:
        alias, canonical = next(iter(aliases.items()))
    except StopIteration:
        return None
    return alias, canonical


def discover_pantry_only_candidate(db_path: str, exclude_ingredient: Optional[str] = None) -> Optional[Tuple[int, str, List[str]]]:
    """
    Finds one real recipe with a small number (1-3) of distinct, non-null
    ingredient identities, small enough to fully cover with pantry
    inventory items and deterministically exercise the `pantry_only`
    category with real data. Returns (recipe_id, recipe_name,
    [ingredient_ids]), or None if no such recipe exists in the current
    data -- in which case Scenario 6 reports pantry_only as
    NOT AVAILABLE IN CURRENT REAL DATA rather than manufacturing a
    synthetic recipe to force the category to appear (work order section
    10's explicit instruction).

    `exclude_ingredient`, when given, excludes any candidate recipe that
    ALSO contains that ingredient identity -- specifically the one
    already seeded into fridge inventory earlier in Scenario 6. Without
    this, a candidate recipe that happens to share the fridge ingredient
    would score a fridge match too, producing `perfect` instead of
    `pantry_only` and silently failing to exercise the category despite
    a nominally-suitable recipe being found -- this was caught during
    this tool's own verification, not assumed.
    """
    conn = _connect_readonly(db_path)
    try:
        params: Tuple[Any, ...] = ()
        exclude_clause = ""
        if exclude_ingredient:
            exclude_clause = (
                "AND parsed.recipe_id NOT IN ("
                "  SELECT recipe_id FROM recipe_ingredient_lines_parsed WHERE ingredient_id = ?"
                ") "
            )
            params = (exclude_ingredient,)
        row = conn.execute(
            f"""
            SELECT parsed.recipe_id AS recipe_id, recipes.recipe_name AS recipe_name,
                   GROUP_CONCAT(DISTINCT parsed.ingredient_id) AS ids,
                   COUNT(DISTINCT parsed.ingredient_id) AS n
            FROM recipe_ingredient_lines_parsed AS parsed
            JOIN recipes ON recipes.id = parsed.recipe_id
            WHERE parsed.ingredient_id IS NOT NULL AND TRIM(parsed.ingredient_id) != ''
            {exclude_clause}
            GROUP BY parsed.recipe_id
            HAVING n BETWEEN 1 AND 3
            ORDER BY n ASC, parsed.recipe_id ASC
            LIMIT 1
            """,
            params,
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return row["recipe_id"], row["recipe_name"], row["ids"].split(",")


# ---------------------------------------------------------------------------
# Chain-of-custody recording
# ---------------------------------------------------------------------------


@dataclass
class Step:
    number: int
    label: str
    boundary: str
    operation: str
    detail: Any
    status: str = "info"


@dataclass
class ScenarioResult:
    name: str
    real_data: bool = True
    steps: List[Step] = field(default_factory=list)
    outcome: str = STATUS_EXPECTED_PASS
    subsystem: Optional[str] = None
    summary: Optional[str] = None


def _json_default(value: Any) -> Any:
    return str(value)


class ChainRecorder:
    """
    Prints and accumulates the numbered chain-of-custody steps for one
    scenario. Printing happens as each step is recorded (streaming),
    so a crash mid-scenario still leaves a readable partial trace on
    stdout -- the tool's whole purpose is diagnosing exactly that kind
    of situation.
    """

    def __init__(self, name: str, real_data: bool = True):
        self.result = ScenarioResult(name=name, real_data=real_data)
        self._next_number = 1
        print("\n" + "=" * 60)
        print(f"SCENARIO: {name}")
        print("=" * 60)
        if not real_data:
            print("(uses synthetic data -- see step notes for why)")

    def record(self, label: str, boundary: str, operation: str, detail: Any, status: str = "info") -> Step:
        step = Step(
            number=self._next_number, label=label, boundary=boundary,
            operation=operation, detail=detail, status=status,
        )
        self.result.steps.append(step)
        self._next_number += 1

        print(f"\n[{step.number}] {label}")
        if operation:
            print(operation)
        if isinstance(detail, (dict, list)):
            print(json.dumps(detail, indent=2, default=_json_default))
        elif detail is not None:
            print(detail)

        return step

    def record_http(self, label_prefix: str, result: HttpResult) -> Step:
        """Records a request/response pair as two steps -- REQUEST then
        RESPONSE -- matching the work order's illustrated output shape.
        """
        self.record(
            f"{label_prefix} REQUEST", "http", f"{result.method} {result.url}",
            result.request_body,
        )
        response_status_line = f"{result.status}" if result.status is not None else f"CONNECTION ERROR: {result.error}"
        return self.record(
            f"{label_prefix} RESPONSE", "http", response_status_line,
            result.response_body,
        )

    def finish(self, outcome: str, subsystem: Optional[str] = None, summary: Optional[str] = None) -> ScenarioResult:
        self.result.outcome = outcome
        self.result.subsystem = subsystem
        self.result.summary = summary
        print(f"\n[{self._next_number}] SCENARIO RESULT")
        print(outcome)
        if summary:
            print(summary)
        if subsystem:
            print(f"Likely subsystem: {subsystem}")
        return self.result


def _diagnose_last_success_and_first_unexpected(recorder: ChainRecorder) -> Tuple[Optional[Step], Optional[Step]]:
    """Walks a scenario's recorded steps to find the last clearly-passing
    boundary and the first step that broke the expected chain, for
    UNEXPECTED/ERROR reports (work order section 13's failure-diagnosis
    format). Used by scenarios that detect a problem after the fact.
    """
    last_pass: Optional[Step] = None
    first_unexpected: Optional[Step] = None
    for step in recorder.result.steps:
        if step.status in ("pass", "info"):
            last_pass = step
        if step.status in ("unexpected", "fail", "error") and first_unexpected is None:
            first_unexpected = step
    return last_pass, first_unexpected


# ---------------------------------------------------------------------------
# Scenario support
# ---------------------------------------------------------------------------


def _inventory_url(base_url: str, item_id: Optional[int] = None) -> str:
    if item_id is None:
        return f"{base_url}/api/inventory"
    return f"{base_url}/api/inventory/{item_id}"


def _create_inventory_item(
    recorder: ChainRecorder, base_url: str, original_input: str, location: str, label_prefix: str = ""
) -> HttpResult:
    body = {"original_input": original_input, "location": location}
    recorder.record(f"{label_prefix}FRONTEND INPUT".strip(), "frontend", "User submits inventory item", body)
    result = http_request("POST", _inventory_url(base_url), json_body=body)
    recorder.record_http(f"{label_prefix}HTTP".strip(), result)
    return result


# ---------------------------------------------------------------------------
# Scenario 1: create inventory item
# ---------------------------------------------------------------------------


def scenario_create_inventory(base_url: str, db_path: str, real_ingredient: Optional[str]) -> ScenarioResult:
    recorder = ChainRecorder("Create Inventory Item", real_data=real_ingredient is not None)

    if real_ingredient is None:
        return recorder.finish(
            STATUS_DATA_LIMITATION, SUBSYSTEM_DATA_LIMITATION_TAG,
            "No real ingredient identity could be discovered from recipe_ingredient_lines_parsed; "
            "cannot select an input the current rebuilt knowledge database is known to resolve.",
        )

    original_input = f"2 {real_ingredient}"
    result = _create_inventory_item(recorder, base_url, original_input, "fridge")

    if not result.ok:
        recorder.record("DIAGNOSTIC", "diagnostic", "POST /api/inventory did not return 2xx", None, status="unexpected")
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"Expected 201, got {result.status}")

    item_id = result.response_body.get("id") if isinstance(result.response_body, dict) else None
    if item_id is None:
        return recorder.finish(STATUS_UNEXPECTED, SUBSYSTEM_API, "201 response did not contain an item id")

    persisted = fetch_inventory_item(db_path, item_id)
    recorder.record("PERSISTED STATE", "persistence", f"inventory_items WHERE id = {item_id}", persisted)

    if persisted is None:
        return recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_PERSISTENCE,
            "API reported success but no matching row exists in inventory_items",
        )

    resolution_status = result.response_body.get("resolution_status")
    outcome = STATUS_EXPECTED_PASS
    subsystem = None
    summary = f"Created item {item_id}; resolution_status={resolution_status!r}"
    if resolution_status != "resolved":
        # Not necessarily a defect -- the chosen ingredient may not
        # resolve cleanly even though it appears in recipe data (e.g. if
        # recipe ingestion and inventory understanding disagree on some
        # edge case). Reported, not silently treated as pass.
        outcome = STATUS_UNEXPECTED
        subsystem = SUBSYSTEM_UNDERSTANDING
        summary += " -- expected 'resolved' since this ingredient identity was sourced from real recipe data"

    scenario_result = recorder.finish(outcome, subsystem, summary)
    scenario_result.steps.append(Step(number=-1, label="_item_id", boundary="internal", operation="", detail=item_id, status="internal"))
    return scenario_result


# ---------------------------------------------------------------------------
# Scenario 2: update inventory text
# ---------------------------------------------------------------------------


def scenario_update_inventory(base_url: str, db_path: str, real_ingredient: Optional[str], real_ingredient_alt: Optional[str]) -> ScenarioResult:
    recorder = ChainRecorder("Update Inventory Text", real_data=real_ingredient is not None)

    if real_ingredient is None:
        return recorder.finish(
            STATUS_DATA_LIMITATION, SUBSYSTEM_DATA_LIMITATION_TAG,
            "No real ingredient identity available to seed this scenario.",
        )

    create_result = _create_inventory_item(recorder, base_url, real_ingredient, "fridge", label_prefix="INITIAL ")
    if not create_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"Initial creation failed: {create_result.status}")

    item_id = create_result.response_body.get("id")
    initial_persisted = fetch_inventory_item(db_path, item_id)
    recorder.record("INITIAL PERSISTED STATE", "persistence", f"inventory_items WHERE id = {item_id}", initial_persisted)

    updated_text = real_ingredient_alt or f"chopped {real_ingredient}"
    recorder.record("FRONTEND INPUT (correction)", "frontend", "User edits the submitted text", {"original_input": updated_text})

    update_result = http_request("PATCH", _inventory_url(base_url, item_id), json_body={"original_input": updated_text})
    recorder.record_http("UPDATE HTTP", update_result)

    if not update_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"PATCH failed: {update_result.status}")

    updated_persisted = fetch_inventory_item(db_path, item_id)
    recorder.record("UPDATED PERSISTED STATE", "persistence", f"inventory_items WHERE id = {item_id}", updated_persisted)

    if updated_persisted is None:
        return recorder.finish(STATUS_UNEXPECTED, SUBSYSTEM_PERSISTENCE, "Row disappeared after PATCH")

    if updated_persisted.get("original_input") != updated_text:
        return recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_PERSISTENCE,
            f"original_input is {updated_persisted.get('original_input')!r}, expected {updated_text!r}",
        )

    # The behavioral contract under test: changing original_input must
    # re-run understanding, not just overwrite the text column.
    if initial_persisted and updated_persisted.get("analysis_result_json") == initial_persisted.get("analysis_result_json"):
        scenario_result = recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_APPLICATION,
            "analysis_result_json is byte-identical before and after the text change -- "
            "understanding does not appear to have re-run",
        )
    else:
        scenario_result = recorder.finish(
            STATUS_EXPECTED_PASS, None,
            f"Item {item_id} updated; analysis_result_json changed, confirming re-understanding ran",
        )

    scenario_result.steps.append(Step(number=-1, label="_item_id", boundary="internal", operation="", detail=item_id, status="internal"))
    return scenario_result


# ---------------------------------------------------------------------------
# Scenario 3: delete inventory item
# ---------------------------------------------------------------------------


def scenario_delete_inventory(base_url: str, db_path: str, real_ingredient: Optional[str]) -> ScenarioResult:
    recorder = ChainRecorder("Delete Inventory Item", real_data=real_ingredient is not None)

    seed_text = real_ingredient or "cabbage"
    if real_ingredient is None:
        recorder.record(
            "NOTE", "diagnostic",
            "No real ingredient identity discovered; using literal 'cabbage' as a plausible fallback "
            "purely to exercise the CRUD lifecycle -- resolution_status is not asserted below.",
            None,
        )

    create_result = _create_inventory_item(recorder, base_url, seed_text, "pantry")
    if not create_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"Creation failed: {create_result.status}")

    item_id = create_result.response_body.get("id")

    delete_result = http_request("DELETE", _inventory_url(base_url, item_id))
    recorder.record_http("DELETE HTTP", delete_result)

    if delete_result.status != 204:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"Expected 204, got {delete_result.status}")

    list_result = http_request("GET", _inventory_url(base_url))
    recorder.record_http("VERIFY (list inventory) HTTP", list_result)

    if not list_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"GET /api/inventory failed: {list_result.status}")

    items = list_result.response_body.get("items", []) if isinstance(list_result.response_body, dict) else []
    still_present = any(item.get("id") == item_id for item in items)

    persisted = fetch_inventory_item(db_path, item_id)
    recorder.record("PERSISTED STATE CHECK", "persistence", f"inventory_items WHERE id = {item_id}", persisted)

    if still_present or persisted is not None:
        return recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_PERSISTENCE,
            f"Item {item_id} still visible after DELETE returned 204",
        )

    return recorder.finish(STATUS_EXPECTED_PASS, None, f"Item {item_id} created, deleted, and confirmed absent")


# ---------------------------------------------------------------------------
# Scenario 4: recipe name search -> retrieval
# ---------------------------------------------------------------------------


def scenario_recipe_name_search(base_url: str, db_path: str) -> ScenarioResult:
    query = discover_real_recipe_name_query(db_path)
    recorder = ChainRecorder("Recipe Name Search -> Retrieval", real_data=query is not None)

    if query is None:
        return recorder.finish(
            STATUS_DATA_LIMITATION, SUBSYSTEM_DATA_LIMITATION_TAG,
            "No recipes found in the recipes table -- cannot select a real search query.",
        )

    recorder.record("FRONTEND INPUT", "frontend", "User searches recipes by name", {"q": query, "type": "name"})
    search_result = http_request("GET", f"{base_url}/api/recipes/search?q={urllib.parse.quote(query)}&type=name")
    recorder.record_http("SEARCH HTTP", search_result)

    if not search_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"Search failed: {search_result.status}")

    results = search_result.response_body.get("results", []) if isinstance(search_result.response_body, dict) else []
    if not results:
        return recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_RECIPE_DATA,
            f"Query {query!r} was derived from an actual recipe_name but returned zero results",
        )

    recipe_id = results[0]["recipe_id"]
    recorder.record("FRONTEND ACTION", "frontend", f"User selects recipe {recipe_id}", None)

    get_result = http_request("GET", f"{base_url}/api/recipes/{recipe_id}")
    recorder.record_http("RETRIEVAL HTTP", get_result)

    if not get_result.ok:
        return recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_API,
            f"Recipe {recipe_id} was a valid search result but retrieval returned {get_result.status}",
        )

    return recorder.finish(STATUS_EXPECTED_PASS, None, f"Search for {query!r} -> recipe {recipe_id} -> full retrieval succeeded")


# ---------------------------------------------------------------------------
# Scenario 5: recipe ingredient search (canonical + alias) -> retrieval
# ---------------------------------------------------------------------------


def scenario_recipe_ingredient_search(base_url: str, db_path: str, real_ingredient: Optional[str]) -> ScenarioResult:
    recorder = ChainRecorder("Recipe Ingredient Search -> Retrieval", real_data=real_ingredient is not None)

    if real_ingredient is None:
        return recorder.finish(
            STATUS_DATA_LIMITATION, SUBSYSTEM_DATA_LIMITATION_TAG,
            "No real ingredient identity found in recipe_ingredient_lines_parsed.",
        )

    recorder.record("FRONTEND INPUT", "frontend", "User searches recipes by ingredient", {"q": real_ingredient, "type": "ingredient"})
    search_result = http_request(
        "GET", f"{base_url}/api/recipes/search?q={urllib.parse.quote(real_ingredient)}&type=ingredient"
    )
    recorder.record_http("SEARCH HTTP", search_result)

    if not search_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"Search failed: {search_result.status}")

    results = search_result.response_body.get("results", []) if isinstance(search_result.response_body, dict) else []
    if not results:
        return recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_MATCHING,
            f"{real_ingredient!r} was sourced from real recipe_ingredient_lines_parsed data but "
            f"returned zero ingredient-search results",
        )

    match_info = {k: results[0].get(k) for k in ("matched_ingredient", "match_type", "matched_term")}
    recorder.record("MATCH INFO (from response)", "http", "matcher-derived fields on the first result", match_info)

    recipe_id = results[0]["recipe_id"]
    get_result = http_request("GET", f"{base_url}/api/recipes/{recipe_id}")
    recorder.record_http("RETRIEVAL HTTP", get_result)

    canonical_outcome = STATUS_EXPECTED_PASS if get_result.ok else STATUS_UNEXPECTED

    # Alias variant, if a real alias could be discovered (never invented).
    alias_pair = discover_real_alias()
    if alias_pair is None:
        recorder.record(
            "ALIAS VARIANT", "diagnostic",
            "No real alias could be discovered from the runtime knowledge loader "
            "(ingredient_aliases is empty, unavailable, or unimportable in this environment).",
            None,
        )
        alias_note = "alias variant: DATA LIMITATION (no real alias available)"
    else:
        alias, canonical = alias_pair
        recorder.record("FRONTEND INPUT (alias)", "frontend", "User searches using a known alias", {"q": alias, "type": "ingredient"})
        alias_search = http_request("GET", f"{base_url}/api/recipes/search?q={urllib.parse.quote(alias)}&type=ingredient")
        recorder.record_http("ALIAS SEARCH HTTP", alias_search)
        if alias_search.ok:
            alias_results = alias_search.response_body.get("results", []) if isinstance(alias_search.response_body, dict) else []
            alias_note = f"alias {alias!r} -> canonical {canonical!r}: {len(alias_results)} result(s)"
        else:
            alias_note = f"alias search HTTP failed: {alias_search.status}"

    return recorder.finish(
        canonical_outcome, None if canonical_outcome == STATUS_EXPECTED_PASS else SUBSYSTEM_API,
        f"Canonical search for {real_ingredient!r} -> recipe {recipe_id} -> retrieval; {alias_note}",
    )


# ---------------------------------------------------------------------------
# Scenario 6 & 7: inventory -> matching -> recipe retrieval
# ---------------------------------------------------------------------------


def scenario_inventory_matching(
    base_url: str, db_path: str, real_ingredient: Optional[str]
) -> Tuple[ScenarioResult, List[int]]:
    """Returns (result, created_inventory_item_ids) -- the ids are needed
    by main() for end-of-run cleanup."""
    recorder = ChainRecorder("Inventory -> Recipe Matching -> Recipe Retrieval", real_data=real_ingredient is not None)
    created_ids: List[int] = []

    if real_ingredient is None:
        result = recorder.finish(
            STATUS_DATA_LIMITATION, SUBSYSTEM_DATA_LIMITATION_TAG,
            "No real ingredient identity available to seed fridge inventory for matching.",
        )
        return result, created_ids

    fridge_result = _create_inventory_item(recorder, base_url, real_ingredient, "fridge", label_prefix="FRIDGE ")
    if not fridge_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"Fridge item creation failed: {fridge_result.status}"), created_ids
    created_ids.append(fridge_result.response_body["id"])

    pantry_candidate = discover_pantry_only_candidate(db_path, exclude_ingredient=real_ingredient)
    if pantry_candidate is not None:
        pantry_recipe_id, pantry_recipe_name, pantry_ingredient_ids = pantry_candidate
        recorder.record(
            "NOTE", "diagnostic",
            f"Seeding pantry inventory to cover all {len(pantry_ingredient_ids)} ingredient(s) of "
            f"real recipe {pantry_recipe_id} ({pantry_recipe_name!r}) to exercise pantry_only.",
            None,
        )
        for ingredient_id in pantry_ingredient_ids:
            pantry_result = _create_inventory_item(recorder, base_url, ingredient_id, "pantry", label_prefix="PANTRY ")
            if pantry_result.ok:
                created_ids.append(pantry_result.response_body["id"])
    else:
        recorder.record(
            "NOTE", "diagnostic",
            "No real recipe with 1-3 distinct ingredients was found -- pantry_only cannot be "
            "deterministically exercised with real data this run.",
            None,
        )

    recorder.record("FRONTEND ACTION", "frontend", "User requests recipes matching current inventory", None)
    matches_result = http_request("GET", f"{base_url}/api/recipes/matches")
    recorder.record_http("MATCH HTTP", matches_result)

    if not matches_result.ok:
        return recorder.finish(STATUS_HTTP_ERROR, SUBSYSTEM_API, f"GET /api/recipes/matches failed: {matches_result.status}"), created_ids

    results = matches_result.response_body.get("results", []) if isinstance(matches_result.response_body, dict) else []
    categories_seen = {r.get("match_category") for r in results}

    category_report = {
        "perfect": "OBSERVED" if "perfect" in categories_seen else "NOT AVAILABLE IN CURRENT REAL DATA",
        "imperfect": "OBSERVED" if "imperfect" in categories_seen else "NOT AVAILABLE IN CURRENT REAL DATA",
        "pantry_only": "OBSERVED" if "pantry_only" in categories_seen else "NOT AVAILABLE IN CURRENT REAL DATA",
    }
    recorder.record("CATEGORY COVERAGE", "diagnostic", "Which categories this run's real data actually exercised", category_report)

    if not results:
        result = recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_MATCHING,
            f"Inventory seeded from real ingredient {real_ingredient!r} (known to appear in recipe data) "
            f"but /api/recipes/matches returned zero results",
        )
        return result, created_ids

    # Scenario 7: retrieve one matched recipe.
    selected = results[0]
    recipe_id = selected["recipe_id"]
    recorder.record("FRONTEND ACTION", "frontend", f"User selects matched recipe {recipe_id} ({selected.get('recipe_name')!r})", None)

    get_result = http_request("GET", f"{base_url}/api/recipes/{recipe_id}")
    recorder.record_http("RETRIEVAL HTTP", get_result)

    if not get_result.ok:
        result = recorder.finish(
            STATUS_UNEXPECTED, SUBSYSTEM_API,
            f"Recipe {recipe_id} was returned by /api/recipes/matches but retrieval returned {get_result.status}",
        )
        return result, created_ids

    result = recorder.finish(
        STATUS_EXPECTED_PASS, None,
        f"{len(results)} match(es); categories this run: {category_report}; "
        f"selected recipe {recipe_id} retrieved successfully",
    )
    return result, created_ids


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def cleanup_created_inventory(base_url: str, item_ids: List[int]) -> None:
    if not item_ids:
        return
    print("\n" + "=" * 60)
    print("CLEANUP")
    print("=" * 60)
    for item_id in item_ids:
        result = http_request("DELETE", _inventory_url(base_url, item_id))
        status = "deleted" if result.status == 204 else f"FAILED ({result.status})"
        print(f"DELETE /api/inventory/{item_id} -> {status}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _extract_internal_item_id(result: ScenarioResult) -> Optional[int]:
    for step in result.steps:
        if step.label == "_item_id":
            return step.detail
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Running Gastrometric API base URL")
    parser.add_argument("--db-path", default=None, help="Path to the SQLite database (defaults to the configured DB_PATH)")
    parser.add_argument("--json-out", default=None, help="Optional path to write a machine-readable JSON trace")
    parser.add_argument("--keep-data", action="store_true", help="Do not delete inventory items this run created")
    args = parser.parse_args()

    db_path = args.db_path or _CONFIGURED_DB_PATH
    if not db_path:
        print("ERROR: no --db-path given and gastrometric.config.paths.DB_PATH could not be imported.", file=sys.stderr)
        return 2
    # DB_PATH may be a pathlib.Path (matching the rest of the codebase's
    # `str(db_path or DB_PATH)` convention, e.g. inventory_repository.py)
    # rather than already a str -- every function in this file is typed
    # `db_path: str`, so coerce once here rather than at each call site.
    db_path = str(db_path)

    if not table_exists(db_path, "inventory_items") or not table_exists(db_path, "recipes"):
        print(f"ERROR: {db_path} does not look like an initialized Gastrometric database "
              f"(inventory_items/recipes table missing). Run rebuild_db first.", file=sys.stderr)
        return 2

    ping = http_request("GET", f"{args.base_url}/api/inventory")
    if not ping.ok:
        print(
            f"ERROR: could not reach {args.base_url}/api/inventory "
            f"(status={ping.status}, error={ping.error}). Is the API running?",
            file=sys.stderr,
        )
        return 2

    real_ingredient = discover_real_recipe_ingredient(db_path)
    real_ingredient_alt = discover_second_real_recipe_ingredient(db_path, exclude=real_ingredient)

    results: List[ScenarioResult] = []
    created_inventory_ids: List[int] = []

    r1 = scenario_create_inventory(args.base_url, db_path, real_ingredient)
    results.append(r1)
    item_id = _extract_internal_item_id(r1)
    if item_id is not None:
        created_inventory_ids.append(item_id)

    r2 = scenario_update_inventory(args.base_url, db_path, real_ingredient, real_ingredient_alt)
    results.append(r2)
    item_id = _extract_internal_item_id(r2)
    if item_id is not None:
        created_inventory_ids.append(item_id)

    results.append(scenario_delete_inventory(args.base_url, db_path, real_ingredient))
    results.append(scenario_recipe_name_search(args.base_url, db_path))
    results.append(scenario_recipe_ingredient_search(args.base_url, db_path, real_ingredient))

    r6, matching_ids = scenario_inventory_matching(args.base_url, db_path, real_ingredient)
    results.append(r6)
    created_inventory_ids.extend(matching_ids)

    if not args.keep_data:
        cleanup_created_inventory(args.base_url, created_inventory_ids)
    else:
        print(f"\n--keep-data set: leaving {len(created_inventory_ids)} inventory item(s) in place: {created_inventory_ids}")

    print("\n" + "=" * 60)
    print("RUN SUMMARY")
    print("=" * 60)
    exit_code = 0
    for result in results:
        marker = "synthetic" if not result.real_data else "real-data"
        print(f"{result.name:45s} {result.outcome:25s} ({marker})")
        if result.outcome not in (STATUS_EXPECTED_PASS, STATUS_EXPECTED_EMPTY, STATUS_DATA_LIMITATION):
            exit_code = 1

    if args.json_out:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "base_url": args.base_url,
            "db_path": db_path,
            "scenarios": [
                {
                    "scenario": r.name,
                    "status": r.outcome,
                    "real_data": r.real_data,
                    "subsystem": r.subsystem,
                    "summary": r.summary,
                    "steps": [
                        {
                            "step": s.number,
                            "boundary": s.boundary,
                            "operation": s.operation,
                            "detail": s.detail,
                            "status": s.status,
                        }
                        for s in r.steps
                        if s.boundary != "internal"
                    ],
                }
                for r in results
            ],
        }
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=_json_default)
        print(f"\nMachine-readable trace written to {args.json_out}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())