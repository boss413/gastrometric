"""
API-level tests for the inventory HTTP API (BE-03).

Tests go through HTTP (FastAPI's TestClient), never calling
inventory_editor/orchestrator/repository functions directly -- that's
what proves the route wiring itself, not just the layers underneath it,
which are already covered by test_inventory_ingredient_orchestrator.py
and test_inventory_editor_integration.py.

Uses a temporary on-disk SQLite database for every test, injected via
`app.dependency_overrides[get_db_path]` -- never the development
database.
"""

import sqlite3
from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from gastrometric.api.api_app import app
from gastrometric.api.api_dependencies import get_db_path

RECIPE_ONLY_TABLES = [
    "lexical_spans",
    "ingredient_parse_trees",
    "analysis_records",
    "analysis_candidate_evaluations",
    "analysis_evidence",
]


def _build_schema(db_path: Path) -> None:
    """Builds the full development schema at `db_path` by monkeypatching
    DB_PATH and calling the real, confirmed-no-argument `init_db()` --
    never the development database.
    """
    import gastrometric.db.init_db as init_db_module
    from gastrometric.config import paths as paths_module

    original = paths_module.DB_PATH
    paths_module.DB_PATH = str(db_path)
    init_db_module.DB_PATH = str(db_path)
    try:
        init_db_module.init_db()
    finally:
        paths_module.DB_PATH = original
        init_db_module.DB_PATH = original


@pytest.fixture()
def db_path(tmp_path: Path) -> Iterator[str]:
    path = tmp_path / "test_gastrometric.db"
    _build_schema(path)
    yield str(path)


@pytest.fixture()
def client(db_path: str) -> Iterator[TestClient]:
    app.dependency_overrides[get_db_path] = lambda: db_path
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.pop(get_db_path, None)


def _table_count(db_path: str, table_name: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/inventory
# ---------------------------------------------------------------------------


def test_get_empty_inventory(client):
    response = client.get("/api/inventory")
    assert response.status_code == 200
    assert response.json() == {"items": []}


def test_get_returns_structured_analysis_result_not_a_string(client):
    client.post("/api/inventory", json={"original_input": "cabbage", "location": "fridge"})

    response = client.get("/api/inventory")
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert isinstance(items[0]["analysis_result"], dict)
    assert {"status", "interpretations", "selected_interpretation"}.issubset(
        items[0]["analysis_result"].keys()
    )


def test_get_returns_multiple_fridge_and_pantry_observations(client):
    client.post("/api/inventory", json={"original_input": "cabbage", "location": "fridge"})
    client.post("/api/inventory", json={"original_input": "cabbage", "location": "pantry"})

    response = client.get("/api/inventory")
    items = response.json()["items"]
    assert len(items) == 2
    locations = {item["location"] for item in items}
    assert locations == {"fridge", "pantry"}


# ---------------------------------------------------------------------------
# POST /api/inventory
# ---------------------------------------------------------------------------


def test_post_cabbage_resolves(client):
    response = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["ingredient_id"] == "cabbage"
    assert body["resolution_status"] == "resolved"
    assert body["original_input"] == "cabbage"
    assert body["location"] == "fridge"


def test_post_quantified_cabbage_resolves_and_preserves_quantity_semantics(client):
    response = client.post(
        "/api/inventory", json={"original_input": "2 heads cabbage", "location": "fridge"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["ingredient_id"] == "cabbage"
    assert body["resolution_status"] == "resolved"

    result = body["analysis_result"]
    interpretation = next(
        i for i in result["interpretations"] if i["id"] == result["selected_interpretation"]
    )
    assert "2" in str(interpretation["references"])


def test_post_purple_cabbage_thing_unresolved_but_ingredient_identified(client):
    response = client.post(
        "/api/inventory", json={"original_input": "purple cabbage thing", "location": "fridge"}
    )
    assert response.status_code == 201
    body = response.json()

    # The distinction this whole architecture exists to preserve.
    assert body["ingredient_id"] == "cabbage"
    assert body["resolution_status"] == "unresolved"

    serialized = str(body["analysis_result"])
    assert "purple" in serialized
    assert "thing" in serialized


def test_post_missing_required_field_is_malformed_request(client):
    response = client.post("/api/inventory", json={"location": "fridge"})
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "malformed_request"


def test_post_invalid_location_is_validation_error(client):
    response = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "counter"}
    )
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "validation_error"


# ---------------------------------------------------------------------------
# PATCH /api/inventory/{id}
# ---------------------------------------------------------------------------


def test_patch_changing_original_input_causes_reunderstanding(client):
    created = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    ).json()

    response = client.patch(
        f"/api/inventory/{created['id']}",
        json={"original_input": "purple cabbage thing"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ingredient_id"] == "cabbage"
    assert body["resolution_status"] == "unresolved"
    assert body["analysis_result"] != created["analysis_result"]


def test_patch_changing_only_location_does_not_change_analysis_result(client):
    created = client.post(
        "/api/inventory", json={"original_input": "purple cabbage thing", "location": "fridge"}
    ).json()

    response = client.patch(f"/api/inventory/{created['id']}", json={"location": "pantry"})
    assert response.status_code == 200
    body = response.json()
    assert body["location"] == "pantry"
    assert body["ingredient_id"] == created["ingredient_id"]
    assert body["resolution_status"] == created["resolution_status"]
    assert body["analysis_result"] == created["analysis_result"]


def test_patch_changing_only_quantity_does_not_change_analysis_result(client):
    created = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    ).json()

    response = client.patch(f"/api/inventory/{created['id']}", json={"quantity": "3"})
    assert response.status_code == 200
    body = response.json()
    assert body["quantity"] == "3"
    assert body["analysis_result"] == created["analysis_result"]


def test_patch_changing_only_unit_does_not_change_analysis_result(client):
    created = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    ).json()

    response = client.patch(f"/api/inventory/{created['id']}", json={"unit": "heads"})
    assert response.status_code == 200
    body = response.json()
    assert body["unit"] == "heads"
    assert body["analysis_result"] == created["analysis_result"]


def test_patch_unknown_id_is_not_found(client):
    response = client.patch("/api/inventory/999999", json={"location": "pantry"})
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


def test_patch_partial_update_combines_correctly(client):
    created = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    ).json()

    response = client.patch(
        f"/api/inventory/{created['id']}", json={"location": "pantry", "quantity": "5"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["location"] == "pantry"
    assert body["quantity"] == "5"
    assert body["unit"] == created["unit"]
    assert body["analysis_result"] == created["analysis_result"]


def test_patch_explicit_null_quantity_clears_it(client):
    created = client.post(
        "/api/inventory",
        json={"original_input": "cabbage", "location": "fridge", "quantity": "2"},
    ).json()
    assert created["quantity"] == "2"

    response = client.patch(f"/api/inventory/{created['id']}", json={"quantity": None})
    assert response.status_code == 200
    assert response.json()["quantity"] is None


def test_patch_explicit_null_location_is_rejected_as_validation_error(client):
    """An explicit null for a NOT NULL, constrained field (location) is
    not silently ignored and not treated the same as omitting it -- it's
    passed through to inventory_editor's existing validation, which
    correctly rejects None as not one of the valid locations.
    """
    created = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    ).json()

    response = client.patch(f"/api/inventory/{created['id']}", json={"location": None})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_error"


# ---------------------------------------------------------------------------
# DELETE /api/inventory/{id}
# ---------------------------------------------------------------------------


def test_delete_existing_item(client):
    created = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    ).json()

    response = client.delete(f"/api/inventory/{created['id']}")
    assert response.status_code == 204

    remaining = client.get("/api/inventory").json()["items"]
    assert all(item["id"] != created["id"] for item in remaining)


def test_delete_unknown_id_is_not_found(client):
    response = client.delete("/api/inventory/999999")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


def test_delete_does_not_affect_other_items(client):
    first = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "fridge"}
    ).json()
    second = client.post(
        "/api/inventory", json={"original_input": "cabbage", "location": "pantry"}
    ).json()

    client.delete(f"/api/inventory/{first['id']}")

    remaining = client.get("/api/inventory").json()["items"]
    assert len(remaining) == 1
    assert remaining[0]["id"] == second["id"]


# ---------------------------------------------------------------------------
# Architectural test: no recipe-analysis persistence via the API
# ---------------------------------------------------------------------------


def _table_exists(db_path: str, table_name: str) -> bool:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table_name,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def test_inventory_api_does_not_write_recipe_specific_tables(client, db_path):
    # Asserts the recipe-only tables actually exist in the temp DB first,
    # so a future schema change that removed them would fail loudly here
    # rather than this test passing vacuously against tables that were
    # never there to write to.
    existing_recipe_tables = [t for t in RECIPE_ONLY_TABLES if _table_exists(db_path, t)]
    assert existing_recipe_tables, "expected init_db() to create the recipe-only tables"

    counts_before = {t: _table_count(db_path, t) for t in existing_recipe_tables}

    client.post("/api/inventory", json={"original_input": "cabbage", "location": "fridge"})
    client.post("/api/inventory", json={"original_input": "2 heads cabbage", "location": "fridge"})
    client.post(
        "/api/inventory", json={"original_input": "purple cabbage thing", "location": "fridge"}
    )

    for table, count_before in counts_before.items():
        assert _table_count(db_path, table) == count_before