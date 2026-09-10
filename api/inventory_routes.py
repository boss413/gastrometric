"""
Inventory HTTP routes.

Architecture this module must not violate:

    API route (this module)
        -> application.inventory_editor
            -> orchestration.inventory_ingredient_orchestrator
                -> repository / understanding

Every route handler below does exactly: parse the request (via the
Pydantic models FastAPI already validated), call one
`inventory_editor` function, shape the result into a response model, and
translate `inventory_editor`'s exceptions/None-returns into the API's
error contract. Nothing here calls `lex()`, `IngredientParser`,
`analyze_parse_result()`, the orchestrator, or the repository directly,
derives `ingredient_id`, computes `resolution_status`, decides whether
re-understanding is needed, or touches SQLite.
"""

from typing import Any, Dict, Optional, Union

from fastapi import APIRouter, Depends

from gastrometric.api.api_dependencies import get_db_path
from gastrometric.api.api_errors import not_found_error, validation_error
from gastrometric.api.inventory_models import (
    CreateInventoryItemRequest,
    ErrorResponse,
    InventoryItemResponse,
    InventoryListResponse,
    UpdateInventoryItemRequest,
)
from gastrometric.application.inventory_editor import (
    InventoryValidationError,
    create_inventory_item,
    delete_inventory_item,
    list_inventory_items,
    update_inventory_item,
)

router = APIRouter(prefix="/api")

# Explicitly typed to match FastAPI's `responses` parameter
# (Dict[int | str, Dict[str, Any]]) exactly. Without this annotation,
# type checkers infer Dict[int, Dict[str, Type[ErrorResponse]]] from the
# literal below -- and since dict key/value types are invariant, that
# inferred type isn't assignable to the broader parameter type even
# though every concrete key/value pair is compatible with it.
_ERROR_RESPONSES: Dict[Union[int, str], Dict[str, Any]] = {
    400: {"model": ErrorResponse},
    404: {"model": ErrorResponse},
    422: {"model": ErrorResponse},
    500: {"model": ErrorResponse},
}


def _provided_fields(payload: UpdateInventoryItemRequest) -> Dict[str, Any]:
    """Returns only the fields actually present in the PATCH request
    body, distinguishing "field omitted" from "field explicitly set to
    null" via `exclude_unset` -- an omitted field must retain its
    current value (inventory_editor's `_UNSET` sentinel), while an
    explicit `null` is passed through as a real `None` for
    inventory_editor's existing validation to accept or reject on its
    own terms. This module does not add its own opinion about which
    explicit nulls are valid.
    """
    if hasattr(payload, "model_dump"):
        return payload.model_dump(exclude_unset=True)
    return payload.dict(exclude_unset=True)  # pydantic v1 fallback


@router.get("/inventory", response_model=InventoryListResponse)
def get_inventory(db_path: Optional[str] = Depends(get_db_path)) -> InventoryListResponse:
    """GET /api/inventory -- all inventory observations, ordered
    however `inventory_editor.list_inventory_items` already orders them
    (no ordering rule invented here).
    """
    items = list_inventory_items(db_path=db_path)
    return InventoryListResponse(items=[InventoryItemResponse(**item) for item in items])


@router.post(
    "/inventory",
    response_model=InventoryItemResponse,
    status_code=201,
    responses=_ERROR_RESPONSES,
)
def post_inventory(
    payload: CreateInventoryItemRequest,
    db_path: Optional[str] = Depends(get_db_path),
) -> InventoryItemResponse:
    """POST /api/inventory -- creates one inventory observation from raw
    human text. `ingredient_id`, `resolution_status`, and
    `analysis_result` are never accepted from the client; they come back
    from `create_inventory_item`, which is where the real understanding
    happens.
    """
    try:
        item = create_inventory_item(
            payload.original_input,
            payload.location,
            quantity=payload.quantity,
            unit=payload.unit,
            db_path=db_path,
        )
    except InventoryValidationError as exc:
        raise validation_error(str(exc)) from exc
    return InventoryItemResponse(**item)


@router.patch(
    "/inventory/{item_id}",
    response_model=InventoryItemResponse,
    responses=_ERROR_RESPONSES,
)
def patch_inventory(
    item_id: int,
    payload: UpdateInventoryItemRequest,
    db_path: Optional[str] = Depends(get_db_path),
) -> InventoryItemResponse:
    """PATCH /api/inventory/{id} -- updates whichever fields were
    actually present in the request body. Whether changing a field
    triggers re-understanding is entirely `inventory_editor.update_inventory_item`'s
    decision (specifically: only when `original_input` changed) -- this
    route does not duplicate that rule.
    """
    provided = _provided_fields(payload)
    try:
        item = update_inventory_item(item_id, db_path=db_path, **provided)
    except InventoryValidationError as exc:
        raise validation_error(str(exc)) from exc
    if item is None:
        raise not_found_error("inventory_item", item_id)
    return InventoryItemResponse(**item)


@router.delete("/inventory/{item_id}", status_code=204, responses=_ERROR_RESPONSES)
def delete_inventory(item_id: int, db_path: Optional[str] = Depends(get_db_path)) -> None:
    """DELETE /api/inventory/{id}. A missing ID produces a 404, not a
    silent no-op and not a 500.
    """
    deleted = delete_inventory_item(item_id, db_path=db_path)
    if not deleted:
        raise not_found_error("inventory_item", item_id)
    return None