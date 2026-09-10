"""
Typed request/response models for the inventory HTTP API.

These describe the transport shape only. `analysis_result` is kept as a
generic `Dict[str, Any]` deliberately -- the analyzer result is
intentionally extensible (interpretations, references, evidence, scores,
future fields), and this API's job is to transport it unchanged, not to
understand or constrain its internal structure. See
`inventory_ingredient_understanding.py` / `inventory_ingredient_orchestrator.py`
for the actual semantics.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class CreateInventoryItemRequest(BaseModel):
    """Body for POST /api/inventory. Represents exactly what the user
    typed -- ingredient_id, resolution_status, and analysis_result are
    backend-derived and must never be accepted from the client.
    """

    original_input: str
    location: str
    quantity: Optional[str] = None
    unit: Optional[str] = None


class UpdateInventoryItemRequest(BaseModel):
    """
    Body for PATCH /api/inventory/{id}. All fields optional -- only
    fields actually present in the request body are applied (see
    `inventory_routes._provided_fields`, which uses
    `exclude_unset` to distinguish "field omitted" from "field
    explicitly set to null").

    Whether setting a field to an explicit `null` is accepted or
    rejected is entirely up to `inventory_editor.update_inventory_item`'s
    existing validation (e.g. an explicit `null` for `location` fails
    application validation, since `None` is not a valid location) -- this
    model does not add its own opinion about that.
    """

    original_input: Optional[str] = None
    location: Optional[str] = None
    quantity: Optional[str] = None
    unit: Optional[str] = None


class InventoryItemResponse(BaseModel):
    """
    The application-facing representation of one inventory observation --
    matches the dict shape `inventory_editor`'s functions already return
    (`_record_to_item`), field for field. `analysis_result` is the
    complete, unmodified analyzer result, already parsed from
    `analysis_result_json` by the application layer; this model does not
    re-parse or reduce it further.
    """

    id: int
    original_input: str
    ingredient_id: Optional[str] = None
    location: str
    quantity: Optional[str] = None
    unit: Optional[str] = None
    resolution_status: str
    analysis_result: Dict[str, Any]
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class InventoryListResponse(BaseModel):
    items: List[InventoryItemResponse]


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    """
    {"error": {"code": "...", "message": "..."}}

    Used both to document the error shape in the OpenAPI schema (see
    `responses=` on the routes in inventory_routes.py) and as the
    conceptual contract `errors.ApiError`'s exception handler serializes
    to directly.
    """

    error: ErrorDetail