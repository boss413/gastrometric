"""
API-level error contract for the inventory HTTP API.

Establishes one JSON error shape for every API-level error:

    {"error": {"code": "...", "message": "..."}}

`ApiError` is the one exception type route handlers raise for
domain-level problems (validation failures, not-found). It deliberately
does NOT use FastAPI's built-in `HTTPException`, whose default handler
wraps whatever `detail` you pass under a top-level `"detail"` key
(`{"detail": ...}`) -- that would not match the shape above without
either accepting the extra wrapper or writing a custom handler anyway,
so `ApiError` + a dedicated handler is more direct.

Four categories are distinguished, matching the work order:
    * malformed request      -> RequestValidationError (Pydantic/FastAPI
                                 request parsing) -> 422, code
                                 "malformed_request"
    * validation failure      -> InventoryValidationError from the
                                 application layer -> ApiError(400,
                                 "validation_error", ...) -> 400
    * inventory item not found -> ApiError(404, "not_found", ...) -> 404
    * unexpected server error -> any other Exception -> 500, code
                                 "internal_error", generic message; the
                                 real exception is logged, never returned
                                 to the client.
"""

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class ApiError(Exception):
    """Raised by route handlers for domain-level HTTP errors (validation
    failures translated from the application layer, or not-found). Not
    used for unexpected/internal errors -- those fall through to the
    generic `Exception` handler instead, so a bug can never accidentally
    present itself as a clean, expected API error.
    """

    def __init__(self, status_code: int, code: str, message: str):
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(message)


def not_found_error(resource: str, resource_id: Any) -> ApiError:
    return ApiError(404, "not_found", f"{resource} {resource_id!r} was not found.")


def validation_error(message: str) -> ApiError:
    """Wraps an application-layer `InventoryValidationError`'s message.
    That message is already a clean, user-facing domain message (e.g.
    "location must be one of ('fridge', 'pantry'), got 'counter'") --
    not a traceback or internal detail -- so it's safe to pass through
    unchanged rather than needing to be re-authored at the HTTP layer.
    """
    return ApiError(400, "validation_error", message)


def register_exception_handlers(app: FastAPI) -> None:
    """Registers all four error-category handlers on `app`. Call this
    once from `app.py` when constructing the application.
    """

    @app.exception_handler(ApiError)
    async def handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # Deliberately does not include exc.errors() verbatim in the
        # response: FastAPI's default validation error detail can include
        # internal type/location info that isn't useful to the frontend
        # and edges toward "implementation detail" per the work order's
        # error contract. A stable, generic message is enough for a PoC;
        # the underlying detail is still visible in server logs via
        # FastAPI's own request logging if enabled.
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "malformed_request",
                    "message": "The request body is missing required fields or has invalid types.",
                }
            },
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        # NOTE: uses the standard library `logging` module. I don't have
        # visibility into whether this repository has an established
        # logging convention (a configured logger, structured logging,
        # etc.) -- please point me at it if one exists so this can be
        # switched to match rather than introducing a second convention.
        logger.exception("Unhandled error in inventory API: %s", request.url.path)
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "An unexpected error occurred.",
                }
            },
        )