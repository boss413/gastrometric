"""
Gastrometric HTTP API application entry point (BE-03: inventory; BE-05:
recipe read).

ASSUMPTION FLAGGED FOR REVIEW: this creates a new, standalone FastAPI
application because I don't have visibility into whether the repository
already has an HTTP framework or app entry point elsewhere. Per the work
order: "If an existing framework/application entry point is present,
extend it" -- if one exists, this file's routers (`inventory_router`,
`recipe_router`) should be mounted into that existing app instead of
running as its own process, and this file's CORS/exception-handler setup
merged with whatever already exists there rather than duplicated.

--------------------------------------------------------------------
Running for local/LAN development
--------------------------------------------------------------------

Requires `fastapi` and an ASGI server (`uvicorn` used below) as
dependencies -- neither confirmed present in the repository's existing
dependency list; please add them if they aren't already there.

From the repository root:

    uvicorn gastrometric.api.api_app:app --host 0.0.0.0 --port 8000

or simply:

    python -m gastrometric.api.api_app

Binding to `0.0.0.0` (not `127.0.0.1`) is what makes the server
reachable from a phone on the same LAN, at:

    http://<desktop-LAN-IP>:8000/api/inventory
    http://<desktop-LAN-IP>:8000/api/recipes/2

Find `<desktop-LAN-IP>` with `ipconfig` (Windows) or `ifconfig`/`ip a`
(macOS/Linux) -- whichever address is on the same local network as the
phone, not `127.0.0.1` or `localhost`.

--------------------------------------------------------------------
CORS
--------------------------------------------------------------------

Configured permissively (`allow_origins=["*"]`) for this development
PoC, since the phone browser's origin (wherever the frontend ends up
being served from during development) isn't fixed yet and will likely
differ from this API's own origin. This is intentionally NOT a
production-appropriate configuration -- if the frontend later ends up
served from this same FastAPI app/origin, this permissive CORS
middleware should be removed rather than left in "just in case."
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from gastrometric.api.api_errors import register_exception_handlers
from gastrometric.api.inventory_routes import router as inventory_router
from gastrometric.api.recipe_routes import router as recipe_router

# Title left unchanged from BE-03 -- the work order explicitly says
# renaming it (e.g. to "Gastrometric API") is acceptable but not
# required, and the smallest change satisfying BE-05 is to leave it as
# it was.
app = FastAPI(title="Gastrometric Inventory API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception_handlers(app)

app.include_router(inventory_router)
app.include_router(recipe_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)