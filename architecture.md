# Architecture

## Stack (initial)
- Python
- SQLite
- No framework (yet) OR FastAPI later

## Structure
- src/ = application code
- data/ = database + seeds
- tests/ = test suite

## Database
SQLite file stored at:
data/gastrometric.db

## Execution
Scripts in /scripts will:
- initialize DB
- run migrations (manual for now)