---
description: Python and FastAPI development standards for this project
globs: ["**/*.py"]
alwaysApply: true
---

# Python & FastAPI Standards

## Code Style

- Write concise, technically precise Python with accurate type hints on all function signatures.
- Target Python 3.13 and use modern standard-library typing syntax.
- Prefer functional, declarative patterns; avoid classes unless they provide clear value.
- Favour modularisation and iteration over duplication.
- Name variables descriptively using auxiliary verbs: `is_active`, `has_permission`, `should_retry`.
- Use lowercase with underscores for all new Python file and directory names.
- Apply the RORO pattern (Receive an Object, Return an Object) for non-trivial function boundaries.
- Follow `.ruff.toml` for formatting, import ordering, lint rules, and ignored rules.

## Functions and Async

- Use `def` for pure synchronous functions and for sync database/geospatial repository code.
- Use `async def` for FastAPI handlers only when they await request/body parsing, async clients, or background orchestration.
- Do not block the event loop from `async def`; offload CPU-bound, geospatial, sync SQLAlchemy, or other blocking work with `asyncio.to_thread()` or move it behind a worker.
- Prefer Pydantic models over raw `dict` for API input validation and response schemas.
- Keep route handlers thin — delegate logic to service or utility functions.
- Keep GeoDataFrame, CRS, PostGIS, and assessment business logic outside route handlers where practical.

## Module Layout

Organise new modules in the repo's existing style. For FastAPI router modules, prefer this order:

1. Imports
2. Logger and `APIRouter`
3. Configuration/constants
4. Pydantic request/response models
5. Repository or dependency helpers
6. Supporting utilities
7. Route handlers

## Error Handling

- Handle errors and edge cases at the top of functions — validate early, return early.
- Use guard clauses rather than deeply nested conditionals.
- Avoid `else` after a `return`; let the happy path fall through naturally.
- Raise `HTTPException` for expected, client-facing errors with meaningful detail messages.
- Handle unexpected errors in middleware — not inside route handlers.
- Log errors with enough context to diagnose without exposing internals to the caller.
- Avoid returning raw exception text from public endpoints unless the route is explicitly test/debug-only.

## FastAPI Conventions

- Define routes with explicit return type annotations.
- Use FastAPI's dependency injection for shared state, DB sessions, and auth.
- Prefer `lifespan` context managers over deprecated `@app.on_event("startup/shutdown")`.
- Apply middleware for cross-cutting concerns: logging, error monitoring, request tracing.
- Use `Pydantic BaseModel` consistently for request bodies and response schemas.
- Use `Annotated[..., Query/Form/Body]` for endpoint parameter metadata.
- Set `response_model` on public endpoints and use `response_model_by_alias=True` when API responses require aliases.
- Keep test-only endpoints behind existing test configuration gates.

## Performance

- Never block the event loop from async request handlers.
- This project currently uses sync SQLAlchemy/PostGIS and geospatial libraries; do not convert to async DB access unless the architecture and driver stack are changed intentionally.
- Cache static or frequently read data (Redis or in-memory) where latency matters.
- Use lazy loading for large datasets; avoid pulling unnecessary columns or rows.
- Optimise serialisation — let Pydantic handle validation rather than manual dict manipulation.
- Do not expand module-level mutable state for durable production workflows. In-memory state is only acceptable when bounded, tested, and safe across restarts/workers.

## Dependencies

- **FastAPI** — web framework
- **Pydantic v2** — validation and serialisation
- **Pydantic Settings** — environment-backed configuration
- **SQLAlchemy 2.0** — sync ORM/database access in the current architecture
- **psycopg2** — PostgreSQL driver
- **GeoPandas / GeoAlchemy2** — geospatial processing and PostGIS integration
- **uv** — dependency and task execution
- **Ruff** — formatting and linting
- **pytest** — test runner

## Pydantic v2

- Use `model_config` for Pydantic model configuration.
- Use `Field(...)` for aliases, validation constraints, examples, and descriptions.
- Use `model_dump()` / `model_validate()` instead of Pydantic v1 APIs.
- Preserve external camelCase aliases where backend/API contracts require them.

## Testing

- Add or update focused pytest coverage for API behaviour, validation, error paths, and geospatial edge cases.
- Prefer existing test fixtures and helper patterns under `tests/`.
- Run the smallest relevant test set first, then broader tests when shared behaviour changes.

## Key Priorities

1. Correctness first — edge cases and errors handled explicitly.
2. Readability — code should be self-documenting through naming, not comments.
3. Performance — keep async request paths responsive and offload blocking work deliberately.
4. Maintainability — small, focused functions; clear dependency boundaries.
