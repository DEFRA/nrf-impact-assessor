---
name: geospatial-python-fastapi
description: Use when changing Python, FastAPI, SQLAlchemy, PostGIS, GeoAlchemy2, GeoPandas, Shapely, Alembic, CRS handling, spatial APIs, geometry file upload, tiles, boundary checks, GCN, nutrient assessment, or geospatial tests in nrf-impact-assessor.
---

# Geospatial Python FastAPI

## First Reads

Read these project rules before editing geospatial code:

- `.ai/rules/python-fastapi.md`
- `.ai/rules/geospatial.md`

Use repository examples as the source of truth:

- `app/models/db.py` for SQLAlchemy 2 + GeoAlchemy model style.
- `app/boundary/router.py`, `app/wwtw/router.py`, and `app/tiles/router.py` for API boundary and CRS patterns.
- `app/repositories/repository.py` for PostGIS query patterns.
- `scripts/load_data.py` for batch GeoPandas ingestion.

## API Endpoint Checklists

### Geometry endpoints (`boundary`, `wwtw`)

- Validate file type, size, CRS, bbox, coordinate order, and required shapefile companions.
- Reject unsupported CRS values with client-facing errors.
- Document expected geometry format: GeoJSON geometry, Feature, WKT, bbox, tile coordinates, or lon/lat pair.
- Constrain response sizes with limits, pagination, tiling, simplification, or bounded queries.
- Log SRID, bbox/tile, layer name, feature count, and timing when useful. Do not log full geometry payloads.

### Tile endpoints (`tiles`)

- Validate z/x/y are within sensible bounds before querying PostGIS.
- Tile geometries are served in EPSG:3857 (`ST_Transform(..., 3857)`) — do not change CRS without updating the client contract.
- Check the LRU tile cache before querying; include layer version in the cache key to avoid stale tiles after data updates.
- Keep MVT queries bounded — tile queries must not return unbounded feature sets.

## Migrations

Migration files live under `alembic/versions/` in this repo.

## Testing

- Test harness distinction: `tests/integration/` runs against a temporary `test_nrf_impact` DB (Alembic applies migrations on each run); `tests/regression/` runs against the production `nrf_impact` DB. Pick the right harness — do not add regression-style tests to integration and vice versa.
- Prefer existing fixtures under `tests/data/fixtures/` and helpers in `tests/unit/spatial/`, `tests/unit/api/`, and `tests/integration/`.
- Run the smallest relevant test set first, then broader tests when shared spatial behaviour changes.
