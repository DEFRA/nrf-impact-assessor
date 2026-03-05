# Integration Tests

This directory contains integration tests for the PostGIS Repository layer.

## Overview

Integration tests validate that the Repository correctly interacts with a live PostGIS database using **minimal test fixtures**.

- **Database**: `test_nrf_impact` (automatically created/destroyed)
- **Data**: Small sample data loaded via fixtures
- **Speed**: Fast (<5 seconds)
- **Mark**: `@pytest.mark.integration`

## Test Structure

```
tests/integration/
├── README.md           # This file
├── conftest.py         # Test fixtures (database, sample data)
└── test_repository.py  # Repository query tests
```

## Running Integration Tests

```bash
# Run integration tests only
uv run pytest -m integration

# Run with verbose output
uv run pytest -m integration -v

# Run specific test file
uv run pytest tests/integration/test_repository.py
```

## Prerequisites

1. **Docker installed**
2. **PostgreSQL with PostGIS running**

```bash
# Start PostgreSQL
docker compose up -d

# Verify it's running
docker compose ps
```

## Test Database

Integration tests use a separate test database (`test_nrf_impact`) that is:
- Created automatically at session start
- Populated with minimal sample data (3 polygons, 2 catchments, etc.)
- Truncated between tests for isolation
- Destroyed after all tests complete

**No manual setup required** - fixtures handle everything.

## What's Tested

- Basic query operations (`select()`, `where()`, `order_by()`)
- GeoDataFrame conversion (`as_gdf=True`)
- ORM object queries (`as_gdf=False`)
- Spatial queries (`ST_Intersects`)
- JSONB data access (lookup tables)
- Session management and context managers
- Version filtering

## Fixtures

Available in `conftest.py`:

- `test_engine` - SQLAlchemy engine for test database (session-scoped)
- `repository` - Repository instance with clean tables (function-scoped)
- `sample_coefficient_data` - 3 sample coefficient polygons
- `sample_spatial_data` - 2 sample NN catchment boundaries
- `sample_lookup_data` - Small WwTW lookup table

## Comparison with Regression Tests

| Aspect | Integration Tests | Regression Tests |
|--------|------------------|------------------|
| **Location** | `tests/integration/` | `tests/regression/` |
| **Mark** | `@pytest.mark.integration` | `@pytest.mark.regression` |
| **Database** | `test_nrf_impact` (test DB) | `nrf_impact` (production DB) |
| **Data** | Small fixtures (~10 rows) | Full dataset (~5.4M polygons) |
| **Setup** | Automatic (fixtures) | Manual (`load_data.py`) |
| **Speed** | Fast (<5s) | Slow (~3 min) |
| **Purpose** | Test Repository layer | Test end-to-end results |
| **Run** | `uv run pytest -m integration` | `uv run pytest -m regression` |

## Troubleshooting

### Database connection fails
```bash
# Check PostgreSQL is running
docker compose ps

# Check connection
psql postgresql://postgres@localhost:5432/postgres -c "SELECT version();"
```

### Tests fail with "database does not exist"
The test database is created automatically. If you see this error:
1. Ensure PostgreSQL is running
2. Check that the postgres user has CREATE DATABASE permission
3. Try dropping the test database manually: `dropdb test_nrf_impact`

### PostGIS extension not available
```bash
# Connect to postgres and verify PostGIS
psql postgresql://postgres@localhost:5432/postgres -c "SELECT * FROM pg_available_extensions WHERE name = 'postgis';"
```
