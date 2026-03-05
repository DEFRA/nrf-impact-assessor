# Regression Tests

This directory contains end-to-end regression tests that validate the PostGIS implementation produces identical results to the legacy file-based system.

## Overview

Regression tests compare PostGIS-based assessment outputs against known-good baseline CSV files from the legacy script.

- **Database**: `nrf_impact` (production database)
- **Data**: Full reference dataset (~5.4M coefficient polygons)
- **Speed**: Slow (~3 minutes per test)
- **Mark**: `@pytest.mark.regression`

## Test Structure

```
tests/regression/
├── README.md              # This file
├── conftest.py            # Test fixtures (repository, tolerance)
└── test_regression.py     # End-to-end regression tests
```

## Running Regression Tests

```bash
# Run regression tests only
uv run pytest -m regression

# Run with verbose output
uv run pytest -m regression -v

# Run specific test
uv run pytest tests/regression/test_regression.py::test_postgis_single_site_assessment
```

## Prerequisites

### 1. Docker installed

### 2. IAT Input data
Download and extract `IAT Input.zip` to `iat_input/` directory (see main README for details)

### 3. One-Time Setup

The regression tests require the complete reference dataset to be loaded into PostgreSQL. This is a **one-time setup**.

```bash
# 1. Start PostgreSQL with PostGIS
docker compose up -d

# 2. Wait for PostgreSQL to be ready
docker compose logs postgres

# 3. Run database migrations
uv run alembic upgrade head

# 4. Load ALL reference data into PostGIS (takes ~5-10 minutes)
uv run python scripts/load_data.py

# 5. Verify data loaded correctly
psql postgresql://postgres:password@localhost:5432/nrf_impact -c "
    SELECT
        (SELECT COUNT(*) FROM nrf_reference.coefficient_layer) as coefficients,
        (SELECT COUNT(*) FROM nrf_reference.spatial_layer) as spatial_layers,
        (SELECT COUNT(*) FROM nrf_reference.lookup_table) as lookups;
"
```

**Expected output:**
```
 coefficients | spatial_layers | lookups
--------------+----------------+---------
      5459418 |          12172 |       2
```

## What's Tested

Regression tests validate:
- **End-to-end assessment pipeline** - From geometry input to final CSV output
- **Numerical accuracy** - Results match baseline within tolerance (0.01 kg/year absolute, 0.1% relative)
- **Column completeness** - All expected columns present
- **Edge cases** - Developments inside/outside catchments, missing data, etc.

Tests use real-world development geometries and compare against pre-generated baseline CSVs.

## Test Data

Located in `tests/data/`:

```
tests/data/
├── inputs/
│   ├── BnW_small_under_1_hectare/           # Shapefile format
│   │   └── BnW_small_under_1_hectare.shp
│   └── BnW_small_under_1_hectare_geojson/   # GeoJSON format
│       └── BnW_small_under_1_hectare.geojson
│
└── expected/
    └── BnW_small_under_1_hectare.csv        # Known-good baseline
```

**Baseline files are the source of truth** - generated once from the legacy script and committed to git.

## Comparison with Integration Tests

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

## CI/CD Considerations

Regression tests are **not run by default** in CI due to:
1. Long execution time (~3 minutes)
2. Large data requirements (~5.4M polygons)
3. Database dependency

### Options for CI:

**Option A: Manual execution**
- Run regression tests manually before releases
- Keep fast unit tests in automated CI

**Option B: Cached database**
- Save database dump after loading
- Restore from dump in CI
- Speeds up subsequent runs

**Option C: Subset data**
- Create a smaller test dataset for CI
- Run full regression locally

## Troubleshooting

### Database connection fails
```bash
# Check PostgreSQL is running
docker compose ps

# Test connection
psql postgresql://postgres:password@localhost:5432/nrf_impact -c "SELECT version();"
```

### Data not loaded
```bash
# Check if data exists
psql postgresql://postgres:password@localhost:5432/nrf_impact -c "
    SELECT COUNT(*) FROM nrf_reference.coefficient_layer;
"

# Re-run data loading if needed
uv run python scripts/load_data.py
```

### Tests are slow
This is expected. The coefficient layer has 5.4M polygons. Each test:
- Loads full dataset into memory
- Performs spatial intersections
- Calculates nutrient impacts

### Tests are skipped
Regression tests are deselected by default. You must explicitly run them:

```bash
# This skips regression tests
uv run pytest

# This runs them
uv run pytest -m regression
```

### Numerical differences
Small differences (<0.01 kg/year) are expected due to:
- Floating-point arithmetic
- Different calculation order
- Rounding differences

If differences are larger, investigate the calculation logic.

## Adding New Regression Tests

1. Generate baseline CSV from legacy script
2. Add baseline to `tests/data/expected/`
3. Add input geometry to `tests/data/inputs/`
4. Add test case to `test_regression.py`
5. Commit baseline and inputs to git

Example:
```python
@pytest.mark.regression
@pytest.mark.parametrize(
    "geometry_file",
    ["new_test_site/new_test_site.shp"],
    ids=["new_test"],
)
def test_postgis_new_site(
    production_repository: Repository,
    test_data_dir: Path,
    tolerance: dict[str, float],
    tmp_path: Path,
    geometry_file: str,
):
    # Test implementation...
```
