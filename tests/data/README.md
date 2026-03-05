# Test data

This directory contains test input files and expected output baselines for regression testing.

## Data source

Test data was provided by the **Natural England Modelling Team** and is available on the AD3 SharePoint.

We copied sample RLB shapefiles and expected CSV outputs from the SharePoint and committed them here so that:
- Tests can run without needing access to SharePoint
- Expected outputs serve as regression baselines to verify our calculations match the legacy implementation

## Directory structure

```
tests/data/
├── inputs/
│   ├── nutrients/                      # Nutrient assessment test RLBs
│   │   ├── BnW_cleaned_110925/         # Full Broads & Wensum dataset (245 sites)
│   │   ├── BnW_small_under_1_hectare/  # Single small site (shapefile)
│   │   └── BnW_small_under_1_hectare_geojson/  # Same site (GeoJSON format)
│   │
│   └── gcn/                            # GCN assessment test data
│       ├── SiteBoundaries/             # Test site RLBs
│       └── SitePonds/                  # Survey pond data (for survey route tests)
│
└── expected/
    ├── nutrients/                      # Expected nutrient assessment outputs
    └── gcn/                            # Expected GCN assessment outputs
```

## Usage

- **Unit tests** use mocked data and don't require these files
- **Regression tests** (`pytest -m regression`) use these files to validate end-to-end assessment outputs against known-good baselines
- **Local development** can use these files with `scripts/submit_job.py` for manual testing

## Relationship to reference data

This test data is **separate from reference data** in `iat_input/`:

| Directory | Contents | In git? |
|-----------|----------|---------|
| `tests/data/` | Sample RLBs, expected outputs | Yes (committed) |
| `iat_input/` | Coefficient layers, catchments, risk zones | No (git-ignored) |

Reference data must be downloaded separately - see [Data Inventory](../../docs/data-inventory.md).
