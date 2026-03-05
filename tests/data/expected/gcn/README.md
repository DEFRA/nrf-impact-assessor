# GCN assessment baseline

This directory contains baseline outputs used for regression testing the GCN (Great Crested Newt) impact assessment.

## Current baseline

The regression tests in `tests/regression/test_gcn_regression.py` validate against the following baseline directories:

- **`baseline_no_survey/`** - No-survey route (using national ponds dataset)
- **`baseline_survey/`** - Survey route (using site-specific survey ponds)

These baseline files were generated using **`legacy/opensource_gcn.py`** - the open-source reference implementation that replaces ArcPy dependencies with GeoPandas/Shapely.

### Baseline Files

Each baseline directory contains:
- `Habitat_Impact.psv` - Habitat impact areas by risk zone
- `Habitat_Impact_EDPSplit.psv` - Habitat impact split by EDP edges
- `Ponds_Impact_Frequency.psv` - Pond counts by zone and survey status
- `Ponds_Impact_Frequency_EDPSplit.psv` - Pond frequency split by EDP edges

## Legacy baselines (reference)

The following directories contain legacy baseline files for historical reference:

- **`20250915_115712_935389_survey/`** - Survey route (legacy)
- **`20250915_120444_807393_nosurvey/`** - No-survey route (legacy)

These legacy files were created using **`legacy/0.1_IIAT_Module_GCN.py`** - the original ArcPy-based implementation.

## Validation approach

The regression tests compare:
- **Habitat impact Shape_Area** by risk zone (0.1% relative tolerance)
- **Pond frequency counts** by zone and status (exact match)
- **Zone classifications** (exact match)

These baselines represent the validated "source of truth" for GCN assessment outputs, ensuring the pluggable worker implementation produces results consistent with the legacy open-source reference implementation.