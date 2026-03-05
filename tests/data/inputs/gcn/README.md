# GCN Assessment Test Data

This directory contains small test input files for GCN (Great Crested Newt) assessment regression testing.

## Contents

### SiteBoundaries/
Development site Red Line Boundaries (RLBs) - the subject of impact assessments.

- `SiteBoundary_00001.shp` - Test site #1
- `SiteBoundary_00002.shp` - Test site #2

**Purpose**: Represents proposed development footprints/extents.

### SitePonds/
Site-specific pond survey data (optional - used in "survey route" workflow).

- `SitePonds_00001.shp` - Pond survey for test site #1

**Purpose**: Site-specific pond survey data that overrides the national pond dataset.

**Required fields**:
- `PANS` - Presence/Absence/Not Surveyed (values: `P`, `A`, `NS`)
- `TmpImp` - Temporary Impact flag (values: `T`, `F`)

## Test vs Reference Data

**This directory (tests/data/inputs/gcn/)**: Small test inputs committed to git
- Site boundaries (KB each)
- Optional pond surveys
- Used for regression testing

**Reference data (iat_input/gcn/)**: Large national datasets NOT in git
- `RZ.gdb` - Risk zones (100s of MB)
- `IIAT_Layers.gdb` - National ponds + EDP boundaries
- Must be downloaded separately (see main README)

## Usage in Tests

These test files are used by regression tests to validate that the GCN assessment produces correct outputs:

```python
# Example test usage
rlb_path = "tests/data/inputs/gcn/SiteBoundaries/SiteBoundary_00001.shp"
ponds_path = "tests/data/inputs/gcn/SitePonds/SitePonds_00001.shp"

# Run assessment
result = run_gcn_assessment(rlb_path, survey_ponds=ponds_path)

# Validate outputs
assert result.habitat_impact_file.exists()
assert result.pond_frequency_file.exists()
```
