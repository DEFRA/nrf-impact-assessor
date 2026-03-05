"""GCN regression tests validating pluggable assessment implementation against Phase 0 baseline.

Pluggable Architecture Regression Testing
==========================================
This test validates that the pluggable GCN assessment (worker/assessments/gcn.py)
produces results that match the Phase 0 baseline (legacy/opensource_gcn.py)
within acceptable tolerance.

The baseline PSV files were generated using legacy/opensource_gcn.py and are stored in:
- tests/data/expected/gcn/opensource_survey/ - Survey route (with survey ponds)
- tests/data/expected/gcn/opensource_nosurvey/ - No-survey route (national ponds)

These baseline files are considered the "source of truth" for regression testing.

PREREQUISITES:
1. PostGIS database with GCN reference data loaded (risk zones, ponds, EDP edges)
2. Test inputs exist in tests/data/inputs/gcn/ (SiteBoundaries and SitePonds)
3. Baseline outputs exist in tests/data/expected/gcn/opensource_*/

Validation approach:
- Compare habitat impact shape areas by risk zone
- (allow 0.1% relative tolerance for geometry variance)
- Compare pond frequency counts (exact match expected)
- Verify zone classifications match (exact match expected)
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from app.repositories.repository import Repository
from app.runner.runner import run_assessment

# Mark all tests in this module as regression tests
pytestmark = pytest.mark.regression


def compare_dataframe_to_baseline(
    result_df: pd.DataFrame,
    baseline_file: Path,
    compare_cols: list[str],
    sort_cols: list[str],
    label: str = "output",
) -> None:
    """Compare DataFrame directly to baseline PSV file.

    Args:
        result_df: DataFrame from assessment
        baseline_file: Path to expected baseline PSV
        compare_cols: List of column names to compare
        sort_cols: List of column names to use for sorting (must be subset of compare_cols)
        label: Label for error messages
    """
    # Read baseline PSV
    baseline_df = pd.read_csv(baseline_file, sep="|")

    # Extract comparison columns and sort by stable identifier columns only
    result_subset = (
        result_df[compare_cols].sort_values(by=sort_cols).reset_index(drop=True)
    )
    baseline_subset = (
        baseline_df[compare_cols].sort_values(by=sort_cols).reset_index(drop=True)
    )

    # Check shape
    if result_subset.shape != baseline_subset.shape:
        print(f"\n{label}: Shape mismatch")
        print(f"Result shape: {result_subset.shape}")
        print(f"Baseline shape: {baseline_subset.shape}")
        print(f"\nResult data:\n{result_subset}")
        print(f"\nBaseline data:\n{baseline_subset}")

    assert result_subset.shape == baseline_subset.shape, (
        f"{label}: Shape mismatch - result={result_subset.shape}, baseline={baseline_subset.shape}"
    )

    # Compare each column
    for col in compare_cols:
        if col == "Shape_Area":
            # Allow 0.1% tolerance for Shape_Area (floating-point precision)
            # After applying precision snapping fixes (docs/bug-fix.md), the worker
            # now produces Shape_Area values that match the legacy baseline within
            # floating-point precision
            max_rel_diff = (
                abs(result_subset[col] - baseline_subset[col]) / baseline_subset[col]
            ).max()
            assert max_rel_diff < 0.001, (  # 0.1% tolerance (effectively exact match)
                f"{label}: Column '{col}' exceeds 0.1% tolerance (max={max_rel_diff * 100:.2f}%)\n"
                f"Result:\n{result_subset[col]}\n"
                f"Baseline:\n{baseline_subset[col]}"
            )
        else:
            assert result_subset[col].equals(baseline_subset[col]), (
                f"{label}: Column '{col}' mismatch\n"
                f"Result:\n{result_subset[col]}\n"
                f"Baseline:\n{baseline_subset[col]}"
            )


def test_gcn_assessment_no_survey_route(
    production_repository: Repository,
    test_data_dir: Path,
) -> None:
    """Test GCN assessment using national ponds dataset (no-survey route).

    Validates that the pluggable GCN assessment (worker/assessments/gcn.py)
    produces results matching Phase 0 baseline within acceptable tolerance.

    Args:
        production_repository: Repository fixture (provides PostGIS connection to production DB)
        test_data_dir: Path to tests/data directory (fixture)
    """
    # Input files
    rlb_path = (
        test_data_dir / "inputs" / "gcn" / "SiteBoundaries" / "SiteBoundary_00001.shp"
    )

    # Expected outputs - Phase 0 baseline (from opensource_gcn.py)
    baseline_dir = test_data_dir / "expected" / "gcn" / "baseline_no_survey"
    baseline_habitat = baseline_dir / "Habitat_Impact.psv"
    baseline_ponds = baseline_dir / "Ponds_Impact_Frequency.psv"

    # Verify prerequisites
    assert rlb_path.exists(), f"Test RLB not found: {rlb_path}"
    assert baseline_habitat.exists(), (
        f"Baseline habitat output not found: {baseline_habitat}"
    )
    assert baseline_ponds.exists(), f"Baseline ponds output not found: {baseline_ponds}"

    # Load test RLB
    rlb_gdf = gpd.read_file(rlb_path)

    # Run assessment through runner
    metadata = {"unique_ref": "20250118_TEST"}
    results = run_assessment("gcn", rlb_gdf, metadata, production_repository)

    # Compare pond frequencies against baseline (sort by identifiers, not counts)
    compare_dataframe_to_baseline(
        result_df=results["pond_frequency"],
        baseline_file=baseline_ponds,
        compare_cols=["FREQUENCY", "PANS", "Area", "MaxZone", "TmpImp"],
        sort_cols=["PANS", "Area", "MaxZone", "TmpImp"],  # Don't sort by FREQUENCY
        label="Pond Frequency (No Survey)",
    )

    # Compare habitat impact against baseline (sort by identifiers, not areas)
    compare_dataframe_to_baseline(
        result_df=results["habitat_impact"],
        baseline_file=baseline_habitat,
        compare_cols=["Area", "RZ", "Shape_Area"],
        sort_cols=["Area", "RZ"],  # Don't sort by Shape_Area
        label="Habitat Impact (No Survey)",
    )


def test_gcn_assessment_survey_route(
    production_repository: Repository,
    test_data_dir: Path,
) -> None:
    """Test GCN assessment using site-specific survey ponds (survey route).

    Validates that the pluggable GCN assessment produces results matching
    Phase 0 baseline for the survey route.

    Args:
        production_repository: Repository fixture (provides PostGIS connection to production DB)
        test_data_dir: Path to tests/data directory (fixture)
    """
    # Input files
    rlb_path = (
        test_data_dir / "inputs" / "gcn" / "SiteBoundaries" / "SiteBoundary_00001.shp"
    )
    ponds_path = test_data_dir / "inputs" / "gcn" / "SitePonds" / "SitePonds_00001.shp"

    # Expected outputs - Phase 0 baseline (from opensource_gcn.py)
    baseline_dir = test_data_dir / "expected" / "gcn" / "baseline_survey"
    baseline_habitat = baseline_dir / "Habitat_Impact.psv"
    baseline_ponds = baseline_dir / "Ponds_Impact_Frequency.psv"

    # Verify prerequisites
    assert rlb_path.exists(), f"Test RLB not found: {rlb_path}"
    assert ponds_path.exists(), f"Test survey ponds not found: {ponds_path}"
    assert baseline_habitat.exists(), (
        f"Baseline habitat output not found: {baseline_habitat}"
    )
    assert baseline_ponds.exists(), f"Baseline ponds output not found: {baseline_ponds}"

    # Load test inputs
    rlb_gdf = gpd.read_file(rlb_path)

    # Run assessment through runner with survey ponds
    metadata = {
        "unique_ref": "20250118_TEST_SURVEY",
        "survey_ponds_path": str(ponds_path),
    }
    results = run_assessment("gcn", rlb_gdf, metadata, production_repository)

    # Compare pond frequencies against baseline (sort by identifiers, not counts)
    compare_dataframe_to_baseline(
        result_df=results["pond_frequency"],
        baseline_file=baseline_ponds,
        compare_cols=["FREQUENCY", "PANS", "Area", "MaxZone", "TmpImp"],
        sort_cols=["PANS", "Area", "MaxZone", "TmpImp"],  # Don't sort by FREQUENCY
        label="Pond Frequency (Survey Route)",
    )

    # Compare habitat impact against baseline (sort by identifiers, not areas)
    compare_dataframe_to_baseline(
        result_df=results["habitat_impact"],
        baseline_file=baseline_habitat,
        compare_cols=["Area", "RZ", "Shape_Area"],
        sort_cols=["Area", "RZ"],  # Don't sort by Shape_Area
        label="Habitat Impact (Survey Route)",
    )
