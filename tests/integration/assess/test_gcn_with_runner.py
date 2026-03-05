"""Integration test for GCN assessment with runner."""

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from app.repositories.repository import Repository
from app.runner.runner import run_assessment

pytestmark = pytest.mark.integration


def test_gcn_assessment_with_runner(
    repository: Repository,
    sample_gcn_risk_zones: gpd.GeoDataFrame,
    sample_gcn_ponds: gpd.GeoDataFrame,
    sample_edp_edges: gpd.GeoDataFrame,
    test_data_dir: Path,
):
    """Test GCN assessment through runner with real PostGIS data.

    This test verifies that:
    1. The runner can load and execute the gcn assessment module
    2. The GCN assessment can query PostGIS for reference data
    3. Results are returned in the expected format
    """
    # Create synthetic RLB that overlaps with test fixtures
    # Test fixtures are located around (450000, 100000)

    rlb_gdf = gpd.GeoDataFrame(
        {
            "id": [1],
            "geometry": [
                Polygon(
                    [
                        (450100, 100100),
                        (450400, 100100),
                        (450400, 100400),
                        (450100, 100400),
                        (450100, 100100),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )

    # Run assessment through runner
    metadata = {"unique_ref": "20250118_TEST"}

    results = run_assessment("gcn", rlb_gdf, metadata, repository)

    # Verify structure
    assert isinstance(results, dict)
    assert "habitat_impact" in results
    assert "pond_frequency" in results

    # Verify results are DataFrames
    assert len(results["habitat_impact"]) > 0, "Expected habitat impact results"
    assert len(results["pond_frequency"]) > 0, "Expected pond frequency results"

    # Verify expected columns
    habitat_cols = set(results["habitat_impact"].columns)
    assert "Area" in habitat_cols
    assert "RZ" in habitat_cols
    assert "Shape_Area" in habitat_cols

    pond_cols = set(results["pond_frequency"].columns)
    assert "PANS" in pond_cols
    assert "Area" in pond_cols
    assert "MaxZone" in pond_cols
    assert "TmpImp" in pond_cols
    assert "FREQUENCY" in pond_cols

    # Verify values are reasonable
    assert results["habitat_impact"]["Shape_Area"].min() >= 0
    assert results["pond_frequency"]["FREQUENCY"].min() >= 0
