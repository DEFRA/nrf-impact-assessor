"""Unit tests for GCN assessment module."""

from unittest.mock import Mock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, Polygon

from app.assessments.gcn import (
    GcnAssessment,
    _calculate_habitat_impact,
    _calculate_pond_frequency,
)
from app.config import GcnConfig
from app.models.enums import SpatialLayerType


@pytest.fixture
def sample_rlb():
    """Create a sample RLB GeoDataFrame."""
    return gpd.GeoDataFrame(
        {
            "id": [1],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450100, 100000),
                        (450100, 100100),
                        (450000, 100100),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_risk_zones():
    """Create sample GCN risk zones."""
    return gpd.GeoDataFrame(
        {
            "RZ": ["Red", "Amber", "Green"],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450050, 100000),
                        (450050, 100050),
                        (450000, 100050),
                    ]
                ),
                Polygon(
                    [
                        (450050, 100000),
                        (450100, 100000),
                        (450100, 100050),
                        (450050, 100050),
                    ]
                ),
                Polygon(
                    [
                        (450000, 100050),
                        (450050, 100050),
                        (450050, 100100),
                        (450000, 100100),
                    ]
                ),
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_ponds():
    """Create sample ponds."""
    return gpd.GeoDataFrame(
        {
            "pond_id": [1, 2],
            "geometry": [
                Point(450025, 100025),  # Inside RLB
                Point(450500, 100500),  # Outside RLB (will be in buffer)
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_edp_edges():
    """Create sample EDP edges."""
    return gpd.GeoDataFrame(
        {
            "edge_id": [1],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450200, 100000),
                        (450200, 100200),
                        (450000, 100200),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )


def _extract_layer_type(where) -> SpatialLayerType | None:
    """Extract SpatialLayerType from a simple or compound SQLAlchemy WHERE clause."""
    try:
        return where.right.value
    except AttributeError:
        pass
    try:
        for clause in where.clauses:
            if (
                hasattr(clause, "right")
                and hasattr(clause.right, "value")
                and isinstance(clause.right.value, SpatialLayerType)
            ):
                return clause.right.value
    except AttributeError:
        pass
    return None


def _gdf_for_layer_type(
    layer_type: SpatialLayerType | None,
    risk_zones: gpd.GeoDataFrame,
    ponds: gpd.GeoDataFrame,
    edp_edges: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Return a copy of the sample GDF matching the given layer type."""
    if layer_type == SpatialLayerType.GCN_RISK_ZONES:
        return risk_zones.copy()
    if layer_type == SpatialLayerType.GCN_PONDS:
        return ponds.copy()
    if layer_type == SpatialLayerType.EDP_EDGES:
        return edp_edges.copy()
    return gpd.GeoDataFrame()


@pytest.fixture
def mock_repository(sample_risk_zones, sample_ponds, sample_edp_edges):
    """Create a mock repository that returns sample data based on the query."""
    repo = Mock()

    def execute_query_side_effect(stmt, as_gdf=False):
        layer_type = _extract_layer_type(stmt.whereclause)
        return _gdf_for_layer_type(layer_type, sample_risk_zones, sample_ponds, sample_edp_edges)

    def intersection_postgis_side_effect(input_gdf, overlay_table, overlay_filter, overlay_columns):
        layer_type = _extract_layer_type(overlay_filter)
        return _gdf_for_layer_type(layer_type, sample_risk_zones, sample_ponds, sample_edp_edges)

    repo.execute_query.side_effect = execute_query_side_effect
    repo.intersection_postgis.side_effect = intersection_postgis_side_effect
    return repo


def test_run_assessment_basic(sample_rlb, mock_repository):
    """Test basic GCN assessment execution."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = GcnAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    # Verify structure
    assert isinstance(results, dict)
    assert "habitat_impact" in results
    assert "pond_frequency" in results

    # Verify results are DataFrames
    assert isinstance(results["habitat_impact"], pd.DataFrame)
    assert isinstance(results["pond_frequency"], pd.DataFrame)

    # Verify repository calls:
    # - risk zones via server-side intersection
    # - ponds via execute_query (national route)
    assert mock_repository.intersection_postgis.call_count == 1
    assert mock_repository.execute_query.call_count == 1


def test_run_assessment_with_survey_ponds(sample_rlb, mock_repository, tmp_path):
    """Test GCN assessment with survey ponds from file."""
    # Create a temporary shapefile with survey ponds
    survey_ponds = gpd.GeoDataFrame(
        {
            "pond_id": [1],
            "PANS": ["P"],  # Present
            "TmpImp": ["F"],  # No temporary impact
            "geometry": [Point(450025, 100025)],
        },
        crs="EPSG:27700",
    )

    survey_path = tmp_path / "survey_ponds.shp"
    survey_ponds.to_file(survey_path)

    metadata = {"unique_ref": "20250115123456", "survey_ponds_path": str(survey_path)}

    assessment = GcnAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    # Should still return valid results
    assert "habitat_impact" in results
    assert "pond_frequency" in results


def test_run_assessment_respects_custom_buffer_config(sample_rlb, mock_repository):
    """Test GCN config wiring by forcing zero RLB/buffer distances."""
    metadata = {"unique_ref": "20250115123456"}
    config = GcnConfig(buffer_distance_m=0, pond_buffer_distance_m=0)

    assessment = GcnAssessment(sample_rlb, metadata, mock_repository, config=config)
    results = assessment.run()

    # With zero RLB buffer, there should be no "Buffer" pond assignments
    if len(results["pond_frequency"]) > 0:
        assert not (results["pond_frequency"]["Area"] == "Buffer").any()


def test_run_assessment_missing_unique_ref(sample_rlb, mock_repository):
    """Test error when unique_ref is missing from metadata."""
    metadata = {}  # Missing unique_ref

    assessment = GcnAssessment(sample_rlb, metadata, mock_repository)
    with pytest.raises(KeyError):
        assessment.run()


def test_run_assessment_survey_ponds_missing_pans(
    sample_rlb, mock_repository, tmp_path
):
    """Test error when survey ponds missing required PANS column."""
    # Create survey ponds without PANS column
    survey_ponds = gpd.GeoDataFrame(
        {"pond_id": [1], "geometry": [Point(450025, 100025)]}, crs="EPSG:27700"
    )

    survey_path = tmp_path / "bad_ponds.shp"
    survey_ponds.to_file(survey_path)

    metadata = {"unique_ref": "20250115123456", "survey_ponds_path": str(survey_path)}

    assessment = GcnAssessment(sample_rlb, metadata, mock_repository)
    with pytest.raises(ValueError, match="Survey ponds must have 'PANS' column"):
        assessment.run()


def test_run_assessment_raises_when_risk_zones_missing_rz(sample_rlb):
    """Test explicit error when risk zones lack required RZ field."""
    repo = Mock()

    risk_zones_missing_rz = gpd.GeoDataFrame(
        {
            "name": ["zone_1"],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450200, 100000),
                        (450200, 100200),
                        (450000, 100200),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )

    national_ponds = gpd.GeoDataFrame(
        {"geometry": [Point(450050, 100050)]},
        crs="EPSG:27700",
    )

    # GCN run() queries risk zones via intersection_postgis, then ponds via execute_query
    repo.intersection_postgis.return_value = risk_zones_missing_rz
    repo.execute_query.return_value = national_ponds

    assessment = GcnAssessment(sample_rlb, {"unique_ref": "20250115123456"}, repo)
    with pytest.raises(ValueError, match="Risk zones missing required 'RZ' attribute"):
        assessment.run()


def test_calculate_habitat_impact_basic():
    """Test basic habitat impact calculation."""
    # Create RLB with buffer
    rlb_with_buffer = gpd.GeoDataFrame(
        {
            "Area": ["RLB", "Buffer"],
            "geometry": [
                Polygon([(0, 0), (100, 0), (100, 100), (0, 100)]),
                Polygon([(100, 0), (200, 0), (200, 100), (100, 100)]),
            ],
        },
        crs="EPSG:27700",
    )

    # Create risk zones
    risk_zones = gpd.GeoDataFrame(
        {
            "RZ": ["Red", "Amber"],
            "geometry": [
                Polygon([(0, 0), (50, 0), (50, 100), (0, 100)]),
                Polygon([(50, 0), (100, 0), (100, 100), (50, 100)]),
            ],
        },
        crs="EPSG:27700",
    )

    # Create ponds
    ponds = gpd.GeoDataFrame(
        {"geometry": [Point(25, 50), Point(75, 50)]}, crs="EPSG:27700"
    )

    result = _calculate_habitat_impact(rlb_with_buffer, risk_zones, ponds)

    # Verify result structure
    assert isinstance(result, pd.DataFrame)
    assert "Area" in result.columns
    assert "RZ" in result.columns
    assert "Shape_Area" in result.columns

    # Verify areas are positive
    assert all(result["Shape_Area"] > 0)


def test_calculate_habitat_impact_no_overlap():
    """Test habitat impact when no overlap between zones and RLB."""
    # RLB in one location
    rlb_with_buffer = gpd.GeoDataFrame(
        {"Area": ["RLB"], "geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:27700",
    )

    # Risk zones far away
    risk_zones = gpd.GeoDataFrame(
        {
            "RZ": ["Red"],
            "geometry": [
                Polygon([(1000, 1000), (1010, 1000), (1010, 1010), (1000, 1010)])
            ],
        },
        crs="EPSG:27700",
    )

    # Ponds far away
    ponds = gpd.GeoDataFrame({"geometry": [Point(1005, 1005)]}, crs="EPSG:27700")

    result = _calculate_habitat_impact(rlb_with_buffer, risk_zones, ponds)

    # Should return empty result (no overlap)
    assert len(result) == 0


def test_calculate_pond_frequency_basic():
    """Test basic pond frequency calculation."""
    # Create ponds in RLB
    ponds_in_rlb = gpd.GeoDataFrame(
        {
            "PANS": ["P", "NS"],
            "TmpImp": ["F", "F"],
            "Area": ["RLB", "RLB"],
            "geometry": [Point(10, 10), Point(20, 20)],
        },
        crs="EPSG:27700",
    )

    # Create ponds in buffer
    ponds_in_buffer = gpd.GeoDataFrame(
        {
            "PANS": ["P"],
            "TmpImp": ["T"],
            "Area": ["Buffer"],
            "geometry": [Point(30, 30)],
        },
        crs="EPSG:27700",
    )

    # Create risk zones
    risk_zones = gpd.GeoDataFrame(
        {
            "RZ": ["Red", "Amber"],
            "geometry": [
                Polygon([(0, 0), (15, 0), (15, 15), (0, 15)]),
                Polygon([(15, 15), (35, 15), (35, 35), (15, 35)]),
            ],
        },
        crs="EPSG:27700",
    )

    result = _calculate_pond_frequency(ponds_in_rlb, ponds_in_buffer, risk_zones)

    # Verify result structure
    assert isinstance(result, pd.DataFrame)
    assert "PANS" in result.columns
    assert "Area" in result.columns
    assert "MaxZone" in result.columns
    assert "TmpImp" in result.columns
    assert "FREQUENCY" in result.columns

    # Verify frequencies are positive
    assert all(result["FREQUENCY"] > 0)


def test_calculate_pond_frequency_max_zone_priority():
    """Test that MaxZone correctly prioritizes Red > Amber > Green."""
    # Create one pond that intersects multiple zones
    ponds_in_rlb = gpd.GeoDataFrame(
        {"PANS": ["P"], "TmpImp": ["F"], "Area": ["RLB"], "geometry": [Point(10, 10)]},
        crs="EPSG:27700",
    )

    ponds_in_buffer = gpd.GeoDataFrame(
        {"PANS": [], "TmpImp": [], "Area": [], "geometry": []}, crs="EPSG:27700"
    )

    # Create overlapping risk zones (all covering the pond)
    risk_zones = gpd.GeoDataFrame(
        {
            "RZ": ["Red", "Amber", "Green"],
            "geometry": [
                Polygon([(0, 0), (20, 0), (20, 20), (0, 20)]),
                Polygon([(0, 0), (20, 0), (20, 20), (0, 20)]),
                Polygon([(0, 0), (20, 0), (20, 20), (0, 20)]),
            ],
        },
        crs="EPSG:27700",
    )

    result = _calculate_pond_frequency(ponds_in_rlb, ponds_in_buffer, risk_zones)

    # Should have MaxZone = "Red" (highest priority)
    assert len(result) == 1
    assert result.iloc[0]["MaxZone"] == "Red"


def test_calculate_pond_frequency_empty_ponds():
    """Test pond frequency with no ponds."""
    ponds_in_rlb = gpd.GeoDataFrame(
        {"PANS": [], "TmpImp": [], "Area": [], "geometry": []}, crs="EPSG:27700"
    )

    ponds_in_buffer = gpd.GeoDataFrame(
        {"PANS": [], "TmpImp": [], "Area": [], "geometry": []}, crs="EPSG:27700"
    )

    risk_zones = gpd.GeoDataFrame(
        {"RZ": ["Red"], "geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:27700",
    )

    result = _calculate_pond_frequency(ponds_in_rlb, ponds_in_buffer, risk_zones)

    # Should return empty result
    assert len(result) == 0
