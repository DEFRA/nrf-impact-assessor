"""Unit tests for spatial utilities."""

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, Polygon


@pytest.fixture
def simple_target_gdf():
    """Create simple target GeoDataFrame (3 developments)."""
    return gpd.GeoDataFrame(
        {
            "RLB_ID": [1, 2, 3],
            "name": ["Site A", "Site B", "Site C"],
        },
        geometry=[
            Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]),  # 100 sq units
            Polygon([(15, 0), (25, 0), (25, 10), (15, 10)]),  # 100 sq units
            Polygon([(30, 0), (40, 0), (40, 10), (30, 10)]),  # 100 sq units
        ],
        crs="EPSG:27700",
    )


@pytest.fixture
def simple_overlay_gdf():
    """Create simple overlay GeoDataFrame (2 catchments)."""
    return gpd.GeoDataFrame(
        {
            "WwTw_ID": [101, 102],
            "name": ["Catchment A", "Catchment B"],
        },
        geometry=[
            Polygon([(-5, -5), (20, -5), (20, 15), (-5, 15)]),  # Overlaps 1, 2
            Polygon([(25, -5), (45, -5), (45, 15), (25, 15)]),  # Overlaps 2, 3
        ],
        crs="EPSG:27700",
    )


def test_ensure_crs_no_transformation_when_already_correct():
    """Test that no transformation occurs when GDF is already in target CRS."""
    from app.spatial import ensure_crs

    # Arrange: Create GeoDataFrame in BNG (EPSG:27700)
    gdf = gpd.GeoDataFrame(
        {"id": [1, 2]},
        geometry=[Point(529000, 179000), Point(530000, 180000)],
        crs="EPSG:27700",
    )

    # Act
    result = ensure_crs(gdf, target_crs="EPSG:27700")

    # Assert: Should return the original object (no copy/transform)
    assert result.crs == "EPSG:27700"
    assert result is gdf


def test_ensure_crs_transformation_when_different():
    """Test that transformation occurs when CRS differs."""
    from app.spatial import ensure_crs

    # Arrange: Create GeoDataFrame in WGS84 (EPSG:4326)
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[Point(-1.5, 53.8)], crs="EPSG:4326")

    # Act
    result = ensure_crs(gdf, target_crs="EPSG:27700")

    # Assert: Should be transformed copy with coordinates in BNG metres
    assert result.crs == "EPSG:27700"
    assert result is not gdf
    assert result.geometry.iloc[0].x > 400000  # BNG easting in metres
    assert result.geometry.iloc[0].y > 400000  # BNG northing in metres


def test_ensure_crs_raises_error_when_no_crs():
    """Test that error is raised when input has no CRS."""
    from app.spatial import ensure_crs

    # Arrange: Create GeoDataFrame without CRS
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[Point(0, 0)], crs=None)

    # Act & Assert: Should raise ValueError
    with pytest.raises(ValueError, match="no CRS defined"):
        ensure_crs(gdf)


def test_ensure_crs_custom_target():
    """Test that custom target CRS works."""
    from app.spatial import ensure_crs

    # Arrange: Create GeoDataFrame in BNG
    gdf = gpd.GeoDataFrame(
        {"id": [1]}, geometry=[Point(529000, 179000)], crs="EPSG:27700"
    )

    # Act
    result = ensure_crs(gdf, target_crs="EPSG:4326")

    # Assert: Should transform to WGS84 with lon/lat in UK range
    assert result.crs == "EPSG:4326"
    assert -2 < result.geometry.iloc[0].x < 0  # Longitude
    assert 50 < result.geometry.iloc[0].y < 55  # Latitude


def test_majority_overlap_basic(simple_target_gdf, simple_overlay_gdf):
    """Test basic majority overlap assignment."""
    from app.spatial import majority_overlap

    # Act
    result = majority_overlap(
        input_gdf=simple_target_gdf,
        overlay_gdf=simple_overlay_gdf,
        input_id_col="RLB_ID",
        overlay_attr_col="WwTw_ID",
        output_field="wwtw_assignment",
    )

    # Assert: Each site should be assigned to the catchment with largest overlap
    assert len(result) == 3
    assert "wwtw_assignment" in result.columns
    assert "RLB_ID" in result.columns
    assert "name" in result.columns  # Original columns preserved
    # Site 1 (0-10) mostly overlaps catchment 101 (-5 to 20)
    assert result.loc[result["RLB_ID"] == 1, "wwtw_assignment"].iloc[0] == 101
    # Site 2 overlaps both but more with 101 (15-20) vs 102 (20-25)
    assert result.loc[result["RLB_ID"] == 2, "wwtw_assignment"].iloc[0] == 101
    # Site 3 (30-40) only overlaps catchment 102 (25-45)
    assert result.loc[result["RLB_ID"] == 3, "wwtw_assignment"].iloc[0] == 102


def test_majority_overlap_no_overlap_with_default_value(simple_target_gdf):
    """Test features with no overlap get default value."""
    from app.spatial import majority_overlap

    # Arrange: Create overlay that doesn't overlap any targets
    overlay_gdf = gpd.GeoDataFrame(
        {"WwTw_ID": [999]},
        geometry=[Polygon([(100, 100), (110, 100), (110, 110), (100, 110)])],
        crs="EPSG:27700",
    )

    # Act
    result = majority_overlap(
        input_gdf=simple_target_gdf,
        overlay_gdf=overlay_gdf,
        input_id_col="RLB_ID",
        overlay_attr_col="WwTw_ID",
        output_field="wwtw_assignment",
        default_value=141,  # Default from legacy script
    )

    # Assert: All sites should get default value since no overlap
    assert all(result["wwtw_assignment"] == 141)


def test_majority_overlap_partial_overlap_with_default(simple_target_gdf):
    """Test that features without overlap get default while others assigned."""
    from app.spatial import majority_overlap

    # Arrange: Create overlay that only overlaps site 1
    overlay_gdf = gpd.GeoDataFrame(
        {"WwTw_ID": [101]},
        geometry=[Polygon([(-5, -5), (12, -5), (12, 15), (-5, 15)])],
        crs="EPSG:27700",
    )

    # Act
    result = majority_overlap(
        input_gdf=simple_target_gdf,
        overlay_gdf=overlay_gdf,
        input_id_col="RLB_ID",
        overlay_attr_col="WwTw_ID",
        output_field="wwtw_assignment",
        default_value=141,
    )

    # Assert: Site 1 overlaps, gets 101; Sites 2 and 3 don't overlap, get default
    assert result.loc[result["RLB_ID"] == 1, "wwtw_assignment"].iloc[0] == 101
    assert result.loc[result["RLB_ID"] == 2, "wwtw_assignment"].iloc[0] == 141
    assert result.loc[result["RLB_ID"] == 3, "wwtw_assignment"].iloc[0] == 141


def test_majority_overlap_crs_mismatch_handled_automatically(simple_target_gdf):
    """Test that CRS mismatch is handled automatically."""
    from app.spatial import majority_overlap

    # Arrange: Create overlay in different CRS (WGS84)
    overlay_gdf = gpd.GeoDataFrame(
        {"WwTw_ID": [101]},
        geometry=[Polygon([(-1, 53), (-0.5, 53), (-0.5, 53.5), (-1, 53.5)])],
        crs="EPSG:4326",
    )

    # Act: Should not raise error, should handle CRS transformation internally
    result = majority_overlap(
        input_gdf=simple_target_gdf,
        overlay_gdf=overlay_gdf,
        input_id_col="RLB_ID",
        overlay_attr_col="WwTw_ID",
        output_field="wwtw_assignment",
        default_value=141,
    )

    # Assert: Should return valid result despite CRS mismatch
    assert len(result) == 3
    assert "wwtw_assignment" in result.columns


def test_majority_overlap_string_attribute_assignment(simple_target_gdf):
    """Test assignment works with string attributes (e.g., LPA names)."""
    from app.spatial import majority_overlap

    # Arrange
    target_gdf = gpd.GeoDataFrame(
        {"RLB_ID": [1, 2]},
        geometry=[
            Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]),
            Polygon([(15, 0), (25, 0), (25, 10), (15, 10)]),
        ],
        crs="EPSG:27700",
    )

    overlay_gdf = gpd.GeoDataFrame(
        {"NAME": ["Norfolk", "Suffolk"]},
        geometry=[
            Polygon([(-5, -5), (18, -5), (18, 15), (-5, 15)]),  # Overlaps site 1 fully
            Polygon([(18, -5), (30, -5), (30, 15), (18, 15)]),  # Overlaps site 2 mostly
        ],
        crs="EPSG:27700",
    )

    # Act
    result = majority_overlap(
        input_gdf=target_gdf,
        overlay_gdf=overlay_gdf,
        input_id_col="RLB_ID",
        overlay_attr_col="NAME",
        output_field="lpa_name",
        default_value="UNKNOWN",
    )

    # Assert: String attributes should be assigned correctly
    assert result.loc[result["RLB_ID"] == 1, "lpa_name"].iloc[0] == "Norfolk"
    assert result.loc[result["RLB_ID"] == 2, "lpa_name"].iloc[0] == "Suffolk"


def test_majority_overlap_raises_error_for_missing_input_column(
    simple_target_gdf, simple_overlay_gdf
):
    """Test error raised when input_id_col doesn't exist."""
    from app.spatial import majority_overlap

    # Act & Assert: Should raise ValueError for missing column
    with pytest.raises(ValueError, match="input_id_col.*not found"):
        majority_overlap(
            input_gdf=simple_target_gdf,
            overlay_gdf=simple_overlay_gdf,
            input_id_col="MISSING_COL",
            overlay_attr_col="WwTw_ID",
            output_field="wwtw_assignment",
        )


def test_majority_overlap_raises_error_for_missing_overlay_column(
    simple_target_gdf, simple_overlay_gdf
):
    """Test error raised when overlay_attr_col doesn't exist."""
    from app.spatial import majority_overlap

    # Act & Assert: Should raise ValueError for missing column
    with pytest.raises(ValueError, match="overlay_attr_col.*not found"):
        majority_overlap(
            input_gdf=simple_target_gdf,
            overlay_gdf=simple_overlay_gdf,
            input_id_col="RLB_ID",
            overlay_attr_col="MISSING_COL",
            output_field="wwtw_assignment",
        )


def test_majority_overlap_returns_geodataframe(simple_target_gdf, simple_overlay_gdf):
    """Test that result is a GeoDataFrame with geometry preserved."""
    from app.spatial import majority_overlap

    # Act
    result = majority_overlap(
        input_gdf=simple_target_gdf,
        overlay_gdf=simple_overlay_gdf,
        input_id_col="RLB_ID",
        overlay_attr_col="WwTw_ID",
        output_field="wwtw_assignment",
    )

    # Assert: Result should be GeoDataFrame with geometry
    assert isinstance(result, gpd.GeoDataFrame)
    assert "geometry" in result.columns
    assert len(result) == len(simple_target_gdf)  # Same number of rows


def test_partition_by_bounds_no_duplicate_rows_on_chunk_boundary():
    """Test chunk partitioning does not duplicate features on boundaries."""
    from app.spatial.assignments import _partition_by_bounds

    # Build 100 features
    input_geoms = [
        Polygon([(i, 0), (i + 1, 0), (i + 1, 1), (i, 1)]) for i in range(100)
    ]
    input_gdf = gpd.GeoDataFrame(
        {"RLB_ID": list(range(100))},
        geometry=input_geoms,
        crs="EPSG:27700",
    )

    chunks = _partition_by_bounds(input_gdf, n_chunks=2)
    assert len(chunks) == 2

    # Every original row should appear exactly once across chunks
    combined_ids = pd.concat([chunk["RLB_ID"] for chunk in chunks], ignore_index=True)
    assert len(combined_ids) == len(input_gdf)
    assert combined_ids.nunique() == len(input_gdf)
