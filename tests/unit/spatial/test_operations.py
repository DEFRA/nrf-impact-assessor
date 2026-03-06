"""Unit tests for spatial operations."""

import geopandas as gpd
import pytest
from shapely.geometry import Point, Polygon

from app.spatial.operations import (
    clip_gdf,
    make_valid_geometries,
    spatial_join_intersect,
)


def test_clip_gdf_to_extent():
    """Test clipping features to a mask extent."""
    # Create input features (two polygons)
    input_gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2],
            "name": ["A", "B"],
            "geometry": [
                Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]),  # Inside mask
                Polygon([(20, 20), (30, 20), (30, 30), (20, 30)]),  # Outside mask
            ],
        },
        crs="EPSG:27700",
    )

    # Create mask (clip extent)
    mask_gdf = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (15, 0), (15, 15), (0, 15)])]},
        crs="EPSG:27700",
    )

    # Clip
    clipped = clip_gdf(input_gdf, mask_gdf)

    # Should only have the first polygon (fully inside mask)
    assert len(clipped) == 1
    assert clipped.iloc[0]["name"] == "A"


def test_clip_gdf_partial_overlap():
    """Test clipping when features partially overlap mask."""
    # Create input feature that extends beyond mask
    input_gdf = gpd.GeoDataFrame(
        {
            "id": [1],
            "geometry": [Polygon([(5, 5), (15, 5), (15, 15), (5, 15)])],
        },
        crs="EPSG:27700",
    )

    # Create smaller mask
    mask_gdf = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:27700",
    )

    # Clip
    clipped = clip_gdf(input_gdf, mask_gdf)

    # Should have one feature with reduced area
    assert len(clipped) == 1
    assert clipped.iloc[0].geometry.area < input_gdf.iloc[0].geometry.area


def test_clip_gdf_handles_crs_mismatch():
    """Test that clipping handles CRS mismatch."""
    # Create input in BNG
    input_gdf = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:27700",
    )

    # Create mask in WGS84
    mask_gdf = gpd.GeoDataFrame(
        {"geometry": [Polygon([(-1, 51), (-1, 52), (0, 52), (0, 51)])]},
        crs="EPSG:4326",
    )

    # Should not raise error
    clipped = clip_gdf(input_gdf, mask_gdf)
    assert clipped.crs == input_gdf.crs


def test_spatial_join_intersecting_features():
    """Test joining features that intersect."""
    # Create left features (points)
    left_gdf = gpd.GeoDataFrame(
        {
            "point_id": [1, 2, 3],
            "geometry": [Point(5, 5), Point(15, 15), Point(25, 25)],
        },
        crs="EPSG:27700",
    )

    # Create right features (polygon)
    right_gdf = gpd.GeoDataFrame(
        {
            "poly_id": [1],
            "name": ["Zone A"],
            "geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        },
        crs="EPSG:27700",
    )

    # Join
    joined = spatial_join_intersect(left_gdf, right_gdf)

    # Only point 1 intersects the polygon
    assert len(joined) == 1
    assert joined.iloc[0]["point_id"] == 1
    assert joined.iloc[0]["name"] == "Zone A"


def test_spatial_join_multiple_intersections():
    """Test joining when left features intersect multiple right features."""
    # Create left feature (point)
    left_gdf = gpd.GeoDataFrame(
        {"point_id": [1], "geometry": [Point(5, 5)]},
        crs="EPSG:27700",
    )

    # Create overlapping right features
    right_gdf = gpd.GeoDataFrame(
        {
            "poly_id": [1, 2],
            "name": ["Zone A", "Zone B"],
            "geometry": [
                Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]),
                Polygon([(5, 5), (15, 5), (15, 15), (5, 15)]),
            ],
        },
        crs="EPSG:27700",
    )

    # Join
    joined = spatial_join_intersect(left_gdf, right_gdf)

    # Point should match both polygons
    assert len(joined) == 2
    assert set(joined["name"]) == {"Zone A", "Zone B"}


def test_spatial_join_no_intersections():
    """Test joining when no features intersect."""
    # Create left features
    left_gdf = gpd.GeoDataFrame(
        {"point_id": [1], "geometry": [Point(50, 50)]},
        crs="EPSG:27700",
    )

    # Create right features (far away)
    right_gdf = gpd.GeoDataFrame(
        {"poly_id": [1], "geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:27700",
    )

    # Join
    joined = spatial_join_intersect(left_gdf, right_gdf)

    # No intersections
    assert len(joined) == 0


def test_spatial_join_handles_crs_mismatch():
    """Test that join handles CRS mismatch."""
    # Create left in BNG
    left_gdf = gpd.GeoDataFrame(
        {"geometry": [Point(450000, 100000)]},
        crs="EPSG:27700",
    )

    # Create right in WGS84
    right_gdf = gpd.GeoDataFrame(
        {"geometry": [Polygon([(-1, 51), (-1, 52), (0, 52), (0, 51)])]},
        crs="EPSG:4326",
    )

    # Should not raise error
    joined = spatial_join_intersect(left_gdf, right_gdf)
    assert joined.crs == left_gdf.crs


def test_make_valid_geometries_repair_invalid():
    """Test repairing an invalid geometry (self-intersecting polygon)."""
    # Create self-intersecting polygon (bowtie shape)
    invalid_polygon = Polygon([(0, 0), (10, 10), (10, 0), (0, 10), (0, 0)])

    gdf = gpd.GeoDataFrame(
        {"id": [1], "geometry": [invalid_polygon]},
        crs="EPSG:27700",
    )

    # Verify it's invalid
    assert not gdf.iloc[0].geometry.is_valid

    # Repair
    repaired = make_valid_geometries(gdf)

    # Should be valid now
    assert repaired.iloc[0].geometry.is_valid


def test_make_valid_geometries_preserve_valid():
    """Test that valid geometries are preserved."""
    valid_polygon = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])

    gdf = gpd.GeoDataFrame(
        {"id": [1], "geometry": [valid_polygon]},
        crs="EPSG:27700",
    )

    # Verify it's valid
    assert gdf.iloc[0].geometry.is_valid

    # Process
    result = make_valid_geometries(gdf)

    # Should still be valid and essentially unchanged
    assert result.iloc[0].geometry.is_valid
    assert result.iloc[0].geometry.area == pytest.approx(valid_polygon.area)


def test_make_valid_geometries_handle_none():
    """Test handling of None geometries."""
    gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2],
            "geometry": [None, Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
        },
        crs="EPSG:27700",
    )

    # Should not raise error
    result = make_valid_geometries(gdf)

    # First geometry should still be None
    assert result.iloc[0].geometry is None
    # Second should be valid
    assert result.iloc[1].geometry.is_valid


def test_make_valid_geometries_does_not_modify_original():
    """Test that original GeoDataFrame is not modified."""
    invalid_polygon = Polygon([(0, 0), (10, 10), (10, 0), (0, 10), (0, 0)])

    gdf = gpd.GeoDataFrame(
        {"id": [1], "geometry": [invalid_polygon]},
        crs="EPSG:27700",
    )

    # Repair
    repaired = make_valid_geometries(gdf)

    # Original should still be invalid
    assert not gdf.iloc[0].geometry.is_valid
    # Repaired should be valid
    assert repaired.iloc[0].geometry.is_valid
