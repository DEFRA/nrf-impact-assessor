"""Unit tests for spatial overlay operations."""

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from app.spatial.overlay import spatial_difference_with_precision


def test_spatial_difference_with_precision_basic():
    """Difference removes overlapping area from left geometry."""
    left = gpd.GeoDataFrame(
        {"id": [1], "geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:27700",
    )
    right = gpd.GeoDataFrame(
        {"id": [1], "geometry": [Polygon([(5, 0), (10, 0), (10, 10), (5, 10)])]},
        crs="EPSG:27700",
    )

    result = spatial_difference_with_precision(left, right, parallel=False)

    assert len(result) == 1
    assert result.iloc[0].geometry.area == pytest.approx(50.0)


def test_spatial_difference_with_precision_handles_crs_mismatch():
    """Difference should handle CRS mismatch by transforming right to left CRS."""
    left = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (1000, 0), (1000, 1000), (0, 1000)])]},
        crs="EPSG:27700",
    )
    right = gpd.GeoDataFrame(
        {"geometry": [Polygon([(-1, 51), (-1, 52), (0, 52), (0, 51)])]},
        crs="EPSG:4326",
    )

    result = spatial_difference_with_precision(left, right, parallel=False)
    assert result.crs == left.crs


def test_spatial_difference_with_precision_parallel_fallback(monkeypatch):
    """Parallel mode falls back to sequential if process pools are unavailable."""
    left = gpd.GeoDataFrame(
        {
            "id": list(range(120)),
            "geometry": [
                Polygon([(i, 0), (i + 1, 0), (i + 1, 1), (i, 1)]) for i in range(120)
            ],
        },
        crs="EPSG:27700",
    )
    right = gpd.GeoDataFrame(
        {"geometry": [Polygon([(40, -1), (80, -1), (80, 2), (40, 2)])]},
        crs="EPSG:27700",
    )

    def _raise_permission_error(*_args, **_kwargs):
        msg = "blocked in test"
        raise PermissionError(msg)

    monkeypatch.setattr(
        "app.spatial.overlay.ProcessPoolExecutor", _raise_permission_error
    )

    result_parallel = spatial_difference_with_precision(
        left, right, parallel=True, max_workers=2
    )
    result_sequential = spatial_difference_with_precision(left, right, parallel=False)

    assert len(result_parallel) == len(result_sequential)
    assert result_parallel.geometry.area.sum() == pytest.approx(
        result_sequential.geometry.area.sum()
    )
