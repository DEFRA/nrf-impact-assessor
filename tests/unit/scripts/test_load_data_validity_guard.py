"""Reference-data loads must reject invalid geometries at load time,
before they can break PostGIS overlay queries."""

import geopandas as gpd
import pytest
from load_data import SpatialDataLoader
from shapely.geometry import Polygon


def _bowtie() -> Polygon:
    """Self-intersecting 'bowtie' polygon — the canonical invalid geometry."""
    return Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])


def _square() -> Polygon:
    return Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])


def _gdf(geoms) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({"geometry": geoms}, crs="EPSG:27700")


def test_invalid_geometry_fails_the_load():
    gdf = _gdf([_square(), _bowtie()])

    with pytest.raises(ValueError, match="edp_excluded_areas"):
        SpatialDataLoader._check_geometry_validity(gdf, "edp_excluded_areas")


def test_error_reports_how_many_rows_are_invalid():
    gdf = _gdf([_bowtie(), _square(), _bowtie()])

    with pytest.raises(ValueError, match="2 invalid"):
        SpatialDataLoader._check_geometry_validity(gdf, "nn_catchments")


def test_null_geometry_counts_as_invalid():
    gdf = _gdf([_square(), None])

    with pytest.raises(ValueError, match="1 invalid"):
        SpatialDataLoader._check_geometry_validity(gdf, "edp_boundary_layer")


def test_valid_geometries_pass():
    SpatialDataLoader._check_geometry_validity(_gdf([_square()]), "edp_excluded_areas")
