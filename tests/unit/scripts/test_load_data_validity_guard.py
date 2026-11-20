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


def _touching_ring() -> Polygon:
    """Ring that touches itself at one point (GEOS 'Ring Self-intersection').

    A common export artefact: make_valid splits it into two triangles with
    exactly the same area, so it is safe to repair rather than reject.
    """
    return Polygon([(0, 0), (2, 0), (1, 1), (2, 2), (0, 2), (1, 1), (0, 0)])


def test_touching_ring_is_repaired_without_changing_area():
    gdf = _gdf([_square(), _touching_ring()])

    repaired = SpatialDataLoader._repair_invalid_geometries(gdf, "lpa_boundaries")

    assert repaired.geometry.is_valid.all()
    assert repaired.geometry.area.tolist() == pytest.approx([1.0, 2.0])
    SpatialDataLoader._check_geometry_validity(repaired, "lpa_boundaries")


def test_repair_reports_how_many_geometries_it_fixed(capsys):
    gdf = _gdf([_touching_ring(), _square(), _touching_ring()])

    SpatialDataLoader._repair_invalid_geometries(gdf, "wwtw_catchments")

    assert "wwtw_catchments: repaired 2 invalid geometries" in capsys.readouterr().out


def test_bowtie_is_not_repaired_because_repair_changes_its_area():
    gdf = _gdf([_square(), _bowtie()])

    repaired = SpatialDataLoader._repair_invalid_geometries(gdf, "nn_catchments")

    with pytest.raises(ValueError, match="1 invalid"):
        SpatialDataLoader._check_geometry_validity(repaired, "nn_catchments")


def test_null_geometry_is_left_for_the_validity_check():
    gdf = _gdf([_square(), None])

    repaired = SpatialDataLoader._repair_invalid_geometries(gdf, "edp_boundary_layer")

    with pytest.raises(ValueError, match="1 invalid"):
        SpatialDataLoader._check_geometry_validity(repaired, "edp_boundary_layer")


def test_repair_does_not_modify_the_input_frame():
    gdf = _gdf([_touching_ring()])

    SpatialDataLoader._repair_invalid_geometries(gdf, "gcn_risk_zones")

    assert not gdf.geometry.is_valid.any()
