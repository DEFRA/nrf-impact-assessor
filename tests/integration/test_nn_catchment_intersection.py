"""_find_intersecting_catchments against real PostGIS.

Every catchment is inserted by the test itself (`_insert_catchment`), so nothing
here reads the nn_catchments fixture — the squares are arbitrary EPSG:27700
coordinates chosen only to be self-consistent. The boundary used throughout is
1000m x 1000m == 1,000,000 m^2, so an inserted area in m^2 maps to a percentage
by dividing by 10,000.
"""

import geopandas as gpd
import pytest
from shapely.geometry import box
from sqlalchemy import text

from app.boundary.router import _find_intersecting_catchments
from app.repositories.repository import Repository

from .conftest import set_active_version

pytestmark = pytest.mark.integration

_BOUNDARY = box(600000, 300000, 601000, 301000)

# Sentinel: "site_name defaults to name". None is a real, tested value here.
_SAME = object()


def _insert_catchment(repository: Repository, name, wkt, version=1, site_name=_SAME):
    """Insert one catchment polygon.

    `site_name` becomes attributes->>'N2K_Site_N' — the field the query
    actually labels from — and defaults to `name` so callers that do not care
    about the distinction can pass one value. It may be None to test NULL
    handling. `name` is the separate top-level column, only ever set here so a
    test can prove the label does not come from it."""
    if site_name is _SAME:
        site_name = name
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.nn_catchments "
                "(id, version, geometry, name, attributes) VALUES "
                "(gen_random_uuid(), :v, ST_GeomFromText(:wkt, 27700), :n, "
                "jsonb_build_object('N2K_Site_N', cast(:site_name as text)))"
            ),
            {"v": version, "wkt": wkt, "n": name, "site_name": site_name},
        )
        session.commit()


def _gdf(geom):
    return gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")


def test_label_comes_from_the_n2k_site_name_attribute(repository: Repository):
    """The `name` column differs from the attribute here so a query that
    labelled from the column instead would fail rather than coincide."""
    _insert_catchment(
        repository,
        "not-the-label",
        box(600000, 300000, 601000, 301000).wkt,
        site_name="Broads SAC",
    )
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [{"label": "Broads SAC", "catchmentOverlapPercentage": 100.0}]


def test_boundary_split_across_two_catchments_reports_both_shares(
    repository: Repository,
):
    _insert_catchment(repository, "Broads SAC", box(600000, 300000, 600700, 301000).wkt)
    _insert_catchment(
        repository, "River Wensum SAC", box(600700, 300000, 601000, 301000).wkt
    )
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [
        {"label": "Broads SAC", "catchmentOverlapPercentage": 70.0},
        {"label": "River Wensum SAC", "catchmentOverlapPercentage": 30.0},
    ]


def test_a_multi_polygon_catchment_is_reported_once(repository: Repository):
    """One catchment is several rows. Summing per name before dividing is what
    stops it being reported once per polygon."""
    _insert_catchment(repository, "Broads SAC", box(600000, 300000, 600400, 301000).wkt)
    _insert_catchment(repository, "Broads SAC", box(600500, 300000, 600800, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [{"label": "Broads SAC", "catchmentOverlapPercentage": 70.0}]


def test_overlapping_polygons_of_one_catchment_are_not_double_counted(
    repository: Repository,
):
    """Same-name polygons are not guaranteed disjoint — the loaded data has
    Broads features overlapping by ~257 m2. Summing their intersections would
    count the shared strip twice and can report over 100% for one catchment.
    """
    _insert_catchment(repository, "Broads SAC", box(600000, 300000, 600600, 301000).wkt)
    _insert_catchment(repository, "Broads SAC", box(600400, 300000, 601000, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    # The two polygons together cover the boundary exactly once. Summing areas
    # would give 120% (the 600400-600600 strip counted twice).
    assert result == [{"label": "Broads SAC", "catchmentOverlapPercentage": 100.0}]


def test_a_boundary_inside_an_overlap_does_not_exceed_one_hundred(
    repository: Repository,
):
    """The worst case: a boundary lying wholly inside the shared strip would
    otherwise report 200%."""
    _insert_catchment(repository, "Broads SAC", box(600000, 300000, 600600, 301000).wkt)
    _insert_catchment(repository, "Broads SAC", box(600400, 300000, 601000, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    inside_the_overlap = box(600450, 300400, 600550, 300600)

    result = _find_intersecting_catchments(_gdf(inside_the_overlap), repository)

    assert result == [{"label": "Broads SAC", "catchmentOverlapPercentage": 100.0}]


def test_only_the_active_version_is_counted(repository: Repository):
    """A staged v2 alongside v1 would otherwise double every share."""
    _insert_catchment(
        repository, "Broads SAC", box(600000, 300000, 600700, 301000).wkt, version=1
    )
    _insert_catchment(
        repository, "Broads SAC", box(600000, 300000, 600700, 301000).wkt, version=2
    )
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [{"label": "Broads SAC", "catchmentOverlapPercentage": 70.0}]


def test_a_catchment_sharing_only_an_edge_is_not_reported(repository: Repository):
    """Touching contributes no area, so reporting it at 0.00% would be noise."""
    _insert_catchment(repository, "Broads SAC", box(601000, 300000, 602000, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == []


def test_a_disjoint_catchment_is_not_reported(repository: Repository):
    _insert_catchment(repository, "Broads SAC", box(700000, 400000, 701000, 401000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == []


def test_shares_need_not_sum_to_one_hundred(repository: Repository):
    """Part of the boundary can sit outside every catchment."""
    _insert_catchment(repository, "Broads SAC", box(600000, 300000, 600250, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [{"label": "Broads SAC", "catchmentOverlapPercentage": 25.0}]


def test_an_unnamed_catchment_is_dropped(repository: Repository):
    _insert_catchment(repository, None, box(600000, 300000, 600700, 301000).wkt)
    _insert_catchment(repository, "Broads SAC", box(600700, 300000, 601000, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [{"label": "Broads SAC", "catchmentOverlapPercentage": 30.0}]
