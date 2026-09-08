"""_find_intersecting_catchments against real PostGIS.

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


def _insert_catchment(
    repository: Repository, name, wkt, version=1, site_name=_SAME, oid=None
):
    """Insert one catchment polygon.

    `site_name` becomes attributes->>'Label' — the field the query actually
    labels from — and defaults to `name` so callers that do not care about the
    distinction can pass one value. It may be None to test NULL handling.
    `name` is the separate top-level column, only ever set here so a test can
    prove the label does not come from it."""
    if site_name is _SAME:
        site_name = name
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.nn_catchments "
                "(id, version, geometry, name, attributes) VALUES "
                "(gen_random_uuid(), :v, ST_GeomFromText(:wkt, 27700), :n, "
                "jsonb_build_object('Label', cast(:site_name as text), "
                "'N2K_Site_N', 'The Broads SAC', "
                "'OID', cast(:oid as text)))"
            ),
            {
                "v": version,
                "wkt": wkt,
                "n": name,
                "site_name": site_name,
                "oid": oid,
            },
        )
        session.commit()


def _gdf(geom):
    return gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")


def test_label_comes_from_the_label_attribute(repository: Repository):
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

    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 100.0,
            "catchmentId": None,
        }
    ]


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
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 70.0,
            "catchmentId": None,
        },
        {
            "label": "River Wensum SAC",
            "catchmentOverlapPercentage": 30.0,
            "catchmentId": None,
        },
    ]


def test_a_multi_polygon_catchment_is_reported_once(repository: Repository):
    """One catchment is several rows. Summing per name before dividing is what
    stops it being reported once per polygon."""
    _insert_catchment(repository, "Broads SAC", box(600000, 300000, 600400, 301000).wkt)
    _insert_catchment(repository, "Broads SAC", box(600500, 300000, 600800, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 70.0,
            "catchmentId": None,
        }
    ]


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
    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 100.0,
            "catchmentId": None,
        }
    ]


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

    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 100.0,
            "catchmentId": None,
        }
    ]


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

    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 70.0,
            "catchmentId": None,
        }
    ]


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

    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 25.0,
            "catchmentId": None,
        }
    ]


def test_an_unnamed_catchment_is_dropped(repository: Repository):
    _insert_catchment(repository, None, box(600000, 300000, 600700, 301000).wkt)
    _insert_catchment(repository, "Broads SAC", box(600700, 300000, 601000, 301000).wkt)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 30.0,
            "catchmentId": None,
        }
    ]


def test_the_catchment_id_is_returned(repository: Repository):
    _insert_catchment(repository, "Broads SAC", _BOUNDARY.wkt, oid="1042")
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [
        {
            "label": "Broads SAC",
            "catchmentOverlapPercentage": 100.0,
            "catchmentId": "1042",
        }
    ]


def test_a_boundary_straddling_two_polygons_reports_the_lowest_id(
    repository: Repository,
):
    """A single id must discard the others deterministically, and the
    percentage must still cover both polygons."""
    _insert_catchment(
        repository, "Broads SAC", box(600000, 300000, 600500, 301000).wkt, oid="1042"
    )
    _insert_catchment(
        repository, "Broads SAC", box(600500, 300000, 601000, 301000).wkt, oid="1043"
    )
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert len(result) == 1
    assert result[0]["catchmentId"] == "1042"
    assert result[0]["catchmentOverlapPercentage"] == 100.0


def test_a_catchment_without_an_oid_reports_no_id(repository: Repository):
    _insert_catchment(repository, "Broads SAC", _BOUNDARY.wkt, oid=None)
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result[0]["catchmentId"] is None


def test_catchments_of_one_natura_site_are_reported_separately(
    repository: Repository,
):
    """The four Broads catchments share N2K_Site_N "The Broads SAC". Grouping
    by the site collapsed them into one entry at the combined percentage, with
    an id belonging to whichever polygon sorted lowest — so a boundary could
    not be told which catchments it actually fell in."""
    _insert_catchment(
        repository,
        "Yare Broads and Marshes",
        box(600000, 300000, 600400, 301000).wkt,
        oid="32",
    )
    _insert_catchment(
        repository,
        "Bure Broads and Marshes",
        box(600400, 300000, 600700, 301000).wkt,
        oid="29",
    )
    _insert_catchment(
        repository,
        "Ant Broads and Marshes",
        box(600700, 300000, 600800, 301000).wkt,
        oid="33",
    )
    set_active_version(repository, "nn_catchments", 1)

    result = _find_intersecting_catchments(_gdf(_BOUNDARY), repository)

    assert result == [
        {
            "label": "Ant Broads and Marshes",
            "catchmentOverlapPercentage": 10.0,
            "catchmentId": "33",
        },
        {
            "label": "Bure Broads and Marshes",
            "catchmentOverlapPercentage": 30.0,
            "catchmentId": "29",
        },
        {
            "label": "Yare Broads and Marshes",
            "catchmentOverlapPercentage": 40.0,
            "catchmentId": "32",
        },
    ]
