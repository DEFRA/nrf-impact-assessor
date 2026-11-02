"""_is_within_edp against real PostGIS.

The whole red line boundary must lie inside the EDP area: any breach of the
EDP's outer boundary takes the development outside the EDP's legal scope.

Every EDP is inserted by the test itself (`_insert_edp`), so the squares are
arbitrary EPSG:27700 coordinates chosen only to be self-consistent.
"""

import geopandas as gpd
import pytest
from shapely.geometry import Polygon, box
from sqlalchemy import text

from app.boundary.router import _is_within_edp
from app.repositories.repository import Repository

from .conftest import set_active_version

pytestmark = pytest.mark.integration

_EDP = box(600000, 300000, 601000, 301000)


def _insert_edp(repository: Repository, name, wkt, version=1):
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.edp_boundary_layer "
                "(id, version, geometry, name, attributes) VALUES "
                "(gen_random_uuid(), :v, ST_GeomFromText(:wkt, 27700), :n, "
                "jsonb_build_object('EDP_Name', :n))"
            ),
            {"v": version, "wkt": wkt, "n": name},
        )
        session.commit()


def _gdf(geom):
    return gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")


@pytest.mark.parametrize(
    ("boundary", "expected"),
    [
        pytest.param(box(600400, 300400, 600600, 300600), True, id="wholly-inside"),
        pytest.param(box(600900, 300400, 601100, 300600), False, id="partly-outside"),
        pytest.param(box(650000, 350000, 651000, 351000), False, id="wholly-outside"),
        # Scenario 8: shares 600 m of the EDP's northern edge, otherwise inside.
        pytest.param(
            box(600200, 300500, 600800, 301000), True, id="runs-along-the-edge"
        ),
        # Scenario 5: runs along the northern edge, then crosses it further east.
        pytest.param(
            Polygon(
                [
                    (600200, 300500),
                    (600200, 301000),
                    (600600, 301000),
                    (600600, 301200),
                    (600800, 301200),
                    (600800, 300500),
                ]
            ),
            False,
            id="runs-along-the-edge-then-crosses-it",
        ),
        # Only touches the EDP from outside: no part of it is inside.
        pytest.param(
            box(601000, 300400, 601200, 300600), False, id="touches-from-outside"
        ),
    ],
)
def test_boundary_position_against_the_edp(repository, boundary, expected):
    _insert_edp(repository, "Norfolk EDP", _EDP.wkt)

    assert _is_within_edp(_gdf(boundary), repository) is expected


def test_overshoot_inside_the_tolerance_is_within(repository: Repository):
    """Coordinate rounding past the edge is not a breach."""
    _insert_edp(repository, "Norfolk EDP", _EDP.wkt)

    # Pokes 9 cm beyond the northern edge.
    boundary = box(600400, 300500, 600600, 301000.09)

    assert _is_within_edp(_gdf(boundary), repository) is True


def test_overshoot_beyond_the_tolerance_is_not_within(repository: Repository):
    """Any real breach puts the boundary out.

    Paired with the test above it, this pins the tolerance from both sides:
    2 cm more of the same overshoot is the whole difference.
    """
    _insert_edp(repository, "Norfolk EDP", _EDP.wkt)

    # Pokes 11 cm beyond the northern edge.
    boundary = box(600400, 300500, 600600, 301000.11)

    assert _is_within_edp(_gdf(boundary), repository) is False


def test_long_edge_traced_with_a_small_overshoot_is_within(repository: Repository):
    """The tolerance is a distance, so it does not shrink as the edge grows.

    Tracing the whole 1 km northern edge 5 cm too far leaves 50 m² outside,
    thousands of times any area floor, yet it is still an edge that was
    followed rather than crossed.
    """
    _insert_edp(repository, "Norfolk EDP", _EDP.wkt)

    boundary = box(600000, 300500, 601000, 301000.05)

    assert _is_within_edp(_gdf(boundary), repository) is True


def test_narrow_spur_reaching_far_outside_is_not_within(repository: Repository):
    """The tolerance limits how far a boundary may reach out, not how wide.

    A 10 cm wide spur running 100 m past the northern edge is well inside
    the tolerance in width, yet every point of it beyond the first 10 cm is a
    breach.
    """
    _insert_edp(repository, "Norfolk EDP", _EDP.wkt)

    boundary = Polygon(
        [
            (600400, 300500),
            (600600, 300500),
            (600600, 300900),
            (600500.1, 300900),
            (600500.1, 301100),
            (600500, 301100),
            (600500, 300900),
            (600400, 300900),
        ]
    )

    assert _is_within_edp(_gdf(boundary), repository) is False


def test_boundary_spanning_two_adjacent_edps_is_within(repository: Repository):
    """Neither EDP covers it alone, but together they do."""
    _insert_edp(repository, "West EDP", _EDP.wkt)
    _insert_edp(repository, "East EDP", box(601000, 300000, 602000, 301000).wkt)

    boundary = box(600900, 300400, 601100, 300600)

    assert _is_within_edp(_gdf(boundary), repository) is True


def test_non_active_version_rows_are_ignored(repository: Repository):
    """Only the active version's extent counts.

    The stale version covers the boundary and the current one does not, so a
    query spanning both versions would wrongly report it as within.
    """
    _insert_edp(repository, "Norfolk EDP", box(600000, 300000, 602000, 301000).wkt)
    _insert_edp(repository, "Norfolk EDP", _EDP.wkt, version=2)
    set_active_version(repository, "edp_boundary_layer", 2)

    boundary = box(600900, 300400, 601100, 300600)

    assert _is_within_edp(_gdf(boundary), repository) is False


def test_no_edp_rows_is_not_within(repository: Repository):
    assert (
        _is_within_edp(_gdf(box(600400, 300400, 600600, 300600)), repository) is False
    )
