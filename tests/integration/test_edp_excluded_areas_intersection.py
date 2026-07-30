"""_find_intersecting_excluded_areas against real PostGIS.

Squares are in EPSG:27700 and sit in the same geography as the committed
fixture in scripts/make_edp_excluded_areas_fixture.py (600000-601000 etc.).
"""

import geopandas as gpd
import pytest
from shapely.geometry import box
from sqlalchemy import text

from app.boundary.router import (
    _find_intersecting_edps,
    _find_intersecting_excluded_areas,
)
from app.repositories.repository import Repository

pytestmark = pytest.mark.integration


def _insert_zone(repository: Repository, name, wkt, version=1):
    """Insert one exclusion polygon. `name` may be None to test NULL handling."""
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.edp_excluded_areas "
                "(id, version, geometry, name, attributes) VALUES "
                "(gen_random_uuid(), :v, ST_GeomFromText(:wkt, 27700), :n, '{}')"
            ),
            {"v": version, "wkt": wkt, "n": name},
        )
        session.commit()


def _set_active_version(repository: Repository, table: str, version: int):
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.data_active_version "
                "(table_name, active_version, updated_at) "
                "VALUES (:t, :v, now()) "
                "ON CONFLICT (table_name) DO UPDATE SET active_version = :v"
            ),
            {"t": table, "v": version},
        )
        session.commit()


def _gdf(geom):
    return gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")


def test_overlapping_boundary_returns_site_name(repository: Repository):
    _insert_zone(
        repository,
        "Yare Broads and Marshes SSSI",
        box(600000, 300000, 601000, 301000).wkt,
    )

    # Overlaps the zone's upper-right quadrant.
    result = _find_intersecting_excluded_areas(
        _gdf(box(600500, 300500, 601500, 301500)), repository
    )

    assert result == ["Yare Broads and Marshes SSSI"]


def test_boundary_sharing_only_an_edge_is_not_excluded(repository: Repository):
    """Touch-only contact must not exclude: the zones are already buffered."""
    _insert_zone(
        repository,
        "Yare Broads and Marshes SSSI",
        box(600000, 300000, 601000, 301000).wkt,
    )

    # Shares exactly the x=601000 edge, so the intersection has zero area.
    result = _find_intersecting_excluded_areas(
        _gdf(box(601000, 300000, 602000, 301000)), repository
    )

    assert result == []


def test_disjoint_boundary_returns_empty(repository: Repository):
    _insert_zone(
        repository,
        "Yare Broads and Marshes SSSI",
        box(600000, 300000, 601000, 301000).wkt,
    )

    result = _find_intersecting_excluded_areas(
        _gdf(box(650000, 350000, 651000, 351000)), repository
    )

    assert result == []


def test_non_active_version_rows_are_ignored(repository: Repository):
    zone = box(600000, 300000, 601000, 301000).wkt
    _insert_zone(repository, "Stale SSSI", zone, version=1)
    _insert_zone(repository, "Current SSSI", zone, version=2)
    _set_active_version(repository, "edp_excluded_areas", 2)

    result = _find_intersecting_excluded_areas(
        _gdf(box(600500, 300500, 601500, 301500)), repository
    )

    assert result == ["Current SSSI"]


def test_names_are_deduplicated_sorted_and_null_free(repository: Repository):
    """Dedup, sort and NULL handling are contractual.

    The committed fixture cannot demonstrate them: its three site names are
    unique, already in alphabetical order, and non-null.
    """
    zone_a = box(600000, 300000, 601000, 301000).wkt
    zone_b = box(600000, 301000, 601000, 302000).wkt
    zone_c = box(600000, 302000, 601000, 303000).wkt
    # Same site, two polygons -> one name.
    _insert_zone(repository, "Zulu SSSI", zone_a)
    _insert_zone(repository, "Zulu SSSI", zone_b)
    # Inserted after Zulu, so ordering cannot come from insertion order.
    _insert_zone(repository, "Alpha SSSI", zone_c)
    # Unnamed row: dropped rather than emitted as null into a list of strings.
    _insert_zone(repository, None, zone_c)

    result = _find_intersecting_excluded_areas(
        _gdf(box(600500, 300500, 600600, 302500)), repository
    )

    assert result == ["Alpha SSSI", "Zulu SSSI"]


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


def test_edp_query_ignores_non_active_version_rows(repository: Repository):
    """Version filtering needs its own test for the EDP query.

    An exclusion hit skips the EDP query entirely, so no exclusion row is
    inserted here — otherwise this code path would never execute.
    """
    edp = box(600000, 300000, 601000, 301000).wkt
    _insert_edp(repository, "Stale EDP", edp, version=1)
    _insert_edp(repository, "Current EDP", edp, version=2)
    _set_active_version(repository, "edp_boundary_layer", 2)

    results = _find_intersecting_edps(
        _gdf(box(600500, 300500, 601500, 301500)), repository
    )

    assert [r["label"] for r in results] == ["Current EDP"]
