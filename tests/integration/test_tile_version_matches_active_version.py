"""The tile endpoint and the reads it is drawn against must agree on version.

The map is the only thing a user can see, so a tile served from a version the
eligibility check is not using makes the two disagree silently: the boundary
looks like it overlaps an exclusion zone on screen while /check-boundary,
reading a different version, reports it eligible.
"""

from unittest.mock import patch

import pytest
from sqlalchemy import text

import app.tiles.router as tiles_router_module
from app.data_sync.active_version import get_active_version, set_active_version
from app.repositories.repository import Repository
from app.tiles.router import TILE_LAYERS, _resolve_layer_version

pytestmark = pytest.mark.integration

_EXCLUDED_AREAS = "edp_excluded_areas"
_SQUARE_WKT = (
    "POLYGON((600000 300000, 601000 300000, "
    "601000 301000, 600000 301000, 600000 300000))"
)


@pytest.fixture(autouse=True)
def reset_version_cache():
    """The resolved version is memoised per slug for a TTL; isolate each test."""
    tiles_router_module._version_cache.clear()
    yield
    tiles_router_module._version_cache.clear()


def _insert_polygon(
    repository: Repository, table: str, version: int, offset: int
) -> None:
    """Insert one polygon into `table` at `version`, shifted by `offset` metres.

    Both served layers share the same spatial-layer columns, so one insert
    covers either table. The offset makes the versions geometrically
    distinguishable, which is the whole point of pinning the right one.
    """
    with repository.session() as session:
        session.execute(
            text(
                f"INSERT INTO public.{table} "  # noqa: S608
                "(id, version, geometry, name, attributes) VALUES "
                "(gen_random_uuid(), :v, "
                "ST_Translate(ST_GeomFromText(:wkt, 27700), :o, :o), "
                "'Yare Broads and Marshes SSSI', '{}')"
            ),
            {"v": version, "wkt": _SQUARE_WKT, "o": offset},
        )
        session.commit()


def _pin_active_version(repository: Repository, table: str, version: int) -> None:
    with repository.session() as session:
        set_active_version(session, table, version)
        session.commit()


def test_tile_version_follows_the_active_version_pointer(repository: Repository):
    """A staged-but-unpromoted version must not reach the map.

    A sync loads version N+1 before the QC gate decides whether to promote it.
    Until the pointer moves, reads stay on N — and so must the tiles.
    """
    _insert_polygon(repository, _EXCLUDED_AREAS, version=1, offset=0)
    _insert_polygon(repository, _EXCLUDED_AREAS, version=2, offset=5)
    _pin_active_version(repository, _EXCLUDED_AREAS, 1)

    with patch("app.tiles.router._get_repository", return_value=repository):
        assert _resolve_layer_version(_EXCLUDED_AREAS) == 1


def test_tile_version_follows_a_rollback(repository: Repository):
    """After a rollback the map must fall back with the reads it illustrates."""
    _insert_polygon(repository, _EXCLUDED_AREAS, version=1, offset=0)
    _insert_polygon(repository, _EXCLUDED_AREAS, version=2, offset=5)
    _pin_active_version(repository, _EXCLUDED_AREAS, 2)

    with patch("app.tiles.router._get_repository", return_value=repository):
        assert _resolve_layer_version(_EXCLUDED_AREAS) == 2

    _pin_active_version(repository, _EXCLUDED_AREAS, 1)
    tiles_router_module._version_cache.clear()

    with patch("app.tiles.router._get_repository", return_value=repository):
        assert _resolve_layer_version(_EXCLUDED_AREAS) == 1


@pytest.mark.parametrize("slug", sorted(TILE_LAYERS))
def test_tile_version_matches_the_version_reads_use(repository: Repository, slug: str):
    """Every served layer resolves to exactly what a read of it would resolve to."""
    table = TILE_LAYERS[slug].removeprefix("public.")
    _insert_polygon(repository, table, version=1, offset=0)
    _insert_polygon(repository, table, version=2, offset=5)
    _pin_active_version(repository, table, 1)

    with patch("app.tiles.router._get_repository", return_value=repository):
        tile_version = _resolve_layer_version(slug)

    with repository.session() as session:
        assert tile_version == get_active_version(session, table)


def test_tile_version_falls_back_to_max_when_no_pointer_exists(
    repository: Repository,
):
    """Before any sync or rollback writes a pointer, behaviour is unchanged."""
    _insert_polygon(repository, _EXCLUDED_AREAS, version=1, offset=0)
    _insert_polygon(repository, _EXCLUDED_AREAS, version=2, offset=5)

    with patch("app.tiles.router._get_repository", return_value=repository):
        assert _resolve_layer_version(_EXCLUDED_AREAS) == 2
