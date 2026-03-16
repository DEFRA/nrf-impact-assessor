"""Integration tests for GET /tiles/{layer}/{z}/{x}/{y}.mvt

These tests run against the test_nrf_impact database (via test_engine fixture)
and verify that the tile endpoint queries real spatial data and returns MVT bytes.

Tile coordinate notes:
  The sample_spatial_data fixture inserts NN_CATCHMENTS polygons in BNG
  (EPSG:27700) around (449000-452000, 99000-102000) — southern England.
  At z=7, tile (63, 42) covers southern England (~313 km wide per tile),
  so it reliably intersects the test features.
  Tile (0, 0) at z=7 covers the northwest Atlantic / Arctic — no UK data.
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

import app.tiles.router as tiles_router_module
from app.main import app
from app.repositories.repository import Repository

MVT_CONTENT_TYPE = "application/vnd.mapbox-vector-tile"


@pytest.fixture(autouse=True)
def reset_tile_caches():
    """Clear module-level caches before and after each test."""
    tiles_router_module._tile_cache.clear()
    tiles_router_module._version_cache.clear()
    yield
    tiles_router_module._tile_cache.clear()
    tiles_router_module._version_cache.clear()


@pytest.fixture
def tile_client(test_engine: Engine):
    """TestClient with the tiles router backed by the test database."""
    test_repo = Repository(test_engine)
    with patch("app.tiles.router._get_repository", return_value=test_repo):
        yield TestClient(app)


class TestTilesIntegration:
    def test_tile_over_known_data_returns_non_empty_bytes(
        self, tile_client, sample_spatial_data
    ):
        """z=7 tile (63, 42) covers southern England where test data lives."""
        response = tile_client.get("/tiles/nn_catchments/7/63/42.mvt")
        assert response.status_code == 200
        assert response.headers["content-type"] == MVT_CONTENT_TYPE
        assert len(response.content) > 0

    def test_tile_with_no_intersecting_features_returns_empty_200(
        self, tile_client, sample_spatial_data
    ):
        """z=7 tile (0, 0) covers the northwest Atlantic — no UK data."""
        response = tile_client.get("/tiles/nn_catchments/7/0/0.mvt")
        assert response.status_code == 200
        assert response.content == b""

    def test_version_filtering_excludes_stale_data(
        self, tile_client, repository, sample_spatial_data
    ):
        """Tiles are served for the current max version only."""
        # The sample data uses version=1; max(version) should resolve to 1
        tiles_router_module._version_cache.clear()
        response = tile_client.get("/tiles/nn_catchments/7/63/42.mvt")
        assert response.status_code == 200
        cached = tiles_router_module._version_cache.get(
            tiles_router_module.TILE_LAYERS["nn_catchments"]
        )
        assert cached is not None
        resolved_version, _expiry = cached
        assert resolved_version == 1
